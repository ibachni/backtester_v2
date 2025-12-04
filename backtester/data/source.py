from __future__ import annotations

import asyncio
import os
from typing import Any, AsyncIterable, AsyncIterator, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from backtester.config.configs import DataSubscriptionConfig, RunContext
from backtester.core.bus import Bus
from backtester.core.clock import Clock
from backtester.errors.errors import SourceError
from backtester.types.data import SourceMeta
from backtester.types.topics import T_LOG
from backtester.types.types import Candle, LogEvent
from backtester.utils.utility import month_range

"""
Additions (MVP):
- Configurable behavior for non-final or non-monotonic bars, controlled via DataSubConfig
- Optional deterministic checksum (e.g., SHA-256 over concat batches), stored in SourceMeta
to prove data integrity per run

Post-MVP:
- Gap-detection metrics ((count, total missing minutes) via telemetry)
"""

# --- Abstract Source ---


class CandleSource(AsyncIterable[Candle]):
    """
    Abstract base class for candle sources.
    Questions: Also for other sources?
    """

    def __aiter__(self) -> AsyncIterator[Candle]:
        raise NotImplementedError

    def meta(self) -> SourceMeta:
        raise NotImplementedError

    def stats(self) -> dict[str, Any]:
        """
        Retrieve stats of the iterator.
        """
        m = self.meta()
        return {
            "rows_read": m.rows_read,
            "rows_emitted": m.rows_emitted,
            "rows_dropped": m.rows_dropped,
            "rows_failed": m.rows_failed,
            "first_ts": m.first_ts,
            "last_ts": m.last_ts,
            # "sha256": m.sha256,
        }


# --- Concrete Source Implementation ---


class ParquetCandleSource(CandleSource):
    """
    Reads monthly-partitioned Parquet files at timeframe_data resolution.
    Yields raw candles in deterministic order; resampling to target TF
    is handled by BarFeed._ResampleIter wrapper if needed.
    """

    def __init__(
        self,
        run_ctx: RunContext,
        bus: Bus,
        clock: Clock,
        sub_config: DataSubscriptionConfig,
    ) -> None:
        self._ctx = run_ctx
        self._bus = bus
        self._clock = clock
        self._symbol = sub_config.symbol
        self._start_dt = sub_config.start_dt
        self._start_ms = sub_config.start_ms
        self._end_dt = sub_config.end_dt
        self._end_ms = sub_config.end_ms
        self._timeframe = sub_config.timeframe
        self._timeframe_data = sub_config.timeframe_data
        self._tf_ms = sub_config.tf_ms
        self._tf_ms_data = sub_config.tf_ms_data
        self._batch_size = sub_config.batch_size
        self._data_type = sub_config.data_type

        # path
        self._target_path = sub_config.target_path
        self._paths = [
            # TODO (historical data still only has "m", not "m:02d" which it should!)
            self._target_path
            / f"year={y:04d}/{self._symbol}-{self._timeframe_data}-{y}-{m}.parquet"
            for (y, m) in month_range(self._start_dt, self._end_dt)
        ]

        self._meta = SourceMeta(
            symbol=self._symbol,
            timeframe=self._timeframe,
            format="parquet",
            paths=self._paths,
            rows_read=0,
            rows_emitted=0,
            rows_dropped=0,
            rows_failed=0,
            first_ts=None,
            last_ts=None,
        )

        # Note: Cannot await in __init__, so we fire-and-forget or skip init log
        # The first file open will log anyway
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                log_event = LogEvent(
                    level="INFO",
                    component="Source",
                    msg="Source initialized",
                    payload={
                        "event": "SOURCE_INIT",
                        "symbol": self._symbol,
                        "timeframe": self._timeframe,
                        "num_paths": len(self._paths),
                    },
                    sim_time=self._clock.now(),
                )
                asyncio.create_task(
                    self._bus.publish(topic=T_LOG, ts_utc=self._clock.now(), payload=log_event)
                )
        except (RuntimeError, AttributeError):
            pass

    def meta(self) -> SourceMeta:
        """Helper to return metadata collector"""
        return self._meta

    def get_stats(self) -> dict[str, Any]:
        return {
            "rows_read": self._meta.rows_read,
            "rows_emitted": self._meta.rows_emitted,
            "rows_dropped": self._meta.rows_dropped,
            "rows_failed": self._meta.rows_failed,
        }

    async def __aiter__(self) -> AsyncIterator[Candle]:
        """
        Load the next candle
        """
        d_type = self._data_type
        tf_ms = self._tf_ms_data  # normalize using input TF (e.g., 1m)
        prev_start: Optional[int] = None

        # collect relevant columns (must correspond with Candle datatype)
        cols = [
            d_type.start_col,
            d_type.end_col,
            d_type.o_col,
            d_type.h_col,
            d_type.l_col,
            d_type.c_col,
            d_type.v_col,
            d_type.n_trades_col,
            d_type.is_final_col,
        ]

        # Open the files iteratively
        for path in self._paths:
            if not os.path.exists(path):
                raise SourceError(f"Path ({path}) does not exist")

            # Yield control to event loop before heavy I/O
            await asyncio.sleep(0)

            with pq.ParquetFile(path) as pf:
                self._validate_schema(pf, cols, path)
                await self._emit_log(
                    level="INFO",
                    msg="Opening parquet file",
                    payload={"event": "SOURCE_FILE_OPEN", "path": str(path)},
                )

                for batch in pf.iter_batches(
                    batch_size=self._batch_size, columns=cols, use_threads=True
                ):
                    # Yield control to event loop periodically
                    await asyncio.sleep(0)

                    tbl = pa.Table.from_batches([batch])
                    self._meta.rows_read += tbl.num_rows

                    # 1. Filter by time range (Vectorized)
                    # Keep rows where start_ms >= self._start_ms AND start_ms < self._end_ms
                    start_col_data = tbl[d_type.start_col]
                    mask = pc.and_(
                        pc.greater_equal(start_col_data, self._start_ms),
                        pc.less(start_col_data, self._end_ms),
                    )

                    # Apply filter
                    tbl = tbl.filter(mask)
                    if tbl.num_rows == 0:
                        continue

                    # 2. Extract columns as python lists (faster .as_py() per cell)
                    # Use of to_pylist() which is optimized in PyArrow
                    row_iterator = zip(
                        tbl[d_type.start_col].to_pylist(),
                        tbl[d_type.o_col].to_pylist(),
                        tbl[d_type.h_col].to_pylist(),
                        tbl[d_type.l_col].to_pylist(),
                        tbl[d_type.c_col].to_pylist(),
                        tbl[d_type.v_col].to_pylist(),
                        # Handle optional columns inline
                        tbl[d_type.n_trades_col].to_pylist()
                        if d_type.n_trades_col in tbl.schema.names
                        else [0] * tbl.num_rows,
                        tbl[d_type.is_final_col].to_pylist()
                        if d_type.is_final_col in tbl.schema.names
                        else [True] * tbl.num_rows,
                    )

                    # 3. Iterate and yield
                    for start_val, o, h, l_, c, v, n, f in row_iterator:
                        start_ms = int(start_val)
                        end_ms = start_ms + tf_ms

                        # Monotonicity & Gap Check
                        if prev_start is not None:
                            if start_ms < prev_start:
                                self._meta.rows_dropped += 1
                                await self._emit_log(
                                    level="WARN",
                                    msg="Row dropped due to monotonicity violation",
                                    payload={
                                        "event": "SOURCE_ROW_DROPPED_MONOTONICITY",
                                        "start_ms": start_ms,
                                        "prev_start": prev_start,
                                    },
                                )
                                continue
                            elif start_ms == prev_start:
                                self._meta.rows_dropped += 1
                                await self._emit_log(
                                    level="WARN",
                                    msg="Row dropped due to duplicate timestamp",
                                    payload={
                                        "event": "SOURCE_ROW_DROPPED_DUPLICATE",
                                        "start_ms": start_ms,
                                    },
                                )
                                continue
                            elif start_ms > prev_start + tf_ms:
                                # Gap detected
                                gap_size = start_ms - (prev_start + tf_ms)
                                await self._emit_log(
                                    level="WARN",
                                    msg="Gap detected in data",
                                    payload={
                                        "event": "SOURCE_GAP_DETECTED",
                                        "gap_ms": gap_size,
                                        "prev_end": prev_start + tf_ms,
                                        "curr_start": start_ms,
                                    },
                                )

                        is_final = bool(f) if f is not None else True
                        # is_final = bool(f_arr[i]) if f_arr[i] is not None else True
                        if not is_final:
                            # Skip non-final candles silently (tracked in meta.rows_failed)
                            self._meta.rows_failed += 1
                            continue

                        candle = Candle(
                            symbol=self._symbol,
                            timeframe=self._timeframe_data,
                            start_ms=start_ms,
                            end_ms=end_ms,
                            open=float(o),
                            high=float(h),
                            low=float(l_),
                            close=float(c),
                            volume=float(v),
                            trades=int(n) if n is not None else 0,
                            is_final=is_final,
                        )

                        # Metadata managing
                        self._meta.rows_emitted += 1
                        if self._meta.first_ts is None:
                            self._meta.first_ts = start_ms
                        self._meta.last_ts = start_ms

                        prev_start = start_ms
                        yield candle

    def _validate_schema(self, pf: pq.ParquetFile, required_cols: list[str], path: Any) -> None:
        schema_cols = set(pf.schema.names)
        # Filter out columns that are optional or we handle gracefully if missing
        # (like n_trades, is_final)

        critical_cols = [
            self._data_type.start_col,
            self._data_type.o_col,
            self._data_type.h_col,
            self._data_type.l_col,
            self._data_type.c_col,
            self._data_type.v_col,
        ]
        missing_critical = [col for col in critical_cols if col not in schema_cols]

        if missing_critical:
            raise SourceError(
                f"{self._ctx.run_id}: {path} missing critical columns {missing_critical}"
            )

    async def _emit_log(
        self,
        level: str,
        msg: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> None:
        """Emit a log event to the bus using the LogEvent pattern."""
        if not hasattr(self, "_bus") or self._bus is None:
            return

        log_event = LogEvent(
            level=level,
            component="Source",
            msg=msg,
            payload=payload or {},
            sim_time=self._clock.now(),
        )
        await self._bus.publish(topic=T_LOG, ts_utc=self._clock.now(), payload=log_event)
