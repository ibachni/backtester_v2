from __future__ import annotations

import os
from typing import Any, Iterable, Iterator, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from backtester.config.configs import DataSubscriptionConfig, RunContext
from backtester.core.audit import AuditWriter
from backtester.core.utility import month_range
from backtester.errors.errors import SourceError
from backtester.types.data import SourceMeta
from backtester.types.types import Candle

"""
# TODO: Candles violating monotonicity are skipped; what behavior instead?
Additions (MVP):
- Configurable behavior for non-final or non-monotonic bars, controlled via DataSubConfig
- Optional deterministic checksum (e.g., SHA-256 over concat batches), stored in SourceMeta
to prove data integrity per run

Post-MVP:
- Gap-detection metrics ((count, total missing minutes) via telemetry)
"""

# --- Abstract Source ---


class CandleSource(Iterable[Candle]):
    """
    Abstract base class for candle sources.
    Questions: Also for other sources?
    """

    def __iter__(self) -> Iterator[Candle]:
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
        audit: AuditWriter,
        sub_config: DataSubscriptionConfig,
    ) -> None:
        self._ctx = run_ctx
        self._audit = audit
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
            # TODO (historical data still only has "m", not "m:02d" which it shou)
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

        self._emit_audit("SOURCE_INIT")

    def meta(self) -> SourceMeta:
        """Helper to return metadata collector"""
        return self._meta

    def __iter__(self) -> Iterator[Candle]:
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

            with pq.ParquetFile(path) as pf:
                self._validate_schema(pf, cols, path)
                self._emit_audit("SOURCE_FILE_OPEN", payload={"path": str(path)})

                for batch in pf.iter_batches(
                    batch_size=self._batch_size, columns=cols, use_threads=True
                ):
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
                    start_arr = tbl[d_type.start_col].to_pylist()
                    o_arr = tbl[d_type.o_col].to_pylist()
                    h_arr = tbl[d_type.h_col].to_pylist()
                    l_arr = tbl[d_type.l_col].to_pylist()
                    c_arr = tbl[d_type.c_col].to_pylist()
                    v_arr = tbl[d_type.v_col].to_pylist()

                    # Handle optional columns
                    n_arr = (
                        tbl[d_type.n_trades_col].to_pylist()
                        if d_type.n_trades_col in tbl.schema.names
                        else [0] * tbl.num_rows
                    )
                    f_arr = (
                        tbl[d_type.is_final_col].to_pylist()
                        if d_type.is_final_col in tbl.schema.names
                        else [True] * tbl.num_rows
                    )

                    # 3. Iterate and yield
                    for i in range(tbl.num_rows):
                        start_ms = int(start_arr[i])
                        end_ms = start_ms + tf_ms

                        # Monotonicity & Gap Check
                        if prev_start is not None:
                            if start_ms < prev_start:
                                self._meta.rows_dropped += 1
                                self._emit_audit(
                                    "SOURCE_ROW_DROPPED_MONOTONICITY",
                                    level="WARN",
                                    payload={"start_ms": start_ms, "prev_start": prev_start},
                                )
                                continue
                            elif start_ms == prev_start:
                                self._meta.rows_dropped += 1
                                self._emit_audit(
                                    "SOURCE_ROW_DROPPED_DUPLICATE",
                                    level="WARN",
                                    payload={"start_ms": start_ms},
                                )
                                continue
                            elif start_ms > prev_start + tf_ms:
                                # Gap detected
                                gap_size = start_ms - (prev_start + tf_ms)
                                self._emit_audit(
                                    "SOURCE_GAP_DETECTED",
                                    level="WARN",
                                    payload={
                                        "gap_ms": gap_size,
                                        "prev_end": prev_start + tf_ms,
                                        "curr_start": start_ms,
                                    },
                                )

                        is_final = bool(f_arr[i]) if f_arr[i] is not None else True
                        if not is_final:
                            self._emit_audit(
                                "SOURCE_ROW_FAILED_NON_FINAL",
                                level="DEBUG",
                                payload={"ts": start_ms},
                            )
                            self._meta.rows_failed += 1
                            continue

                        candle = Candle(
                            symbol=self._symbol,
                            timeframe=self._timeframe_data,
                            start_ms=start_ms,
                            end_ms=end_ms,
                            open=float(o_arr[i]),
                            high=float(h_arr[i]),
                            low=float(l_arr[i]),
                            close=float(c_arr[i]),
                            volume=float(v_arr[i]),
                            trades=int(n_arr[i]) if n_arr[i] is not None else 0,
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

    def _emit_audit(
        self,
        event: str,
        *,
        component: str = "Source",
        level: str = "INFO",
        simple: bool = True,
        payload: Optional[Any] = None,
        symbol: Optional[str] = None,
    ) -> None:
        if not hasattr(self, "_audit") or self._audit is None:
            return
        if symbol is None:
            symbol = self._symbol
        self._audit.emit(
            component=component,
            event=event,
            level=level,
            simple=simple,
            symbol=symbol,
            payload=payload or {},
        )
