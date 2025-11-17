from __future__ import annotations

import os
from typing import Any, Iterable, Iterator, Optional

import pyarrow as pa
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
            self._target_path
            / f"year={y:04d}/{self._symbol}-{self._timeframe_data}-{y}-{m:02d}.parquet"
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
        prev_close: Optional[int] = None

        # collect relevant columns (must correspond with Candle datatype)
        cols = [
            self._data_type.start_col,
            self._data_type.end_col,
            self._data_type.o_col,
            self._data_type.h_col,
            self._data_type.c_col,
            self._data_type.l_col,
            self._data_type.v_col,
            self._data_type.n_trades_col,
            self._data_type.is_final_col,
        ]

        # Open the files iteratively
        for path in self._paths:
            if not os.path.exists(path):
                raise SourceError(f"Path ({path}) does not exist")
            with pq.ParquetFile(path) as pf:
                schema_cols = set(pf.schema.names)
                missing = [col for col in cols if col not in schema_cols]
                if missing:
                    raise SourceError(f"{self._ctx.run_id}: {path} missing columns {missing}")
                self._emit_audit("SOURCE_FILE_OPEN")
                for batch in pf.iter_batches(
                    batch_size=self._batch_size, columns=cols, use_threads=True
                ):
                    tbl = pa.Table.from_batches([batch])
                    self._meta.rows_read += tbl.num_rows

                    # Columns arrays
                    start_arr = tbl[d_type.start_col]  # start_ms
                    # end_arr not relevant, as end_ms = start_ms + tf_ms
                    # end_arr = tbl[d_type.end_col] if d_type.end_col in tbl.schema.names else None
                    o_arr = tbl[d_type.o_col]
                    h_arr = tbl[d_type.h_col]
                    l_arr = tbl[d_type.l_col]
                    c_arr = tbl[d_type.c_col]
                    v_arr = tbl[d_type.v_col]
                    n_arr = (
                        tbl[d_type.n_trades_col]
                        if d_type.n_trades_col in tbl.schema.names
                        else None
                    )
                    f_arr = (
                        tbl[d_type.is_final_col]
                        if d_type.is_final_col in tbl.schema.names
                        else None
                    )

                    for i in range(tbl.num_rows):
                        # validate field and return
                        trades_val = (
                            int(n_arr[i].as_py())
                            if n_arr is not None and n_arr[i].as_py() is not None
                            else 0
                        )
                        is_final_val = (
                            bool(f_arr[i].as_py())
                            if f_arr is not None and f_arr[i].as_py() is not None
                            else None
                        )
                        start_ms = int(start_arr[i].as_py())
                        end_ms = start_ms + tf_ms  # normalize close to exact bucket end
                        candle = Candle(
                            symbol=self._symbol,
                            timeframe=self._timeframe_data,
                            start_ms=start_ms,
                            end_ms=end_ms,
                            open=float(o_arr[i].as_py()),
                            high=float(h_arr[i].as_py()),
                            low=float(l_arr[i].as_py()),
                            close=float(c_arr[i].as_py()),
                            volume=float(v_arr[i].as_py()),
                            trades=trades_val,
                            is_final=is_final_val,
                        )

                        # Light checks: Final bar and monotonicity guard rails
                        close_ms = start_ms + tf_ms
                        if is_final_val is False:
                            self._emit_audit(
                                "SOURCE_ROW_FAILED_NON_FINAL", level="DEBUG", payload=candle
                            )
                            self._meta.rows_failed += 1
                            continue
                            raise SourceError(f"Not final_value at {path} for {close_ms}")

                        # Monotonicity Guard rails
                        if prev_close is not None and close_ms < prev_close:
                            self._meta.rows_dropped += 1
                            self._emit_audit(
                                "SOURCE_ROW_DROPPED_MONOTONICITY",
                                level="WARN",
                                payload={"candle": candle, "prev_close": prev_close},
                            )
                            continue
                            raise RuntimeError(
                                f"Non-monotonic time at {path}: {close_ms} < {prev_close}"
                            )
                            # Could be a double candle as well.

                        # Metadata managing
                        self._meta.rows_emitted += 1
                        if self._meta.first_ts is None:
                            self._meta.first_ts = close_ms
                        self._meta.last_ts = close_ms

                        prev_close = close_ms

                        yield candle

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
