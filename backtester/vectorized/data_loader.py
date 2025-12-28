from __future__ import annotations

import datetime as dt
import hashlib
import logging
from dataclasses import dataclass, field
from typing import Sequence

import polars as pl

from backtester.core.clock import parse_timeframe
from backtester.types.data import BinanceCandleType, CandleType
from backtester.utils.utility import month_range, target_path
from backtester.vectorized.contracts import DataSpec

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class LoadStats:
    """Debug/observability stats for data loading."""

    paths_scanned: int = 0
    paths_found: int = 0
    symbols: tuple[str, ...] = ()
    timeframe: str = ""
    date_range_ms: tuple[int, int] = (0, 0)
    schema_hash: str = ""
    columns_selected: tuple[str, ...] = ()
    float32_cols: tuple[str, ...] = ()
    filters_applied: list[str] = field(default_factory=list)


class ParquetLoader:
    """
    Research-first loader: lazy scan with early filters and minimal normalization.

    Design principles (per vectorized.md spec):
    - Always use LazyFrame until the last moment (lets Polars optimize)
    - Apply filter pushdowns immediately (date/symbol before collecting)
    - Cast floats to Float32 for speed and Numba compatibility
    - Ensure timestamps are Int64 milliseconds
    - Sort by timestamp immediately
    - Support resampling from timeframe_data to target timeframe
    """

    def load_dataset(
        self,
        spec: DataSpec,
        *,
        debug: bool = False,
    ) -> pl.LazyFrame | tuple[pl.LazyFrame, LoadStats]:
        """
        Load partitioned parquet files as a LazyFrame with early filtering.

        If spec.timeframe != spec.timeframe_data, automatically resamples
        to the target timeframe after loading.

        Args:
            spec: Data specification defining what to load.
            debug: If True, returns (LazyFrame, LoadStats) tuple for inspection.

        Returns:
            LazyFrame with filtered, normalized data. If debug=True, also returns
            LoadStats with metadata about the load operation.
        """
        stats = (
            LoadStats(
                symbols=spec.symbols,
                timeframe=spec.timeframe,
                date_range_ms=spec.date_range.to_epoch_ms(),
            )
            if debug
            else None
        )

        paths = self._paths_for_spec(spec)

        if stats:
            stats.paths_scanned = len(spec.symbols) * self._month_count(spec)
            stats.paths_found = len(paths)

        if not paths:
            logger.warning(
                "No parquet files found for spec: symbols=%s, timeframe_data=%s, range=%s",
                spec.symbols,
                spec.timeframe_data,
                spec.date_range,
            )
            empty_lf = pl.LazyFrame()
            return (empty_lf, stats) if debug and stats else empty_lf

        lf = pl.scan_parquet(paths, hive_partitioning=False)
        schema = lf.collect_schema()

        if stats:
            stats.schema_hash = self._schema_hash(schema)

        lf = self._apply_filters(lf, spec, schema, stats)
        lf = self._select_columns(lf, spec, schema, stats)

        schema = lf.collect_schema()
        lf = self._normalize_schema(lf, spec, schema, stats)
        lf = self._sort(lf, spec, schema)

        # Resample if target timeframe differs from data timeframe
        if spec.timeframe != spec.timeframe_data:
            tf_ms = parse_timeframe(spec.timeframe)
            tf_ms_data = parse_timeframe(spec.timeframe_data)  # type: ignore[arg-type]
            if (tf_ms / tf_ms_data) % 1 != 0:
                raise ValueError(
                    f"Target timeframe {spec.timeframe} must be an integer multiple "
                    f"of data timeframe {spec.timeframe_data}"
                )
            lf = self.resample_data(
                lf,
                spec.timeframe,
                candle_type=spec.candle_type,
                by=["symbol"] if "symbol" in schema else None,
            )

        return (lf, stats) if debug and stats else lf

    def _month_count(self, spec: DataSpec) -> int:
        """Count months in date range for stats."""
        return sum(1 for _ in month_range(spec.date_range.start.date(), spec.date_range.end.date()))

    def _schema_hash(self, schema: pl.Schema) -> str:
        """Generate a short hash of the schema for reproducibility tracking."""
        schema_str = str(sorted((name, str(dtype)) for name, dtype in schema.items()))
        return hashlib.md5(schema_str.encode(), usedforsecurity=False).hexdigest()[:8]

    def resample_data(
        self,
        lf: pl.LazyFrame,
        timeframe: str,
        *,
        candle_type: CandleType | None = None,
        by: Sequence[str] | None = None,
    ) -> pl.LazyFrame:
        ct = candle_type or BinanceCandleType()
        schema = lf.collect_schema()

        missing = [c for c in (ct.o_col, ct.h_col, ct.l_col, ct.c_col, ct.v_col) if c not in schema]
        if missing:
            raise ValueError(f"Missing required columns for resample: {sorted(missing)}")

        time_col = (
            ct.end_col if ct.end_col in schema else ct.start_col if ct.start_col in schema else ""
        )
        if not time_col:
            raise ValueError("No timestamp column found for resample")

        by_cols = list(by or [])
        for col in ("symbol", "timeframe"):
            if col in schema and col not in by_cols:
                by_cols.append(col)

        ts_col = "__ts"
        agg_exprs = [
            pl.col(ct.o_col).first().alias(ct.o_col),
            pl.col(ct.h_col).max().alias(ct.h_col),
            pl.col(ct.l_col).min().alias(ct.l_col),
            pl.col(ct.c_col).last().alias(ct.c_col),
            pl.col(ct.v_col).sum().alias(ct.v_col),
        ]
        if ct.n_trades_col in schema:
            agg_exprs.append(pl.col(ct.n_trades_col).sum().alias(ct.n_trades_col))
        if ct.is_final_col in schema:
            agg_exprs.append(pl.col(ct.is_final_col).all().alias(ct.is_final_col))

        ts_expr = pl.from_epoch(pl.col(time_col), time_unit="ms").alias(ts_col)
        gb = lf.with_columns(ts_expr).group_by_dynamic(
            ts_col,
            every=timeframe,
            closed="left",
            label="right",
            group_by=by_cols if by_cols else None,
        )
        out = gb.agg(agg_exprs)

        tf_ms = parse_timeframe(timeframe)
        out = out.with_columns(
            pl.col(ts_col).cast(pl.Int64).alias(ct.end_col),
            (pl.col(ts_col).cast(pl.Int64) - tf_ms).alias(ct.start_col),
        )
        if "timeframe" in schema:
            out = out.with_columns(pl.lit(timeframe).alias("timeframe"))

        # Preserve Float32 for OHLCV columns (Numba compatibility)
        ohlcv_cols = [ct.o_col, ct.h_col, ct.l_col, ct.c_col, ct.v_col]
        out = out.with_columns([pl.col(c).cast(pl.Float32) for c in ohlcv_cols])

        return out.drop(ts_col)

    def slice_by_time(
        self,
        lf: pl.LazyFrame,
        start_ms: int,
        end_ms: int,
        *,
        time_col: str = "end_ms",
    ) -> pl.LazyFrame:
        """
        Slice a LazyFrame by time range (useful for WFA train/test splits).

        Args:
            lf: Input LazyFrame with a timestamp column.
            start_ms: Start timestamp (inclusive) in milliseconds.
            end_ms: End timestamp (exclusive) in milliseconds.
            time_col: Column name to filter on.

        Returns:
            Filtered LazyFrame containing only rows in [start_ms, end_ms).
        """
        return lf.filter((pl.col(time_col) >= start_ms) & (pl.col(time_col) < end_ms))

    def with_warmup_buffer(
        self,
        lf: pl.LazyFrame,
        start_ms: int,
        end_ms: int,
        warmup_bars: int,
        *,
        time_col: str = "end_ms",
    ) -> tuple[pl.LazyFrame, int]:
        """
        Load data with extra warmup bars before the target period.

        This is crucial for WFA (Walk-Forward Analysis) where indicators need
        historical data to be valid on the first day of the test period.

        Args:
            lf: Input LazyFrame (should contain data before start_ms).
            start_ms: Start of the target period in milliseconds.
            end_ms: End of the target period in milliseconds.
            warmup_bars: Number of bars needed for indicator warmup.
            time_col: Column name to filter on.

        Returns:
            Tuple of (filtered LazyFrame with warmup, actual_start_ms).
            The actual_start_ms indicates where the warmup begins.

        Example:
            For a 200-bar SMA on 1h data, warmup_bars=200 loads 200 extra
            hours before the test period starts.
        """
        # Get warmup_bars rows before start_ms, then data up to end_ms
        # We achieve this by filtering all data < end_ms, then taking rows
        # where timestamp >= (estimated warmup start) or is in last N before start
        before_start = lf.filter(pl.col(time_col) < start_ms)
        # Collect the timestamp of the warmup start (N bars before start_ms)
        warmup_lf = before_start.tail(warmup_bars)
        target_lf = lf.filter((pl.col(time_col) >= start_ms) & (pl.col(time_col) < end_ms))

        combined = pl.concat([warmup_lf, target_lf])
        return combined, start_ms

    def _paths_for_spec(self, spec: DataSpec) -> list[str]:
        start_date = spec.date_range.start.date()
        end_date = spec.date_range.end.date()
        # timeframe_data is guaranteed non-None after DataSpec.__post_init__
        tf_data = spec.timeframe_data or spec.timeframe

        paths: list[str] = []
        for symbol in spec.symbols:
            for y, m in month_range(start_date, end_date):
                year_dir = target_path(
                    root=spec.base_dir,
                    exchange=spec.exchange,
                    market=spec.market,
                    symbol=symbol,
                    data_type=spec.data_type,
                    interval=tf_data,
                    d=dt.date(y, m, 1),
                )
                padded = year_dir / f"{symbol}-{tf_data}-{y}-{m:02d}.parquet"
                if padded.exists():
                    paths.append(str(padded))
                    continue
                unpadded = year_dir / f"{symbol}-{tf_data}-{y}-{m}.parquet"
                if unpadded.exists():
                    paths.append(str(unpadded))
        return paths

    def _apply_filters(
        self,
        lf: pl.LazyFrame,
        spec: DataSpec,
        schema: pl.Schema,
        stats: LoadStats | None = None,
    ) -> pl.LazyFrame:
        time_col = (
            spec.candle_type.end_col
            if spec.candle_type.end_col in schema
            else spec.candle_type.start_col
            if spec.candle_type.start_col in schema
            else ""
        )
        if time_col:
            start_ms, end_ms = spec.date_range.to_epoch_ms()
            lf = lf.filter((pl.col(time_col) >= start_ms) & (pl.col(time_col) < end_ms))
            if stats:
                stats.filters_applied.append(f"time:{time_col}[{start_ms},{end_ms})")

        if spec.final_only and spec.candle_type.is_final_col in schema:
            lf = lf.filter(pl.col(spec.candle_type.is_final_col))
            if stats:
                stats.filters_applied.append("final_only")

        if "symbol" in schema:
            lf = lf.filter(pl.col("symbol").is_in(list(spec.symbols)))
            if stats:
                stats.filters_applied.append(f"symbols:{len(spec.symbols)}")
        # Filter by timeframe_data since that's what's in the parquet files
        if "timeframe" in schema:
            lf = lf.filter(pl.col("timeframe") == spec.timeframe_data)
            if stats:
                stats.filters_applied.append(f"timeframe:{spec.timeframe_data}")

        return lf

    def _select_columns(
        self,
        lf: pl.LazyFrame,
        spec: DataSpec,
        schema: pl.Schema,
        stats: LoadStats | None = None,
    ) -> pl.LazyFrame:
        if spec.columns is None:
            if stats:
                stats.columns_selected = tuple(schema.names())
            return lf

        must = []
        for col in ("symbol", "timeframe", spec.candle_type.start_col, spec.candle_type.end_col):
            if col and col in schema and col not in must:
                must.append(col)

        wanted: list[str] = []
        for col in spec.columns:
            if col in schema and col not in wanted:
                wanted.append(col)
        for col in must:
            if col not in wanted:
                wanted.append(col)

        if stats:
            stats.columns_selected = tuple(wanted)

        return lf.select(wanted) if wanted else lf

    def _normalize_schema(
        self,
        lf: pl.LazyFrame,
        spec: DataSpec,
        schema: pl.Schema,
        stats: LoadStats | None = None,
    ) -> pl.LazyFrame:
        float_cols = [name for name, dtype in schema.items() if dtype == pl.Float64]
        if float_cols:
            lf = lf.with_columns([pl.col(c).cast(pl.Float32) for c in float_cols])
            if stats:
                stats.float32_cols = tuple(float_cols)

        time_cols = [
            c for c in (spec.candle_type.start_col, spec.candle_type.end_col) if c in schema
        ]
        if time_cols:
            lf = lf.with_columns([pl.col(c).cast(pl.Int64) for c in time_cols])

        return lf

    def _sort(self, lf: pl.LazyFrame, spec: DataSpec, schema: pl.Schema) -> pl.LazyFrame:
        if spec.sort_keys:
            return lf.sort(list(spec.sort_keys))

        sort_keys: list[str] = []
        for col in ("symbol", "timeframe"):
            if col in schema:
                sort_keys.append(col)

        time_col = (
            spec.candle_type.end_col
            if spec.candle_type.end_col in schema
            else spec.candle_type.start_col
            if spec.candle_type.start_col in schema
            else ""
        )
        if time_col:
            sort_keys.append(time_col)

        return lf.sort(sort_keys) if sort_keys else lf
