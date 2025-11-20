from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import Iterable, Literal, Optional, overload

import polars as pl

from backtester.core.utility import month_range


@overload
def load_manifest(
    base_dir: str | None = None,
    exchange: str | None = None,
    market: str | None = None,
    symbols: Iterable[str] | None = None,
    intervals: Iterable[str] | None = None,
    granularity: str | None = None,
    period_min: str | None = None,
    period_max: str | None = None,
    *,
    lazy: Literal[True],
) -> pl.LazyFrame: ...


@overload
def load_manifest(
    base_dir: str | None = None,
    exchange: str | None = None,
    market: str | None = None,
    symbols: Iterable[str] | None = None,
    intervals: Iterable[str] | None = None,
    granularity: str | None = None,
    period_min: str | None = None,
    period_max: str | None = None,
    *,
    lazy: Literal[False] = False,
) -> pl.DataFrame: ...


def load_manifest(
    base_dir: str | None = None,
    exchange: str | None = None,
    market: str | None = None,
    symbols: Iterable[str] | None = None,
    intervals: Iterable[str] | None = None,
    granularity: str | None = None,
    period_min: str | None = None,
    period_max: str | None = None,
    *,
    lazy: bool = False,
) -> pl.DataFrame | pl.LazyFrame:
    """
    Load and filter the klines manifest as a Polars DataFrame (default) or LazyFrame.
    String filters are exact matches; period_* are lexicographic (works for YYYY-MM and YYYY-MM-DD).
    """
    if base_dir is None:
        raise ValueError("base_dir is required for load_manifest")

    fp = Path(base_dir) / "_meta" / "manifest.parquet"
    if not fp.exists():
        return pl.LazyFrame() if lazy else pl.DataFrame()

    lf = pl.scan_parquet(fp)

    if exchange:
        lf = lf.filter(pl.col("exchange") == exchange.lower())
    if market:
        lf = lf.filter(pl.col("market") == market.lower())
    if symbols:
        lf = lf.filter(pl.col("symbol").is_in(list(symbols)))
    if intervals:
        lf = lf.filter(pl.col("interval").is_in(list(intervals)))
    if granularity:
        lf = lf.filter(pl.col("granularity") == granularity)
    if period_min:
        lf = lf.filter(pl.col("period") >= period_min)
    if period_max:
        lf = lf.filter(pl.col("period") <= period_max)

    lf = lf.sort(["exchange", "market", "symbol", "interval", "granularity", "period"])

    return lf if lazy else lf.collect(engine="streaming")


def manifest_available_months(
    base_dir: str,
    exchange: str,
    market: str,
    symbol: str,
    interval: Optional[str],
    granularity: Optional[str] = "monthly",  # "monthly", "daily"
) -> list[str]:
    """
    Convenience: return a sorted list of 'YYYY-MM' that exist in the manifest
    for monthly granularity.
    """
    df = load_manifest(
        base_dir,
        exchange=exchange,
        market=market,
        symbols=[symbol],
        intervals=[interval] if interval is not None else None,
        granularity=granularity,
        lazy=False,
    )
    if df.is_empty():
        return []
    return sorted(df["period"].unique().to_list())


def _to_epoch_ms(ts: dt.datetime) -> int:
    # Treat naive timestamps as UTC to maintain determinism
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    else:
        ts = ts.astimezone(dt.timezone.utc)
    return int(ts.timestamp() * 1000)


def _monthly_paths_for_range(
    base_dir: str,
    exchange: str,
    market: str,
    data_type: str,
    symbol: str,
    timeframe: str,
    start: dt.datetime,
    end: dt.datetime,
) -> list[str]:
    """
    Get the paths for the relevant subset of data
    """
    relevant_months = month_range(start, end)
    fps: list[str] = []
    for y, m in relevant_months:
        fp = os.path.join(
            base_dir,
            f"exchange={exchange}",
            f"market={market}",
            f"symbol={symbol}",
            f"type={data_type}",
            f"timeframe={timeframe}",
            f"year={y:04d}",
            f"{symbol}-{timeframe}-{y}-{m}.parquet",
        )
        if os.path.exists(fp):
            fps.append(fp)
    return fps


def load_monthly_klines_df(
    base_dir: str,
    exchange: str,
    market: str,
    data_type: str,
    symbol: str,
    timeframe: str,
    start: dt.datetime,
    end: dt.datetime,
    *,
    columns: list[str] | None = None,
    final_only: bool = True,
    lazy: bool = False,
) -> pl.LazyFrame | pl.DataFrame:
    """
    Load monthly-partitioned candles as a Polars DataFrame (default) or LazyFrame (lazy=True).
    - Prunes by monthly partitions
    - Filters to [start, end)
    - final_only filters is_final==True (recommended)
    - Deterministic sort by (symbol,timeframe,end_ms)
    """

    # 1. Get the paths
    fps = _monthly_paths_for_range(
        base_dir, exchange, market, data_type, symbol, timeframe, start, end
    )
    if not fps:
        return pl.LazyFrame() if lazy is True else pl.DataFrame()

    # 2. Load into df
    lf = pl.scan_parquet(fps)

    lf = lf.filter(
        (pl.col("end_ms") >= _to_epoch_ms(start)) & (pl.col("end_ms") < _to_epoch_ms(end))
    )

    # only select final candles
    if final_only:
        if "is_final" in lf.collect_schema().names():
            lf = lf.filter(pl.col("is_final"))

    lf = lf.sort(["symbol", "timeframe", "end_ms"])

    if columns:
        # keep essential keys even if not requested
        must = {"symbol", "timeframe", "start_ms", "end_ms"}
        want = [c for c in list(must.union(columns)) if c in lf.collect_schema().names()]
        lf = lf.select(want)

    return lf if lazy else lf.collect(engine="streaming")
