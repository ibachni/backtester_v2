from __future__ import annotations

import datetime as dt
import heapq
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Literal, Optional, overload

import polars as pl

from backtester.config.configs import DataSubscriptionConfig
from backtester.core.audit import AuditWriter
from backtester.core.clock import Millis, SimClock
from backtester.core.utility import month_range
from backtester.data.source import CandleSource
from backtester.types.types import Candle, UnixMillis

# TODO Problem: Clock missalignmed UTC vs dt.datetime (gives local time)
# TODO Corrupted bars handling
# TODO Missing bars handling
"""

Open files and stream to the main engine!

Bar feeder: merges one or more deterministic candle sources into aligned frames,
advances the simulation clock to each frame's *close* time, and yields the frame.

Usage (typical):
    clock = SimClock(start_ms=...)
    feed = BarFeed(clock, align="outer")
    feed.subscribe("BTCUSDT", "3m", source_csv_or_partitioned)
    feed.subscribe("ETHUSDT", "3m", source_csv_or_partitioned)
    for t_close, frame in feed.iter_frames():
        # clock.now() == t_close here
        # frame is { "BTCUSDT": Candle|None, "ETHUSDT": Candle|None }
"""


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
    # TODO Inject name only for the folder (as part of config!)
    # Path("/Users/nicolas/data/crypto_data/parquet/_meta/klines_manifest.parquet")
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


# --- Monthly parquet reader (historical data) ---


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
            f"{symbol}-{timeframe}-{y}-{m}.parquet",  # TODO need to standardize download to m:02d
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


# --- BarFeeder ---


@dataclass(slots=True)
class FeederConfig:
    align: AlignPolicy = "outer"


@dataclass(slots=True)
class Subscription:
    sub_config: DataSubscriptionConfig
    source: CandleSource
    it: Iterator = field(init=False)
    sha: str = field(init=False)
    next_candle: Optional[Candle] = None
    next_close: Optional[int] = None  # ts + tf_ms
    bars_emitted: int = 0
    missing_frames: int = 0  # incremented only for align="outer"

    def __post_init__(self) -> None:
        self.it = iter(self.source)
        self.sha = self.sub_config.sha_256

        # do not do this
        # If input TF != target TF, wrap with resampler so BarFeed sees target TF stream
        if self.sub_config.timeframe != self.sub_config.timeframe_data:
            self.it = iter(
                _ResampleIter(
                    it=self.it,
                    symbol=self.sub_config.symbol,
                    timeframe_out=self.sub_config.timeframe,
                    tf_ms_out=self.sub_config.tf_ms,
                )
            )


class FeedError(Exception):
    """Raised for feed-level alignment or configuration errors."""


AlignPolicy = Literal["outer", "inner"]
CandleFrame = dict[str, Optional[Candle]]


@dataclass(slots=True)
class _ResampleIter:
    """
    Wrap a lower-tf candle Iterator (e.g., "1 min") and emit higher timeframe aggregated candles
    (e.g., 5 min); BarFeed is not touched.
    """

    it: Iterator[Candle]
    symbol: str
    timeframe_out: str
    tf_ms_out: Millis
    _carry: Optional[Candle] = None
    _done: bool = False

    def __iter__(self) -> "_ResampleIter":
        return self

    def __next__(self) -> Candle:
        if self._done:
            raise StopIteration

        # Seed the bucket with the first candle (from carry or upstream)
        c = self._carry if self._carry is not None else next(self.it)
        self._carry = None

        # Calculate the starting point of the bucket
        bucket_start = (c.start_ms // self.tf_ms_out) * self.tf_ms_out
        # Calculate the end point of the bucket
        bucket_end = bucket_start + self.tf_ms_out

        o = c.open
        h = c.high
        low_ = c.low
        v = c.volume
        trades = c.trades or 0
        last_close = c.close

        all_final = True if c.is_final is None else bool(c.is_final)

        while True:
            try:
                n = next(self.it)
            except StopIteration:
                self._done = True
                return Candle(
                    symbol=self.symbol,
                    timeframe=self.timeframe_out,
                    start_ms=bucket_start,
                    end_ms=bucket_end,
                    open=o,
                    high=h,
                    low=low_,
                    close=last_close,
                    volume=v,
                    trades=trades,
                    is_final=all_final,
                )

            if n.start_ms < bucket_end:
                # Same bucket
                h = max(h, n.high)
                low_ = min(low_, n.low)
                v += n.volume
                trades += n.trades or 0
                last_close = n.close
                if n.is_final is False:
                    all_final = False
            else:
                # Next bucket begins with n
                self._carry = n
                return Candle(
                    symbol=self.symbol,
                    timeframe=self.timeframe_out,
                    start_ms=bucket_start,
                    end_ms=bucket_end,
                    open=o,
                    high=h,
                    low=low_,
                    close=last_close,
                    volume=v,
                    trades=trades,
                    is_final=all_final,
                )


class BarFeed:
    """
    Purpose:
    - Merge multiple per-symbol candle sources into time-aligned frames keyed by close time.
    - Adcance the sim clock to each frame's close before yielding
    - Support "outer" alignment (includes unmatched rows) and "inner" across symbols

    Produces aligned frames and advances the SimClock.
    Basically: Load -> iterate -> clock_advance -> bus.publish(candles:<sym>:<tf>), candle)
    Important: Topics Management!
    """

    def __init__(
        self, feeder_cfg: FeederConfig, clock: SimClock, audit: Optional[AuditWriter] = None
    ) -> None:
        self._feeder_cfg = feeder_cfg
        self._clock = clock
        self._audit = audit
        self._align = feeder_cfg.align

        # Subscription list
        self._subs: list[Subscription] = []
        self._heap: list[tuple[int, int]] = []
        # uniqueness guard
        self._sub_keys: set[str] = set()
        self._initialized = False
        self._running = False

        # Obs
        self._frame_emitted: int = 0
        self._first_close: Optional[int] = None
        self._last_close: Optional[int] = None
        self._missing_once: set[str] = set()

    def _emit_audit(
        self,
        event: str,
        *,
        component: str = "data.feed",
        level: str = "INFO",
        simple: bool = True,
        sim_time: Optional[int] = None,
        payload: Optional[dict[str, Any]] = None,
    ) -> None:
        if self._audit is None:
            return
        self._audit.emit(
            component=component,
            event=event,
            level=level,
            simple=simple,
            sim_time=sim_time,
            payload=payload or {},
        )

    def start(self) -> None:
        self._running = True
        self._emit_audit(
            "FEED_STATE",
            simple=True,
            payload={
                "state": "started",
                "subscriptions": len(self._subs),
                "align": self._align,
            },
        )

    def stop(self) -> None:
        self._running = False
        self._emit_audit(
            "FEED_STATE",
            simple=True,
            payload={
                "state": "stopped",
                "frames_emitted": self._frame_emitted,
                "last_close": self._last_close,
            },
        )

    # --- Public API ---

    def subscribe(self, sub: Subscription) -> None:
        """
        Register a symbol/timeframe/source triplet (one at a time).
        """
        key = sub.sub_config.sha_256
        if key in self._sub_keys:
            msg = (
                f"Duplicate subscription for key: {key}, symbol: {sub.sub_config.symbol}, "
                f"timeframe: {sub.sub_config.timeframe}"
            )
            raise FeedError(msg)
        self._subs.append(sub)
        self._sub_keys.add(key)
        self._initialized = False
        cfg = sub.sub_config
        meta_paths: Optional[list[str]] = None
        try:
            meta = sub.source.meta()
            paths = getattr(meta, "paths", [])
            meta_paths = [str(p) for p in paths[:3]]
        except Exception:
            pass
        payload = {
            "symbol": cfg.symbol,
            "timeframe": cfg.timeframe,
            "timeframe_data": cfg.timeframe_data,
            "start_ms": cfg.start_ms,
            "end_ms": cfg.end_ms,
            "sha": cfg.sha_256,
            "batch_size": cfg.batch_size,
        }
        if meta_paths:
            payload["paths_sample"] = meta_paths
        self._emit_audit("FEED_SUBSCRIBED", simple=True, payload=payload)

    def iter_frames(self) -> Iterator[tuple[UnixMillis, CandleFrame]]:
        """
        Merge all subscribes sources into aligned frames
        Yields (t_close, frame) with t_close in ascending order.
        Advances SimClock to t_close before yielding.
        """
        if not self._subs:
            raise FeedError("No subscriptions registered")
        self._prime_heap()

        # main merge loop:
        drained = False
        try:
            while self._heap:
                t_min = self._heap[0][0]
                contributors: dict[int, Candle] = {}

                # 1. pop all subs that have a bar at t_min(one per sub at most)
                # 2. Add next candle to the dict of contributors
                # 3. Advance the sub to the next candle
                while self._heap and self._heap[0][0] == t_min:
                    _, idx = heapq.heappop(self._heap)
                    sub = self._subs[idx]
                    if sub.next_candle is not None:
                        sub.bars_emitted += 1
                        contributors[idx] = sub.next_candle

                    # advance that sub to its next candle
                    self._advance_sub(idx)

                if self._align == "inner":
                    raise NotImplementedError("align='inner' is not implemented yet")

                else:
                    frame: dict[str, Optional[Candle]] = {}
                    missing_symbols: list[str] = []
                    for i, sub in enumerate(self._subs):
                        c = contributors.get(i)
                        symbol = sub.sub_config.symbol
                        if c is None:
                            frame[symbol] = None
                            missing_symbols.append(symbol)
                        else:
                            frame[symbol] = c

                # Advance the SimClock to the frame's close time before yielding
                if t_min < self._clock.now():
                    msg = {
                        "Non-monotonic t_close form heap",
                        f"{t_min} < clock.now()={self._clock.now()}",
                    }
                    raise FeedError(msg)
                self._clock.advance_to(t_min)
                if self._first_close is None:
                    self._first_close = t_min
                self._last_close = t_min
                self._frame_emitted += 1
                if missing_symbols:
                    for sym in missing_symbols:
                        if sym not in self._missing_once:
                            self._missing_once.add(sym)
                            self._emit_audit(
                                "FEED_MISSING_SYMBOL",
                                level="WARN",
                                sim_time=t_min,
                                simple=True,
                                payload={
                                    "symbol": sym,
                                    "frame_close": t_min,
                                    "frames_emitted": self._frame_emitted,
                                },
                            )
                yield t_min, frame
            drained = True
        finally:
            self._emit_audit(
                "FEED_ITERATION_END",
                simple=True,
                payload={
                    "drained": drained,
                    "frames_emitted": self._frame_emitted,
                    "first_close": self._first_close,
                    "last_close": self._last_close,
                    "remaining_sources": len(
                        [1 for sub in self._subs if sub.next_close is not None]
                    ),
                },
            )

    # --- helpers ---

    def _prime_heap(self) -> None:
        """Initialize or refresh the heap with each subscription's first bar."""
        if self._initialized:
            return
        self._heap.clear()
        for idx, _ in enumerate(self._subs):
            self._pull_first(idx)
        # push available subs
        for idx, sub in enumerate(self._subs):
            if sub.next_close is not None:
                heapq.heappush(self._heap, (sub.next_close, idx))
        self._initialized = True

    def _pull_first(self, idx: int) -> None:
        """
        Fast-forward a subscription to the first candle whose close time
        is >= the subscription start_ms, and set next_candle/next_close.
        """
        sub = self._subs[idx]
        try:
            c = next(sub.it)
        except StopIteration:
            sub.next_candle = None
            sub.next_close = None
            self._emit_audit(
                "FEED_SOURCE_EMPTY",
                simple=True,
                level="WARN",
                payload={"symbol": sub.sub_config.symbol, "timeframe": sub.sub_config.timeframe},
            )
            return

        skipped = 0
        while c.end_ms < sub.sub_config.start_ms:
            skipped += 1
            try:
                c = next(sub.it)
            except StopIteration:
                sub.next_candle = None
                sub.next_close = None
                self._emit_audit(
                    "FEED_SOURCE_UNDERRUN",
                    level="WARN",
                    simple=True,
                    payload={
                        "symbol": sub.sub_config.symbol,
                        "timeframe": sub.sub_config.timeframe,
                        "skipped": skipped,
                    },
                )
                return
        sub.missing_frames += skipped
        if skipped > 0:
            self._emit_audit(
                "FEED_SKIP_LEADING",
                level="DEBUG",
                simple=True,
                payload={
                    "symbol": sub.sub_config.symbol,
                    "timeframe": sub.sub_config.timeframe,
                    "skipped": skipped,
                },
            )

        # Schedule on the candle's close time
        next_close = c.end_ms
        sub.next_candle = c
        sub.next_close = next_close

    def _advance_sub(self, idx: int) -> None:
        """Advance a subscription to its next candle and (re)insert into the heap if available."""
        sub = self._subs[idx]
        prev_close = sub.next_close
        try:
            c = next(sub.it)
        except StopIteration:
            sub.next_candle = None
            sub.next_close = None
            self._emit_audit(
                "FEED_SOURCE_EXHAUSTED",
                simple=True,
                payload={
                    "symbol": sub.sub_config.symbol,
                    "timeframe": sub.sub_config.timeframe,
                    "last_close": prev_close,
                    "bars_emitted": sub.bars_emitted,
                },
            )
            return

        # schedule on the candle's close time.
        next_close = c.end_ms
        # TODO How to manage data inconsistencies
        if prev_close is not None and next_close < prev_close:
            raise FeedError(
                f"Non-monotonic source for {sub.sub_config.symbol}@{sub.sub_config.timeframe}: "
                f"{next_close} < {prev_close}"
            )
        sub.next_candle = c
        sub.next_close = next_close
        heapq.heappush(self._heap, (next_close, idx))
