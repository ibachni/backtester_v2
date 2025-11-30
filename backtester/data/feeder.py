from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Optional

from backtester.config.configs import DataSubscriptionConfig, FeederConfig
from backtester.core.bus import Bus
from backtester.core.clock import SimClock
from backtester.data.resampler import ResampleIter
from backtester.data.source import CandleSource
from backtester.errors.errors import FeedError
from backtester.types.aliases import UnixMillis
from backtester.types.topics import T_LOG
from backtester.types.types import Candle, LogEvent

"""
- Bar feed and resampleiter runs in pure python
-> bottleneck for tick data
- Implement align inner
- chose either pl or pa

Post MVP: Gap detection
- (metrics, report data gaps)
- Handling of missing bars, corrupted bars etc.
- Convert to async iterator for live trading
"""

# --- BarFeeder ---


CandleFrame = dict[str, Optional[Candle]]


@dataclass(slots=True)
class DataSubscription:
    sub_config: DataSubscriptionConfig
    source: CandleSource
    it: AsyncIterator = field(init=False)
    sha: str = field(init=False)
    next_candle: Optional[Candle] = None
    next_close: Optional[int] = None  # ts + tf_ms
    last_candle: Optional[Candle] = None  # <--- ADD THIS FIELD
    bars_emitted: int = 0
    missing_frames: int = 0  # incremented only for align="outer"

    def __post_init__(self) -> None:
        self.it = self.source.__aiter__()
        self.sha = self.sub_config.sha_256

        # If input TF != target TF, wrap with resampler so BarFeed sees target TF stream
        if self.sub_config.timeframe != self.sub_config.timeframe_data:
            self.it = ResampleIter(
                it=self.it,
                symbol=self.sub_config.symbol,
                timeframe_out=self.sub_config.timeframe,
                tf_ms_out=self.sub_config.tf_ms,
            ).__aiter__()


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

    def __init__(self, feeder_cfg: FeederConfig, clock: SimClock, bus: Bus) -> None:
        self._feeder_cfg = feeder_cfg
        self._clock = clock
        self._bus = bus
        self._align = feeder_cfg.align

        # Subscription list
        self._subs: list[DataSubscription] = []
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
            component="BarFeed",
            msg=msg,
            payload=payload or {},
            sim_time=self._clock.now(),
        )
        await self._bus.publish(topic=T_LOG, ts_utc=self._clock.now(), payload=log_event)

    async def start(self) -> None:
        self._running = True
        await self._emit_log(
            level="INFO",
            msg="Feed started",
            payload={
                "event": "FEED_STATE",
                "state": "started",
                "subscriptions": len(self._subs),
                "align": self._align,
            },
        )

    async def stop(self) -> None:
        self._running = False
        await self._emit_log(
            level="INFO",
            msg="Feed stopped",
            payload={
                "event": "FEED_STATE",
                "state": "stopped",
                "frames_emitted": self._frame_emitted,
                "last_close": self._last_close,
            },
        )

    def get_stats(self) -> dict[str, dict[str, Any]]:
        stats_by_source = {}
        for sub in self._subs:
            stats_by_source[sub.sub_config.symbol] = sub.source.stats()

        return stats_by_source

    # --- Public API ---

    def subscribe(self, sub: DataSubscription) -> None:
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
        payload["event"] = "FEED_SUBSCRIBED"

    async def iter_frames(self) -> AsyncIterator[tuple[UnixMillis, CandleFrame]]:
        """
        Merge all subscribes sources into aligned frames
        Yields (t_close, frame) with t_close in ascending order.
        Advances SimClock to t_close before yielding.
        """
        if not self._running:
            raise FeedError("BarFeed not started. Call start() first.")

        if not self._subs:
            raise FeedError("No subscriptions registered")
        await self._prime_heap()

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
                        sub.last_candle = sub.next_candle
                        contributors[idx] = sub.next_candle

                    # advance that sub to its next candle
                    await self._advance_sub(idx)

                if self._align == "inner":
                    # TODO: Implement inner join (intersection of all sources)
                    raise NotImplementedError("align='inner' is not implemented yet")

                else:
                    frame: dict[str, Optional[Candle]] = {}
                    missing_symbols: list[str] = []
                    for i, sub in enumerate(self._subs):
                        c = contributors.get(i)
                        symbol = sub.sub_config.symbol
                        if c is not None:
                            frame[symbol] = c
                        elif sub.last_candle is not None:
                            last_close = sub.last_candle.close
                            tf_ms = sub.sub_config.tf_ms
                            # create a synthetic 0 volume candle at the previous close price
                            synthetic = Candle(
                                symbol=symbol,
                                timeframe=sub.sub_config.timeframe,
                                start_ms=t_min - tf_ms,
                                end_ms=t_min,
                                open=last_close,
                                high=last_close,
                                low=last_close,
                                close=last_close,
                                volume=0.0,
                                trades=0,
                                is_final=True,
                            )
                            frame[symbol] = synthetic

                        else:
                            # Gap at start of stream (no previous data available)
                            frame[symbol] = None
                            missing_symbols.append(symbol)

                if t_min < self._clock.now():
                    msg = {
                        "Non-monotonic t_close form heap",
                        f"{t_min} < clock.now()={self._clock.now()}",
                    }
                    raise FeedError(msg)
                if self._first_close is None:
                    self._first_close = t_min
                self._last_close = t_min
                self._frame_emitted += 1
                if missing_symbols:
                    for sym in missing_symbols:
                        if sym not in self._missing_once:
                            self._missing_once.add(sym)
                            await self._emit_log(
                                level="WARN",
                                msg="Symbol missing from frame",
                                payload={
                                    "event": "FEED_MISSING_SYMBOL",
                                    "symbol": sym,
                                    "frame_close": t_min,
                                    "frames_emitted": self._frame_emitted,
                                },
                            )
                yield t_min, frame
            drained = True
        finally:
            await self._emit_log(
                level="INFO",
                msg="Feed iteration ended",
                payload={
                    "event": "FEED_ITERATION_END",
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

    async def _prime_heap(self) -> None:
        """Initialize or refresh the heap with each subscription's first bar."""
        if self._initialized:
            return
        self._heap.clear()
        for idx, _ in enumerate(self._subs):
            await self._pull_first(idx)
        # push available subs
        for idx, sub in enumerate(self._subs):
            if sub.next_close is not None:
                heapq.heappush(self._heap, (sub.next_close, idx))
        self._initialized = True

    async def _pull_first(self, idx: int) -> None:
        """
        Fast-forward a subscription to the first candle whose close time
        is >= the subscription start_ms, and set next_candle/next_close.
        """
        sub = self._subs[idx]
        try:
            c = await anext(sub.it)
        except StopAsyncIteration:
            sub.next_candle = None
            sub.next_close = None
            await self._emit_log(
                level="WARN",
                msg="Source is empty",
                payload={
                    "event": "FEED_SOURCE_EMPTY",
                    "symbol": sub.sub_config.symbol,
                    "timeframe": sub.sub_config.timeframe,
                },
            )
            return

        skipped = 0
        while c.end_ms < sub.sub_config.start_ms:
            skipped += 1
            try:
                c = await anext(sub.it)
            except StopAsyncIteration:
                sub.next_candle = None
                sub.next_close = None
                await self._emit_log(
                    level="WARN",
                    msg="Source underrun during skip",
                    payload={
                        "event": "FEED_SOURCE_UNDERRUN",
                        "symbol": sub.sub_config.symbol,
                        "timeframe": sub.sub_config.timeframe,
                        "skipped": skipped,
                    },
                )
                return
        sub.missing_frames += skipped
        # Skip leading bars silently (tracked in sub.missing_frames)

        # Schedule on the candle's close time
        next_close = c.end_ms
        sub.next_candle = c
        sub.next_close = next_close

    async def _advance_sub(self, idx: int) -> None:
        """Advance a subscription to its next candle and (re)insert into the heap if available."""
        sub = self._subs[idx]
        prev_close = sub.next_close
        try:
            c = await anext(sub.it)
        except StopAsyncIteration:
            sub.next_candle = None
            sub.next_close = None
            await self._emit_log(
                level="INFO",
                msg="Source exhausted",
                payload={
                    "event": "FEED_SOURCE_EXHAUSTED",
                    "symbol": sub.sub_config.symbol,
                    "timeframe": sub.sub_config.timeframe,
                    "last_close": prev_close,
                    "bars_emitted": sub.bars_emitted,
                },
            )
            return

        # schedule on the candle's close time.
        next_close = c.end_ms
        if prev_close is not None and next_close < prev_close:
            raise FeedError(
                f"Non-monotonic source for {sub.sub_config.symbol}@{sub.sub_config.timeframe}: "
                f"{next_close} < {prev_close}"
            )
        sub.next_candle = c
        sub.next_close = next_close
        heapq.heappush(self._heap, (next_close, idx))
