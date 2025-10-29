from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from backtester.adapters.types import Candle, ControlEvent, OrderIntent
from backtester.core.bus import Bus, Subscription, SubscriptionConfig, TopicPriority
from backtester.core.clock import Clock
from backtester.data.feeder import BarFeed
from backtester.strategy.base import StrategyInfo
from backtester.strategy.sma import SMACrossParams, SMACrossStrategy

# TODO Clock single source of truth

# ---------- topics ----------
T_CANDLES = "mkt.candles"
T_ORDERS_INTENT = "orders.intent"
T_ORDERS_SANITIZED = "orders.sanitized"
T_ORDERS_ACK = "orders.ack"
T_FILLS = "orders.fills"
T_METRICS = "acct.metrics"  # e.g., NAV, DD, turnover
T_CONTROL = "control"


class EngineError(Exception):
    """
    Raised for any errors related to the functioning og the backtest engine.
    """


@dataclass(frozen=True)
class BacktestReport:
    """Lightweight run artifacts for programmatic consumption."""

    snapshot: (
        tuple  # tuple[PortfolioSnapshot, ...]  # a tuple of PortfolioSnapshots of length 0 or more
    )
    fills: tuple[object, ...]  # backtester.core.types.Fill but avoid circular import here
    acks: tuple  # tuple[OrderAck, ...]
    stats: Mapping[str, int]


class BacktestEngine:
    """Drive the event loop: Feed candles from BarFeed into the bus."""

    def __init__(self, clock: Clock, feed: BarFeed) -> None:
        self._clock = clock

        # Data
        self._feed = feed

        # strateggy
        self._sma: Optional[SMACrossStrategy] = None

        # Bus
        self._bus = Bus()

    # --- BUS SECTION --- (Topic Management, Subscriber Management)

    async def register_topics(self) -> None:
        await self._bus.register_topic(
            name=T_CANDLES,
            schema=Candle,
            priority=TopicPriority.HIGH,
            validate_schema=False,
        )
        await self._bus.register_topic(
            name=T_CONTROL,
            schema=ControlEvent,
            priority=TopicPriority.HIGH,
            validate_schema=False,
        )
        await self._bus.register_topic(
            name=T_ORDERS_INTENT,
            schema=OrderIntent,
            priority=TopicPriority.HIGH,
            validate_schema=False,
        )

    async def register_subscribers(self) -> Subscription:
        sma_sub_config = SubscriptionConfig(topics={"mkt.candles", "control"}, buffer_size=2000)
        sma_subscription = await self._bus.subscribe(
            name="sma_strategy", bus_sub_config=sma_sub_config
        )
        return sma_subscription

    # --- STRATEGY SECTION ---

    def init_strat(self) -> None:
        self._sma = SMACrossStrategy(
            params=SMACrossParams(timeframe="1h"), info=StrategyInfo(name="sma_cross")
        )
        self._sma.on_start()

    async def strategy_task(self, bus: Bus, sub: Subscription) -> None:
        intents = []
        if self._sma is None:
            raise EngineError("Strategy not initialized")

        while True:
            env = await sub.queue.get()
            if env is None:
                # bus closed/unsubscribed
                break

            if env.topic == T_CANDLES and isinstance(env.payload, Candle):
                intents = self._sma.on_candle(candle=env.payload)
                for intent in intents:
                    await bus.publish(
                        topic=T_ORDERS_INTENT, ts_utc=self._clock.now(), payload=intent
                    )

            elif env.topic == T_CONTROL and isinstance(env.payload, ControlEvent):
                if getattr(env.payload, "type", None) == "eof" and env.payload.source == T_CANDLES:
                    break  # graceful shutdown on EOF

        try:
            self._sma.on_end()
        except Exception:
            pass

        # optional cleanup

    # --- FEED SECTION ----

    async def feed_candles(self) -> dict[str, int]:
        """
        Producer only
        """
        async_publish = self._bus.publish
        frames = 0
        candles = 0
        t0 = time.perf_counter()

        for t_close, frame in self._feed.iter_frames():
            frames += 1
            t_close = int(t_close)  # BarFeed already advanced SimClock
            for symbol, c in frame.items():
                if c is None:
                    continue
                candles += 1
                await async_publish(topic=T_CANDLES, ts_utc=t_close, payload=c)
                # print(c.symbol, c.close)

        dt = time.perf_counter() - t0
        return {"frames": frames, "candles": candles, "duration_ms": int(dt * 1000)}

    # --- Other ---

    async def _await_pump_and_publish_eof(
        self, pump_task: asyncio.Task[dict[str, int]]
    ) -> dict[str, int]:
        metrics: dict[str, int] = {}
        try:
            metrics = await pump_task
        finally:
            await self.publish_eof(details=metrics or {})
        return metrics

    async def publish_eof(self, details: dict[str, Any]) -> None:
        now_ms = int(self._clock.now())
        eof = ControlEvent(type="eof", source=T_CANDLES, ts_utc=now_ms, details=details or {})
        await self._bus.publish(topic=T_CONTROL, ts_utc=now_ms, payload=eof)
        print(f"eof {eof} successfully published")

    # --- Main Loop ---

    async def run_async(self) -> None:
        await self.register_topics()
        strat_sub = await self.register_subscribers()
        print(f"topics {self._bus._topics} registered and subscribers {self._bus._subscriptions}")
        self.init_strat()
        print("Strategy initialized")

        self._feed.start()
        try:
            async with asyncio.TaskGroup() as tg:
                pump_task = tg.create_task(self.feed_candles())
                eof_task = tg.create_task(self._await_pump_and_publish_eof(pump_task))
                tg.create_task(self.strategy_task(self._bus, strat_sub))
            _metrics = eof_task.result()
        except Exception as e:
            print(f"problem {e}")
        finally:
            try:
                await self._bus.close(drain=True, timeout=1.0)
            except Exception:
                pass
            self._feed.stop()

    def run(self) -> None:
        """Synchronous wrapper for environments without an existing event loop."""
        asyncio.run(self.run_async())

    def run_2(self) -> None:
        pass
        # # Subscriptions
        # sub_strat = bus.subscribe({T_CANDLES, T_CONTROL}, maxsize=1024)
        # sub_risk = bus.subscribe({T_ORDERS_INTENT, T_CONTROL}, maxsize=1024)
        # sub_exec = bus.subscribe({T_ORDERS_SANITIZED, T_CANDLES, T_CONTROL}, maxsize=2048)
        # sub_acct = bus.subscribe({T_FILLS, T_CANDLES, T_CONTROL}, maxsize=4096)
        # sub_rprt = bus.subscribe({T_METRICS, T_CONTROL}, maxsize=4096)

        # # Launch tasks
        # tasks = [
        #     asyncio.create_task(feed_candles(bus, candles)),
        #     asyncio.create_task(strategy_task(bus, sub_strat, symbol=symbol)),
        #     asyncio.create_task(risk_task(bus, sub_risk)),
        #     asyncio.create_task(execution_task(bus, sub_exec)),
        #     asyncio.create_task(accounting_task(bus, sub_acct)),
        #     asyncio.create_task(report_task(bus, sub_rprt)),
        # ]
