from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Coroutine, Mapping, Optional

from backtester.config.configs import AccountConfig, AuditConfig, BacktestConfig, RunContext
from backtester.core.account import Account
from backtester.core.audit import AuditWriter
from backtester.core.bus import Bus, Subscription, SubscriptionConfig, TopicPriority
from backtester.core.clock import Clock
from backtester.core.performance import MetricsConfig, MetricsEngine
from backtester.core.utility import dec
from backtester.data.feeder import BarFeed
from backtester.errors.errors import EngineError
from backtester.sim.sim import ExecutionSimulator, SlippageModel
from backtester.strategy.order_validation import OrderValidation
from backtester.strategy.sma_extended import SMAExtendedParams, SMAExtendedStrategy
from backtester.types.types import (
    Candle,
    ControlEvent,
    Fill,
    LotClosedEvent,
    OrderAck,
    OrderIntent,
    PortfolioSnapshot,
    StrategyInfo,
    TradeEvent,
    ValidatedOrderIntent,
)

# TODO Clock single source of truth
# TODO BUG: First OrderIntent is immediately sanitized
# TODO Move AuditWritter to a Subscriber who listens to ALL events;
# TODO Avoid emitting audit logs in different files etc. (clutters everything)

# ---------- topics ----------
T_CANDLES = "mkt.candles"
T_ORDERS_INTENT = "orders.intent"
T_ORDERS_SANITIZED = "orders.sanitized"
T_ORDERS_ACK = "orders.ack"
T_ORDERS_CANCELED = "orders.canceled"
T_FILLS = "orders.fills"
T_METRICS = "account.metrics"  # e.g., NAV, DD, turnover
T_CONTROL = "control"
T_TRADE_EVENT = "account.trade"
T_ACCOUNT_SNAPSHOT = "account.snapshot"


@dataclass
class EngineComponent:
    name: str
    topics: set[str]
    buffer_size: int
    runner: Callable[[Subscription], Coroutine[Any, Any, None]]  # coroutine returning None

    subscription: Subscription | None = None

    async def subscribe(self, bus: Bus) -> None:
        cfg = SubscriptionConfig(topics=self.topics, buffer_size=self.buffer_size)
        self.subscription = await bus.subscribe(name=self.name, bus_sub_config=cfg)

    async def start(self, tg: asyncio.TaskGroup) -> None:
        assert self.subscription is not None
        tg.create_task(self.runner(self.subscription), name=self.name)


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

    def __init__(
        self,
        run_ctx: RunContext,
        backtest_cfg: BacktestConfig,
        clock: Clock,
        feed: BarFeed,
        *,
        audit_path: str | Path | None = None,
    ) -> None:
        """
        Only store dependencies, not IO or side effects.
        """
        # Basics
        self._run_ctx = run_ctx
        self._cfg = backtest_cfg
        self._clock = clock
        self._feed = feed
        self._audit_path = audit_path

        self._run_ctx = run_ctx
        self._clock = clock
        self._feed = feed
        self._audit_path = audit_path

        # Will be created later
        # self._bus: Bus | None = None
        # self._audit: AuditWriter | None = None
        # self._components: list[EngineComponent] = []
        # self._state = "created"

        # Bus
        self._bus = Bus()

        # Auditing
        self._audit = self._init_audit_writer(audit_path)
        self._bus.set_audit(self._audit)

        # Order Validation
        self._OrderVal = OrderValidation(bus=self._bus, clock=self._clock, audit=self._audit)

        # Exec Simulation
        self._sim = ExecutionSimulator(
            bus=self._bus, clock=self._clock, slip_model=SlippageModel(bps=1), audit=self._audit
        )

        # Account
        self._account = Account(
            cfg=AccountConfig(),
            bus=self._bus,
            audit_writer=self._audit,
            clock=self._clock,
            starting_cash=Decimal("20000"),
        )

        # Data
        self._feed = feed
        self._feed._audit = self._audit

        # Performance
        metrics_cfg = MetricsConfig(trading_interval="1h")
        self._performance = MetricsEngine(metrics_cfg)

    # --- AUDIT SECTION ---

    def _init_audit_writer(self, audit_path: str | Path | None) -> AuditWriter:
        base = Path(audit_path) if audit_path is not None else Path("runs") / "audit"
        if base.is_dir():
            fp = base / f"{int(time.time() * 1000)}.ndjson"
        elif base.suffix:
            fp = base
        else:
            fp = base / f"{int(time.time() * 1000)}.ndjson"
        self._audit_path = fp
        return AuditWriter(run_ctx=self._run_ctx, cfg=AuditConfig(), path=str(fp))

    def _emit_audit(
        self,
        event: str,
        *,
        component: str = "engine",
        level: str = "INFO",
        simple: bool = False,
        sim_time: Optional[int] = None,
        payload: Optional[dict[str, Any]] = None,
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        parent_id: Optional[str] = None,
    ) -> None:
        if not hasattr(self, "_audit") or self._audit is None:
            return
        self._audit.emit(
            component=component,
            event=event,
            level=level,
            simple=simple,
            sim_time=sim_time,
            payload=payload or {},
            symbol=symbol,
            order_id=order_id,
            parent_id=parent_id,
        )

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
        await self._bus.register_topic(
            name=T_ORDERS_ACK,
            schema=OrderAck,
            priority=TopicPriority.HIGH,
            validate_schema=False,
        )
        await self._bus.register_topic(
            name=T_ORDERS_SANITIZED,
            schema=OrderIntent,
            priority=TopicPriority.HIGH,
            validate_schema=False,
        )
        await self._bus.register_topic(
            name=T_ORDERS_CANCELED,
            schema=OrderIntent,
            priority=TopicPriority.HIGH,
            validate_schema=False,
        )
        await self._bus.register_topic(
            name=T_FILLS,
            schema=Fill,
            priority=TopicPriority.HIGH,
            validate_schema=False,
        )
        await self._bus.register_topic(
            name=T_TRADE_EVENT,
            schema=LotClosedEvent,
            priority=TopicPriority.HIGH,
            validate_schema=False,
        )
        # Validate here
        await self._bus.register_topic(
            name=T_ACCOUNT_SNAPSHOT,
            schema=PortfolioSnapshot,
            priority=TopicPriority.HIGH,
            validate_schema=False,
        )

    async def register_subscribers(self) -> list[Subscription]:
        # Configs
        sma_config = SubscriptionConfig(topics={T_CANDLES, T_CONTROL, T_FILLS}, buffer_size=2048)
        order_val_config = SubscriptionConfig(topics={T_ORDERS_INTENT, T_CONTROL}, buffer_size=2048)
        sim_config = SubscriptionConfig(
            topics={T_ORDERS_SANITIZED, T_CANDLES, T_CONTROL}, buffer_size=2048
        )
        account_config = SubscriptionConfig(
            topics={T_CANDLES, T_FILLS, T_CONTROL}, buffer_size=2048
        )
        performance_config = SubscriptionConfig(
            topics={T_CANDLES, T_TRADE_EVENT, T_ACCOUNT_SNAPSHOT, T_CONTROL}, buffer_size=2048
        )

        # Subcriptions
        sma_subscription = await self._bus.subscribe(name="sma_strategy", bus_sub_config=sma_config)
        order_val_subscription = await self._bus.subscribe(
            name="order_val", bus_sub_config=order_val_config
        )
        sim_subscription = await self._bus.subscribe(name="sim", bus_sub_config=sim_config)
        account_subscription = await self._bus.subscribe(
            name="account", bus_sub_config=account_config
        )
        performance_subscription = await self._bus.subscribe(
            name="performance", bus_sub_config=performance_config
        )

        return [
            sma_subscription,
            order_val_subscription,
            sim_subscription,
            account_subscription,
            performance_subscription,
        ]

    # --- STRATEGY SECTION ---

    def init_strat(self) -> None:
        params = SMAExtendedParams(
            symbols=("BTCUSDT", "ETHUSDT"),
            timeframe="1h",
            fast=10,
            slow=40,
            qty=Decimal("0.01"),
            allow_reentry=True,
            max_positions=4,
            cooldown_bars=3,
            tp_pct=0.03,
            sl_pct=0.01,
            trail_pct=0.005,
            daily_loss_limit=150.0,
        )
        self._strategy = SMAExtendedStrategy(
            clock=self._clock,
            params=params.__dict__,
            info=StrategyInfo(name="sma_extended", version="0.2.0"),
        )
        self._strategy.on_start()
        self._emit_audit(
            "STRATEGY_START",
            component="strategy",
            simple=True,
            payload={
                "strategy": self._strategy.info.name,
                "params": params.__dict__,
                "params_hash": self._strategy.params_hash,
            },
        )

    async def strategy_task(self, bus: Bus, sub: Subscription) -> None:
        intents = []
        if self._strategy is None:
            raise EngineError("Strategy not initialized")

        while True:
            env = await sub.queue.get()
            if env is None:
                # bus closed/unsubscribed
                break

            elif env.topic == T_CANDLES and isinstance(env.payload, Candle):
                intents = self._strategy.on_candle(candle=env.payload)
                await bus.publish_batch(
                    topic=T_ORDERS_INTENT, ts_utc=self._clock.now(), payloads=intents
                )

                payload = {
                    "strategy": self._strategy.info.name,
                    "count": len(intents),
                    "bar_end": env.payload.end_ms,
                    "intents": [self._audit.intent_to_payload(i) for i in intents],
                }
                if intents:
                    for intent in intents:
                        payload = {
                            "strategy": self._strategy.info.name,
                            "bar_end": env.payload.end_ms,
                            "intents": intent,
                        }
                        self._emit_audit(
                            "STRATEGY_INTENT",
                            component="strategy",
                            sim_time=env.payload.end_ms,
                            payload=payload,
                            symbol=env.payload.symbol,
                            order_id=intent.id,
                            parent_id="False",
                        )

            elif env.topic == T_FILLS and isinstance(env.payload, Fill):
                follow_up = self._strategy.on_fill(fill=env.payload)

                payload = {
                    "strategy": self._strategy.info.name,
                    "fill": self._audit.fill_to_payload(env.payload),
                    "follow_up_count": len(follow_up or []),
                }
                if follow_up:
                    payload["follow_up_intents"] = [
                        self._audit.intent_to_payload(i) for i in follow_up
                    ]
                self._emit_audit(
                    "STRATEGY_FILL",
                    component="strategy",
                    sim_time=int(env.payload.ts),
                    payload=payload,
                    symbol=env.payload.symbol,
                    order_id=getattr(env.payload, "order_id", None),
                )

            elif env.topic == T_CONTROL and isinstance(env.payload, ControlEvent):
                if getattr(env.payload, "type", None) == "eof" and env.payload.source == T_CANDLES:
                    self._emit_audit(
                        "STRATEGY_EOF",
                        component="strategy",
                        simple=True,
                        payload={
                            "strategy": self._strategy.info.name if self._strategy else None,
                            "details": dict(env.payload.details or {}),
                        },
                    )
                    break  # graceful shutdown on EOF

        try:
            self._strategy.on_end()
            self._emit_audit(
                "STRATEGY_END",
                component="strategy",
                simple=True,
                payload={"strategy": self._strategy.info.name if self._strategy else None},
            )
        except Exception:
            pass

    async def order_validation_task(self, sub: Subscription) -> None:
        while True:
            env = await sub.queue.get()
            if env is None:
                break

            # if env.topic == T_CANDLES and isinstance(env.payload, Candle):
            #     self._OrderVal._last_candle[env.payload.symbol] = env.payload

            elif env.topic == T_ORDERS_INTENT and isinstance(env.payload, OrderIntent):
                await self._OrderVal.validate_order(env.payload)

            elif env.topic == T_CONTROL and isinstance(env.payload, ControlEvent):
                if getattr(env.payload, "type", None) == "eof" and env.payload.source == T_CANDLES:
                    break  # graceful shutdown on EOF

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

    #         sub_config_2 = SubscriptionConfig(
    # symbol = "ETHUSDT",
    # start_dt = dt.datetime(2019,1,1, tzinfo=dt.timezone.utc),
    # end_dt = dt.datetime(2025,1,1, tzinfo=dt.timezone.utc),
    # timeframe = "1h",
    # timeframe_data = "1m"

    # --- SIM SECTION ----

    async def execution_simulation_task(self, sub: Subscription) -> None:
        current_index = 1
        while True:
            env = await sub.queue.get()
            if env is None:
                break

            elif env.topic == T_CANDLES and isinstance(env.payload, Candle):
                await self._sim.on_candle(symbol=env.payload.symbol, candle=env.payload)
                current_index += 1

            elif env.topic == T_ORDERS_SANITIZED and isinstance(env.payload, ValidatedOrderIntent):
                await self._sim.on_order(intent=env.payload)

            elif env.topic == T_CONTROL and isinstance(env.payload, ControlEvent):
                if getattr(env.payload, "type", None) == "eof" and env.payload.source == T_CANDLES:
                    break  # graceful shutdown on EOF

    # --- ACCOUNT SECTION ----

    async def account_task(self, sub: Subscription) -> None:
        while True:
            env = await sub.queue.get()
            if env is None:
                break

            elif env.topic == T_CANDLES and isinstance(env.payload, Candle):
                self._account.set_mark(
                    symbol=env.payload.symbol, price=dec(env.payload.close), ts=env.payload.end_ms
                )
                await self._account.snapshot(ts=env.payload.end_ms)

            elif env.topic == T_FILLS and isinstance(env.payload, Fill):
                await self._account.apply_fill(fill=env.payload)

            elif env.topic == T_CONTROL and isinstance(env.payload, ControlEvent):
                if getattr(env.payload, "type", None) == "eof" and env.payload.source == T_CANDLES:
                    # flatten and break
                    await self._account.close_all()
                    ts_now = int(self._clock.now())
                    self._emit_audit(
                        "ACCOUNT_FLATTEN",
                        component="account",
                        simple=True,
                        sim_time=ts_now,
                        payload={"reason": "eof"},
                    )
                    await self._bus.publish(
                        topic=T_CONTROL,
                        ts_utc=ts_now,
                        payload=ControlEvent(
                            type="eof",
                            source=T_ACCOUNT_SNAPSHOT,
                            ts_utc=ts_now,
                            details={"phase": "account_flatten"},
                        ),
                    )
                    break  # graceful shutdown on EOF

    # --- PERFORMANCE SECTION ---

    async def performance_task(self, sub: Subscription) -> None:
        last_equity: float | None = None
        shutdown_requested = False

        while True:
            env = await sub.queue.get()
            if env is None:
                break

            elif env.topic == T_CANDLES and isinstance(env.payload, Candle):
                self._performance.on_bar(bar=env.payload)

            elif env.topic == T_ACCOUNT_SNAPSHOT and isinstance(env.payload, PortfolioSnapshot):
                self._performance.on_snapshot(snap=env.payload)
                last_equity = float(env.payload.equity)

            elif env.topic == T_TRADE_EVENT and isinstance(env.payload, TradeEvent):
                self._performance.on_trade(tr=env.payload, account_equity=last_equity)

            elif env.topic == T_TRADE_EVENT and isinstance(env.payload, LotClosedEvent):
                self._performance.on_lot_closed(lot_closed=env.payload)

            elif env.topic == T_CONTROL and isinstance(env.payload, ControlEvent):
                payload_type = getattr(env.payload, "type", None)
                payload_source = env.payload.source
                if payload_type == "eof" and payload_source == T_CANDLES:
                    shutdown_requested = True
                    continue
                if payload_type == "eof" and payload_source == T_ACCOUNT_SNAPSHOT:
                    shutdown_requested = True
                    break  # wait for account flush before finalizing

        if shutdown_requested:
            summary = self._performance.summary()
            print(summary)

    # --- Other ---

    async def _await_pump_and_publish_eof(
        self, pump_task: asyncio.Task[dict[str, int]]
    ) -> dict[str, int]:
        metrics: dict[str, int] = {}
        try:
            metrics = await pump_task
            self._emit_audit("FEED_COMPLETED", simple=True, payload=metrics or {})
        finally:
            await self.publish_eof(details=metrics or {})
        return metrics

    async def publish_eof(self, details: dict[str, Any]) -> None:
        now_ms = int(self._clock.now())
        eof = ControlEvent(type="eof", source=T_CANDLES, ts_utc=now_ms, details=details or {})
        await self._bus.publish(topic=T_CONTROL, ts_utc=now_ms, payload=eof)
        self._emit_audit("ENGINE_EOF", simple=True, payload={"details": details or {}})

    # --- Main Loop ---

    async def run_async(self) -> None:
        self._emit_audit(
            "RUN_START",
            simple=True,
            payload={
                "clock_now": self._clock.now(),
                "audit_path": str(self._audit_path) if self._audit_path else None,
            },
        )
        self._emit_audit(
            "ENGINE_BOOT",
            simple=True,
            payload={
                "topics_configured": len(self._bus._topics),
                "subscribers": len(self._bus._subscriptions),
            },
        )
        await self.register_topics()
        sub_list = await self.register_subscribers()
        self.init_strat()
        topic_names = list(self._bus._topics.keys())
        subscriber_names = [sub.name for sub in sub_list]
        strategy_name = self._strategy.info.name if self._strategy is not None else None
        self._emit_audit(
            "ENGINE_READY",
            simple=True,
            payload={
                "topics": topic_names,
                "subscribers": subscriber_names,
                "strategy": strategy_name,
            },
        )
        self._feed.start()
        self._emit_audit(
            "FEED_START",
            simple=True,
            payload={"subscription_count": len(getattr(self._feed, "_subs", []))},
        )
        await self._account.snapshot()  # Emits initial snapshot

        metrics: Optional[dict[str, Any]] = None
        bus_close_error: Optional[str] = None
        try:
            self._emit_audit("ENGINE_LOOP_START", simple=True, payload={})
            async with asyncio.TaskGroup() as tg:
                pump_task = tg.create_task(self.feed_candles())
                tg.create_task(self.strategy_task(self._bus, sub_list[0]))
                tg.create_task(self.order_validation_task(sub_list[1]))
                tg.create_task(self.execution_simulation_task(sub_list[2]))
                tg.create_task(self.account_task(sub_list[3]))
                tg.create_task(self.performance_task(sub_list[4]))
                eof_task = tg.create_task(self._await_pump_and_publish_eof(pump_task))
            metrics = eof_task.result()
            self._emit_audit(
                "ENGINE_LOOP_COMPLETE", simple=True, payload={"metrics": metrics or {}}
            )
        except Exception as e:
            print(f"problem {e}")
            self._emit_audit(
                "ENGINE_EXCEPTION",
                level="ERROR",
                simple=True,
                payload={"error": repr(e)},
            )
        finally:
            try:
                await self._bus.close(drain=True, timeout=1.0)
            except Exception as exc:
                bus_close_error = repr(exc)
            self._feed.stop()
            self._emit_audit(
                "RUN_STOP",
                simple=True,
                payload={
                    "metrics": metrics or {},
                    "bus_close_error": bus_close_error,
                    "feed_running": False,
                },
            )

    def run(self) -> None:
        """Synchronous wrapper for environments without an existing event loop."""
        try:
            asyncio.run(self.run_async())
        finally:
            if hasattr(self, "_audit") and self._audit is not None:
                try:
                    self._audit.close()
                except Exception:
                    pass
