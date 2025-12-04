from __future__ import annotations

import asyncio
import datetime
import time
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import Any, AsyncIterator, Optional

from backtester.audit.audit import AuditWriter
from backtester.audit.monitor import ConsoleMonitor
from backtester.config.configs import (
    AccountConfig,
    AuditConfig,
    BacktestConfig,
    PerformanceConfig,
    RunContext,
    SimConfig,
    StrategyConfig,
    SubscriptionConfig,
    ValidationConfig,
)
from backtester.core.account import Account
from backtester.core.bus import Bus, Subscription, TopicPriority
from backtester.core.clock import Clock, SimClock, parse_timeframe
from backtester.core.ctx import Context, ReadOnlyPortfolio, SymbolSpecs
from backtester.core.performance import PerformanceEngine
from backtester.data.feeder import BarFeed, DataSubscription
from backtester.data.source import ParquetCandleSource
from backtester.errors.errors import EngineError
from backtester.risk.order_validation import OrderValidation
from backtester.sim.sim import ExecutionSimulator
from backtester.strategy.sma_extended import SMAExtendedStrategy
from backtester.types.topics import (
    T_ACCOUNT_SNAPSHOT,
    T_CANDLES,
    T_CONTROL,
    T_FILLS,
    T_LOG,
    T_LOT_EVENT,
    T_ORDERS_ACK,
    T_ORDERS_CANCELED,
    T_ORDERS_INTENT,
    T_ORDERS_REJECTED,
    T_ORDERS_SANITIZED,
    T_REPORT,
    T_TRADE_EVENT,
)
from backtester.types.types import (
    Candle,
    ControlEvent,
    Fill,
    LogEvent,
    LotClosedEvent,
    OrderAck,
    OrderIntent,
    PortfolioSnapshot,
    TradeEvent,
    ValidatedOrderIntent,
)
from backtester.utils.utility import dec

"""
The Core Philosophy: "Tick-and-Drain"
- It must not allow time to advance from t to t+1 until every
consequence of time t has been resolved. The Golden Rule: The Event Queue must be empty (drained)
before the Orchestrator requests the next data point.
- Orchestrator cannot emit next candle until is receives conformation that all consequences of
the current event have settleted
- Barrier only lifted when the global event count is zero and all worker threads are idle
- Orechstrator must enforce logic priority, regardless of queue speed
- Cascade flow: Structure orchstration loop
    - Data phase
    - signal phase
    - risk/order phase
    - execution phase
    - accounting phase
- Requirement: the loop must control the data Feeder, rather than the feeder pushing blindly
- Crash safety: Orchestrator must have a Global exception hookl
    - if any sub queue encounters an unhandled exception, emit of systemkill event
    - Orchestrator must catch, halt the sim and surface the stack trace to the user.
    - Backtest should be binary (success/fail)
- Idempotency / state rest
    - Orchestrator responsible for teardown / setup.
    - Before run starts, the orch sends a reset signal to all queues to clear internal state
    - This ensures that running the backtest twice in the same session yields identical results
    (idempotency)

Control lifecycle:
- Init
- Start
- run
- stop

Control sim time and termination:
- Define what is means for a run to be done:
    - data draned
    - explicit stop / shutdown event published
    - Hard limits: max simulated date, max number of events, max drawdown etc
- Maintain a canonical sim clock

Handle errors:
- Decide what to do: retry / restart / Skip event / instrument / abort gracefully

Own high-level obs
- Consisten logging context
- Expose core metrics:
    - event throughput, lag between data time and sim time, queue sizes, error counts


Shutdown:
- Signal
- Drain
- Collect
"""

"""
Notes:
- Batch feed candles (instead of emitting 500 individual ones (and waiting for the
bus 500 times, is too slow))
- Strategy should emit or list[OrderIntent] rather than single intents
- Define strict waterfall logic,
- The Fix: The AccountingService should be the "Source of Truth."
    - Fast Path: The Risk module should probably have a direct (synchronous)
    read-only reference to the Portfolio object held by Accounting, OR
    - Event path

Dangers:
- Infinite ping pong: Strategy tries to fix rejected orders immediately
    - However, gets rejected again;
- A checks B, B checks A, bot queues are full; Bus sees tasks pending
    - Wait until idle blocks forever (backpressure is set to block.)
FIX: Waterfall flow:
    - Data must flow downhil within a single tick
    - Level 1 (Source): T_CANDLES
    - Level 2 (Decision): T_ORDERS_INTENT
    - Level 3 (Validation): T_ORDERS_SANITIZED / T_REJECT
    - Level 4 (Execution): T_FILLS
    - Level 5 (Settlement): T_ACCOUNT_SNAPSHOT
Strategy must not emit T_ORDER_INTENT in response to a fill during the same function call
It must wait for next T_CANDLE event to calc new logic based on the fill

Filter inputs based on priority


"""


class BacktestService(ABC):
    """
    A base class for any component that connects to the bus.
    Encapsulates both the subscription logic and the while logic.
    From the single BacktesterConfig, the individual congigs are passed to the modules.
    """

    def __init__(self, name: str, topics: set[str], buffer_size: int = 2048):
        self.name = name  # lowercase name of file
        self.topics = topics
        self.buffer_size = buffer_size

    async def connect(self, bus: Bus) -> Subscription:
        """Register subscription with the bus."""
        cfg = SubscriptionConfig(topics=self.topics, buffer_size=self.buffer_size)
        return await bus.subscribe(name=self.name, bus_sub_config=cfg)

    @abstractmethod
    async def run(self, sub: Subscription) -> None:
        """
        The main event loop for this service.
        Should break on none sentinel (final).
        Can act on EOF message:
            eof = ControlEvent(type="eof", source=T_CANDLES, ts_utc=self._clock.now())
            await self._bus.publish(T_CONTROL, self._clock.now(), eof)
        """
        pass

    @abstractmethod
    async def get_metrics(self) -> Any:
        """Collect final metrics asynchronously."""
        pass


# --- Concrete Services ---


class AuditRunner(BacktestService):
    def __init__(
        self, cfg: AuditConfig, bus: Bus, clock: Clock, audit: AuditWriter, buffer_size: int = 2048
    ):
        super().__init__(
            name="audit",
            topics={
                T_ORDERS_INTENT,
                T_ORDERS_SANITIZED,
                T_ORDERS_ACK,
                T_ORDERS_CANCELED,
                T_ORDERS_REJECTED,
                T_FILLS,
                T_TRADE_EVENT,
                T_ACCOUNT_SNAPSHOT,
                T_CONTROL,
                T_LOG,
                T_REPORT,
            },
            buffer_size=buffer_size,
        )
        self._cfg = cfg
        self._bus = bus
        self._clock = clock
        self._audit = audit

    async def run(self, sub: Subscription) -> None:
        if self._audit is None:
            raise EngineError("Audit not given")
        while True:
            async with sub.consume() as env:
                if env is None:
                    break
                try:
                    await self._audit.on_event(env)
                except Exception as e:
                    print(f"AuditRunner Error: {e}")

    async def get_metrics(self) -> dict[str, Any]:
        return self._audit.get_stats()


class StrategyRunner(BacktestService):
    def __init__(
        self,
        cfg: StrategyConfig,
        bus: Bus,
        clock: Clock,
        audit: AuditWriter,
        buffer_size: int = 2048,
    ) -> None:
        super().__init__(
            name="strategy",
            topics={T_CANDLES, T_CONTROL, T_FILLS, T_ORDERS_REJECTED, T_ORDERS_CANCELED},
            buffer_size=buffer_size,
        )
        self._cfg = cfg
        self._bus = bus
        self._clock = clock
        self._audit = audit
        self._ctx: Optional[Context] = None

    async def init_strategy(self, ctx: Context) -> None:
        self._ctx = ctx
        self._strategy = SMAExtendedStrategy(
            ctx, clock=self._clock, params=self._cfg.strategy_params, info=self._cfg.info
        )
        self._strategy.on_start()
        start_strat_event = LogEvent(
            level="INFO",
            component="STRATEGY_RUNNER",
            msg="strategy initialized",
            payload=asdict(self._cfg),
            sim_time=self._clock.now(),
            wall_time=str(time.time()),
        )
        await self._bus.publish(topic=T_LOG, ts_utc=self._clock.now(), payload=start_strat_event)

    async def run(self, sub: Subscription) -> None:
        if self._strategy is None:
            raise EngineError("Strategy not initialized")

        while True:
            async with sub.consume() as env:
                if env is None:
                    break

                if env.topic == T_CANDLES and isinstance(env.payload, Candle):
                    intents = self._strategy.on_candle(candle=env.payload)
                    if intents:
                        await self._bus.publish_batch(
                            topic=T_ORDERS_INTENT, ts_utc=self._clock.now(), payloads=intents
                        )

                elif env.topic == T_FILLS and isinstance(env.payload, Fill):
                    follow_up = self._strategy.on_fill(fill=env.payload)
                    if follow_up:
                        await self._bus.publish_batch(
                            topic=T_ORDERS_INTENT, ts_utc=self._clock.now(), payloads=follow_up
                        )

                elif (
                    env.topic == T_ORDERS_CANCELED or env.topic == T_ORDERS_REJECTED
                ) and isinstance(env.payload, OrderAck):
                    self._strategy.on_reject(ack=env.payload)

                elif env.topic == T_CONTROL and isinstance(env.payload, ControlEvent):
                    if (
                        getattr(env.payload, "type", None) == "eof"
                        and env.payload.source == T_CANDLES
                    ):
                        # Do not break on EOF; keep bus alive until bus.close() sends None.
                        pass

    async def get_metrics(self) -> dict[str, Any]:
        return self._strategy.get_stats()


class ValidationRunner(BacktestService):
    def __init__(self, cfg: ValidationConfig, bus: Bus, clock: Clock):
        super().__init__("validation", {T_ORDERS_INTENT, T_CONTROL}, 2048)
        self._validator = OrderValidation(cfg, bus, clock)
        self._ctx: Optional[Context] = None

    def init_validator(self, ctx: Context) -> None:
        """
        Set the symbol information, based on the relevant symbols from the strategy
        """
        # self._ctx = ctx
        self._validator._get_specs = ctx.specs.require

    async def run(self, sub: Subscription) -> None:
        if self._validator is None:
            raise EngineError("Validator not initialized")

        while True:
            async with sub.consume() as env:
                if env is None:
                    break

                if env and env.topic == T_ORDERS_INTENT and isinstance(env.payload, OrderIntent):
                    await self._validator.validate_order(env.payload)

                elif env.topic == T_CONTROL and isinstance(env.payload, ControlEvent):
                    if (
                        getattr(env.payload, "type", None) == "eof"
                        and env.payload.source == T_CANDLES
                    ):
                        # Do not break on EOF; keep bus alive until bus.close() sends None.
                        pass

    async def get_metrics(self) -> dict[str, Any]:
        return self._validator.get_stats()


class SimRunner(BacktestService):
    def __init__(self, cfg: SimConfig, bus: Bus, clock: Clock, audit: AuditWriter) -> None:
        super().__init__("sim", {T_ORDERS_SANITIZED, T_CANDLES, T_CONTROL}, 2048)
        self._cfg = cfg
        self._bus = bus
        self._clock = clock
        self._audit = audit

    def init_sim(self, ctx: Context) -> None:
        self._sim = ExecutionSimulator(ctx, self._cfg, self._bus, self._clock, self._audit)

    async def run(self, sub: Subscription) -> None:
        if self._sim is None:
            raise EngineError("Sim not initialized")

        while True:
            async with sub.consume() as env:
                if env is None:
                    break

                elif env.topic == T_CANDLES and isinstance(env.payload, Candle):
                    await self._sim.on_candle(symbol=env.payload.symbol, candle=env.payload)

                elif env.topic == T_ORDERS_SANITIZED and isinstance(
                    env.payload, ValidatedOrderIntent
                ):
                    await self._sim.on_order(intent=env.payload)

                elif env.topic == T_CONTROL and isinstance(env.payload, ControlEvent):
                    if (
                        getattr(env.payload, "type", None) == "eof"
                        and env.payload.source == T_CANDLES
                    ):
                        # Do not break on EOF; keep bus alive until bus.close() sends None.
                        pass

    async def get_metrics(self) -> dict[str, Any]:
        return self._sim.get_stats()


class AccountRunner(BacktestService):
    def __init__(self, cfg: AccountConfig, bus: Bus, audit: AuditWriter, clock: Clock):
        super().__init__(
            "account",
            {
                T_FILLS,
                T_CANDLES,
            },
            2048,
        )
        self._cfg = cfg
        self._bus = bus
        self._audit = audit
        self._clock = clock
        self._account = Account(
            self._cfg,
            self._bus,
            self._audit,
            self._clock,
        )

    def init_account(self, ctx: Context) -> None:
        self._account._get_specs = ctx.specs.require

    async def publish_latest_snapshot(self) -> None:
        """
        Called by the engine after each fully-processed bar (tick-and-drain).
        """
        await self._account.publish_latest_snapshot()

    async def run(self, sub: Subscription) -> None:
        if self._account is None:
            raise EngineError("Sim not initialized")

        while True:
            async with sub.consume() as env:
                if env is None:
                    break

                elif env.topic == T_CANDLES and isinstance(env.payload, Candle):
                    self._account.set_mark(
                        symbol=env.payload.symbol,
                        price=dec(env.payload.close),
                        ts=env.payload.end_ms,
                    )

                elif env.topic == T_FILLS and isinstance(env.payload, Fill):
                    await self._account.apply_fill(fill=env.payload)

                elif env.topic == T_CONTROL and isinstance(env.payload, ControlEvent):
                    if (
                        getattr(env.payload, "type", None) == "eof"
                        and env.payload.source == T_CANDLES
                    ):
                        # Do not break on EOF; keep bus alive until bus.close() sends None.
                        pass

    async def get_metrics(self) -> dict[str, Any]:
        return self._account.get_stats()


class PerformanceRunner(BacktestService):
    def __init__(self, cfg: PerformanceConfig):
        super().__init__(
            "performance",
            {T_CANDLES, T_ACCOUNT_SNAPSHOT, T_TRADE_EVENT, T_LOT_EVENT, T_CONTROL},
            2048,
        )
        self._performance = PerformanceEngine(cfg)

    async def run(self, sub: Subscription) -> None:
        last_equity: float | None = None

        while True:
            async with sub.consume() as env:
                if env is None:
                    break

                elif env.topic == T_CANDLES and isinstance(env.payload, Candle):
                    self._performance.on_bar(bar=env.payload)

                elif env.topic == T_ACCOUNT_SNAPSHOT and isinstance(env.payload, PortfolioSnapshot):
                    self._performance.on_snapshot(snap=env.payload)
                    last_equity = float(env.payload.equity)

                elif env.topic == T_TRADE_EVENT and isinstance(env.payload, TradeEvent):
                    self._performance.on_trade(tr=env.payload, account_equity=last_equity)

                elif env.topic == T_LOT_EVENT and isinstance(env.payload, LotClosedEvent):
                    self._performance.on_lot_closed(lot_closed=env.payload)

                elif env.topic == T_CONTROL and isinstance(env.payload, ControlEvent):
                    pass

    async def get_metrics(self) -> dict[str, Any]:
        return self._performance.summary()


class MonitorRunner(BacktestService):
    def __init__(self) -> None:
        super().__init__("monitor", {T_CANDLES, T_ACCOUNT_SNAPSHOT}, 2048)

    def init_console_monitor(self, sim_start_ms: int, timeframe_ms: int) -> None:
        self._monitor = ConsoleMonitor(sim_start_ms, timeframe_ms)

    async def run(self, sub: Subscription) -> None:
        while True:
            async with sub.consume() as env:
                if env is None:
                    break

                elif env.topic == T_CANDLES and isinstance(env.payload, Candle):
                    self._monitor.on_market_event(env.payload)

                elif env.topic == T_ACCOUNT_SNAPSHOT and isinstance(env.payload, PortfolioSnapshot):
                    self._monitor.on_portfolio_update(env.payload)

    async def get_metrics(self) -> dict[str, Any]:
        self._monitor.on_finish()
        return {}


# --- Other ----


class BacktestEngine:
    """Drive the event loop: Feed candles from BarFeed into the bus."""

    def __init__(
        self,
        run_ctx: RunContext,
        backtest_cfg: BacktestConfig,
        clock: Clock,
    ) -> None:
        """
        Only store dependencies, not IO or side effects.
        """
        # Basics
        self._run_ctx = run_ctx
        self._cfg = backtest_cfg
        self._clock = clock

        # Modules
        self._audit = AuditWriter(self._run_ctx, self._cfg.audit_cfg)
        self._bus = Bus(cfg=self._cfg.bus_cfg, clock=self._clock, audit=self._audit)
        self._bus.set_audit(self._audit)
        self._state = "created"

        # Create self._feed
        self.feeder_factory()

        # Specific services with startup
        self._strategy_runner = StrategyRunner(
            cfg=self._cfg.strategy_cfg, bus=self._bus, clock=self._clock, audit=self._audit
        )
        self._validator_runner = ValidationRunner(self._cfg.orderval_cfg, self._bus, self._clock)
        self._account_runner = AccountRunner(
            self._cfg.account_cfg, self._bus, self._audit, self._clock
        )
        self._sim_runner = SimRunner(self._cfg.sim_cfg, self._bus, self._clock, self._audit)
        # Fix: Instantiate here for consistency
        self._performance_runner = PerformanceRunner(self._cfg.performance_cfg)
        self._minitor_runner = MonitorRunner()

        # collect services
        self._services: dict[str, BacktestService] = {
            # Connect audit first!
            "audit": AuditRunner(self._cfg.audit_cfg, self._bus, self._clock, audit=self._audit),
            "strategy": self._strategy_runner,
            "validation": self._validator_runner,
            "sim": self._sim_runner,
            "account": self._account_runner,
            "performance": self._performance_runner,
            "monitor": self._minitor_runner,
        }
        self._advance = self._clock.advance_to if isinstance(self._clock, SimClock) else None

    # --- AUDIT SECTION ---

    async def _emit_log(
        self,
        level: str,
        msg: str,
        sim_time: int,
        payload: Optional[dict[str, Any]] = None,
    ) -> None:
        log_event = LogEvent(
            level=level,
            component="ENGINE",
            msg=msg,
            payload=payload or {},
            sim_time=sim_time,
            wall_time=str(time.time()),
        )
        await self._bus.publish(topic=T_LOG, ts_utc=sim_time, payload=log_event)

    # --- DATA SECTION ---

    def _create_data_subscriptions(self) -> list[DataSubscription]:
        subs = []
        for sub_cfg in self._cfg.strategy_cfg.data_cfg.values():
            source = ParquetCandleSource(
                run_ctx=self._run_ctx, bus=self._bus, clock=self._clock, sub_config=sub_cfg
            )
            subs.append(DataSubscription(sub_config=sub_cfg, source=source))
        return subs

    def feeder_factory(self) -> None:
        # 1. Create feeder
        if not isinstance(self._clock, SimClock):
            raise AttributeError(f"clock needs to be of type SimClock, but is {type(self._clock)}")
        self._feed = BarFeed(feeder_cfg=self._cfg.feed_cfg, clock=self._clock, bus=self._bus)
        # 2. Subscribe using the data_subscriptions
        subs = self._create_data_subscriptions()
        if subs:
            for sub in subs:
                self._feed.subscribe(sub=sub)

    # --- BUS SECTION --- (Topic Management)

    async def startup_logging(self) -> Subscription:
        await self._bus.register_topic(
            T_LOG, schema=LogEvent, priority=TopicPriority.NORMAL, validate_schema=False
        )

        # Bootstrap: Create a temporary channel to capture startup logs
        bootstrap_cfg = SubscriptionConfig(topics={T_LOG}, buffer_size=4096)
        bootstrap_sub = await self._bus.subscribe("BOOTSTRAP_AUDIT", bootstrap_cfg)

        return bootstrap_sub

    async def drain_startup_logging(self, bootstrap_sub: Subscription) -> None:
        try:
            while bootstrap_sub.depth() > 0:
                async with bootstrap_sub.consume() as env:
                    if env:
                        await self._audit.on_event(env)
        finally:
            await self._bus.unsubscribe(bootstrap_sub)

    async def register_topics(self) -> None:
        if self._bus is None:
            raise RuntimeError("Bus not initialized. Call _initialize() first.")

        # Register "normal topics" (calls will emit logs into bootstrap_sub)
        topics = [
            (T_CANDLES, Candle, TopicPriority.NORMAL, False),
            (T_ORDERS_INTENT, OrderIntent, TopicPriority.HIGH, False),
            (T_ORDERS_SANITIZED, ValidatedOrderIntent, TopicPriority.HIGH, False),
            (T_ORDERS_ACK, OrderAck, TopicPriority.HIGH, False),
            (T_ORDERS_CANCELED, OrderAck, TopicPriority.HIGH, False),
            (T_ORDERS_REJECTED, OrderAck, TopicPriority.HIGH, False),
            (T_FILLS, Fill, TopicPriority.HIGH, False),
            (T_TRADE_EVENT, LotClosedEvent, TopicPriority.HIGH, False),
            (T_LOT_EVENT, LotClosedEvent, TopicPriority.HIGH, False),
            (T_ACCOUNT_SNAPSHOT, PortfolioSnapshot, TopicPriority.HIGH, False),
            (T_CONTROL, ControlEvent, TopicPriority.HIGH, False),
            (T_LOG, None, TopicPriority.NORMAL, False),
            (T_REPORT, None, TopicPriority.HIGH, False),
        ]
        for name, schema, priority, validation in topics:
            await self._bus.register_topic(
                name=name, schema=schema, priority=priority, validate_schema=validation
            )

    # --- Data Feed ---

    async def feed_candles(self) -> dict[str, int]:
        """
        Producer & Clock Master.
        Drives the simulation in strict Micro-Batch steps.
        """
        if self._bus is None or self._feed is None:
            raise RuntimeError("Bus not initialized. Call register_topics() first.")
        async_publish = self._bus.publish
        frames = 0
        candles = 0
        t0 = time.perf_counter()
        if self._advance is None:
            raise ValueError(f"Wrong clock of type {type(self._clock)}")

        # The Feed Iterator advances self._clock automatically
        async for t_close, frame in self._feed.iter_frames():
            frames += 1
            t_close = int(t_close)
            self._advance(t_close)

            # 1. Publish Phase: Inject all data for this timestamp
            for symbol, c in frame.items():
                if c is None:
                    continue
                candles += 1
                await async_publish(topic=T_CANDLES, ts_utc=t_close, payload=c)

            # 2. Barrier Phase: Wait for the system to digest this tick
            # This blocks until ALL consumers have finished processing
            # (including any resulting orders, fills, and accounting updates)
            await self._bus.wait_until_idle()

            # 3. Snapshot Phase: after tick-and-drain, publish latest account snapshot
            await self._account_runner.publish_latest_snapshot()

        dt = time.perf_counter() - t0
        return {"frames": frames, "candles": candles, "duration_ms": int(dt * 1000)}

    # --- Collect reports ---

    async def _collect_report(self) -> dict[str, Any]:
        """Ask domain components for their final state"""
        """
        Performance:
        - Net profit
        - sharpe
        - max_drawdown
        - trade_count
        - win_rate (pct of profitable trades)

        Integrity:
        - Bus drops
        - audit drops
        - data gaps (number of discontinuities detected in the feed)
        - monotonicity vialtions

        Infra: (speed & profiling)
        - throughput eps
        - latency avg (avg time to process a tick) (optional)
        - bottleneck (slowest component)
        """
        # 1. Gather raw results
        tasks = {}
        tasks = {name: service.get_metrics() for name, service in self._services.items()}

        # return_exceptions=True prevents one crash from halting the whole report
        raw_results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        # 2. Normalize Results: Separate Data from Errors
        metrics_map: dict[str, dict] = {}
        error_map: dict[str, str] = {}

        for name, result in zip(tasks.keys(), raw_results):
            if isinstance(result, BaseException):
                # Service crashed during get_metrics or earlier
                error_map[name] = f"{type(result).__name__}: {str(result)}"
                metrics_map[name] = {}  # Empty dict allows .get() calls to fail safely later
            elif result is None:
                metrics_map[name] = {}
            else:
                metrics_map[name] = result

        bus_stats = await self._bus.get_summary_stats()
        feeder_stats = self._feed.get_stats()

        def get_metric(service: str, key: str, default: Any = 0) -> Any:
            return metrics_map.get(service, {}).get(key, default)

        # 3. Structure the data:
        # Context, performance, integrity
        report = {
            "meta": {
                "run_id": self._run_ctx.run_id,
                "strategy": self._cfg.strategy_cfg.info.name,
                "strategy_version": self._cfg.strategy_cfg.info.version,
                "params": self._cfg.strategy_cfg.strategy_params,
                "wall_time": time.time() - self._start_time_wall,  # capture this in run()
                "sim_start": self._cfg.strategy_cfg.start_dt,  # Expose these in Feed
                "sim_end": self._cfg.strategy_cfg.end_dt,
                "sim_period_days": self._cfg.strategy_cfg.end_dt - self._cfg.strategy_cfg.start_dt,
                "config_checksum": self._cfg.stable_hash(),
                "service_errors": error_map,
            },
            "integrity": {
                "Bus_drops_total": bus_stats["drops_total"],
                "audit_drops": get_metric("audit", "audit_dropped_events", 0),
                "strategy_signals_generated": get_metric("strategy", "strategy_signals_generated"),
                "strategy_signals_rejected": get_metric("strategy", "strategy_signals_rejected"),
                "strategy_positions_held": get_metric("strategy", "strategy_positions_held"),
                # Validation stats
                "validation_received": get_metric("validation", "validation_received"),
                "validation_accepted": get_metric("validation", "validation_accepted"),
                "validation_rejected": get_metric("validation", "validation_rejected"),
                "validation_rejection_rate": get_metric("validation", "validation_rejection_rate"),
                "validation_rejection_reason": get_metric(
                    "validation", "validation_rejection_reason"
                ),
                # Sim stats
                "sim_orders_received": get_metric("sim", "orders_received"),
                "sim_fills_generated": get_metric("sim", "fills_generated"),
                "sim_volume_traded": get_metric("sim", "volume_traded"),
                "sim_budget_exhaustions": get_metric("sim", "budget_exhaustions"),
                # Account stats
                "account_final_equity": get_metric("account", "final_equity"),
                "account_total_rpnl": get_metric("account", "total_rpnl"),
                "account_peak_equity": get_metric("account", "peak_equity"),
                "account_max_drawdown_pct": get_metric("account", "max_drawdown_pct"),
                "account_total_fees": get_metric("account", "total_fees"),
                "account_total_turnover": get_metric("account", "total_turnover"),
                "account_dust_cleanups": get_metric("account", "dust_cleanups"),
                "account_open_positions": get_metric("account", "open_positions"),
                # Data source gaps!
            },
            "infrastructure": {
                "Bus_total_delivered": bus_stats["total_delivered"],
                "Bus_avg_throughput_eps": bus_stats["avg_throughput_eps"],
                "Bus_slowest_subscriber": bus_stats["slowest_subscriber"],
                "Bus_slowest_subscriber_depth": bus_stats["slowest_subscriber_depth"],
                "Feeder_stats": feeder_stats,
            },
            "performance": {
                "pnl_strategy": get_metric("strategy", "pnl_strategy"),
                **metrics_map.get("performance", {}),
            },
        }

        # 3. Add derived Status Flag
        report["integrity"]["status"] = (
            "FAIL" if report["integrity"]["Bus_drops_total"] > 0 else "PASS"
        )

        return report

    # --- Shutdown ---

    async def _await_pump_and_publish_eof(
        self, pump_task: asyncio.Task[dict[str, int]]
    ) -> dict[str, int]:
        metrics: dict[str, int] = {}
        try:
            metrics = await pump_task
            await self._emit_log(
                level="INFO", msg="FEED_COMPLETED", sim_time=self._clock.now(), payload=metrics
            )
        finally:
            eof = ControlEvent(type="eof", source=T_CANDLES, ts_utc=self._clock.now())
            # Publish eof to T_CONTROL for all subscribers to recognice
            await self._bus.publish(T_CONTROL, self._clock.now(), eof)
            await self._bus.wait_until_idle()
            await self._emit_log(level="INFO", msg="ENGINE_EOF", sim_time=self._clock.now())

        return metrics

    async def _teardown(
        self, audit_task: Optional[asyncio.Task] = None, audit_sub: Optional[Subscription] = None
    ) -> None:
        """
        Graceful shutdown sequence.
        Order is critical to avoid deadlocks.
        """

        print("teardown initiated")

        # Step 1: Emit final lifecycle event (optional, before bus closes)
        try:
            await self._emit_log("INFO", "RUN_STOP", self._clock.now(), {"feed_running": False})
        except Exception as e:
            print(f"Warning: Failed to emit RUN_STOP: {e}")

        # Step 2: Close the Bus (sends None sentinel to all subscribers)
        # This is what unblocks AuditRunner.run() and allows audit_task to finish.
        if self._bus:
            try:
                await self._bus.close()
            except Exception as e:
                print(f"Warning: Bus close failed: {e}")

        # Step 3: Wait for AuditRunner to process the sentinel and exit
        # Now that the Bus sent None, audit_task will finish quickly.
        if audit_task:
            try:
                await asyncio.wait_for(audit_task, timeout=5.0)
            except asyncio.TimeoutError:
                print("Warning: AuditRunner did not finish within 5s")
            except Exception as e:
                print(f"Warning: Audit task failed during teardown: {e}")

        # Step 4: Stop the AuditWriter (disk I/O worker)
        # This enqueues the sentinel for the _io_worker task.
        if self._audit:
            try:
                self._audit.stop()
                # Give the worker a moment to flush
                await asyncio.sleep(0.1)
            except Exception as e:
                print(f"Warning: Audit stop failed: {e}")

        if audit_task and audit_sub:
            try:
                while audit_sub.depth() > 0:
                    async with audit_sub.consume() as env:
                        if env:
                            await self._audit.on_event(env)
                await audit_task
            except Exception as e:
                print(f"Warning: Audit task failed during teardown: {e}")

        # Step 5: Stop Order Validation, Sim, Account, Performance

        print("teardown finished")

    # --- Running Section ---

    async def run_async(self) -> None:
        """
        Orchestrate lifecycle using Structured Concurrency.
        1. Context Entry (Resources UP)
        2. TaskGroup Execution (Services & Feed UP)
        3. Context Exit (Resources DOWN)
        """
        self._start_time_wall: int = int(time.time())
        try:
            # PHASE 1: Startup (Deterministic)
            # We use an AsyncContextManager to handle the "setup/teardown" symmetry automatically
            async with self._engine_lifecycle():
                # PHASE 2: Execution (Concurrent)
                # TaskGroup ensures that if Feed fails, Strategy is cancelled, and vice-versa.
                async with asyncio.TaskGroup() as tg:
                    # A. Spawn Consumers (Strategy, AuditRunner)
                    # They will run until they receive None from the Bus
                    for name, service in self._services.items():
                        sub = await service.connect(self._bus)
                        tg.create_task(service.run(sub), name=f"svc_{name}")

                    # B. Run Producer (Feed)
                    # This is the "Main" task. When this finishes, we initiate shutdown.
                    tg.create_task(self._drive_feed_and_shutdown(), name="driver")

        except ExceptionGroup as eg:
            # Handle crashes in services or feed
            print(f"Run crashed: {eg.exceptions}")
            raise

    def run(self) -> None:
        """sync wrapper"""
        try:
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            # Handle manual stop
            pass

    # --- Helper ---

    async def _drive_feed_and_shutdown(self) -> None:
        """
        1. Start Feed
        2. Wait for Feed to finish (EOF)
        3. Wait for Bus to drain (Processing complete)
        4. Close Bus (Signals consumers to stop)
        """
        await self._feed.start()

        try:
            # 1. Pump Data (This blocks until all candles are emitted)
            metrics = await self.feed_candles()

            # 2. Log Completion
            await self._emit_log("INFO", "FEED_COMPLETED", self._clock.now(), metrics)

            # 3. The "Tick-and-Drain" Final Barrier
            # Ensure the very last event (e.g., last fill) is fully processed by subscribers
            await self._bus.wait_until_idle()

            # 4. Graceful Shutdown Signal
            # Send EOF control event (logic awareness)
            eof = ControlEvent(type="eof", source=T_CANDLES, ts_utc=self._clock.now())
            await self._bus.publish(T_CONTROL, self._clock.now(), eof)

            # Wait for strategies to potentially react to EOF
            await self._bus.wait_until_idle()

            # The system is now static. All trading is done. Services are still alive.
            final_report = await self._collect_report()

            # Publish to T_REPORT, such that it is saved under report.jsonl
            await self._bus.publish(T_REPORT, self._clock.now(), final_report)
            await self._bus.wait_until_idle()

        finally:
            # 5. Hard Stop (Infrastructure awareness)
            # This sends 'None' to all subscribers, causing their loops to break.
            # This allows the TaskGroup to exit cleanly.
            if self._feed:
                await self._feed.stop()

            # Last step: shut down bus and all queues by injected "None"
            if self._bus:
                try:
                    await self._bus.wait_until_idle()
                except Exception:
                    pass
            if self._bus:
                await self._bus.close(drain=True)  # <-- Signals None

    @asynccontextmanager
    async def _engine_lifecycle(self) -> AsyncIterator[None]:
        """
        Handles the strictly ordered setup and teardown of infrastructure.
        Context manager calls __(a)enter__() o entering the block and __(a)exit__() on leaving.
        """

        # --- SETUP ---
        self.stdout_initilization()
        self._audit.start()

        # Capture startup logs
        bootstrap_sub = await self.startup_logging()

        await self.register_topics()
        await self._emit_log(
            "INFO", "RUN_START", self._clock.now(), {"audit_path": str(self._cfg.audit_cfg.log_dir)}
        )
        # Create Context (Read-only adapters for strategy use)
        self._context = Context(
            clock=self._clock,
            portfolio=ReadOnlyPortfolio(
                snapshot_fn=self._account_runner._account.portfolio_view,
                position_qty_fn=self._account_runner._account.position_qty,
                equity_base_fn=self._account_runner._account.equity,
            ),
            specs=SymbolSpecs(self._strategy_runner._cfg.strategy_params.get("symbols", [])),
        )

        await self._strategy_runner.init_strategy(self._context)
        self._validator_runner.init_validator(self._context)
        self._sim_runner.init_sim(self._context)
        self._minitor_runner.init_console_monitor(
            int(self._cfg.strategy_cfg.start_dt.timestamp() * 1000),
            parse_timeframe(self._cfg.strategy_cfg.strategy_params.get("timeframe", None)),
        )

        # Flush bootstrap logs to disk before starting the main loop
        await self.drain_startup_logging(bootstrap_sub)
        total_ticks = int(
            (
                self._cfg.strategy_cfg.end_dt.timestamp() * 1000
                - self._cfg.strategy_cfg.start_dt.timestamp() * 1000
            )
            / parse_timeframe(self._cfg.strategy_cfg.strategy_params.get("timeframe", None))
        )
        self._minitor_runner._monitor.on_start(total_ticks=total_ticks)

        try:
            yield  # <--- Pass control back to run_async
        finally:
            # --- TEARDOWN ---
            # This block runs AFTER the TaskGroup exits (meaning services are dead)

            # 1. Flush any remaining audit events
            if self._audit:
                self._audit.stop()  # Signals worker thread to exit
                # Ideally, wait for the audit writer to actually finish its queue
                # You might need to add a `await self._audit.join()` method to AuditWriter

            print("Teardown complete.")

    # --- Printing Helper ---

    def stdout_initilization(self) -> None:
        """
        Prints an initialization block to stdout.
        """
        # --- Calculations ---
        duration = self._cfg.strategy_cfg.end_dt - self._cfg.strategy_cfg.start_dt
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # --- Formatting Constants ---
        # W defines the width of the label column for perfect alignment
        W = 22

        # --- The Print Block ---
        print("\n" + "=" * 90)
        print(
            f"||  BACKTEST ENGINE v1.0 (MVP)   |   {timestamp} | Run name: {self._cfg.run_name} ||"
        )
        print("=" * 90)

        print("\n[STRATEGY PROFILE]")
        print(f"{'Strategy Name':<{W}}: {self._cfg.strategy_cfg.info.name}")
        print(f"{'Version':<{W}}: {self._cfg.strategy_cfg.info.version}")
        print(f"{'Mode':<{W}}: BACKTEST")

        print("\n[UNIVERSE DATA]")
        msg = "ERROR: Could not be retrieved"
        print(
            f"{'Primary Assets':<{W}}:{self._cfg.strategy_cfg.strategy_params.get('symbols', msg)}"
        )
        next_name = "Data Frequency"
        print(f"{next_name:<{W}}: {self._cfg.strategy_cfg.strategy_params.get('timeframe', msg)}")
        print("\n[TIME HORIZON]")
        print(f"{'Start Date':<{W}}: {self._cfg.strategy_cfg.start_dt.strftime('%Y-%m-%d')}")
        print(f"{'End Date':<{W}}: {self._cfg.strategy_cfg.end_dt.strftime('%Y-%m-%d')}")
        print(f"{'Total Duration':<{W}}: {duration.days} days")

        print("\n[ACCOUNT SETTINGS]")
        print(f"{'Initial Capital':<{W}}: ${self._cfg.account_cfg.starting_cash:,.2f}")

        print("\n[SIM SETTINGS]")
        print(f"{'Slippage':<{W}}: {self._cfg.sim_cfg.slip_model.bps} bps per side")

        print("\n[SYSTEM]")
        print(f"{'Audit Log Path':<{W}}: {self._cfg.audit_cfg.log_dir}")
        print("-" * 90)
        print(">>> ENGINE INITIALIZED. BEGINNING SIMULATION...")
        print("-" * 90 + "\n")
