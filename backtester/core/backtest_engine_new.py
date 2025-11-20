from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from backtester.config.configs import BacktestConfig, BusConfig, RunContext, SubscriptionConfig
from backtester.core.audit import AuditWriter
from backtester.core.bus import Bus, Subscription, TopicPriority
from backtester.core.clock import Clock
from backtester.core.topics import (
    T_ACCOUNT_SNAPSHOT,
    T_CANDLES,
    T_CONTROL,
    T_FILLS,
    T_ORDERS_ACK,
    T_ORDERS_CANCELED,
    T_ORDERS_INTENT,
    T_ORDERS_SANITIZED,
    T_TRADE_EVENT,
)
from backtester.strategy.sma_extended import SMAExtendedStrategy
from backtester.types.types import (
    Candle,
    ControlEvent,
    Fill,
    LotClosedEvent,
    OrderAck,
    OrderIntent,
    PortfolioSnapshot,
    ValidatedOrderIntent,
)

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
"""


class BacktestService(ABC):
    """
    A base class for any component that connects to the bus.
    """

    def __init__(self, name: str, topics: set[str], buffer_size: int = 2048):
        self.name = name
        self.topics = topics
        self.buffer_size = buffer_size

    async def connect(self, bus: Bus) -> Subscription:
        """Register subscription with the bus."""
        cfg = SubscriptionConfig(topics=self.topics, buffer_size=self.buffer_size)
        return await bus.subscribe(name=self.name, bus_sub_config=cfg)

    @abstractmethod
    async def run(self, bus: Bus, sub: Subscription) -> None:
        """The main event loop for this service."""
        pass


# --- Concrete Services ---


class StrategyRunner(BacktestService):
    def __init__(self, clock: Clock, audit: AuditWriter):
        super().__init__(name="strategy", topics={T_CANDLES, T_CONTROL, T_FILLS}, buffer_size=2048)
        self._clock = clock
        self._audit = audit
        self._strategy: Optional[SMAExtendedStrategy] = None

    async def run(self, bus: Bus, sub: Subscription) -> None:
        pass


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
        # self._feed = feed

        # Will be created later
        self._bus: Bus | None = None
        self._audit: AuditWriter | None = None
        self._services: list[BacktestService] = []
        self._state = "created"

        self._initialize()

    def _initialize(self) -> None:
        self._audit = AuditWriter(self._run_ctx, self._cfg.audit_cfg)
        bus_cfg = BusConfig()
        self._bus = Bus(cfg=bus_cfg, audit=self._audit)
        self._bus.set_audit(self._audit)
        self._services = [StrategyRunner(self._clock, self._audit)]

    # --- BUS SECTION --- (Topic Management)

    async def register_topics(self) -> None:
        if self._bus is None:
            raise RuntimeError("Bus not initialized. Call _initialize() first.")
        topics = [
            (T_CANDLES, Candle, TopicPriority.HIGH, False),
            (T_CONTROL, ControlEvent, TopicPriority.HIGH, False),
            (T_ORDERS_INTENT, OrderIntent, TopicPriority.HIGH, False),
            (T_ORDERS_ACK, OrderAck, TopicPriority.HIGH, False),
            (T_ORDERS_SANITIZED, ValidatedOrderIntent, TopicPriority.HIGH, False),
            (T_ORDERS_CANCELED, OrderIntent, TopicPriority.HIGH, False),
            (T_FILLS, Fill, TopicPriority.HIGH, False),
            (T_TRADE_EVENT, LotClosedEvent, TopicPriority.HIGH, False),
            (T_ACCOUNT_SNAPSHOT, PortfolioSnapshot, TopicPriority.HIGH, False),
        ]
        for name, schema, priority, validation in topics:
            await self._bus.register_topic(
                name=name, schema=schema, priority=priority, validate_schema=validation
            )

    # --- Collect reports ---

    async def _collect_report(self) -> None:
        """Ask domain components for their final state"""

    # --- Shutdown ---

    async def _shutdown(self) -> None:
        pass

    # --- Running Section ---

    async def run_async(self) -> None:
        """Orchestrate lifecycle: init - loop - collect - shutdown"""
        pass

    def run(self) -> None:
        """sync wrapper"""
