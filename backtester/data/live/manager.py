"""
Live Data Feed Manager - Top-level orchestration.

Coordinates all live feed components:
- ConnectionManager for WebSocket lifecycle
- MessageRouter for message classification
- Handlers for message parsing
- HealthMonitor for health tracking
- EventPublisher for Bus integration
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

from backtester.core.bus import Bus
from backtester.data.live.config import LiveFeedConfig
from backtester.data.live.connection import ConnectionManager
from backtester.data.live.errors import ConfigurationError, LiveFeedError
from backtester.data.live.handlers import (
    BookHandler,
    FundingHandler,
    KlineHandler,
    TickerHandler,
)
from backtester.data.live.health import HealthMonitor
from backtester.data.live.router import MessageRouter
from backtester.data.live.types import (
    ConnectionState,
    FeedSystemHealth,
    ManagerState,
    MessageType,
)
from backtester.types.topics import (
    T_CANDLES,
    T_FUNDING,
    T_LIVE_HEALTH,
    T_LOG,
    T_ORDERBOOK,
    T_TICKER,
)
from backtester.types.types import (
    FundingRateEvent,
    LiveCandle,
    LogEvent,
    OrderBookSnapshot,
    TickerEvent,
)

logger = logging.getLogger(__name__)


class LiveDataFeedManager:
    """
    Top-level orchestration for live market data feeds.

    Manages the complete lifecycle of live data streaming:
    1. Configuration validation
    2. Component initialization
    3. Connection establishment
    4. Message routing and handling
    5. Event publishing to Bus
    6. Health monitoring
    7. Graceful shutdown

    State Machine:
        [STOPPED] --start()--> [STARTING] --success--> [RUNNING]
                                    |                       |
                                [FAILED]              [STOPPING] --> [STOPPED]

    Usage:
        config = LiveFeedConfig(
            venue=Venue.BINANCE_SPOT,
            symbols=["BTCUSDT", "ETHUSDT"],
        )
        manager = LiveDataFeedManager(config, bus)

        await manager.start()
        # ... system running ...
        await manager.stop()
    """

    def __init__(
        self,
        config: LiveFeedConfig,
        bus: Bus,
        name: str = "live_feed",
    ) -> None:
        """
        Initialize the live data feed manager.

        Args:
            config: Live feed configuration
            bus: Event bus for publishing events
            name: Name for logging purposes
        """
        self._config = config
        self._bus = bus
        self._name = name

        # State
        self._state = ManagerState.STOPPED
        self._started_at: Optional[datetime] = None

        # Components (initialized in _setup_components)
        self._connection: Optional[ConnectionManager] = None
        self._router: Optional[MessageRouter] = None
        self._health_monitor: Optional[HealthMonitor] = None

        # Handlers
        self._kline_handler: Optional[KlineHandler] = None
        self._book_handler: Optional[BookHandler] = None
        self._funding_handler: Optional[FundingHandler] = None
        self._ticker_handler: Optional[TickerHandler] = None

        # Shutdown coordination
        self._shutdown_event = asyncio.Event()

        # Statistics
        self._messages_published = 0
        self._errors_count = 0

    # --- Bus-based logging for critical events ---

    async def _emit_log(
        self,
        level: str,
        msg: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Emit a LogEvent to the Bus for critical infrastructure events.

        Use for events that warrant persistence in audit trail:
        - Lifecycle events (start, stop)
        - Connection state changes
        - Health status changes
        - Errors affecting trading

        Do NOT use for high-frequency operational logs (use logger instead).
        """
        ts_utc = int(time.time() * 1000)
        log_event = LogEvent(
            level=level,
            component=self._name,
            msg=msg,
            payload=payload or {},
            sim_time=ts_utc,
        )
        await self._bus.publish(topic=T_LOG, ts_utc=ts_utc, payload=log_event)

    @property
    def state(self) -> ManagerState:
        """Current manager state."""
        return self._state

    @property
    def is_running(self) -> bool:
        """Check if manager is running."""
        return self._state == ManagerState.RUNNING

    @property
    def config(self) -> LiveFeedConfig:
        """Get configuration."""
        return self._config

    async def start(self) -> None:
        """
        Start the live data feed system.

        Raises:
            ConfigurationError: If configuration is invalid
            LiveFeedError: If startup fails
        """
        if self._state not in (ManagerState.STOPPED, ManagerState.FAILED):
            logger.warning(f"[{self._name}] Cannot start from state: {self._state}")
            return

        logger.info(f"[{self._name}] Starting live data feed...")
        self._state = ManagerState.STARTING
        self._shutdown_event.clear()

        try:
            # Validate configuration
            self._validate_config()

            # Setup components
            self._setup_components()

            # Register feeds with health monitor
            self._register_feeds()

            # Start health monitor
            if self._health_monitor:
                await self._health_monitor.start()
                self._health_monitor.set_manager_state(ManagerState.STARTING)

            # Connect to WebSocket
            if self._connection:
                await self._connection.connect()

            # Success!
            self._state = ManagerState.RUNNING
            self._started_at = datetime.now(timezone.utc)

            if self._health_monitor:
                self._health_monitor.set_manager_state(ManagerState.RUNNING)

            logger.info(f"[{self._name}] Live data feed started successfully")

            # Bridge start event to audit trail
            await self._emit_log(
                "INFO",
                "Live feed started",
                {
                    "event": "FEED_STARTED",
                    "venue": self._config.venue.value,
                    "symbols": list(self._config.get_all_symbols()),
                    "streams": [s.stream_type.value for s in self._config.get_all_streams()],
                },
            )

        except Exception as e:
            self._state = ManagerState.FAILED
            if self._health_monitor:
                self._health_monitor.set_manager_state(ManagerState.FAILED)
            logger.error(f"[{self._name}] Failed to start: {e}")

            # Bridge failure to audit trail (best effort - may fail if bus not ready)
            try:
                await self._emit_log(
                    "ERROR",
                    "Live feed failed to start",
                    {
                        "event": "FEED_START_FAILED",
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
            except Exception:
                pass  # Don't mask the original error

            await self._cleanup()
            raise LiveFeedError(
                f"Failed to start live feed: {e}",
                component="LiveDataFeedManager",
            ) from e

    async def stop(self) -> None:
        """Stop the live data feed system gracefully."""
        if self._state in (ManagerState.STOPPED, ManagerState.STOPPING):
            return

        logger.info(f"[{self._name}] Stopping live data feed...")
        self._state = ManagerState.STOPPING
        self._shutdown_event.set()

        # Bridge stop event to audit trail (before cleanup)
        try:
            await self._emit_log(
                "INFO",
                "Live feed stopping",
                {
                    "event": "FEED_STOPPING",
                    "messages_published": self._messages_published,
                    "errors_count": self._errors_count,
                },
            )
        except Exception:
            pass  # Don't fail stop due to logging

        if self._health_monitor:
            self._health_monitor.set_manager_state(ManagerState.STOPPING)

        await self._cleanup()

        self._state = ManagerState.STOPPED
        logger.info(f"[{self._name}] Live data feed stopped")

    async def _cleanup(self) -> None:
        """Clean up all resources."""
        # Stop connection
        if self._connection:
            try:
                await self._connection.close()
            except Exception as e:
                logger.warning(f"[{self._name}] Error closing connection: {e}")

        # Stop health monitor
        if self._health_monitor:
            try:
                await self._health_monitor.stop()
            except Exception as e:
                logger.warning(f"[{self._name}] Error stopping health monitor: {e}")

    def _validate_config(self) -> None:
        """Validate the configuration."""
        streams = self._config.get_all_streams()
        if not streams:
            raise ConfigurationError(
                "No streams configured",
                field="streams",
            )

        symbols = self._config.get_all_symbols()
        if not symbols:
            raise ConfigurationError(
                "No symbols configured",
                field="symbols",
            )

        logger.debug(
            f"[{self._name}] Configuration validated: "
            f"{len(symbols)} symbols, {len(streams)} streams"
        )

    def _setup_components(self) -> None:
        """Initialize all components."""
        # Setup message router
        self._router = MessageRouter()

        # Setup handlers and register with router
        self._setup_handlers()

        # Setup health monitor
        self._health_monitor = HealthMonitor(
            config=self._config.health,
            on_health_change=self._on_health_change,
            on_stale_feed=self._on_stale_feed,
            on_watchdog_trigger=self._on_watchdog_trigger,
        )

        # Setup connection manager
        ws_url = self._config.get_ws_url()
        self._connection = ConnectionManager(
            url=ws_url,
            config=self._config.connection,
            on_message=self._on_message,
            on_state_change=self._on_connection_state_change,
            on_error=self._on_connection_error,
            name=f"{self._name}_ws",
        )

        # Register connection health provider
        self._health_monitor.register_connection_provider(self._connection.get_health)

        logger.debug(f"[{self._name}] Components initialized")

    def _setup_handlers(self) -> None:
        """Setup message handlers."""
        if not self._router:
            return

        # Kline handler
        self._kline_handler = KlineHandler(
            on_event=self._on_candle,
            emit_partial=self._config.emit_partial_candles,
        )
        self._router.register_handler(MessageType.KLINE, self._kline_handler.handle)

        # Order book handler
        self._book_handler = BookHandler(on_event=self._on_orderbook)
        self._router.register_handler(MessageType.DEPTH, self._book_handler.handle)

        # Funding rate handler
        self._funding_handler = FundingHandler(on_event=self._on_funding)
        self._router.register_handler(MessageType.MARK_PRICE, self._funding_handler.handle)

        # Ticker handler
        self._ticker_handler = TickerHandler(on_event=self._on_ticker)
        self._router.register_handler(MessageType.TICKER, self._ticker_handler.handle)

        logger.debug(f"[{self._name}] Handlers registered")

    def _register_feeds(self) -> None:
        """Register all feeds with health monitor."""
        if not self._health_monitor:
            return

        for stream in self._config.get_all_streams():
            self._health_monitor.register_feed(
                symbol=stream.symbol,
                stream_type=stream.stream_type.value,
            )

    # --- Message callbacks ---

    async def _on_message(self, data: dict[str, Any], recv_ts: int) -> None:
        """Handle incoming WebSocket message."""
        if self._router:
            await self._router.route(data, recv_ts)

    async def _on_candle(self, candle: LiveCandle) -> None:
        """Handle parsed candle event."""
        # Record with health monitor
        if self._health_monitor:
            latency_ms = time.time() * 1000 - candle.ts_recv
            self._health_monitor.record_message(
                symbol=candle.symbol,
                stream_type="kline",
                latency_ms=latency_ms,
            )

        # Publish to bus
        try:
            # Convert to standard Candle for compatibility
            standard_candle = candle.to_candle()
            await self._bus.publish(T_CANDLES, candle.ts_recv, standard_candle)
            self._messages_published += 1
        except Exception as e:
            logger.error(f"[{self._name}] Failed to publish candle: {e}")
            self._errors_count += 1

    async def _on_orderbook(self, orderbook: OrderBookSnapshot) -> None:
        """Handle parsed orderbook event."""
        if self._health_monitor:
            latency_ms = time.time() * 1000 - orderbook.ts_recv
            self._health_monitor.record_message(
                symbol=orderbook.symbol,
                stream_type="depth",
                latency_ms=latency_ms,
            )

        try:
            await self._bus.publish(T_ORDERBOOK, orderbook.ts_recv, orderbook)
            self._messages_published += 1
        except Exception as e:
            logger.error(f"[{self._name}] Failed to publish orderbook: {e}")
            self._errors_count += 1

    async def _on_funding(self, funding: FundingRateEvent) -> None:
        """Handle parsed funding rate event."""
        if self._health_monitor:
            latency_ms = time.time() * 1000 - funding.ts_recv
            self._health_monitor.record_message(
                symbol=funding.symbol,
                stream_type="markPrice",
                latency_ms=latency_ms,
            )

        try:
            await self._bus.publish(T_FUNDING, funding.ts_recv, funding)
            self._messages_published += 1
        except Exception as e:
            logger.error(f"[{self._name}] Failed to publish funding: {e}")
            self._errors_count += 1

    async def _on_ticker(self, ticker: TickerEvent) -> None:
        """Handle parsed ticker event."""
        if self._health_monitor:
            latency_ms = time.time() * 1000 - ticker.ts_recv
            self._health_monitor.record_message(
                symbol=ticker.symbol,
                stream_type="ticker",
                latency_ms=latency_ms,
            )

        try:
            await self._bus.publish(T_TICKER, ticker.ts_recv, ticker)
            self._messages_published += 1
        except Exception as e:
            logger.error(f"[{self._name}] Failed to publish ticker: {e}")
            self._errors_count += 1

    # --- Connection callbacks ---

    async def _on_connection_state_change(self, state: ConnectionState) -> None:
        """Handle connection state changes."""
        logger.info(f"[{self._name}] Connection state: {state.value}")

        # Bridge connection state changes to audit trail
        if state == ConnectionState.CONNECTED:
            await self._emit_log(
                "INFO",
                "WebSocket connected",
                {"event": "CONNECTION_ESTABLISHED", "state": state.value},
            )
        elif state == ConnectionState.RECONNECTING:
            logger.warning(f"[{self._name}] Connection lost, reconnecting...")
            await self._emit_log(
                "WARNING",
                "WebSocket connection lost, reconnecting",
                {"event": "CONNECTION_RECONNECTING", "state": state.value},
            )
        elif state == ConnectionState.DISCONNECTED:
            await self._emit_log(
                "WARNING",
                "WebSocket disconnected",
                {"event": "CONNECTION_DISCONNECTED", "state": state.value},
            )

    async def _on_connection_error(self, error: Exception) -> None:
        """Handle connection errors."""
        logger.error(f"[{self._name}] Connection error: {error}")
        self._errors_count += 1

        # Bridge connection errors to audit trail
        await self._emit_log(
            "ERROR",
            "Connection error",
            {
                "event": "CONNECTION_ERROR",
                "error": str(error),
                "error_type": type(error).__name__,
            },
        )

    # --- Health callbacks ---

    async def _on_health_change(self, health: FeedSystemHealth) -> None:
        """Handle health status changes."""
        try:
            ts_utc = int(time.time() * 1000)
            await self._bus.publish(T_LIVE_HEALTH, ts_utc, health)
        except Exception as e:
            logger.warning(f"[{self._name}] Failed to publish health: {e}")

    async def _on_stale_feed(self, symbol: str, stream_type: str) -> None:
        """Handle stale feed detection."""
        logger.warning(f"[{self._name}] Feed stale: {symbol}:{stream_type}")

        # Bridge stale feed to audit trail
        await self._emit_log(
            "WARNING",
            "Feed became stale",
            {
                "event": "FEED_STALE",
                "symbol": symbol,
                "stream_type": stream_type,
            },
        )

    async def _on_watchdog_trigger(self) -> None:
        """Handle watchdog timeout."""
        logger.error(f"[{self._name}] Watchdog triggered, attempting recovery...")

        # Bridge watchdog trigger to audit trail
        await self._emit_log(
            "ERROR",
            "Watchdog triggered - no messages received",
            {"event": "WATCHDOG_TRIGGERED"},
        )

        # Attempt to reconnect
        if self._connection:
            try:
                await self._connection.close()
                await self._connection.connect()

                await self._emit_log(
                    "INFO",
                    "Watchdog recovery successful",
                    {"event": "WATCHDOG_RECOVERY_SUCCESS"},
                )
            except Exception as e:
                logger.error(f"[{self._name}] Recovery failed: {e}")
                self._state = ManagerState.FAILED
                if self._health_monitor:
                    self._health_monitor.set_manager_state(ManagerState.FAILED)

                await self._emit_log(
                    "ERROR",
                    "Watchdog recovery failed",
                    {
                        "event": "WATCHDOG_RECOVERY_FAILED",
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )

    # --- Public methods ---

    def get_health(self) -> FeedSystemHealth:
        """Get current system health."""
        if self._health_monitor:
            return self._health_monitor.get_health()

        return FeedSystemHealth(
            state=self._state,
            connections=[],
            feeds=[],
        )

    def get_stats(self) -> dict[str, Any]:
        """Get statistics summary."""
        stats: dict[str, Any] = {
            "state": self._state.value,
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "messages_published": self._messages_published,
            "errors_count": self._errors_count,
        }

        if self._router:
            stats["router"] = {
                "total_messages": self._router.stats.total_messages,
                "routed_messages": self._router.stats.routed_messages,
                "dropped_messages": self._router.stats.dropped_messages,
                "by_type": self._router.stats.by_type,
            }

        if self._kline_handler:
            stats["kline_handler"] = {
                "processed": self._kline_handler.stats.messages_processed,
                "skipped": self._kline_handler.stats.messages_skipped,
                "errors": self._kline_handler.stats.parse_errors,
            }

        if self._book_handler:
            stats["book_handler"] = {
                "processed": self._book_handler.stats.messages_processed,
                "errors": self._book_handler.stats.parse_errors,
            }

        return stats
