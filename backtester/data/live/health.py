"""
Health Monitor for live data feeds.

Monitors the health of all live feed components:
- Connection health (connected, reconnecting, errors)
- Feed staleness (no messages for N seconds)
- Metrics aggregation
- Watchdog for automatic recovery
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional

from backtester.data.live.config import HealthConfig
from backtester.data.live.types import (
    ConnectionHealth,
    FeedHealth,
    FeedSystemHealth,
    ManagerState,
)

logger = logging.getLogger(__name__)


@dataclass
class FeedTracker:
    """Internal tracking state for a single feed."""

    symbol: str
    stream_type: str
    last_message_at: Optional[float] = None  # monotonic time
    message_count: int = 0
    latency_samples: list[float] = field(default_factory=list)
    max_samples: int = 100
    last_error: Optional[str] = None

    def record_message(self, latency_ms: Optional[float] = None) -> None:
        """Record a message received for this feed."""
        self.last_message_at = time.monotonic()
        self.message_count += 1
        if latency_ms is not None:
            self.latency_samples.append(latency_ms)
            if len(self.latency_samples) > self.max_samples:
                self.latency_samples.pop(0)

    @property
    def avg_latency_ms(self) -> Optional[float]:
        """Average latency in milliseconds."""
        if not self.latency_samples:
            return None
        return sum(self.latency_samples) / len(self.latency_samples)

    def is_stale(self, threshold_s: float) -> bool:
        """Check if feed is stale."""
        if self.last_message_at is None:
            return False  # Haven't received any messages yet
        return (time.monotonic() - self.last_message_at) > threshold_s

    def staleness_duration_s(self) -> Optional[float]:
        """Get how long the feed has been stale."""
        if self.last_message_at is None:
            return None
        duration = time.monotonic() - self.last_message_at
        return duration if duration > 0 else None


class HealthMonitor:
    """
    Monitors health of the live feed system.

    Responsibilities:
    - Track message timestamps per feed
    - Detect stale feeds
    - Aggregate health from all components
    - Emit health events
    - Implement watchdog for automatic recovery
    """

    def __init__(
        self,
        config: HealthConfig,
        on_health_change: Optional[Callable[[FeedSystemHealth], Awaitable[None]]] = None,
        on_stale_feed: Optional[Callable[[str, str], Awaitable[None]]] = None,
        on_watchdog_trigger: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> None:
        """
        Initialize the health monitor.

        Args:
            config: Health monitoring configuration
            on_health_change: Callback when system health changes
            on_stale_feed: Callback when a feed becomes stale (symbol, stream_type)
            on_watchdog_trigger: Callback when watchdog times out
        """
        self._config = config
        self._on_health_change = on_health_change
        self._on_stale_feed = on_stale_feed
        self._on_watchdog_trigger = on_watchdog_trigger

        # Feed tracking
        self._feeds: dict[str, FeedTracker] = {}  # "SYMBOL:stream_type" -> tracker

        # Connection health providers
        self._connection_health_providers: list[Callable[[], ConnectionHealth]] = []

        # Manager state
        self._manager_state = ManagerState.STOPPED

        # Timestamps
        self._started_at: Optional[datetime] = None
        self._last_health_check_at: Optional[datetime] = None
        self._last_any_message_at: Optional[float] = None

        # Background task
        self._monitor_task: Optional[asyncio.Task[None]] = None
        self._shutdown_event = asyncio.Event()

        # Cached health for efficient access
        self._cached_health: Optional[FeedSystemHealth] = None
        self._health_changed = True

    def _feed_key(self, symbol: str, stream_type: str) -> str:
        """Generate key for feed tracking."""
        return f"{symbol}:{stream_type}"

    def register_feed(self, symbol: str, stream_type: str) -> None:
        """Register a feed for monitoring."""
        key = self._feed_key(symbol, stream_type)
        if key not in self._feeds:
            self._feeds[key] = FeedTracker(symbol=symbol, stream_type=stream_type)
            logger.debug(f"Registered feed for monitoring: {key}")

    def register_connection_provider(self, provider: Callable[[], ConnectionHealth]) -> None:
        """Register a connection health provider."""
        self._connection_health_providers.append(provider)

    def record_message(
        self,
        symbol: str,
        stream_type: str,
        latency_ms: Optional[float] = None,
    ) -> None:
        """Record a message received for a feed."""
        key = self._feed_key(symbol, stream_type)
        tracker = self._feeds.get(key)
        if tracker:
            tracker.record_message(latency_ms)
        else:
            # Auto-register unknown feeds
            self._feeds[key] = FeedTracker(symbol=symbol, stream_type=stream_type)
            self._feeds[key].record_message(latency_ms)

        self._last_any_message_at = time.monotonic()
        self._health_changed = True

    def record_error(self, symbol: str, stream_type: str, error: str) -> None:
        """Record an error for a feed."""
        key = self._feed_key(symbol, stream_type)
        tracker = self._feeds.get(key)
        if tracker:
            tracker.last_error = error
            self._health_changed = True

    def set_manager_state(self, state: ManagerState) -> None:
        """Update the manager state."""
        if self._manager_state != state:
            self._manager_state = state
            self._health_changed = True
            logger.debug(f"Manager state changed to: {state.value}")

    async def start(self) -> None:
        """Start the health monitoring loop."""
        if self._monitor_task is not None:
            logger.warning("Health monitor already running")
            return

        self._shutdown_event.clear()
        self._started_at = datetime.now(timezone.utc)
        self._monitor_task = asyncio.create_task(self._monitor_loop(), name="health_monitor")
        logger.info("Health monitor started")

    async def stop(self) -> None:
        """Stop the health monitoring loop."""
        self._shutdown_event.set()

        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self._monitor_task = None

        logger.info("Health monitor stopped")

    async def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        try:
            while not self._shutdown_event.is_set():
                await self._check_health()
                await asyncio.sleep(self._config.health_check_interval_s)

        except asyncio.CancelledError:
            logger.debug("Health monitor loop cancelled")
            raise

    async def _check_health(self) -> None:
        """Perform a health check."""
        self._last_health_check_at = datetime.now(timezone.utc)

        # Check for stale feeds
        stale_feeds: list[tuple[str, str]] = []
        for key, tracker in self._feeds.items():
            was_stale = tracker.is_stale(self._config.staleness_threshold_s)
            is_now_stale = tracker.is_stale(self._config.staleness_threshold_s)

            if is_now_stale and not was_stale:
                stale_feeds.append((tracker.symbol, tracker.stream_type))
                logger.warning(f"Feed became stale: {key}")

        # Notify about stale feeds
        for symbol, stream_type in stale_feeds:
            if self._on_stale_feed:
                try:
                    await self._on_stale_feed(symbol, stream_type)
                except Exception as e:
                    logger.error(f"Stale feed callback error: {e}")

        # Check watchdog
        await self._check_watchdog()

        # Emit health change if needed
        if self._health_changed and self._on_health_change:
            try:
                health = self.get_health()
                await self._on_health_change(health)
                self._health_changed = False
            except Exception as e:
                logger.error(f"Health change callback error: {e}")

    async def _check_watchdog(self) -> None:
        """Check watchdog timeout."""
        if self._last_any_message_at is None:
            return

        time_since_message = time.monotonic() - self._last_any_message_at
        if time_since_message > self._config.watchdog_timeout_s:
            logger.error(f"Watchdog triggered: no messages for {time_since_message:.1f}s")
            if self._on_watchdog_trigger:
                try:
                    await self._on_watchdog_trigger()
                except Exception as e:
                    logger.error(f"Watchdog callback error: {e}")

    def get_health(self) -> FeedSystemHealth:
        """Get current system health snapshot."""
        # Gather connection health
        connections = [provider() for provider in self._connection_health_providers]

        # Gather feed health
        feeds: list[FeedHealth] = []
        for tracker in self._feeds.values():
            is_stale = tracker.is_stale(self._config.staleness_threshold_s)
            feeds.append(
                FeedHealth(
                    symbol=tracker.symbol,
                    stream_type=tracker.stream_type,
                    last_message_at=(
                        datetime.fromtimestamp(
                            time.time() - (time.monotonic() - tracker.last_message_at),
                            tz=timezone.utc,
                        )
                        if tracker.last_message_at
                        else None
                    ),
                    message_count=tracker.message_count,
                    is_stale=is_stale,
                    staleness_duration_s=tracker.staleness_duration_s() if is_stale else None,
                    avg_latency_ms=tracker.avg_latency_ms,
                    last_error=tracker.last_error,
                )
            )

        # Calculate totals
        total_messages = sum(t.message_count for t in self._feeds.values())
        total_errors = sum(c.error_count for c in connections)

        return FeedSystemHealth(
            state=self._manager_state,
            connections=connections,
            feeds=feeds,
            total_message_count=total_messages,
            total_error_count=total_errors,
            started_at=self._started_at,
            last_health_check_at=self._last_health_check_at,
        )

    def get_feed_health(self, symbol: str, stream_type: str) -> Optional[FeedHealth]:
        """Get health for a specific feed."""
        key = self._feed_key(symbol, stream_type)
        tracker = self._feeds.get(key)
        if not tracker:
            return None

        is_stale = tracker.is_stale(self._config.staleness_threshold_s)
        return FeedHealth(
            symbol=tracker.symbol,
            stream_type=tracker.stream_type,
            last_message_at=(
                datetime.fromtimestamp(
                    time.time() - (time.monotonic() - tracker.last_message_at),
                    tz=timezone.utc,
                )
                if tracker.last_message_at
                else None
            ),
            message_count=tracker.message_count,
            is_stale=is_stale,
            staleness_duration_s=tracker.staleness_duration_s() if is_stale else None,
            avg_latency_ms=tracker.avg_latency_ms,
            last_error=tracker.last_error,
        )

    def reset(self) -> None:
        """Reset all tracking state."""
        self._feeds.clear()
        self._cached_health = None
        self._health_changed = True
        self._last_any_message_at = None
        logger.debug("Health monitor reset")
