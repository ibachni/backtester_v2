"""
Shared types, enums, and data structures for the live data feed module.

This module contains types that are used across multiple components
of the live feed system.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class ManagerState(str, Enum):
    """State machine for LiveDataFeedManager."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"


class ConnectionState(str, Enum):
    """State machine for individual WebSocket connections."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    CLOSING = "closing"
    CLOSED = "closed"


class MessageType(str, Enum):
    """Types of messages received from WebSocket."""

    KLINE = "kline"
    DEPTH = "depth"
    BOOK_TICKER = "bookTicker"
    MARK_PRICE = "markPrice"
    AGG_TRADE = "aggTrade"
    TICKER = "ticker"
    HEARTBEAT = "heartbeat"
    ERROR = "error"
    UNKNOWN = "unknown"


@dataclass
class ConnectionHealth:
    """Health snapshot for a single WebSocket connection."""

    state: ConnectionState
    url: str
    connected_since: Optional[datetime] = None
    last_message_at: Optional[datetime] = None
    reconnect_count: int = 0
    message_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    last_error_at: Optional[datetime] = None
    latency_ms: Optional[float] = None  # Last measured latency

    @property
    def uptime_s(self) -> Optional[float]:
        """Connection uptime in seconds, or None if not connected."""
        if self.connected_since is None:
            return None
        now = datetime.now(timezone.utc)
        return (now - self.connected_since).total_seconds()

    @property
    def is_healthy(self) -> bool:
        """Check if connection is in a healthy state."""
        return self.state == ConnectionState.CONNECTED

    @property
    def seconds_since_message(self) -> Optional[float]:
        """Seconds since last message, or None if no messages yet."""
        if self.last_message_at is None:
            return None
        now = datetime.now(timezone.utc)
        return (now - self.last_message_at).total_seconds()


@dataclass
class FeedHealth:
    """Health status for a single data feed (symbol + stream type)."""

    symbol: str
    stream_type: str
    last_message_at: Optional[datetime] = None
    message_count: int = 0
    is_stale: bool = False
    staleness_duration_s: Optional[float] = None
    avg_latency_ms: Optional[float] = None
    last_error: Optional[str] = None

    @property
    def is_healthy(self) -> bool:
        """Check if feed is healthy (not stale)."""
        return not self.is_stale and self.last_error is None


@dataclass
class FeedSystemHealth:
    """Aggregate health status for the entire feed system."""

    state: ManagerState
    connections: list[ConnectionHealth] = field(default_factory=list)
    feeds: list[FeedHealth] = field(default_factory=list)
    total_message_count: int = 0
    total_error_count: int = 0
    started_at: Optional[datetime] = None
    last_health_check_at: Optional[datetime] = None

    @property
    def is_healthy(self) -> bool:
        """Check if entire system is healthy."""
        if self.state != ManagerState.RUNNING:
            return False
        if not all(c.is_healthy for c in self.connections):
            return False
        if not all(f.is_healthy for f in self.feeds):
            return False
        return True

    @property
    def stale_feeds(self) -> list[str]:
        """Get list of stale feed identifiers."""
        return [f"{f.symbol}:{f.stream_type}" for f in self.feeds if f.is_stale]

    @property
    def unhealthy_connections(self) -> list[str]:
        """Get list of unhealthy connection URLs."""
        return [c.url for c in self.connections if not c.is_healthy]


@dataclass(frozen=True, slots=True)
class RoutedMessage:
    """Parsed message envelope from WebSocket."""

    message_type: MessageType
    stream: str  # e.g., "btcusdt@kline_1m"
    data: dict[str, Any]  # Raw parsed JSON data
    recv_ts: int  # Local receive timestamp (Unix ms)

    @property
    def symbol(self) -> Optional[str]:
        """Extract symbol from stream name."""
        if "@" in self.stream:
            return self.stream.split("@")[0].upper()
        return None


@dataclass
class PublisherStats:
    """Statistics for the event publisher."""

    enqueued: int = 0
    published: int = 0
    dropped: int = 0
    coalesced: int = 0
    queue_depth: int = 0
    max_queue_depth: int = 0
    avg_latency_ms: float = 0.0
    max_latency_ms: float = 0.0


@dataclass
class SyncStats:
    """Synchronization quality metrics."""

    complete_frames: int = 0
    partial_frames: int = 0
    timeout_frames: int = 0
    symbols_expected: int = 0
    avg_symbols_per_partial: float = 0.0


@dataclass
class ConnectionMetrics:
    """Detailed metrics for a WebSocket connection."""

    # Counters
    messages_received: int = 0
    bytes_received: int = 0
    pings_sent: int = 0
    pongs_received: int = 0
    reconnections: int = 0
    errors: int = 0

    # Latency tracking (rolling window)
    latency_samples: list[float] = field(default_factory=list)
    max_latency_samples: int = 100

    # Timing
    connected_at: Optional[float] = None  # monotonic time
    last_message_at: Optional[float] = None  # monotonic time
    last_reconnect_at: Optional[float] = None  # monotonic time

    def record_latency(self, latency_ms: float) -> None:
        """Record a latency sample."""
        self.latency_samples.append(latency_ms)
        if len(self.latency_samples) > self.max_latency_samples:
            self.latency_samples.pop(0)

    @property
    def avg_latency_ms(self) -> Optional[float]:
        """Average latency in milliseconds."""
        if not self.latency_samples:
            return None
        return sum(self.latency_samples) / len(self.latency_samples)

    @property
    def p99_latency_ms(self) -> Optional[float]:
        """99th percentile latency in milliseconds."""
        if not self.latency_samples:
            return None
        sorted_samples = sorted(self.latency_samples)
        idx = int(len(sorted_samples) * 0.99)
        return sorted_samples[min(idx, len(sorted_samples) - 1)]
