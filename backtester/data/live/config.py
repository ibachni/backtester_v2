"""
Configuration types for the live data feed module.

Provides immutable, validated configuration dataclasses for all live feed components.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Literal, Optional

from backtester.data.live.errors import ConfigurationError


class Venue(str, Enum):
    """Supported trading venues."""

    BINANCE_SPOT = "binance_spot"
    BINANCE_FUTURES = "binance_futures"
    BINANCE_SPOT_TESTNET = "binance_spot_testnet"
    BINANCE_FUTURES_TESTNET = "binance_futures_testnet"


class StreamType(str, Enum):
    """Available stream types."""

    KLINE = "kline"
    DEPTH = "depth"
    BOOK_TICKER = "bookTicker"
    MARK_PRICE = "markPrice"
    AGG_TRADE = "aggTrade"
    TICKER = "ticker"


# Binance WebSocket endpoints
BINANCE_WS_ENDPOINTS: dict[Venue, str] = {
    Venue.BINANCE_SPOT: "wss://stream.binance.com:9443",
    Venue.BINANCE_FUTURES: "wss://fstream.binance.com",
    Venue.BINANCE_SPOT_TESTNET: "wss://testnet.binance.vision/ws",
    Venue.BINANCE_FUTURES_TESTNET: "wss://stream.binancefuture.com",
}

# Binance REST endpoints (for snapshots/recovery)
BINANCE_REST_ENDPOINTS: dict[Venue, str] = {
    Venue.BINANCE_SPOT: "https://api.binance.com",
    Venue.BINANCE_FUTURES: "https://fapi.binance.com",
    Venue.BINANCE_SPOT_TESTNET: "https://testnet.binance.vision",
    Venue.BINANCE_FUTURES_TESTNET: "https://testnet.binancefuture.com",
}


@dataclass(frozen=True)
class ConnectionConfig:
    """Configuration for a single WebSocket connection."""

    # Base URL for WebSocket
    base_url: str

    # Connection behavior
    connect_timeout_s: float = 30.0
    ping_interval_s: float = 30.0  # Client-side ping if no message
    ping_timeout_s: float = 10.0
    max_reconnect_attempts: int = 10
    base_reconnect_delay_s: float = 1.0
    max_reconnect_delay_s: float = 60.0
    reconnect_jitter: float = 0.3  # ±30% jitter

    # Binance limits
    max_connection_age_s: float = 23 * 3600  # Refresh at 23 hours (before 24h limit)
    max_streams_per_connection: int = 200  # Binance limit

    def __post_init__(self) -> None:
        if self.connect_timeout_s <= 0:
            raise ConfigurationError(
                "connect_timeout_s must be positive",
                field="connect_timeout_s",
                value=self.connect_timeout_s,
            )
        if self.max_reconnect_attempts < 0:
            raise ConfigurationError(
                "max_reconnect_attempts must be non-negative",
                field="max_reconnect_attempts",
                value=self.max_reconnect_attempts,
            )
        if not (0 <= self.reconnect_jitter <= 1):
            raise ConfigurationError(
                "reconnect_jitter must be between 0 and 1",
                field="reconnect_jitter",
                value=self.reconnect_jitter,
            )


@dataclass(frozen=True)
class StreamConfig:
    """Configuration for a single stream subscription."""

    symbol: str
    stream_type: StreamType
    interval: Optional[str] = None  # For klines: "1m", "5m", etc.
    depth_levels: Optional[int] = None  # For depth: 5, 10, 20
    update_speed: Optional[str] = None  # For depth: "100ms", "1000ms"

    def __post_init__(self) -> None:
        if self.stream_type == StreamType.KLINE and not self.interval:
            raise ConfigurationError(
                "interval required for kline streams",
                field="interval",
            )
        if self.stream_type == StreamType.DEPTH:
            if self.depth_levels not in (5, 10, 20, None):
                raise ConfigurationError(
                    "depth_levels must be 5, 10, or 20",
                    field="depth_levels",
                    value=self.depth_levels,
                )

    @property
    def stream_name(self) -> str:
        """Generate Binance stream name."""
        symbol_lower = self.symbol.lower()

        if self.stream_type == StreamType.KLINE:
            return f"{symbol_lower}@kline_{self.interval}"
        elif self.stream_type == StreamType.DEPTH:
            levels = self.depth_levels or 5
            speed = self.update_speed or "100ms"
            return f"{symbol_lower}@depth{levels}@{speed}"
        elif self.stream_type == StreamType.BOOK_TICKER:
            return f"{symbol_lower}@bookTicker"
        elif self.stream_type == StreamType.MARK_PRICE:
            return f"{symbol_lower}@markPrice"
        elif self.stream_type == StreamType.AGG_TRADE:
            return f"{symbol_lower}@aggTrade"
        elif self.stream_type == StreamType.TICKER:
            return f"{symbol_lower}@ticker"
        else:
            return f"{symbol_lower}@{self.stream_type.value}"


@dataclass(frozen=True)
class HealthConfig:
    """Configuration for health monitoring."""

    # Staleness detection
    staleness_threshold_s: float = 30.0  # Feed is stale if no message for this long
    health_check_interval_s: float = 5.0  # How often to check health

    # Watchdog
    watchdog_timeout_s: float = 60.0  # Kill and restart if stuck

    # Metrics sampling
    latency_sample_rate: float = 0.01  # Sample 1% for latency histograms

    def __post_init__(self) -> None:
        if self.staleness_threshold_s <= 0:
            raise ConfigurationError(
                "staleness_threshold_s must be positive",
                field="staleness_threshold_s",
                value=self.staleness_threshold_s,
            )
        if not (0 < self.latency_sample_rate <= 1):
            raise ConfigurationError(
                "latency_sample_rate must be between 0 and 1",
                field="latency_sample_rate",
                value=self.latency_sample_rate,
            )


@dataclass(frozen=True)
class PublisherConfig:
    """Configuration for event publishing to the Bus."""

    # Backpressure handling
    max_queue_size: int = 10_000
    drop_policy: Literal["oldest", "newest"] = "oldest"

    # Coalescing for high-frequency streams
    coalesce_orderbook: bool = True
    coalesce_window_ms: int = 100  # Coalesce updates within this window

    # Latency tracking
    track_latency: bool = True
    latency_sample_rate: float = 0.01

    def __post_init__(self) -> None:
        if self.max_queue_size <= 0:
            raise ConfigurationError(
                "max_queue_size must be positive",
                field="max_queue_size",
                value=self.max_queue_size,
            )


@dataclass(frozen=True)
class SynchronizerConfig:
    """Configuration for frame synchronization."""

    # Multi-symbol alignment
    enabled: bool = True
    timeout_ms: int = 5000  # Emit partial frame after this timeout
    emit_partial: bool = True  # Whether to emit partial frames on timeout

    def __post_init__(self) -> None:
        if self.timeout_ms <= 0:
            raise ConfigurationError(
                "timeout_ms must be positive",
                field="timeout_ms",
                value=self.timeout_ms,
            )


@dataclass(frozen=True)
class LiveFeedConfig:
    """
    Immutable top-level configuration for the live feed system.

    Example:
        config = LiveFeedConfig(
            venue=Venue.BINANCE_SPOT,
            symbols=["BTCUSDT", "ETHUSDT"],
            streams=[
                StreamConfig("BTCUSDT", StreamType.KLINE, interval="1m"),
                StreamConfig("ETHUSDT", StreamType.KLINE, interval="1m"),
            ],
        )
    """

    # Venue selection
    venue: Venue = Venue.BINANCE_SPOT

    # Symbols to subscribe (convenience for auto-generating streams)
    symbols: tuple[str, ...] = field(default_factory=tuple)

    # Explicit stream configurations (takes precedence)
    streams: tuple[StreamConfig, ...] = field(default_factory=tuple)

    # Default stream types when using symbols shorthand
    default_kline_interval: str = "1m"
    subscribe_orderbook: bool = False
    orderbook_depth: int = 5
    subscribe_funding: bool = False  # Only for futures

    # Component configs
    connection: ConnectionConfig = field(
        default_factory=lambda: ConnectionConfig(base_url=BINANCE_WS_ENDPOINTS[Venue.BINANCE_SPOT])
    )
    health: HealthConfig = field(default_factory=HealthConfig)
    publisher: PublisherConfig = field(default_factory=PublisherConfig)
    synchronizer: SynchronizerConfig = field(default_factory=SynchronizerConfig)

    # Logging
    log_raw_messages: bool = False
    message_log_dir: Path = field(default_factory=lambda: Path("logs/raw_messages"))

    # Runtime behavior
    emit_partial_candles: bool = False  # Emit non-final candles

    def __post_init__(self) -> None:
        # Validate at least one symbol or stream
        if not self.symbols and not self.streams:
            raise ConfigurationError(
                "At least one symbol or stream must be configured",
                field="symbols",
            )

        # Update connection config with correct endpoint if using default
        if self.connection.base_url == BINANCE_WS_ENDPOINTS[Venue.BINANCE_SPOT]:
            # Need to use object.__setattr__ for frozen dataclass
            object.__setattr__(
                self,
                "connection",
                ConnectionConfig(
                    base_url=BINANCE_WS_ENDPOINTS[self.venue],
                    connect_timeout_s=self.connection.connect_timeout_s,
                    ping_interval_s=self.connection.ping_interval_s,
                    ping_timeout_s=self.connection.ping_timeout_s,
                    max_reconnect_attempts=self.connection.max_reconnect_attempts,
                    base_reconnect_delay_s=self.connection.base_reconnect_delay_s,
                    max_reconnect_delay_s=self.connection.max_reconnect_delay_s,
                    reconnect_jitter=self.connection.reconnect_jitter,
                    max_connection_age_s=self.connection.max_connection_age_s,
                    max_streams_per_connection=self.connection.max_streams_per_connection,
                ),
            )

    def get_all_streams(self) -> list[StreamConfig]:
        """
        Get all streams to subscribe to.
        Combines explicit streams with auto-generated streams from symbols.
        """
        streams: list[StreamConfig] = list(self.streams)
        explicit_symbols = {s.symbol for s in self.streams}

        for symbol in self.symbols:
            if symbol not in explicit_symbols:
                # Add default kline stream
                streams.append(
                    StreamConfig(
                        symbol=symbol,
                        stream_type=StreamType.KLINE,
                        interval=self.default_kline_interval,
                    )
                )

                # Add orderbook if configured
                if self.subscribe_orderbook:
                    streams.append(
                        StreamConfig(
                            symbol=symbol,
                            stream_type=StreamType.DEPTH,
                            depth_levels=self.orderbook_depth,
                        )
                    )

                # Add funding rate for futures
                if self.subscribe_funding and self.venue in (
                    Venue.BINANCE_FUTURES,
                    Venue.BINANCE_FUTURES_TESTNET,
                ):
                    streams.append(
                        StreamConfig(
                            symbol=symbol,
                            stream_type=StreamType.MARK_PRICE,
                        )
                    )

        return streams

    def get_ws_url(self) -> str:
        """Build the combined streams WebSocket URL."""
        streams = self.get_all_streams()
        stream_names = [s.stream_name for s in streams]

        # Use combined streams endpoint
        base = self.connection.base_url
        if stream_names:
            combined = "/".join(stream_names)
            return f"{base}/stream?streams={combined}"
        return base

    def get_all_symbols(self) -> set[str]:
        """Get all unique symbols from configuration."""
        symbols = set(self.symbols)
        for stream in self.streams:
            symbols.add(stream.symbol)
        return symbols
