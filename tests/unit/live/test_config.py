"""
Unit tests for live data feed configuration.
"""

import pytest

from backtester.data.live.config import (
    BINANCE_WS_ENDPOINTS,
    ConnectionConfig,
    HealthConfig,
    LiveFeedConfig,
    PublisherConfig,
    StreamConfig,
    StreamType,
    SynchronizerConfig,
    Venue,
)
from backtester.data.live.errors import ConfigurationError


class TestConnectionConfig:
    """Tests for ConnectionConfig."""

    def test_valid_config(self) -> None:
        """Test valid connection configuration."""
        config = ConnectionConfig(
            base_url="wss://example.com",
            connect_timeout_s=30.0,
            max_reconnect_attempts=5,
        )
        assert config.base_url == "wss://example.com"
        assert config.connect_timeout_s == 30.0
        assert config.max_reconnect_attempts == 5

    def test_invalid_timeout(self) -> None:
        """Test that negative timeout raises error."""
        with pytest.raises(ConfigurationError) as exc_info:
            ConnectionConfig(base_url="wss://example.com", connect_timeout_s=-1.0)
        assert "connect_timeout_s must be positive" in str(exc_info.value)

    def test_invalid_reconnect_attempts(self) -> None:
        """Test that negative reconnect attempts raises error."""
        with pytest.raises(ConfigurationError) as exc_info:
            ConnectionConfig(base_url="wss://example.com", max_reconnect_attempts=-1)
        assert "max_reconnect_attempts must be non-negative" in str(exc_info.value)

    def test_invalid_jitter(self) -> None:
        """Test that invalid jitter raises error."""
        with pytest.raises(ConfigurationError) as exc_info:
            ConnectionConfig(base_url="wss://example.com", reconnect_jitter=1.5)
        assert "reconnect_jitter must be between 0 and 1" in str(exc_info.value)


class TestStreamConfig:
    """Tests for StreamConfig."""

    def test_kline_stream_name(self) -> None:
        """Test kline stream name generation."""
        config = StreamConfig(
            symbol="BTCUSDT",
            stream_type=StreamType.KLINE,
            interval="1m",
        )
        assert config.stream_name == "btcusdt@kline_1m"

    def test_depth_stream_name(self) -> None:
        """Test depth stream name generation."""
        config = StreamConfig(
            symbol="ETHUSDT",
            stream_type=StreamType.DEPTH,
            depth_levels=10,
            update_speed="100ms",
        )
        assert config.stream_name == "ethusdt@depth10@100ms"

    def test_depth_stream_defaults(self) -> None:
        """Test depth stream with default values."""
        config = StreamConfig(
            symbol="BTCUSDT",
            stream_type=StreamType.DEPTH,
        )
        assert config.stream_name == "btcusdt@depth5@100ms"

    def test_book_ticker_stream_name(self) -> None:
        """Test bookTicker stream name generation."""
        config = StreamConfig(
            symbol="BTCUSDT",
            stream_type=StreamType.BOOK_TICKER,
        )
        assert config.stream_name == "btcusdt@bookTicker"

    def test_kline_requires_interval(self) -> None:
        """Test that kline stream requires interval."""
        with pytest.raises(ConfigurationError) as exc_info:
            StreamConfig(symbol="BTCUSDT", stream_type=StreamType.KLINE)
        assert "interval required for kline streams" in str(exc_info.value)

    def test_invalid_depth_levels(self) -> None:
        """Test invalid depth levels."""
        with pytest.raises(ConfigurationError) as exc_info:
            StreamConfig(
                symbol="BTCUSDT",
                stream_type=StreamType.DEPTH,
                depth_levels=15,  # Invalid - must be 5, 10, or 20
            )
        assert "depth_levels must be 5, 10, or 20" in str(exc_info.value)


class TestHealthConfig:
    """Tests for HealthConfig."""

    def test_valid_config(self) -> None:
        """Test valid health configuration."""
        config = HealthConfig(
            staleness_threshold_s=60.0,
            health_check_interval_s=10.0,
        )
        assert config.staleness_threshold_s == 60.0

    def test_invalid_staleness_threshold(self) -> None:
        """Test invalid staleness threshold."""
        with pytest.raises(ConfigurationError) as exc_info:
            HealthConfig(staleness_threshold_s=0)
        assert "staleness_threshold_s must be positive" in str(exc_info.value)

    def test_invalid_latency_sample_rate(self) -> None:
        """Test invalid latency sample rate."""
        with pytest.raises(ConfigurationError) as exc_info:
            HealthConfig(latency_sample_rate=0)
        assert "latency_sample_rate must be between 0 and 1" in str(exc_info.value)


class TestPublisherConfig:
    """Tests for PublisherConfig."""

    def test_invalid_queue_size(self) -> None:
        """Test invalid queue size."""
        with pytest.raises(ConfigurationError) as exc_info:
            PublisherConfig(max_queue_size=0)
        assert "max_queue_size must be positive" in str(exc_info.value)


class TestSynchronizerConfig:
    """Tests for SynchronizerConfig."""

    def test_invalid_timeout(self) -> None:
        """Test invalid timeout."""
        with pytest.raises(ConfigurationError) as exc_info:
            SynchronizerConfig(timeout_ms=0)
        assert "timeout_ms must be positive" in str(exc_info.value)


class TestLiveFeedConfig:
    """Tests for LiveFeedConfig."""

    def test_with_symbols(self) -> None:
        """Test configuration with symbols."""
        config = LiveFeedConfig(
            venue=Venue.BINANCE_SPOT,
            symbols=("BTCUSDT", "ETHUSDT"),
        )
        assert config.venue == Venue.BINANCE_SPOT
        assert "BTCUSDT" in config.get_all_symbols()
        assert "ETHUSDT" in config.get_all_symbols()

    def test_with_explicit_streams(self) -> None:
        """Test configuration with explicit streams."""
        config = LiveFeedConfig(
            venue=Venue.BINANCE_FUTURES,
            streams=(
                StreamConfig("BTCUSDT", StreamType.KLINE, interval="5m"),
                StreamConfig("ETHUSDT", StreamType.DEPTH, depth_levels=10),
            ),
        )
        streams = config.get_all_streams()
        assert len(streams) == 2

    def test_get_all_streams_with_symbols(self) -> None:
        """Test stream generation from symbols."""
        config = LiveFeedConfig(
            venue=Venue.BINANCE_SPOT,
            symbols=("BTCUSDT",),
            default_kline_interval="5m",
            subscribe_orderbook=True,
            orderbook_depth=10,
        )
        streams = config.get_all_streams()

        # Should have kline + depth streams
        stream_names = [s.stream_name for s in streams]
        assert "btcusdt@kline_5m" in stream_names
        assert "btcusdt@depth10@100ms" in stream_names

    def test_get_ws_url(self) -> None:
        """Test WebSocket URL generation."""
        config = LiveFeedConfig(
            venue=Venue.BINANCE_SPOT,
            symbols=("BTCUSDT",),
        )
        url = config.get_ws_url()

        assert BINANCE_WS_ENDPOINTS[Venue.BINANCE_SPOT] in url
        assert "btcusdt@kline_1m" in url

    def test_no_streams_or_symbols_raises(self) -> None:
        """Test that no streams or symbols raises error."""
        with pytest.raises(ConfigurationError) as exc_info:
            LiveFeedConfig(venue=Venue.BINANCE_SPOT)
        assert "At least one symbol or stream" in str(exc_info.value)

    def test_futures_venue(self) -> None:
        """Test futures venue configuration."""
        config = LiveFeedConfig(
            venue=Venue.BINANCE_FUTURES,
            symbols=("BTCUSDT",),
            subscribe_funding=True,
        )

        streams = config.get_all_streams()
        stream_names = [s.stream_name for s in streams]

        # Should have funding stream for futures
        assert any("markPrice" in name for name in stream_names)

    def test_testnet_venue(self) -> None:
        """Test testnet venue uses correct endpoint."""
        config = LiveFeedConfig(
            venue=Venue.BINANCE_SPOT_TESTNET,
            symbols=("BTCUSDT",),
        )
        url = config.get_ws_url()
        assert "testnet" in url
