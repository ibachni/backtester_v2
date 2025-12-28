"""
Unit tests for LiveDataFeedManager bus-based logging.

Tests the _emit_log pattern for bridging critical infrastructure
events to the Bus for audit trail persistence.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from backtester.data.live.config import LiveFeedConfig, StreamConfig, StreamType, Venue
from backtester.data.live.manager import LiveDataFeedManager
from backtester.data.live.types import ConnectionState, ManagerState
from backtester.types.topics import T_LOG
from backtester.types.types import LogEvent


class TestEmitLog:
    """Test the _emit_log helper method."""

    @pytest.fixture
    def mock_bus(self) -> MagicMock:
        """Create a mock bus that captures publish calls."""
        bus = MagicMock()
        bus.publish = AsyncMock()
        return bus

    @pytest.fixture
    def minimal_config(self) -> LiveFeedConfig:
        """Create a minimal config for testing."""
        return LiveFeedConfig(
            venue=Venue.BINANCE_SPOT,
            symbols=["BTCUSDT"],
            streams=[StreamConfig(symbol="BTCUSDT", stream_type=StreamType.KLINE, interval="1m")],
        )

    @pytest.fixture
    def manager(self, minimal_config: LiveFeedConfig, mock_bus: MagicMock) -> LiveDataFeedManager:
        """Create a manager instance for testing."""
        return LiveDataFeedManager(
            config=minimal_config,
            bus=mock_bus,
            name="test_feed",
        )

    @pytest.mark.asyncio
    async def test_emit_log_publishes_to_bus(
        self,
        manager: LiveDataFeedManager,
        mock_bus: MagicMock,
    ) -> None:
        """Verify _emit_log publishes a LogEvent to T_LOG."""
        await manager._emit_log("INFO", "Test message", {"key": "value"})

        # Verify bus.publish was called
        mock_bus.publish.assert_called_once()

        # Extract call arguments
        call_kwargs = mock_bus.publish.call_args.kwargs
        assert call_kwargs["topic"] == T_LOG

        # Verify payload is a LogEvent
        payload = call_kwargs["payload"]
        assert isinstance(payload, LogEvent)
        assert payload.level == "INFO"
        assert payload.component == "test_feed"
        assert payload.msg == "Test message"
        assert payload.payload == {"key": "value"}

    @pytest.mark.asyncio
    async def test_emit_log_without_payload(
        self,
        manager: LiveDataFeedManager,
        mock_bus: MagicMock,
    ) -> None:
        """Verify _emit_log works without a payload."""
        await manager._emit_log("WARNING", "Warning message")

        call_kwargs = mock_bus.publish.call_args.kwargs
        payload = call_kwargs["payload"]
        assert payload.payload == {}

    @pytest.mark.asyncio
    async def test_emit_log_includes_timestamp(
        self,
        manager: LiveDataFeedManager,
        mock_bus: MagicMock,
    ) -> None:
        """Verify _emit_log includes appropriate timestamps."""
        await manager._emit_log("DEBUG", "Debug message")

        call_kwargs = mock_bus.publish.call_args.kwargs

        # ts_utc should be present and reasonable (within last second)
        ts_utc = call_kwargs["ts_utc"]
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        assert abs(ts_utc - now_ms) < 1000  # Within 1 second

        # sim_time in payload should match
        payload = call_kwargs["payload"]
        assert payload.sim_time == ts_utc


class TestConnectionStateLogging:
    """Test that connection state changes are bridged to bus."""

    @pytest.fixture
    def mock_bus(self) -> MagicMock:
        """Create a mock bus that captures publish calls."""
        bus = MagicMock()
        bus.publish = AsyncMock()
        return bus

    @pytest.fixture
    def minimal_config(self) -> LiveFeedConfig:
        """Create a minimal config for testing."""
        return LiveFeedConfig(
            venue=Venue.BINANCE_SPOT,
            symbols=["BTCUSDT"],
            streams=[StreamConfig(symbol="BTCUSDT", stream_type=StreamType.KLINE, interval="1m")],
        )

    @pytest.fixture
    def manager(self, minimal_config: LiveFeedConfig, mock_bus: MagicMock) -> LiveDataFeedManager:
        """Create a manager instance for testing."""
        return LiveDataFeedManager(
            config=minimal_config,
            bus=mock_bus,
            name="test_feed",
        )

    @pytest.mark.asyncio
    async def test_connection_established_emits_log(
        self,
        manager: LiveDataFeedManager,
        mock_bus: MagicMock,
    ) -> None:
        """Verify connection established event is bridged to bus."""
        await manager._on_connection_state_change(ConnectionState.CONNECTED)

        # Find the log call
        log_calls = [
            call for call in mock_bus.publish.call_args_list if call.kwargs.get("topic") == T_LOG
        ]
        assert len(log_calls) == 1

        payload = log_calls[0].kwargs["payload"]
        assert payload.level == "INFO"
        assert "CONNECTION_ESTABLISHED" in payload.payload.get("event", "")

    @pytest.mark.asyncio
    async def test_connection_reconnecting_emits_warning(
        self,
        manager: LiveDataFeedManager,
        mock_bus: MagicMock,
    ) -> None:
        """Verify reconnecting state emits warning."""
        await manager._on_connection_state_change(ConnectionState.RECONNECTING)

        log_calls = [
            call for call in mock_bus.publish.call_args_list if call.kwargs.get("topic") == T_LOG
        ]
        assert len(log_calls) == 1

        payload = log_calls[0].kwargs["payload"]
        assert payload.level == "WARNING"
        assert "CONNECTION_RECONNECTING" in payload.payload.get("event", "")

    @pytest.mark.asyncio
    async def test_connection_disconnected_emits_warning(
        self,
        manager: LiveDataFeedManager,
        mock_bus: MagicMock,
    ) -> None:
        """Verify disconnected state emits warning."""
        await manager._on_connection_state_change(ConnectionState.DISCONNECTED)

        log_calls = [
            call for call in mock_bus.publish.call_args_list if call.kwargs.get("topic") == T_LOG
        ]
        assert len(log_calls) == 1

        payload = log_calls[0].kwargs["payload"]
        assert payload.level == "WARNING"
        assert "CONNECTION_DISCONNECTED" in payload.payload.get("event", "")


class TestConnectionErrorLogging:
    """Test that connection errors are bridged to bus."""

    @pytest.fixture
    def mock_bus(self) -> MagicMock:
        """Create a mock bus that captures publish calls."""
        bus = MagicMock()
        bus.publish = AsyncMock()
        return bus

    @pytest.fixture
    def minimal_config(self) -> LiveFeedConfig:
        """Create a minimal config for testing."""
        return LiveFeedConfig(
            venue=Venue.BINANCE_SPOT,
            symbols=["BTCUSDT"],
            streams=[StreamConfig(symbol="BTCUSDT", stream_type=StreamType.KLINE, interval="1m")],
        )

    @pytest.fixture
    def manager(self, minimal_config: LiveFeedConfig, mock_bus: MagicMock) -> LiveDataFeedManager:
        """Create a manager instance for testing."""
        return LiveDataFeedManager(
            config=minimal_config,
            bus=mock_bus,
            name="test_feed",
        )

    @pytest.mark.asyncio
    async def test_connection_error_emits_log(
        self,
        manager: LiveDataFeedManager,
        mock_bus: MagicMock,
    ) -> None:
        """Verify connection error is bridged to bus."""
        error = ValueError("Connection refused")
        await manager._on_connection_error(error)

        log_calls = [
            call for call in mock_bus.publish.call_args_list if call.kwargs.get("topic") == T_LOG
        ]
        assert len(log_calls) == 1

        payload = log_calls[0].kwargs["payload"]
        assert payload.level == "ERROR"
        assert "CONNECTION_ERROR" in payload.payload.get("event", "")
        assert payload.payload["error"] == "Connection refused"
        assert payload.payload["error_type"] == "ValueError"


class TestStaleFeedLogging:
    """Test that stale feed events are bridged to bus."""

    @pytest.fixture
    def mock_bus(self) -> MagicMock:
        """Create a mock bus that captures publish calls."""
        bus = MagicMock()
        bus.publish = AsyncMock()
        return bus

    @pytest.fixture
    def minimal_config(self) -> LiveFeedConfig:
        """Create a minimal config for testing."""
        return LiveFeedConfig(
            venue=Venue.BINANCE_SPOT,
            symbols=["BTCUSDT"],
            streams=[StreamConfig(symbol="BTCUSDT", stream_type=StreamType.KLINE, interval="1m")],
        )

    @pytest.fixture
    def manager(self, minimal_config: LiveFeedConfig, mock_bus: MagicMock) -> LiveDataFeedManager:
        """Create a manager instance for testing."""
        return LiveDataFeedManager(
            config=minimal_config,
            bus=mock_bus,
            name="test_feed",
        )

    @pytest.mark.asyncio
    async def test_stale_feed_emits_warning(
        self,
        manager: LiveDataFeedManager,
        mock_bus: MagicMock,
    ) -> None:
        """Verify stale feed is bridged to bus."""
        await manager._on_stale_feed("BTCUSDT", "kline")

        log_calls = [
            call for call in mock_bus.publish.call_args_list if call.kwargs.get("topic") == T_LOG
        ]
        assert len(log_calls) == 1

        payload = log_calls[0].kwargs["payload"]
        assert payload.level == "WARNING"
        assert "FEED_STALE" in payload.payload.get("event", "")
        assert payload.payload["symbol"] == "BTCUSDT"
        assert payload.payload["stream_type"] == "kline"


class TestStopLogging:
    """Test that stop lifecycle is bridged to bus."""

    @pytest.fixture
    def mock_bus(self) -> MagicMock:
        """Create a mock bus that captures publish calls."""
        bus = MagicMock()
        bus.publish = AsyncMock()
        return bus

    @pytest.fixture
    def minimal_config(self) -> LiveFeedConfig:
        """Create a minimal config for testing."""
        return LiveFeedConfig(
            venue=Venue.BINANCE_SPOT,
            symbols=["BTCUSDT"],
            streams=[StreamConfig(symbol="BTCUSDT", stream_type=StreamType.KLINE, interval="1m")],
        )

    @pytest.fixture
    def manager(self, minimal_config: LiveFeedConfig, mock_bus: MagicMock) -> LiveDataFeedManager:
        """Create a manager instance for testing."""
        mgr = LiveDataFeedManager(
            config=minimal_config,
            bus=mock_bus,
            name="test_feed",
        )
        # Simulate running state
        mgr._state = ManagerState.RUNNING
        return mgr

    @pytest.mark.asyncio
    async def test_stop_emits_log(
        self,
        manager: LiveDataFeedManager,
        mock_bus: MagicMock,
    ) -> None:
        """Verify stop lifecycle is bridged to bus."""
        await manager.stop()

        log_calls = [
            call for call in mock_bus.publish.call_args_list if call.kwargs.get("topic") == T_LOG
        ]
        assert len(log_calls) >= 1

        # Find FEED_STOPPING event
        stopping_events = [
            call.kwargs["payload"]
            for call in log_calls
            if call.kwargs["payload"].payload.get("event") == "FEED_STOPPING"
        ]
        assert len(stopping_events) == 1
        assert stopping_events[0].level == "INFO"
