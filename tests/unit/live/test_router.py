"""
Unit tests for the MessageRouter.
"""

import pytest

from backtester.data.live.router import MessageRouter
from backtester.data.live.types import MessageType, RoutedMessage


class TestMessageRouter:
    """Tests for MessageRouter."""

    @pytest.fixture
    def router(self) -> MessageRouter:
        """Create a fresh router for each test."""
        return MessageRouter()

    @pytest.mark.asyncio
    async def test_route_combined_stream_kline(self, router: MessageRouter) -> None:
        """Test routing combined stream kline message."""
        received: list[RoutedMessage] = []

        async def handler(msg: RoutedMessage) -> None:
            received.append(msg)

        router.register_handler(MessageType.KLINE, handler)

        # Combined stream format
        data = {
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "s": "BTCUSDT",
                "k": {"t": 1234567890000, "o": "100.00", "c": "101.00"},
            },
        }

        await router.route(data, recv_ts=1234567890123)

        assert len(received) == 1
        assert received[0].message_type == MessageType.KLINE
        assert received[0].stream == "btcusdt@kline_1m"
        assert received[0].symbol == "BTCUSDT"

    @pytest.mark.asyncio
    async def test_route_combined_stream_depth(self, router: MessageRouter) -> None:
        """Test routing combined stream depth message."""
        received: list[RoutedMessage] = []

        async def handler(msg: RoutedMessage) -> None:
            received.append(msg)

        router.register_handler(MessageType.DEPTH, handler)

        data = {
            "stream": "btcusdt@depth5@100ms",
            "data": {
                "lastUpdateId": 160,
                "bids": [["16800.00", "1.5"]],
                "asks": [["16800.50", "1.0"]],
            },
        }

        await router.route(data, recv_ts=1234567890123)

        assert len(received) == 1
        assert received[0].message_type == MessageType.DEPTH

    @pytest.mark.asyncio
    async def test_route_direct_stream_format(self, router: MessageRouter) -> None:
        """Test routing direct stream format (no combined wrapper)."""
        received: list[RoutedMessage] = []

        async def handler(msg: RoutedMessage) -> None:
            received.append(msg)

        router.register_handler(MessageType.KLINE, handler)

        # Direct stream format
        data = {
            "e": "kline",
            "s": "BTCUSDT",
            "k": {"t": 1234567890000},
        }

        await router.route(data, recv_ts=1234567890123)

        assert len(received) == 1
        assert received[0].message_type == MessageType.KLINE

    @pytest.mark.asyncio
    async def test_multiple_handlers_same_type(self, router: MessageRouter) -> None:
        """Test multiple handlers for same message type."""
        calls: list[str] = []

        async def handler1(msg: RoutedMessage) -> None:
            calls.append("handler1")

        async def handler2(msg: RoutedMessage) -> None:
            calls.append("handler2")

        router.register_handler(MessageType.KLINE, handler1)
        router.register_handler(MessageType.KLINE, handler2)

        data = {"stream": "btcusdt@kline_1m", "data": {"e": "kline"}}
        await router.route(data, recv_ts=1234567890123)

        assert calls == ["handler1", "handler2"]

    @pytest.mark.asyncio
    async def test_catch_all_handler(self, router: MessageRouter) -> None:
        """Test catch-all handler receives all messages."""
        all_messages: list[RoutedMessage] = []

        async def catch_all(msg: RoutedMessage) -> None:
            all_messages.append(msg)

        router.register_catch_all(catch_all)

        data1 = {"stream": "btcusdt@kline_1m", "data": {"e": "kline"}}
        data2 = {"stream": "btcusdt@depth5", "data": {"lastUpdateId": 1}}

        await router.route(data1, recv_ts=1)
        await router.route(data2, recv_ts=2)

        assert len(all_messages) == 2

    @pytest.mark.asyncio
    async def test_unknown_message_type(self, router: MessageRouter) -> None:
        """Test unknown message type is handled gracefully."""
        data = {"unknown_field": "value"}

        # Should not raise
        await router.route(data, recv_ts=1234567890123)

        assert router.stats.total_messages == 1
        assert router.stats.dropped_messages == 1

    @pytest.mark.asyncio
    async def test_stats_tracking(self, router: MessageRouter) -> None:
        """Test statistics tracking."""

        async def handler(msg: RoutedMessage) -> None:
            pass

        router.register_handler(MessageType.KLINE, handler)

        data = {"stream": "btcusdt@kline_1m", "data": {"e": "kline"}}

        await router.route(data, recv_ts=1)
        await router.route(data, recv_ts=2)
        await router.route(data, recv_ts=3)

        assert router.stats.total_messages == 3
        assert router.stats.routed_messages == 3
        assert router.stats.by_type.get("kline") == 3

    @pytest.mark.asyncio
    async def test_handler_error_does_not_stop_routing(self, router: MessageRouter) -> None:
        """Test that handler errors don't prevent other handlers from running."""
        calls: list[str] = []

        async def bad_handler(msg: RoutedMessage) -> None:
            raise ValueError("Handler error")

        async def good_handler(msg: RoutedMessage) -> None:
            calls.append("good")

        router.register_handler(MessageType.KLINE, bad_handler)
        router.register_handler(MessageType.KLINE, good_handler)

        data = {"stream": "btcusdt@kline_1m", "data": {"e": "kline"}}

        # Should not raise
        await router.route(data, recv_ts=1)

        # Good handler should still have been called
        assert "good" in calls

    def test_clear_handlers(self, router: MessageRouter) -> None:
        """Test clearing all handlers."""

        async def handler(msg: RoutedMessage) -> None:
            pass

        router.register_handler(MessageType.KLINE, handler)
        assert router.get_handler_count(MessageType.KLINE) == 1

        router.clear_handlers()
        assert router.get_handler_count(MessageType.KLINE) == 0

    def test_reset_stats(self, router: MessageRouter) -> None:
        """Test resetting statistics."""
        router._stats.total_messages = 100
        router._stats.routed_messages = 80

        router.reset_stats()

        assert router.stats.total_messages == 0
        assert router.stats.routed_messages == 0


class TestMessageTypeDetection:
    """Tests for message type detection from stream names."""

    @pytest.fixture
    def router(self) -> MessageRouter:
        return MessageRouter()

    @pytest.mark.parametrize(
        "stream,expected_type",
        [
            ("btcusdt@kline_1m", MessageType.KLINE),
            ("ethusdt@kline_5m", MessageType.KLINE),
            ("btcusdt@depth5", MessageType.DEPTH),
            ("btcusdt@depth10@100ms", MessageType.DEPTH),
            ("btcusdt@bookTicker", MessageType.BOOK_TICKER),
            ("btcusdt@markPrice", MessageType.MARK_PRICE),
            ("btcusdt@aggTrade", MessageType.AGG_TRADE),
            ("btcusdt@ticker", MessageType.TICKER),
        ],
    )
    def test_stream_type_detection(
        self, router: MessageRouter, stream: str, expected_type: MessageType
    ) -> None:
        """Test stream type detection from stream name."""
        msg = router._classify_message({"stream": stream, "data": {}}, recv_ts=1234567890123)
        assert msg.message_type == expected_type
