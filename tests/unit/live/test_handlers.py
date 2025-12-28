"""
Unit tests for message handlers (KlineHandler, BookHandler, etc.)
"""

import pytest

from backtester.data.live.errors import MessageParseError
from backtester.data.live.handlers import (
    BookHandler,
    FundingHandler,
    KlineHandler,
    TickerHandler,
    _safe_float,
    _safe_int,
)
from backtester.data.live.types import MessageType, RoutedMessage
from backtester.types.types import (
    FundingRateEvent,
    LiveCandle,
    OrderBookSnapshot,
    TickerEvent,
)


class TestSafeConversions:
    """Tests for safe type conversion helpers."""

    def test_safe_float_from_string(self) -> None:
        """Test float conversion from string."""
        assert _safe_float("123.456", "test") == 123.456

    def test_safe_float_from_int(self) -> None:
        """Test float conversion from int."""
        assert _safe_float(123, "test") == 123.0

    def test_safe_float_from_float(self) -> None:
        """Test float passthrough."""
        f = 123.456
        assert _safe_float(f, "test") == f

    def test_safe_float_invalid(self) -> None:
        """Test invalid float raises error."""
        with pytest.raises(MessageParseError):
            _safe_float("not_a_number", "test")

    def test_safe_int_from_string(self) -> None:
        """Test int conversion from string."""
        assert _safe_int("123", "test") == 123

    def test_safe_int_from_int(self) -> None:
        """Test int passthrough."""
        assert _safe_int(123, "test") == 123

    def test_safe_int_invalid(self) -> None:
        """Test invalid int raises error."""
        with pytest.raises(MessageParseError):
            _safe_int("not_a_number", "test")


class TestKlineHandler:
    """Tests for KlineHandler."""

    @pytest.fixture
    def sample_kline_data(self) -> dict:
        """Sample Binance kline message data."""
        return {
            "e": "kline",
            "E": 1672515782136,
            "s": "BTCUSDT",
            "k": {
                "t": 1672515780000,
                "T": 1672515839999,
                "s": "BTCUSDT",
                "i": "1m",
                "o": "16800.00",
                "c": "16850.50",
                "h": "16860.00",
                "l": "16790.00",
                "v": "100.5",
                "n": 100,
                "x": True,
                "q": "1690000.00",
            },
        }

    @pytest.mark.asyncio
    async def test_parse_final_candle(self, sample_kline_data: dict) -> None:
        """Test parsing a final (closed) candle."""
        received: list[LiveCandle] = []

        async def on_event(candle: LiveCandle) -> None:
            received.append(candle)

        handler = KlineHandler(on_event=on_event, emit_partial=False)

        msg = RoutedMessage(
            message_type=MessageType.KLINE,
            stream="btcusdt@kline_1m",
            data=sample_kline_data,
            recv_ts=1672515782200,
        )

        await handler.handle(msg)

        assert len(received) == 1
        candle = received[0]
        assert candle.symbol == "BTCUSDT"
        assert candle.timeframe == "1m"
        assert candle.open == 16800.00
        assert candle.close == 16850.50
        assert candle.high == 16860.00
        assert candle.low == 16790.00
        assert candle.volume == 100.5
        assert candle.trades == 100
        assert candle.is_final is True
        assert candle.ts_recv == 1672515782200

    @pytest.mark.asyncio
    async def test_skip_partial_candle(self, sample_kline_data: dict) -> None:
        """Test that partial candles are skipped when emit_partial=False."""
        received: list[LiveCandle] = []

        async def on_event(candle: LiveCandle) -> None:
            received.append(candle)

        handler = KlineHandler(on_event=on_event, emit_partial=False)

        # Set candle as not final
        sample_kline_data["k"]["x"] = False

        msg = RoutedMessage(
            message_type=MessageType.KLINE,
            stream="btcusdt@kline_1m",
            data=sample_kline_data,
            recv_ts=1672515782200,
        )

        await handler.handle(msg)

        assert len(received) == 0
        assert handler.stats.messages_skipped == 1

    @pytest.mark.asyncio
    async def test_emit_partial_candle(self, sample_kline_data: dict) -> None:
        """Test emitting partial candles when emit_partial=True."""
        received: list[LiveCandle] = []

        async def on_event(candle: LiveCandle) -> None:
            received.append(candle)

        handler = KlineHandler(on_event=on_event, emit_partial=True)

        sample_kline_data["k"]["x"] = False

        msg = RoutedMessage(
            message_type=MessageType.KLINE,
            stream="btcusdt@kline_1m",
            data=sample_kline_data,
            recv_ts=1672515782200,
        )

        await handler.handle(msg)

        assert len(received) == 1
        assert received[0].is_final is False

    @pytest.mark.asyncio
    async def test_convert_to_standard_candle(self, sample_kline_data: dict) -> None:
        """Test conversion to standard Candle type."""
        received: list[LiveCandle] = []

        async def on_event(candle: LiveCandle) -> None:
            received.append(candle)

        handler = KlineHandler(on_event=on_event)

        msg = RoutedMessage(
            message_type=MessageType.KLINE,
            stream="btcusdt@kline_1m",
            data=sample_kline_data,
            recv_ts=1672515782200,
        )

        await handler.handle(msg)

        standard = received[0].to_candle()
        assert standard.symbol == "BTCUSDT"
        assert standard.open == 16800.00
        assert standard.close == 16850.50


class TestBookHandler:
    """Tests for BookHandler."""

    @pytest.fixture
    def sample_depth_data(self) -> dict:
        """Sample Binance depth snapshot data."""
        return {
            "lastUpdateId": 160,
            "bids": [
                ["16800.00", "1.5"],
                ["16799.50", "2.0"],
                ["16799.00", "3.0"],
            ],
            "asks": [
                ["16800.50", "1.0"],
                ["16801.00", "2.5"],
                ["16801.50", "1.5"],
            ],
        }

    @pytest.mark.asyncio
    async def test_parse_depth_snapshot(self, sample_depth_data: dict) -> None:
        """Test parsing depth snapshot."""
        received: list[OrderBookSnapshot] = []

        async def on_event(book: OrderBookSnapshot) -> None:
            received.append(book)

        handler = BookHandler(on_event=on_event)

        msg = RoutedMessage(
            message_type=MessageType.DEPTH,
            stream="btcusdt@depth5@100ms",
            data=sample_depth_data,
            recv_ts=1672515782200,
        )

        await handler.handle(msg)

        assert len(received) == 1
        book = received[0]
        assert book.symbol == "BTCUSDT"
        assert len(book.bids) == 3
        assert len(book.asks) == 3
        assert book.bids[0].price == 16800.00
        assert book.bids[0].qty == 1.5
        assert book.asks[0].price == 16800.50
        assert book.last_update_id == 160

    @pytest.mark.asyncio
    async def test_orderbook_properties(self, sample_depth_data: dict) -> None:
        """Test OrderBookSnapshot computed properties."""
        received: list[OrderBookSnapshot] = []

        async def on_event(book: OrderBookSnapshot) -> None:
            received.append(book)

        handler = BookHandler(on_event=on_event)

        msg = RoutedMessage(
            message_type=MessageType.DEPTH,
            stream="btcusdt@depth5@100ms",
            data=sample_depth_data,
            recv_ts=1672515782200,
        )

        await handler.handle(msg)

        book = received[0]
        assert book.best_bid == 16800.00
        assert book.best_ask == 16800.50
        assert book.spread == 0.50
        assert book.mid_price == 16800.25

        # Spread in bps
        spread_bps = book.spread_bps
        assert spread_bps is not None
        # 0.50 / 16800.25 * 10000 ≈ 0.2976 bps
        assert abs(spread_bps - 0.2976) < 0.01

    @pytest.mark.asyncio
    async def test_parse_depth_update_format(self) -> None:
        """Test parsing depth update format (with 'b' and 'a' fields)."""
        received: list[OrderBookSnapshot] = []

        async def on_event(book: OrderBookSnapshot) -> None:
            received.append(book)

        handler = BookHandler(on_event=on_event)

        # Depth update format
        data = {
            "e": "depthUpdate",
            "E": 1672515782136,
            "s": "BTCUSDT",
            "U": 157,
            "u": 160,
            "b": [["16800.00", "1.5"]],
            "a": [["16800.50", "1.0"]],
        }

        msg = RoutedMessage(
            message_type=MessageType.DEPTH,
            stream="btcusdt@depth",
            data=data,
            recv_ts=1672515782200,
        )

        await handler.handle(msg)

        assert len(received) == 1
        assert received[0].symbol == "BTCUSDT"


class TestFundingHandler:
    """Tests for FundingHandler."""

    @pytest.fixture
    def sample_funding_data(self) -> dict:
        """Sample Binance mark price data."""
        return {
            "e": "markPriceUpdate",
            "E": 1672515782136,
            "s": "BTCUSDT",
            "p": "16850.50000000",
            "i": "16848.26000000",
            "P": "16855.00000000",
            "r": "0.00010000",
            "T": 1672531200000,
        }

    @pytest.mark.asyncio
    async def test_parse_funding_event(self, sample_funding_data: dict) -> None:
        """Test parsing funding rate event."""
        received: list[FundingRateEvent] = []

        async def on_event(event: FundingRateEvent) -> None:
            received.append(event)

        handler = FundingHandler(on_event=on_event)

        msg = RoutedMessage(
            message_type=MessageType.MARK_PRICE,
            stream="btcusdt@markPrice",
            data=sample_funding_data,
            recv_ts=1672515782200,
        )

        await handler.handle(msg)

        assert len(received) == 1
        event = received[0]
        assert event.symbol == "BTCUSDT"
        assert event.funding_rate == 0.00010000
        assert event.mark_price == 16850.50000000
        assert event.index_price == 16848.26000000
        assert event.next_funding_time == 1672531200000

    @pytest.mark.asyncio
    async def test_annualized_rate(self, sample_funding_data: dict) -> None:
        """Test annualized rate calculation."""
        received: list[FundingRateEvent] = []

        async def on_event(event: FundingRateEvent) -> None:
            received.append(event)

        handler = FundingHandler(on_event=on_event)

        msg = RoutedMessage(
            message_type=MessageType.MARK_PRICE,
            stream="btcusdt@markPrice",
            data=sample_funding_data,
            recv_ts=1672515782200,
        )

        await handler.handle(msg)

        event = received[0]
        # 0.0001 * (365 * 24 / 8) = 0.0001 * 1095 = 0.1095 = 10.95%
        annualized = event.annualized_rate()
        assert abs(annualized - 0.1095) < 0.0001


class TestTickerHandler:
    """Tests for TickerHandler."""

    @pytest.fixture
    def sample_ticker_data(self) -> dict:
        """Sample Binance 24hr ticker data."""
        return {
            "e": "24hrTicker",
            "E": 1672515782136,
            "s": "BTCUSDT",
            "p": "50.50",
            "P": "0.30",
            "w": "16825.00",
            "c": "16850.50",
            "Q": "0.5",
            "o": "16800.00",
            "h": "16900.00",
            "l": "16700.00",
            "v": "10000.5",
            "q": "168000000.00",
            "O": 1672429380000,
            "C": 1672515780000,
        }

    @pytest.mark.asyncio
    async def test_parse_ticker_event(self, sample_ticker_data: dict) -> None:
        """Test parsing ticker event."""
        received: list[TickerEvent] = []

        async def on_event(event: TickerEvent) -> None:
            received.append(event)

        handler = TickerHandler(on_event=on_event)

        msg = RoutedMessage(
            message_type=MessageType.TICKER,
            stream="btcusdt@ticker",
            data=sample_ticker_data,
            recv_ts=1672515782200,
        )

        await handler.handle(msg)

        assert len(received) == 1
        event = received[0]
        assert event.symbol == "BTCUSDT"
        assert event.last_price == 16850.50
        assert event.price_change == 50.50
        assert event.price_change_percent == 0.30
        assert event.volume == 10000.5


class TestHandlerStats:
    """Tests for handler statistics tracking."""

    @pytest.mark.asyncio
    async def test_stats_tracking(self) -> None:
        """Test that handlers track statistics correctly."""
        received: list[LiveCandle] = []

        async def on_event(candle: LiveCandle) -> None:
            received.append(candle)

        handler = KlineHandler(on_event=on_event)

        # Create valid message
        data = {
            "k": {
                "t": 1672515780000,
                "T": 1672515839999,
                "s": "BTCUSDT",
                "i": "1m",
                "o": "100",
                "c": "101",
                "h": "102",
                "l": "99",
                "v": "10",
                "n": 5,
                "x": True,
                "q": "1000",
            }
        }

        msg = RoutedMessage(
            message_type=MessageType.KLINE,
            stream="btcusdt@kline_1m",
            data=data,
            recv_ts=1672515782200,
        )

        await handler.handle(msg)
        await handler.handle(msg)

        assert handler.stats.messages_received == 2
        assert handler.stats.messages_processed == 2
        assert handler.stats.by_symbol.get("BTCUSDT") == 2

    @pytest.mark.asyncio
    async def test_reset_stats(self) -> None:
        """Test resetting handler statistics."""

        async def on_event(candle: LiveCandle) -> None:
            pass

        handler = KlineHandler(on_event=on_event)
        handler._stats.messages_received = 100
        handler._stats.messages_processed = 80

        handler.reset_stats()

        assert handler.stats.messages_received == 0
        assert handler.stats.messages_processed == 0
