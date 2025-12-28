"""
Unit tests for live data types.
"""

import pytest

from backtester.types.types import (
    FundingRateEvent,
    LiveCandle,
    OrderBookLevel,
    OrderBookSnapshot,
    TickerEvent,
)


class TestOrderBookLevel:
    """Tests for OrderBookLevel."""

    def test_creation(self) -> None:
        """Test creating an order book level."""
        level = OrderBookLevel(price=16800.00, qty=1.5)
        assert level.price == 16800.00
        assert level.qty == 1.5

    def test_immutable(self) -> None:
        """Test that OrderBookLevel is immutable (frozen)."""
        level = OrderBookLevel(price=16800.00, qty=1.5)
        with pytest.raises(AttributeError):
            level.price = 17000.00  # type: ignore


class TestOrderBookSnapshot:
    """Tests for OrderBookSnapshot."""

    @pytest.fixture
    def sample_book(self) -> OrderBookSnapshot:
        """Create sample order book."""
        return OrderBookSnapshot(
            symbol="BTCUSDT",
            ts_exchange=1672515782136,
            ts_recv=1672515782200,
            bids=(
                OrderBookLevel(16800.00, 1.5),
                OrderBookLevel(16799.50, 2.0),
            ),
            asks=(
                OrderBookLevel(16800.50, 1.0),
                OrderBookLevel(16801.00, 2.5),
            ),
            last_update_id=160,
        )

    def test_best_bid(self, sample_book: OrderBookSnapshot) -> None:
        """Test best bid property."""
        assert sample_book.best_bid == 16800.00

    def test_best_ask(self, sample_book: OrderBookSnapshot) -> None:
        """Test best ask property."""
        assert sample_book.best_ask == 16800.50

    def test_mid_price(self, sample_book: OrderBookSnapshot) -> None:
        """Test mid price calculation."""
        assert sample_book.mid_price == 16800.25

    def test_spread(self, sample_book: OrderBookSnapshot) -> None:
        """Test spread calculation."""
        assert sample_book.spread == 0.50

    def test_spread_bps(self, sample_book: OrderBookSnapshot) -> None:
        """Test spread in basis points."""
        spread_bps = sample_book.spread_bps
        assert spread_bps is not None
        # 0.50 / 16800.25 * 10000 ≈ 0.2976 bps
        assert abs(spread_bps - 0.2976) < 0.01

    def test_imbalance(self, sample_book: OrderBookSnapshot) -> None:
        """Test order book imbalance."""
        imbalance = sample_book.imbalance()
        assert imbalance is not None
        # bid_qty = 3.5, ask_qty = 3.5 -> imbalance = 0
        assert abs(imbalance - 0.0) < 0.001

    def test_empty_book_properties(self) -> None:
        """Test properties with empty book."""
        book = OrderBookSnapshot(
            symbol="BTCUSDT",
            ts_exchange=1672515782136,
            ts_recv=1672515782200,
            bids=(),
            asks=(),
            last_update_id=0,
        )

        assert book.best_bid is None
        assert book.best_ask is None
        assert book.mid_price is None
        assert book.spread is None
        assert book.spread_bps is None
        assert book.imbalance() is None


class TestFundingRateEvent:
    """Tests for FundingRateEvent."""

    @pytest.fixture
    def sample_funding(self) -> FundingRateEvent:
        """Create sample funding event."""
        return FundingRateEvent(
            symbol="BTCUSDT",
            funding_rate=0.0001,
            mark_price=16850.50,
            index_price=16848.26,
            next_funding_time=1672531200000,
            ts_exchange=1672515782136,
            ts_recv=1672515782200,
        )

    def test_annualized_rate_default(self, sample_funding: FundingRateEvent) -> None:
        """Test annualized rate with default 8-hour interval."""
        # 0.0001 * (365 * 24 / 8) = 0.0001 * 1095 = 0.1095
        rate = sample_funding.annualized_rate()
        assert abs(rate - 0.1095) < 0.0001

    def test_annualized_rate_custom_interval(self, sample_funding: FundingRateEvent) -> None:
        """Test annualized rate with custom interval."""
        # 0.0001 * (365 * 24 / 4) = 0.0001 * 2190 = 0.219
        rate = sample_funding.annualized_rate(funding_interval_hours=4)
        assert abs(rate - 0.219) < 0.0001


class TestLiveCandle:
    """Tests for LiveCandle."""

    @pytest.fixture
    def sample_candle(self) -> LiveCandle:
        """Create sample live candle."""
        return LiveCandle(
            symbol="BTCUSDT",
            timeframe="1m",
            start_ms=1672515780000,
            end_ms=1672515839999,
            open=16800.00,
            high=16860.00,
            low=16790.00,
            close=16850.50,
            volume=100.5,
            quote_volume=1690000.00,
            trades=100,
            is_final=True,
            ts_recv=1672515840100,
        )

    def test_to_candle_conversion(self, sample_candle: LiveCandle) -> None:
        """Test conversion to standard Candle."""
        candle = sample_candle.to_candle()

        assert candle.symbol == "BTCUSDT"
        assert candle.timeframe == "1m"
        assert candle.start_ms == 1672515780000
        assert candle.end_ms == 1672515839999
        assert candle.open == 16800.00
        assert candle.high == 16860.00
        assert candle.low == 16790.00
        assert candle.close == 16850.50
        assert candle.volume == 100.5
        assert candle.trades == 100
        assert candle.is_final is True

    def test_immutable(self, sample_candle: LiveCandle) -> None:
        """Test that LiveCandle is immutable."""
        with pytest.raises(AttributeError):
            sample_candle.symbol = "ETHUSDT"  # type: ignore


class TestTickerEvent:
    """Tests for TickerEvent."""

    def test_creation(self) -> None:
        """Test creating a ticker event."""
        ticker = TickerEvent(
            symbol="BTCUSDT",
            price_change=50.50,
            price_change_percent=0.30,
            weighted_avg_price=16825.00,
            last_price=16850.50,
            last_qty=0.5,
            open_price=16800.00,
            high_price=16900.00,
            low_price=16700.00,
            volume=10000.5,
            quote_volume=168000000.00,
            open_time=1672429380000,
            close_time=1672515780000,
            ts_recv=1672515782200,
        )

        assert ticker.symbol == "BTCUSDT"
        assert ticker.last_price == 16850.50
        assert ticker.price_change_percent == 0.30
