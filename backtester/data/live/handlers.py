"""
Message Handlers for live data feeds.

Handlers parse exchange-specific JSON into normalized internal dataclasses:
- KlineHandler: Candlestick/kline data
- BookHandler: Order book depth snapshots
- FundingHandler: Funding rate events (futures)
- TickerHandler: 24hr ticker statistics

# TODO Analyse whether type checks (isinstance) drags performance too much
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Generic, Optional, TypeVar

from backtester.data.live.errors import HandlerError, MessageParseError
from backtester.data.live.types import RoutedMessage
from backtester.types.types import (
    FundingRateEvent,
    LiveCandle,
    OrderBookLevel,
    OrderBookSnapshot,
    TickerEvent,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class HandlerStats:
    """Statistics for a message handler."""

    messages_received: int = 0
    messages_processed: int = 0
    messages_skipped: int = 0
    parse_errors: int = 0
    by_symbol: dict[str, int] = field(default_factory=dict)


class BaseHandler(ABC, Generic[T]):
    """
    Abstract base class for message handlers.

    Each handler:
    1. Receives RoutedMessage from the router
    2. Parses exchange-specific JSON into internal dataclass
    3. Calls registered callback with normalized data
    """

    def __init__(
        self,
        on_event: Callable[[T], Awaitable[None]],
        name: str = "handler",
    ) -> None:
        """
        Initialize the handler.

        Args:
            on_event: Async callback to receive parsed events
            name: Handler name for logging
        """
        self._on_event = on_event
        self._name = name
        self._stats = HandlerStats()

    @property
    def stats(self) -> HandlerStats:
        """Get handler statistics."""
        return self._stats

    async def handle(self, msg: RoutedMessage) -> None:
        """
        Handle an incoming message.

        Args:
            msg: Routed message from the router
        """
        self._stats.messages_received += 1

        try:
            event = self._parse(msg)
            if event is None:
                self._stats.messages_skipped += 1
                return

            self._stats.messages_processed += 1

            # Track by symbol
            symbol = self._get_symbol(event)
            if symbol:
                self._stats.by_symbol[symbol] = self._stats.by_symbol.get(symbol, 0) + 1

            await self._on_event(event)

        except (MessageParseError, HandlerError) as e:
            self._stats.parse_errors += 1
            logger.warning(f"[{self._name}] Parse error: {e}")
        except Exception as e:
            self._stats.parse_errors += 1
            logger.error(f"[{self._name}] Unexpected error: {e}", exc_info=True)

    @abstractmethod
    def _parse(self, msg: RoutedMessage) -> Optional[T]:
        """Parse the message into an event. Return None to skip."""
        ...

    @abstractmethod
    def _get_symbol(self, event: T) -> Optional[str]:
        """Extract symbol from event for statistics."""
        ...

    def reset_stats(self) -> None:
        """Reset handler statistics."""
        self._stats = HandlerStats()


def _safe_float(value: Any, field_name: str) -> float:
    """Safely convert a value to float."""
    try:
        if isinstance(value, float):
            return value
        return float(value)
    except (ValueError, TypeError) as e:
        raise MessageParseError(
            f"Invalid float value for {field_name}: {value}",
            expected_type="float",
        ) from e


def _safe_int(value: Any, field_name: str) -> int:
    """Safely convert a value to int."""
    try:
        return int(value)
    except (ValueError, TypeError) as e:
        raise MessageParseError(
            f"Invalid integer value for {field_name}: {value}",
            expected_type="int",
        ) from e


class KlineHandler(BaseHandler[LiveCandle]):
    """
    Handler for kline/candlestick messages.

    Binance kline format (in combined stream):
    {
        "e": "kline",
        "E": 1672515782136,  // Event time
        "s": "BTCUSDT",      // Symbol
        "k": {
            "t": 1672515780000,  // Kline start time
            "T": 1672515839999,  // Kline close time
            "s": "BTCUSDT",      // Symbol
            "i": "1m",           // Interval
            "f": 100,            // First trade ID
            "L": 200,            // Last trade ID
            "o": "16800.00",     // Open price
            "c": "16850.50",     // Close price
            "h": "16860.00",     // High price
            "l": "16790.00",     // Low price
            "v": "100.5",        // Base asset volume
            "n": 100,            // Number of trades
            "x": false,          // Is this kline closed?
            "q": "1690000.00",   // Quote asset volume
            "V": "50.25",        // Taker buy base volume
            "Q": "845000.00"     // Taker buy quote volume
        }
    }
    """

    def __init__(
        self,
        on_event: Callable[[LiveCandle], Awaitable[None]],
        emit_partial: bool = False,
    ) -> None:
        """
        Initialize the kline handler.

        Args:
            on_event: Callback for parsed candle events
            emit_partial: Whether to emit non-final candles
        """
        super().__init__(on_event, name="KlineHandler")
        self._emit_partial = emit_partial

    def _parse(self, msg: RoutedMessage) -> Optional[LiveCandle]:
        """Parse kline message into LiveCandle."""
        data = msg.data

        # Get kline data (can be nested under 'k' or flat)
        k = data.get("k", data)

        # Check if candle is final
        is_final = k.get("x", True)
        if not is_final and not self._emit_partial:
            return None

        try:
            return LiveCandle(
                symbol=k.get("s", "").upper(),
                timeframe=k.get("i", ""),
                start_ms=_safe_int(k.get("t"), "start_ms"),
                end_ms=_safe_int(k.get("T"), "end_ms"),
                open=_safe_float(k.get("o"), "open"),
                high=_safe_float(k.get("h"), "high"),
                low=_safe_float(k.get("l"), "low"),
                close=_safe_float(k.get("c"), "close"),
                volume=_safe_float(k.get("v"), "volume"),
                quote_volume=_safe_float(k.get("q", "0"), "quote_volume"),
                trades=_safe_int(k.get("n", 0), "trades"),
                is_final=is_final,
                ts_recv=msg.recv_ts,
            )
        except KeyError as e:
            raise MessageParseError(
                f"Missing required kline field: {e}",
                expected_type="kline",
            ) from e

    def _get_symbol(self, event: LiveCandle) -> Optional[str]:
        return event.symbol


class BookHandler(BaseHandler[OrderBookSnapshot]):
    """
    Handler for order book depth messages.

    Binance partial depth format:
    {
        "lastUpdateId": 160,
        "bids": [["16800.00", "1.5"], ["16799.50", "2.0"]],
        "asks": [["16800.50", "1.0"], ["16801.00", "3.5"]]
    }

    Binance depth update format (stream):
    {
        "e": "depthUpdate",
        "E": 1672515782136,
        "s": "BTCUSDT",
        "U": 157,
        "u": 160,
        "b": [["16800.00", "1.5"]],
        "a": [["16800.50", "1.0"]]
    }
    """

    def __init__(
        self,
        on_event: Callable[[OrderBookSnapshot], Awaitable[None]],
    ) -> None:
        super().__init__(on_event, name="BookHandler")

    def _parse(self, msg: RoutedMessage) -> Optional[OrderBookSnapshot]:
        """Parse depth message into OrderBookSnapshot."""
        data = msg.data

        # Extract symbol from stream or data
        symbol = msg.symbol or data.get("s", "").upper()
        if not symbol:
            # Try to get from stream name
            if "@" in msg.stream:
                symbol = msg.stream.split("@")[0].upper()

        # Handle both snapshot and update formats
        bids_raw = data.get("bids", data.get("b", []))
        asks_raw = data.get("asks", data.get("a", []))

        try:
            bids = tuple(
                OrderBookLevel(
                    price=_safe_float(level[0], "bid_price"),
                    qty=_safe_float(level[1], "bid_qty"),
                )
                for level in bids_raw
                if len(level) >= 2
            )

            asks = tuple(
                OrderBookLevel(
                    price=_safe_float(level[0], "ask_price"),
                    qty=_safe_float(level[1], "ask_qty"),
                )
                for level in asks_raw
                if len(level) >= 2
            )

            # Get update ID (different field names for snapshot vs update)
            update_id = data.get("lastUpdateId", data.get("u", 0))

            # Get exchange timestamp if available
            ts_exchange = data.get("E", msg.recv_ts)

            return OrderBookSnapshot(
                symbol=symbol,
                ts_exchange=ts_exchange,
                ts_recv=msg.recv_ts,
                bids=bids,
                asks=asks,
                last_update_id=_safe_int(update_id, "last_update_id"),
            )

        except (IndexError, KeyError) as e:
            raise MessageParseError(
                f"Invalid depth data structure: {e}",
                expected_type="depth",
            ) from e

    def _get_symbol(self, event: OrderBookSnapshot) -> Optional[str]:
        return event.symbol


class FundingHandler(BaseHandler[FundingRateEvent]):
    """
    Handler for funding rate messages (futures only).

    Binance markPrice stream format:
    {
        "e": "markPriceUpdate",
        "E": 1672515782136,
        "s": "BTCUSDT",
        "p": "16850.50000000",  // Mark price
        "i": "16848.26000000",  // Index price
        "P": "16855.00000000",  // Estimated settle price
        "r": "0.00010000",      // Funding rate
        "T": 1672531200000      // Next funding time
    }
    """

    def __init__(
        self,
        on_event: Callable[[FundingRateEvent], Awaitable[None]],
    ) -> None:
        super().__init__(on_event, name="FundingHandler")

    def _parse(self, msg: RoutedMessage) -> Optional[FundingRateEvent]:
        """Parse mark price message into FundingRateEvent."""
        data = msg.data

        try:
            return FundingRateEvent(
                symbol=data.get("s", "").upper(),
                funding_rate=_safe_float(data.get("r", "0"), "funding_rate"),
                mark_price=_safe_float(data.get("p"), "mark_price"),
                index_price=_safe_float(data.get("i"), "index_price"),
                next_funding_time=_safe_int(data.get("T", 0), "next_funding_time"),
                ts_exchange=_safe_int(data.get("E"), "ts_exchange"),
                ts_recv=msg.recv_ts,
            )

        except KeyError as e:
            raise MessageParseError(
                f"Missing required markPrice field: {e}",
                expected_type="markPrice",
            ) from e

    def _get_symbol(self, event: FundingRateEvent) -> Optional[str]:
        return event.symbol


class TickerHandler(BaseHandler[TickerEvent]):
    """
    Handler for 24hr ticker messages.

    Binance 24hr ticker format:
    {
        "e": "24hrTicker",
        "E": 1672515782136,
        "s": "BTCUSDT",
        "p": "50.50",          // Price change
        "P": "0.30",           // Price change percent
        "w": "16825.00",       // Weighted avg price
        "c": "16850.50",       // Last price
        "Q": "0.5",            // Last qty
        "o": "16800.00",       // Open price
        "h": "16900.00",       // High price
        "l": "16700.00",       // Low price
        "v": "10000.5",        // Volume
        "q": "168000000.00",   // Quote volume
        "O": 1672429380000,    // Stats open time
        "C": 1672515780000,    // Stats close time
        "F": 100,              // First trade ID
        "L": 1000,             // Last trade ID
        "n": 900               // Number of trades
    }
    """

    def __init__(
        self,
        on_event: Callable[[TickerEvent], Awaitable[None]],
    ) -> None:
        super().__init__(on_event, name="TickerHandler")

    def _parse(self, msg: RoutedMessage) -> Optional[TickerEvent]:
        """Parse ticker message into TickerEvent."""
        data = msg.data

        try:
            return TickerEvent(
                symbol=data.get("s", "").upper(),
                price_change=_safe_float(data.get("p", "0"), "price_change"),
                price_change_percent=_safe_float(data.get("P", "0"), "price_change_percent"),
                weighted_avg_price=_safe_float(data.get("w", "0"), "weighted_avg_price"),
                last_price=_safe_float(data.get("c"), "last_price"),
                last_qty=_safe_float(data.get("Q", "0"), "last_qty"),
                open_price=_safe_float(data.get("o"), "open_price"),
                high_price=_safe_float(data.get("h"), "high_price"),
                low_price=_safe_float(data.get("l"), "low_price"),
                volume=_safe_float(data.get("v", "0"), "volume"),
                quote_volume=_safe_float(data.get("q", "0"), "quote_volume"),
                open_time=_safe_int(data.get("O", 0), "open_time"),
                close_time=_safe_int(data.get("C", 0), "close_time"),
                ts_recv=msg.recv_ts,
            )

        except KeyError as e:
            raise MessageParseError(
                f"Missing required ticker field: {e}",
                expected_type="ticker",
            ) from e

    def _get_symbol(self, event: TickerEvent) -> Optional[str]:
        return event.symbol
