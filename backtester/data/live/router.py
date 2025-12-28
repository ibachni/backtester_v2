"""
Message Router for live data feeds.

Routes incoming WebSocket messages to appropriate handlers based on
message type (kline, depth, markPrice, etc.).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from backtester.data.live.errors import MessageParseError
from backtester.data.live.types import MessageType, RoutedMessage

logger = logging.getLogger(__name__)


@dataclass
class RouterStats:
    """Statistics for message routing."""

    total_messages: int = 0
    routed_messages: int = 0
    dropped_messages: int = 0
    parse_errors: int = 0
    by_type: dict[str, int] = field(default_factory=dict)


class MessageRouter:
    """
    Routes incoming WebSocket messages to appropriate handlers.

    The router classifies messages by type and dispatches them to
    registered handlers. Unrecognized message types are logged and dropped.

    Binance combined stream format:
    {
        "stream": "btcusdt@kline_1m",
        "data": { ... kline data ... }
    }

    Individual stream format (direct connection):
    {
        "e": "kline",
        "s": "BTCUSDT",
        ...
    }
    """

    # Map Binance event types to our internal MessageType
    EVENT_TYPE_MAP: dict[str, MessageType] = {
        "kline": MessageType.KLINE,
        "depthUpdate": MessageType.DEPTH,
        "bookTicker": MessageType.BOOK_TICKER,
        "markPriceUpdate": MessageType.MARK_PRICE,
        "aggTrade": MessageType.AGG_TRADE,
        "24hrTicker": MessageType.TICKER,
    }

    def __init__(self) -> None:
        """Initialize the message router."""
        self._handlers: dict[MessageType, list[Callable[[RoutedMessage], Awaitable[None]]]] = {}
        self._catch_all_handler: Optional[Callable[[RoutedMessage], Awaitable[None]]] = None
        self._stats = RouterStats()

    @property
    def stats(self) -> RouterStats:
        """Get routing statistics."""
        return self._stats

    def register_handler(
        self,
        message_type: MessageType,
        handler: Callable[[RoutedMessage], Awaitable[None]],
    ) -> None:
        """
        Register a handler for a specific message type.

        Multiple handlers can be registered for the same type.
        They will be called in registration order.
        """
        if message_type not in self._handlers:
            self._handlers[message_type] = []
        self._handlers[message_type].append(handler)
        logger.debug(f"Registered handler for {message_type.value}")

    def register_catch_all(
        self,
        handler: Callable[[RoutedMessage], Awaitable[None]],
    ) -> None:
        """
        Register a catch-all handler for all messages.

        This handler receives all messages regardless of type,
        useful for logging or debugging.
        """
        self._catch_all_handler = handler

    async def route(self, data: dict[str, Any], recv_ts: int) -> None:
        """
        Route a message to appropriate handlers.

        Args:
            data: Parsed JSON message from WebSocket
            recv_ts: Receive timestamp in milliseconds
        """
        self._stats.total_messages += 1

        try:
            routed_msg = self._classify_message(data, recv_ts)
        except MessageParseError as e:
            self._stats.parse_errors += 1
            logger.warning(f"Failed to classify message: {e}")
            return

        # Update type statistics
        type_key = routed_msg.message_type.value
        self._stats.by_type[type_key] = self._stats.by_type.get(type_key, 0) + 1

        # Call catch-all handler if registered
        if self._catch_all_handler:
            try:
                await self._catch_all_handler(routed_msg)
            except Exception as e:
                logger.error(f"Catch-all handler error: {e}")

        # Get handlers for this message type
        handlers = self._handlers.get(routed_msg.message_type, [])

        if not handlers:
            if routed_msg.message_type not in (MessageType.UNKNOWN, MessageType.HEARTBEAT):
                logger.debug(f"No handler for message type: {routed_msg.message_type.value}")
            self._stats.dropped_messages += 1
            return

        # Dispatch to all registered handlers
        self._stats.routed_messages += 1
        for handler in handlers:
            try:
                await handler(routed_msg)
            except Exception as e:
                logger.error(
                    f"Handler error for {routed_msg.message_type.value}: {e}",
                    exc_info=True,
                )

    def _classify_message(self, data: dict[str, Any], recv_ts: int) -> RoutedMessage:
        """
        Classify a message and create a RoutedMessage.

        Handles both combined stream format and direct stream format.
        """
        # Check for combined stream format
        if "stream" in data and "data" in data:
            stream = data["stream"]
            inner_data = data["data"]
            message_type = self._detect_type_from_stream(stream, inner_data)
            return RoutedMessage(
                message_type=message_type,
                stream=stream,
                data=inner_data,
                recv_ts=recv_ts,
            )

        # Direct stream format - detect type from event field
        event_type = data.get("e")
        if event_type:
            message_type = self.EVENT_TYPE_MAP.get(event_type, MessageType.UNKNOWN)
            # Try to construct stream name from symbol
            symbol = data.get("s", "unknown").lower()
            stream = f"{symbol}@{event_type}"
            return RoutedMessage(
                message_type=message_type,
                stream=stream,
                data=data,
                recv_ts=recv_ts,
            )

        # Check for partial depth snapshot (no event field)
        if "lastUpdateId" in data and ("bids" in data or "asks" in data):
            return RoutedMessage(
                message_type=MessageType.DEPTH,
                stream="unknown@depth",
                data=data,
                recv_ts=recv_ts,
            )

        # Unknown format
        logger.debug(f"Unknown message format: {list(data.keys())[:5]}")
        return RoutedMessage(
            message_type=MessageType.UNKNOWN,
            stream="unknown",
            data=data,
            recv_ts=recv_ts,
        )

    def _detect_type_from_stream(self, stream: str, data: dict[str, Any]) -> MessageType:
        """Detect message type from stream name and data."""
        stream_lower = stream.lower()

        # Kline streams: btcusdt@kline_1m
        if "@kline_" in stream_lower:
            return MessageType.KLINE

        # Depth streams: btcusdt@depth5@100ms, btcusdt@depth
        if "@depth" in stream_lower:
            return MessageType.DEPTH

        # Book ticker: btcusdt@bookTicker
        if "@bookticker" in stream_lower:
            return MessageType.BOOK_TICKER

        # Mark price: btcusdt@markPrice
        if "@markprice" in stream_lower:
            return MessageType.MARK_PRICE

        # Aggregate trades: btcusdt@aggTrade
        if "@aggtrade" in stream_lower:
            return MessageType.AGG_TRADE

        # Ticker: btcusdt@ticker
        if "@ticker" in stream_lower:
            return MessageType.TICKER

        # Fall back to event type in data
        event_type = data.get("e")
        if event_type:
            return self.EVENT_TYPE_MAP.get(event_type, MessageType.UNKNOWN)

        return MessageType.UNKNOWN

    def get_handler_count(self, message_type: MessageType) -> int:
        """Get number of registered handlers for a message type."""
        return len(self._handlers.get(message_type, []))

    def clear_handlers(self) -> None:
        """Clear all registered handlers."""
        self._handlers.clear()
        self._catch_all_handler = None

    def reset_stats(self) -> None:
        """Reset routing statistics."""
        self._stats = RouterStats()
