"""
Live Market Data Feed Module.

This module provides real-time market data streaming from Binance via WebSocket,
integrated with the existing Bus architecture for event-driven backtesting and
paper trading.

Components:
- LiveDataFeedManager: Top-level orchestration and lifecycle management
- ConnectionManager: WebSocket lifecycle, reconnection, subscription management
- MessageRouter: Message classification and routing to handlers
- Handlers: KlineHandler, BookHandler, FundingHandler for parsing/normalizing
- HealthMonitor: Staleness detection, metrics, health aggregation
- EventPublisher: Backpressure handling, Bus integration

Usage:
    from backtester.data.live import LiveDataFeedManager, LiveFeedConfig

    config = LiveFeedConfig(
        symbols=["BTCUSDT", "ETHUSDT"],
        streams=["kline_1m", "depth5"],
    )
    manager = LiveDataFeedManager(config, bus)
    await manager.start()
"""

from backtester.data.live.config import LiveFeedConfig
from backtester.data.live.errors import (
    ConnectionError,
    HandlerError,
    LiveFeedError,
    MessageParseError,
    SubscriptionError,
)
from backtester.data.live.manager import LiveDataFeedManager
from backtester.data.live.types import (
    ConnectionHealth,
    ConnectionState,
    FeedHealth,
    FeedSystemHealth,
    ManagerState,
    MessageType,
)

__all__ = [
    # Main entry point
    "LiveDataFeedManager",
    "LiveFeedConfig",
    # Types
    "ConnectionState",
    "ManagerState",
    "MessageType",
    "ConnectionHealth",
    "FeedHealth",
    "FeedSystemHealth",
    # Errors
    "LiveFeedError",
    "ConnectionError",
    "SubscriptionError",
    "MessageParseError",
    "HandlerError",
]
