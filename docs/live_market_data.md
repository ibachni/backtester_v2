# Live Market Data Module (WIP)

> ⚠️ **Work in Progress**: This module is under active development. APIs may change.

## Overview

The Live Market Data module provides real-time market data streaming from Binance via WebSocket, integrated with the existing Bus architecture. It supports both spot and futures markets, with automatic reconnection, health monitoring, and graceful degradation.

## Quick Start

```python
import asyncio
from backtester.core.bus import Bus
from backtester.data.live import LiveDataFeedManager, LiveFeedConfig
from backtester.data.live.config import Venue, StreamType, StreamConfig

async def main():
    # Create bus
    bus = Bus()

    # Simple configuration - just symbols with defaults
    config = LiveFeedConfig(
        venue=Venue.BINANCE_SPOT,
        symbols=["BTCUSDT", "ETHUSDT"],
    )

    # Create and start manager
    manager = LiveDataFeedManager(config, bus)
    await manager.start()

    # Subscribe to candle events
    async def on_candle(envelope):
        candle = envelope.payload
        print(f"{candle.symbol}: {candle.close}")

    bus.subscribe("mkt.candles", on_candle)

    # Run for some time
    await asyncio.sleep(60)

    # Graceful shutdown
    await manager.stop()

asyncio.run(main())
```

## Configuration

### Venues

The module supports multiple Binance venues:

| Venue | Description |
|-------|-------------|
| `BINANCE_SPOT` | Production spot market |
| `BINANCE_FUTURES` | Production USD-M futures |
| `BINANCE_SPOT_TESTNET` | Testnet spot (for development) |
| `BINANCE_FUTURES_TESTNET` | Testnet futures (for development) |

```python
from backtester.data.live.config import Venue

# Use testnet for development
config = LiveFeedConfig(
    venue=Venue.BINANCE_SPOT_TESTNET,
    symbols=["BTCUSDT"],
)
```

### Stream Types

Available stream types:

| StreamType | Description | Requirements |
|------------|-------------|--------------|
| `KLINE` | Candlestick/kline data | `interval` required (e.g., "1m", "5m", "1h") |
| `DEPTH` | Order book depth | Optional: `depth_levels` (5, 10, 20), `update_speed` |
| `BOOK_TICKER` | Best bid/ask | None |
| `MARK_PRICE` | Mark price & funding (futures) | None |
| `AGG_TRADE` | Aggregated trades | None |
| `TICKER` | 24hr ticker statistics | None |

### Simple Configuration (Symbols Only)

When you just need klines with default settings:

```python
config = LiveFeedConfig(
    venue=Venue.BINANCE_SPOT,
    symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    default_kline_interval="1m",  # Default interval for auto-generated kline streams
)
```

### Extended Configuration (With Order Book)

```python
config = LiveFeedConfig(
    venue=Venue.BINANCE_SPOT,
    symbols=["BTCUSDT", "ETHUSDT"],
    subscribe_orderbook=True,
    orderbook_depth=5,  # 5, 10, or 20 levels
)
```

### Explicit Stream Configuration

For fine-grained control, specify streams explicitly:

```python
from backtester.data.live.config import StreamConfig, StreamType

config = LiveFeedConfig(
    venue=Venue.BINANCE_SPOT,
    streams=[
        # BTCUSDT: 1-minute and 5-minute klines
        StreamConfig("BTCUSDT", StreamType.KLINE, interval="1m"),
        StreamConfig("BTCUSDT", StreamType.KLINE, interval="5m"),

        # BTCUSDT: Order book with 10 levels, 100ms updates
        StreamConfig("BTCUSDT", StreamType.DEPTH, depth_levels=10, update_speed="100ms"),

        # ETHUSDT: Only ticker
        StreamConfig("ETHUSDT", StreamType.TICKER),
    ],
)
```

### Futures Configuration (With Funding Rates)

```python
config = LiveFeedConfig(
    venue=Venue.BINANCE_FUTURES,
    symbols=["BTCUSDT", "ETHUSDT"],
    subscribe_funding=True,  # Subscribe to mark price/funding rate streams
)
```

## Component Configs

### Connection Config

Control WebSocket connection behavior:

```python
from backtester.data.live.config import ConnectionConfig

config = LiveFeedConfig(
    venue=Venue.BINANCE_SPOT,
    symbols=["BTCUSDT"],
    connection=ConnectionConfig(
        base_url="wss://stream.binance.com:9443",  # Auto-set based on venue
        connect_timeout_s=30.0,
        ping_interval_s=30.0,
        ping_timeout_s=10.0,
        max_reconnect_attempts=10,
        base_reconnect_delay_s=1.0,
        max_reconnect_delay_s=60.0,
        reconnect_jitter=0.3,  # ±30% jitter for exponential backoff
        max_connection_age_s=23 * 3600,  # Refresh before Binance 24h limit
    ),
)
```

### Health Config

Configure health monitoring:

```python
from backtester.data.live.config import HealthConfig

config = LiveFeedConfig(
    venue=Venue.BINANCE_SPOT,
    symbols=["BTCUSDT"],
    health=HealthConfig(
        staleness_threshold_s=30.0,  # Feed is stale after 30s of no messages
        health_check_interval_s=5.0,  # Check health every 5 seconds
        watchdog_timeout_s=60.0,      # Kill and restart if no messages for 60s
    ),
)
```

## Bus Topics

The module publishes events to the following Bus topics:

| Topic | Payload Type | Description |
|-------|--------------|-------------|
| `mkt.candles` | `Candle` | Finalized candlestick data |
| `mkt.orderbook` | `OrderBookSnapshot` | Order book snapshots |
| `mkt.funding` | `FundingRateEvent` | Funding rate events (futures) |
| `mkt.ticker` | `TickerEvent` | 24hr ticker statistics |
| `live.health` | `FeedSystemHealth` | System health updates |
| `log.event` | `LogEvent` | Critical infrastructure events |

### Subscribing to Events

```python
from backtester.types.topics import T_CANDLES, T_ORDERBOOK, T_LIVE_HEALTH

# Subscribe to candles
async def on_candle(envelope):
    candle = envelope.payload
    print(f"[{candle.symbol}] Close: {candle.close}, Volume: {candle.volume}")

bus.subscribe(T_CANDLES, on_candle)

# Subscribe to order book
async def on_orderbook(envelope):
    book = envelope.payload
    print(f"[{book.symbol}] Spread: {book.spread_bps:.2f} bps")

bus.subscribe(T_ORDERBOOK, on_orderbook)

# Monitor system health
async def on_health(envelope):
    health = envelope.payload
    print(f"System state: {health.state.value}")
    for feed in health.feeds:
        if feed.is_stale:
            print(f"  ⚠️ {feed.symbol}:{feed.stream_type} is STALE")

bus.subscribe(T_LIVE_HEALTH, on_health)
```

## Health Monitoring

### Manager States

```
[STOPPED] --start()--> [STARTING] --success--> [RUNNING]
                            |                       |
                        [FAILED]              [STOPPING] --> [STOPPED]
```

### Getting Health Status

```python
# Get current health snapshot
health = manager.get_health()

print(f"State: {health.state.value}")
print(f"Total messages: {health.total_message_count}")
print(f"Total errors: {health.total_error_count}")

for conn in health.connections:
    print(f"Connection: {conn.state.value}, uptime: {conn.uptime_s:.0f}s")

for feed in health.feeds:
    status = "STALE" if feed.is_stale else "OK"
    print(f"  {feed.symbol}:{feed.stream_type} [{status}] - {feed.message_count} msgs")
```

### Statistics

```python
stats = manager.get_stats()
print(f"Messages published: {stats['messages_published']}")
print(f"Errors: {stats['errors_count']}")
print(f"Router stats: {stats['router']}")
```

## Error Handling

The module provides specific exception types:

```python
from backtester.data.live import (
    LiveFeedError,      # Base exception
    ConnectionError,    # WebSocket issues
    SubscriptionError,  # Stream subscription failures
    MessageParseError,  # Invalid message format
    HandlerError,       # Handler processing failures
)

try:
    await manager.start()
except ConnectionError as e:
    print(f"Connection failed: {e}")
    print(f"URL: {e.url}, Attempt: {e.reconnect_attempt}")
except LiveFeedError as e:
    print(f"Live feed error: {e}")
```

## Event Logging

Critical infrastructure events are published to `log.event` topic for audit trail:

| Event | Level | Description |
|-------|-------|-------------|
| `FEED_STARTED` | INFO | Manager started successfully |
| `FEED_STOPPING` | INFO | Manager shutting down |
| `FEED_START_FAILED` | ERROR | Manager failed to start |
| `CONNECTION_ESTABLISHED` | INFO | WebSocket connected |
| `CONNECTION_RECONNECTING` | WARNING | Connection lost, reconnecting |
| `CONNECTION_DISCONNECTED` | WARNING | WebSocket disconnected |
| `CONNECTION_ERROR` | ERROR | Connection error occurred |
| `FEED_STALE` | WARNING | Feed became stale |
| `WATCHDOG_TRIGGERED` | ERROR | Watchdog timeout |

## Complete Example

```python
import asyncio
import signal
from backtester.core.bus import Bus
from backtester.data.live import LiveDataFeedManager, LiveFeedConfig
from backtester.data.live.config import (
    Venue,
    StreamType,
    StreamConfig,
    HealthConfig,
)
from backtester.types.topics import T_CANDLES, T_ORDERBOOK


async def main():
    # Setup
    bus = Bus()

    config = LiveFeedConfig(
        venue=Venue.BINANCE_SPOT,
        streams=[
            StreamConfig("BTCUSDT", StreamType.KLINE, interval="1m"),
            StreamConfig("BTCUSDT", StreamType.DEPTH, depth_levels=5),
            StreamConfig("ETHUSDT", StreamType.KLINE, interval="1m"),
        ],
        health=HealthConfig(
            staleness_threshold_s=30.0,
            watchdog_timeout_s=60.0,
        ),
        emit_partial_candles=False,  # Only emit finalized candles
    )

    manager = LiveDataFeedManager(config, bus, name="my_feed")

    # Event handlers
    async def on_candle(envelope):
        c = envelope.payload
        print(f"🕯️ [{c.symbol}] O:{c.open:.2f} H:{c.high:.2f} L:{c.low:.2f} C:{c.close:.2f}")

    async def on_orderbook(envelope):
        book = envelope.payload
        if book.spread_bps:
            print(f"📊 [{book.symbol}] Bid:{book.best_bid:.2f} Ask:{book.best_ask:.2f} Spread:{book.spread_bps:.2f}bps")

    bus.subscribe(T_CANDLES, on_candle)
    bus.subscribe(T_ORDERBOOK, on_orderbook)

    # Graceful shutdown
    shutdown = asyncio.Event()

    def handle_signal():
        print("\nShutting down...")
        shutdown.set()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, handle_signal)
    loop.add_signal_handler(signal.SIGTERM, handle_signal)

    # Start and run
    try:
        await manager.start()
        print(f"✅ Live feed started: {config.get_all_symbols()}")

        await shutdown.wait()

    finally:
        await manager.stop()
        print("✅ Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    LiveDataFeedManager                          │
│  - Lifecycle orchestration                                      │
│  - Component coordination                                       │
│  - Health aggregation                                           │
└──────────────────────────┬──────────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│ Connection    │  │ MessageRouter │  │ HealthMonitor │
│ Manager       │  │               │  │               │
│               │  │ - Classify    │  │ - Staleness   │
│ - WebSocket   │  │ - Route to    │  │ - Watchdog    │
│ - Reconnect   │  │   handlers    │  │ - Metrics     │
│ - Ping/pong   │  │               │  │               │
└───────┬───────┘  └───────┬───────┘  └───────────────┘
        │                  │
        │                  ▼
        │          ┌───────────────┐
        │          │   Handlers    │
        │          │               │
        │          │ - KlineHandler│
        │          │ - BookHandler │
        │          │ - FundingHdlr │
        │          │ - TickerHdlr  │
        │          └───────┬───────┘
        │                  │
        └──────────────────┴──────────────────┐
                                              ▼
                                       ┌─────────────┐
                                       │    Bus      │
                                       │             │
                                       │ mkt.candles │
                                       │ mkt.orderbook│
                                       │ mkt.funding │
                                       │ live.health │
                                       └─────────────┘
```

## Limitations & Known Issues

- **Single venue per manager**: Each `LiveDataFeedManager` connects to one venue. For multi-venue, create multiple managers.
- **Binance stream limits**: Maximum 200 streams per connection (handled automatically).
- **24-hour connection limit**: Binance closes connections after 24 hours. The module auto-refreshes at 23 hours.
- **Order book snapshots only**: Currently publishes snapshots, not incremental updates.

## Related Documentation

- [ADR 007: REST Polling Initial Live Feed](../adr/007-rest-polling-initial-live-feed.md)
- [ADR 024: Logging Architecture Duality](../adr/024-logging-architecture-duality.md)
- [Bus Topology](bus_topology.md)
