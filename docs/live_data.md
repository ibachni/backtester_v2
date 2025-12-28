# Live Market Data Feed Adapter Specification

**Status:** Design
**Date:** December 2024 (Updated: December 2025)
**Target:** Binance Spot & Futures WebSocket Streams

---

## 1. Executive Summary

This document specifies a **Hybrid WebSocket + REST** live data feed adapter for Backtester V2. The adapter integrates with the existing `Bus` architecture, publishing to `T_CANDLES` and new topics (`T_FUNDING`, `T_ORDERBOOK`, `T_TICKER`) in a manner compatible with the current `BarFeed` interface.

**Goal:** Enable paper trading for delta-neutral carry, cross-sectional long/short, and mean reversion strategies by providing real-time market data through the existing event-driven architecture.

---

## 2. Architecture Overview

### 2.1 High-Level Design

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              LiveDataFeedManager                                    │
│         (Orchestration, Lifecycle, Health Aggregation, Graceful Shutdown)           │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌──────────────────────────────────────────────────────────────────────────────┐   │
│  │                        ConnectionManager                                     │   │
│  │  (WebSocket lifecycle, reconnection, ping/pong, subscription management)     │   │
│  │                                                                              │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐               │   │
│  │  │ SpotConnection  │  │FuturesConnection│  │ (future venues) │               │   │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘               │   │
│  └──────────────────────────────────────────────────────────────────────────────┘   │
│                                         │                                           │
│                                         ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────────────┐   │
│  │                          MessageRouter                                       │   │
│  │         (Classify raw messages, route to appropriate handler)                │   │
│  └──────────────────────────────────────────────────────────────────────────────┘   │
│                                         │                                           │
│           ┌─────────────────────────────┼─────────────────────────────┐             │
│           ▼                             ▼                             ▼             │
│  ┌─────────────────┐         ┌─────────────────┐          ┌─────────────────┐       │
│  │  KlineHandler   │         │  BookHandler    │          │ FundingHandler  │       │
│  │  (Parser +      │         │  (Parser +      │          │  (Parser +      │       │
│  │   Normalizer)   │         │   Normalizer)   │          │   Normalizer)   │       │
│  └────────┬────────┘         └────────┬────────┘          └────────┬────────┘       │
│           │                           │                            │                │
│           ▼                           ▼                            ▼                │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │                       FrameSynchronizer                                     │    │
│  │          (Multi-symbol alignment for cross-sectional strategies)            │    │
│  └─────────────────────────────────────────────────────────────────────────────┘    │
│                                         │                                           │
│  ┌──────────────────────────────────────┼───────────────────────────────────────┐   │
│  │                       EventPublisher │                                       │   │
│  │    (Backpressure, coalescing, publish to Bus)                                │   │
│  └──────────────────────────────────────┼───────────────────────────────────────┘   │
│                                         │                                           │
└─────────────────────────────────────────┼───────────────────────────────────────────┘
                                          ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                      Bus                                            │
│              Topics: T_CANDLES, T_ORDERBOOK, T_FUNDING, T_TICKER                    │
└─────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              Supporting Components                                  │
├─────────────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌───────────────┐   │
│  │  RESTClient     │  │  HealthMonitor  │  │  MessageLogger  │  │ SymbolRegistry│   │
│  │  (Snapshots,    │  │  (Staleness,    │  │  (Raw message   │  │ (Normalization│   │
│  │   recovery)     │  │   metrics)      │  │   persistence)  │  │  mapping)     │   │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  └───────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Component Summary

| Component | Responsibility |
|-----------|----------------|
| `LiveDataFeedManager` | Top-level orchestration, lifecycle, health aggregation |
| `ConnectionManager` | WebSocket lifecycle, reconnection, ping/pong, subscriptions |
| `MessageRouter` | Classify and route raw messages to handlers |
| `KlineHandler` | Parse kline JSON → `Candle` events |
| `BookHandler` | Parse depth JSON → `OrderBookSnapshot` events |
| `FundingHandler` | Parse markPrice JSON → `FundingRateEvent` events |
| `FrameSynchronizer` | Align multi-symbol candles for cross-sectional |
| `EventPublisher` | Backpressure handling, coalescing, Bus publishing |
| `HealthMonitor` | Staleness detection, metrics, watchdog |
| `RESTClient` | Snapshots, historical data, rate-limited queries |
| `SymbolRegistry` | Symbol normalization, metadata, validation |
| `MessageLogger` | Raw message persistence for replay/debugging |

---

## 3. Component Specifications

### 3.1 LiveDataFeedManager

**File:** `backtester/data/live/manager.py`

**Responsibilities:**
1. Top-level orchestration and lifecycle management
2. Configuration validation and component initialization
3. Aggregated health status reporting
4. Graceful startup/shutdown coordination
5. Error escalation and circuit breaking

**State Machine:**
```
[STOPPED] --start()--> [STARTING] --success--> [RUNNING]
                            |                       |
                            | failure               | stop() / fatal error
                            v                       v
                        [FAILED]              [STOPPING] --> [STOPPED]
```

**Interface:**

```python
from dataclasses import dataclass
from typing import Literal, Optional
from pathlib import Path


@dataclass(frozen=True)
class LiveFeedConfig:
    """Immutable configuration for the live feed system."""
    # Symbols
    spot_symbols: tuple[str, ...] = ()
    futures_symbols: tuple[str, ...] = ()

    # Stream settings
    timeframe: str = "1m"
    sync_mode: Literal["async", "aligned"] = "aligned"
    sync_timeout_ms: int = 5000

    # Feature flags
    enable_orderbook: bool = False
    enable_funding: bool = False
    orderbook_depth: Literal[5, 10, 20] = 20
    orderbook_update_ms: Literal[100, 1000] = 100

    # Connection settings
    use_testnet: bool = True
    base_url_spot: str = "wss://stream.binance.com:9443"
    base_url_futures: str = "wss://fstream.binance.com"

    # Resilience
    reconnect_base_delay_ms: int = 1000
    reconnect_max_delay_ms: int = 30000
    reconnect_max_attempts: int = 10
    stale_threshold_ms: int = 30000

    # Persistence
    enable_message_logging: bool = False
    message_log_dir: Path = Path("logs/raw_messages")


class LiveDataFeedManager:
    """
    Top-level orchestrator for live market data feeds.

    Lifecycle:
        1. __init__: Store config, no I/O
        2. start(): Initialize all components, establish connections
        3. (running): Components process messages autonomously
        4. stop(): Graceful shutdown with timeout

    Usage:
        manager = LiveDataFeedManager(config, bus, clock)
        await manager.start()
        # ... trading loop ...
        await manager.stop()
    """

    def __init__(
        self,
        config: LiveFeedConfig,
        bus: Bus,
        clock: Clock,
    ) -> None: ...

    async def start(self) -> None:
        """
        Initialize and start all components.
        Raises: ConnectionError if initial connection fails after retries.
        """
        ...

    async def stop(self, timeout_s: float = 10.0) -> None:
        """
        Graceful shutdown.
        1. Stop accepting new messages
        2. Flush pending events to bus
        3. Close connections
        4. Force-kill after timeout
        """
        ...

    def get_health(self) -> FeedSystemHealth:
        """Aggregate health from all components."""
        ...

    @property
    def is_running(self) -> bool: ...

    @property
    def is_healthy(self) -> bool:
        """True if all critical feeds are connected and not stale."""
        ...
```

**Dependencies:**
- `ConnectionManager`: WebSocket lifecycle
- `MessageRouter`: Message classification and routing
- `FrameSynchronizer`: Cross-sectional alignment
- `EventPublisher`: Bus integration
- `HealthMonitor`: Health aggregation
- `RESTClient`: Snapshot fetching

---

### 3.2 ConnectionManager

**File:** `backtester/data/live/connection.py`

**Responsibilities:**
1. WebSocket connection lifecycle (connect, reconnect, close)
2. Exponential backoff with jitter for reconnection
3. Ping/pong keepalive handling
4. Subscription management (subscribe on connect, resubscribe on reconnect)
5. Connection-level metrics (connected time, message count, reconnect count)
6. 24-hour connection refresh (Binance limit)

**Interface:**

```python
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Awaitable, Optional
from datetime import datetime


@dataclass(frozen=True)
class ConnectionConfig:
    """Configuration for a single WebSocket connection."""
    name: str  # e.g., "spot_klines", "futures_funding"
    base_url: str
    streams: tuple[str, ...]  # e.g., ("btcusdt@kline_1m", "ethusdt@kline_1m")

    # Reconnection policy
    reconnect_base_delay_ms: int = 1000
    reconnect_max_delay_ms: int = 30000
    reconnect_max_attempts: int = 10
    jitter_pct: float = 0.1

    # Keepalive
    ping_interval_s: float = 30.0
    pong_timeout_s: float = 10.0

    # Refresh before 24h limit
    max_connection_age_s: float = 23 * 3600  # Refresh at 23 hours


class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass
class ConnectionHealth:
    """Health snapshot for a single connection."""
    name: str
    state: ConnectionState
    connected_since: Optional[datetime]
    last_message_ts: Optional[int]
    messages_received: int
    reconnect_count: int
    consecutive_errors: int

    def is_stale(self, threshold_ms: int = 30000) -> bool:
        """True if no message received within threshold."""
        ...

    @property
    def uptime_s(self) -> Optional[float]: ...


class ConnectionManager:
    """
    Manages a single WebSocket connection with auto-reconnection.

    Message Flow:
        WebSocket -> on_message callback -> MessageRouter

    Threading Model:
        - Runs in asyncio event loop
        - Spawns internal tasks for ping/pong and reconnection

    Combined Streams URL Format:
        wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m
    """

    def __init__(
        self,
        config: ConnectionConfig,
        on_message: Callable[[str, str], Awaitable[None]],  # (connection_name, raw_json)
        on_state_change: Callable[[ConnectionState], Awaitable[None]],
    ) -> None: ...

    async def start(self) -> None:
        """
        Start connection. Returns when initially connected.
        Raises: ConnectionError after max_attempts exhausted.
        """
        ...

    async def stop(self) -> None:
        """Gracefully close connection."""
        ...

    def get_health(self) -> ConnectionHealth: ...

    @property
    def state(self) -> ConnectionState: ...

    # Internal methods
    async def _connect_loop(self) -> None:
        """Main loop: connect, process messages, handle disconnects."""
        ...

    async def _handle_ping(self, data: bytes) -> None:
        """Respond to ping frame immediately."""
        ...

    async def _keepalive_loop(self) -> None:
        """Send pings if no message received recently."""
        ...

    async def _reconnect_with_backoff(self) -> None:
        """Exponential backoff reconnection."""
        ...

    def _calculate_backoff(self, attempt: int) -> float:
        """Calculate delay with exponential backoff + jitter."""
        delay = min(
            self.config.reconnect_base_delay_ms * (2 ** attempt),
            self.config.reconnect_max_delay_ms,
        )
        jitter = delay * self.config.jitter_pct * (random.random() * 2 - 1)
        return (delay + jitter) / 1000  # seconds
```

**Key Implementation Details:**
1. **Combined Streams URL**: Use Binance's combined stream format for multiple symbols in one connection
2. **Ping/Pong**: Binance sends ping frames; respond with pong within 10 minutes. Also implement client-side pings if no message for 30s
3. **24-hour Refresh**: Proactively disconnect and reconnect before the 24-hour limit
4. **Resubscription**: URL-based subscription is automatic after reconnect

---

### 3.3 MessageRouter

**File:** `backtester/data/live/router.py`

**Responsibilities:**
1. Classify incoming messages by type (kline, depth, markPrice, heartbeat, error)
2. Route to appropriate handler
3. Drop/log unrecognized message types
4. Track message rates by type

**Interface:**

```python
from enum import Enum
from dataclasses import dataclass
from typing import Callable, Awaitable


class MessageType(Enum):
    KLINE = "kline"
    DEPTH = "depth"
    BOOK_TICKER = "bookTicker"
    MARK_PRICE = "markPrice"
    AGG_TRADE = "aggTrade"
    HEARTBEAT = "heartbeat"
    ERROR = "error"
    UNKNOWN = "unknown"


@dataclass
class RoutedMessage:
    """Parsed message envelope."""
    msg_type: MessageType
    stream: str       # e.g., "btcusdt@kline_1m"
    symbol: str       # e.g., "BTCUSDT"
    data: dict        # Raw data payload
    recv_ts: int      # Local receive timestamp (Unix ms)


class MessageRouter:
    """
    Classifies and routes raw WebSocket messages.

    Design:
        - Fast path: Classify by stream name pattern (no full JSON parsing yet)
        - Handlers receive semi-parsed RoutedMessage
        - Unrecognized messages logged and dropped
    """

    def __init__(
        self,
        handlers: dict[MessageType, Callable[[RoutedMessage], Awaitable[None]]],
        clock: Clock,
    ) -> None: ...

    async def route(self, connection_name: str, raw_json: str) -> None:
        """
        Parse and route a raw message.

        Steps:
            1. Parse JSON (fast path with orjson)
            2. Extract stream name
            3. Classify message type
            4. Create RoutedMessage
            5. Call appropriate handler
        """
        ...

    def _classify(self, stream: str) -> MessageType:
        """Classify by stream name pattern."""
        if "@kline_" in stream:
            return MessageType.KLINE
        if "@depth" in stream:
            return MessageType.DEPTH
        if "@markPrice" in stream:
            return MessageType.MARK_PRICE
        if "@bookTicker" in stream:
            return MessageType.BOOK_TICKER
        if "@aggTrade" in stream:
            return MessageType.AGG_TRADE
        return MessageType.UNKNOWN

    def get_message_rates(self) -> dict[MessageType, float]:
        """Messages per second by type (rolling 1-minute window)."""
        ...
```

---

### 3.4 Message Handlers (Parser + Normalizer)

**File:** `backtester/data/live/handlers.py`

**Responsibilities:**
1. Parse exchange-specific JSON into internal dataclasses
2. Normalize symbols, timestamps, numeric types
3. Validate schema (lightweight, fail-fast)
4. Track both exchange timestamp and receive timestamp

#### 3.4.1 KlineHandler

```python
@dataclass(frozen=True, slots=True)
class Candle:
    """Normalized candlestick data."""
    symbol: str
    timeframe: str
    open_time: int      # Unix ms
    close_time: int     # Unix ms
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trades: int
    is_final: bool

    # Metadata
    ts_exchange: int    # Event time from exchange
    ts_recv: int        # Local receive time


class KlineHandler:
    """
    Parses Binance kline messages into Candle objects.

    Binance Kline Payload:
        {
            "e": "kline",
            "E": 1672515782136,        // Event time
            "s": "BTCUSDT",            // Symbol
            "k": {
                "t": 1672515780000,    // Kline start time
                "T": 1672515839999,    // Kline close time
                "s": "BTCUSDT",
                "i": "1m",             // Interval
                "o": "16500.00",       // Open
                "c": "16510.50",       // Close
                "h": "16515.00",       // High
                "l": "16495.00",       // Low
                "v": "1000.5",         // Volume
                "n": 100,              // Number of trades
                "x": false,            // Is this kline closed?
                "q": "16505000.00",    // Quote asset volume
                ...
            }
        }
    """

    def __init__(
        self,
        symbol_registry: SymbolRegistry,
        on_candle: Callable[[Candle], Awaitable[None]],
    ) -> None: ...

    async def handle(self, msg: RoutedMessage) -> None:
        """Parse and emit Candle."""
        ...

    def _parse_candle(self, msg: RoutedMessage) -> Candle:
        """Convert raw data to Candle."""
        k = msg.data["k"]
        return Candle(
            symbol=self._symbol_registry.normalize(k["s"]),
            timeframe=k["i"],
            open_time=k["t"],
            close_time=k["T"],
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
            quote_volume=float(k["q"]),
            trades=k["n"],
            is_final=k["x"],
            ts_exchange=msg.data["E"],
            ts_recv=msg.recv_ts,
        )
```

#### 3.4.2 BookHandler

```python
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    """Normalized L2 order book snapshot."""
    symbol: str
    bids: tuple[tuple[Decimal, Decimal], ...]  # (price, qty), descending by price
    asks: tuple[tuple[Decimal, Decimal], ...]  # (price, qty), ascending by price
    last_update_id: int
    ts_exchange: Optional[int]  # Not always provided in partial depth
    ts_recv: int

    @property
    def best_bid(self) -> Optional[tuple[Decimal, Decimal]]:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Optional[tuple[Decimal, Decimal]]:
        return self.asks[0] if self.asks else None

    @property
    def mid_price(self) -> Optional[Decimal]:
        if not self.bids or not self.asks:
            return None
        return (self.bids[0][0] + self.asks[0][0]) / 2

    @property
    def spread_bps(self) -> Optional[float]:
        if not self.bids or not self.asks:
            return None
        mid = self.mid_price
        if not mid:
            return None
        return float((self.asks[0][0] - self.bids[0][0]) / mid * 10000)

    @property
    def imbalance(self) -> float:
        """Top-of-book imbalance: (bid_qty - ask_qty) / (bid_qty + ask_qty)."""
        if not self.bids or not self.asks:
            return 0.0
        bid_qty = float(self.bids[0][1])
        ask_qty = float(self.asks[0][1])
        total = bid_qty + ask_qty
        return (bid_qty - ask_qty) / total if total > 0 else 0.0


class BookHandler:
    """
    Parses partial depth messages into OrderBookSnapshot.

    Uses partial depth stream (simpler than diff depth + REST snapshot).
    Levels: 5, 10, or 20
    Update speed: 100ms or 1000ms
    """

    def __init__(
        self,
        symbol_registry: SymbolRegistry,
        on_book: Callable[[OrderBookSnapshot], Awaitable[None]],
    ) -> None: ...

    async def handle(self, msg: RoutedMessage) -> None:
        """Parse and emit OrderBookSnapshot."""
        ...

    def _parse_book(self, msg: RoutedMessage) -> OrderBookSnapshot:
        """Convert raw data to OrderBookSnapshot."""
        return OrderBookSnapshot(
            symbol=self._symbol_registry.normalize(msg.symbol),
            bids=tuple(
                (Decimal(p), Decimal(q)) for p, q in msg.data["bids"]
            ),
            asks=tuple(
                (Decimal(p), Decimal(q)) for p, q in msg.data["asks"]
            ),
            last_update_id=msg.data["lastUpdateId"],
            ts_exchange=None,  # Partial depth doesn't include event time
            ts_recv=msg.recv_ts,
        )
```

#### 3.4.3 FundingHandler

```python
@dataclass(frozen=True, slots=True)
class FundingRateEvent:
    """Normalized funding rate from markPrice stream."""
    symbol: str
    mark_price: Decimal
    index_price: Decimal
    funding_rate: Decimal       # e.g., 0.0001 = 1 bps
    next_funding_time: int      # Unix ms
    ts_exchange: int
    ts_recv: int

    @property
    def funding_rate_pct(self) -> float:
        """Funding rate as percentage."""
        return float(self.funding_rate) * 100

    @property
    def annualized_rate(self) -> float:
        """Annualized funding rate (3 per day * 365)."""
        return float(self.funding_rate) * 3 * 365 * 100  # As percentage


class FundingHandler:
    """
    Parses markPrice stream for funding rates.

    Binance markPrice Payload:
        {
            "e": "markPriceUpdate",
            "E": 1672515782136,
            "s": "BTCUSDT",
            "p": "16500.50",       // Mark price
            "i": "16498.00",       // Index price
            "P": "16505.00",       // Estimated settle price (ignore)
            "r": "0.00010000",     // Funding rate
            "T": 1672531200000     // Next funding time
        }
    """

    def __init__(
        self,
        symbol_registry: SymbolRegistry,
        on_funding: Callable[[FundingRateEvent], Awaitable[None]],
    ) -> None: ...

    async def handle(self, msg: RoutedMessage) -> None:
        """Parse and emit FundingRateEvent."""
        ...
```

---

### 3.5 FrameSynchronizer

**File:** `backtester/data/live/synchronizer.py`

**Responsibilities:**
1. Buffer candles until all symbols report for a time bucket
2. Emit complete frames when all symbols arrive
3. Emit partial frames on timeout (with `None` for missing symbols)
4. Track synchronization quality metrics

**Interface:**

```python
from typing import Optional
from dataclasses import dataclass


CandleFrame = dict[str, Optional[Candle]]


@dataclass
class SyncStats:
    """Synchronization quality metrics."""
    frames_emitted: int = 0
    complete_frames: int = 0
    partial_frames: int = 0
    late_arrivals_dropped: int = 0
    avg_symbols_per_partial: float = 0.0


class FrameSynchronizer:
    """
    Aligns candles across symbols for cross-sectional strategies.

    Problem:
        WebSocket messages arrive asynchronously. BTCUSDT candle for
        minute 12:00 may arrive 500ms before ETHUSDT's candle.

    Solution:
        Buffer candles by time bucket. Emit when:
        1. All expected symbols have reported, OR
        2. Timeout expires (emit partial with None)

    Only processes is_final=True candles.

    Thread Safety:
        All methods are async and run in the same event loop.
        No explicit locking needed.
    """

    def __init__(
        self,
        symbols: set[str],
        timeout_ms: int,
        on_frame: Callable[[CandleFrame, int], Awaitable[None]],  # (frame, bucket_ts)
        clock: Clock,
    ) -> None:
        self._symbols = symbols
        self._timeout_ms = timeout_ms
        self._on_frame = on_frame
        self._clock = clock

        # Current bucket state
        self._current_bucket_ts: Optional[int] = None
        self._buffer: dict[str, Candle] = {}
        self._timeout_task: Optional[asyncio.Task] = None

        # Stats
        self._stats = SyncStats()

    async def on_candle(self, candle: Candle) -> None:
        """
        Process an incoming candle.

        Logic:
            1. If new bucket (candle.close_time > current), emit current as partial
            2. If same bucket, add to buffer
            3. If old bucket (late arrival), drop
            4. If buffer complete, emit immediately
        """
        ...

    async def flush(self) -> None:
        """Force emit current buffer (for shutdown)."""
        ...

    def get_stats(self) -> SyncStats: ...

    async def _emit_frame(self, is_complete: bool) -> None:
        """Emit current buffer as frame."""
        frame: CandleFrame = {
            sym: self._buffer.get(sym) for sym in self._symbols
        }

        self._stats.frames_emitted += 1
        if is_complete:
            self._stats.complete_frames += 1
        else:
            self._stats.partial_frames += 1

        await self._on_frame(frame, self._current_bucket_ts)

    def _start_timeout(self) -> None:
        """Start async timeout for partial frame emission."""
        self._cancel_timeout()
        self._timeout_task = asyncio.create_task(self._timeout_handler())

    def _cancel_timeout(self) -> None:
        if self._timeout_task and not self._timeout_task.done():
            self._timeout_task.cancel()
            self._timeout_task = None

    async def _timeout_handler(self) -> None:
        await asyncio.sleep(self._timeout_ms / 1000)
        if self._buffer:
            await self._emit_frame(is_complete=False)
            self._reset_bucket()

    def _reset_bucket(self) -> None:
        self._current_bucket_ts = None
        self._buffer.clear()
        self._cancel_timeout()
```

---

### 3.6 EventPublisher

**File:** `backtester/data/live/publisher.py`

**Responsibilities:**
1. Publish events to the Bus
2. Handle backpressure (bounded queue, drop policy)
3. Coalesce high-frequency events (e.g., order book updates)
4. Track publish latency and dropped events

**Interface:**

```python
from dataclasses import dataclass
from typing import Literal, Any
from collections import defaultdict, deque
import asyncio


@dataclass
class PublisherConfig:
    """Configuration for event publishing."""
    max_queue_size: int = 10000
    drop_policy: Literal["oldest", "newest", "none"] = "oldest"

    # Coalescing: For high-frequency streams, only keep latest per symbol
    coalesce_orderbook: bool = True
    coalesce_funding: bool = False

    # Metrics
    latency_sample_rate: float = 0.01  # Sample 1% for latency tracking


class EventPublisher:
    """
    Publishes events to the Bus with backpressure handling.

    Design:
        - Async queue between handlers and bus
        - Background task drains queue and publishes
        - Coalescing: For order book, keep only latest per symbol
        - Drop policy when queue full
    """

    def __init__(
        self,
        config: PublisherConfig,
        bus: Bus,
        clock: Clock,
    ) -> None:
        self._config = config
        self._bus = bus
        self._clock = clock

        self._queue: asyncio.Queue = asyncio.Queue(maxsize=config.max_queue_size)
        self._coalesce_buffer: dict[str, Any] = {}  # symbol -> latest event
        self._running = False
        self._publish_task: Optional[asyncio.Task] = None

        # Stats
        self._events_published: dict[str, int] = defaultdict(int)
        self._events_dropped: dict[str, int] = defaultdict(int)
        self._latencies: deque[float] = deque(maxlen=1000)

    async def start(self) -> None:
        """Start background publish task."""
        self._running = True
        self._publish_task = asyncio.create_task(self._publish_loop())

    async def stop(self) -> None:
        """Stop and flush remaining events."""
        self._running = False
        if self._publish_task:
            await self._publish_task

    async def publish_candle(self, candle: Candle) -> None:
        """Queue a candle for publishing."""
        await self._enqueue(T_CANDLES, candle.close_time, candle)

    async def publish_candle_frame(self, frame: CandleFrame, ts: int) -> None:
        """Queue a synchronized candle frame."""
        await self._enqueue(T_CANDLES, ts, frame)

    async def publish_orderbook(self, book: OrderBookSnapshot) -> None:
        """Queue order book (with coalescing)."""
        if self._config.coalesce_orderbook:
            # Replace any pending book for this symbol
            self._coalesce_buffer[f"book:{book.symbol}"] = (T_ORDERBOOK, book.ts_recv, book)
        else:
            await self._enqueue(T_ORDERBOOK, book.ts_recv, book)

    async def publish_funding(self, event: FundingRateEvent) -> None:
        """Queue funding rate event."""
        await self._enqueue(T_FUNDING, event.ts_recv, event)

    async def _enqueue(self, topic: str, ts: int, event: Any) -> None:
        """Add to queue with backpressure handling."""
        try:
            self._queue.put_nowait((topic, ts, event))
        except asyncio.QueueFull:
            self._handle_backpressure(topic, ts, event)

    def _handle_backpressure(self, topic: str, ts: int, event: Any) -> None:
        """Handle full queue based on drop policy."""
        self._events_dropped[topic] += 1

        if self._config.drop_policy == "newest":
            return  # Drop incoming event
        elif self._config.drop_policy == "oldest":
            try:
                self._queue.get_nowait()  # Drop oldest
                self._queue.put_nowait((topic, ts, event))
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass

    async def _publish_loop(self) -> None:
        """Background task to drain queue and publish."""
        while self._running or not self._queue.empty():
            # First, publish any coalesced events
            await self._flush_coalesced()

            try:
                topic, ts, event = await asyncio.wait_for(
                    self._queue.get(),
                    timeout=0.1,
                )
                await self._bus.publish(topic, ts, event)
                self._events_published[topic] += 1
            except asyncio.TimeoutError:
                continue

    async def _flush_coalesced(self) -> None:
        """Publish coalesced events and clear buffer."""
        if not self._coalesce_buffer:
            return

        for key, (topic, ts, event) in list(self._coalesce_buffer.items()):
            await self._bus.publish(topic, ts, event)
            self._events_published[topic] += 1

        self._coalesce_buffer.clear()

    def get_stats(self) -> dict:
        """Return publishing statistics."""
        return {
            "published": dict(self._events_published),
            "dropped": dict(self._events_dropped),
            "queue_size": self._queue.qsize(),
            "avg_latency_ms": statistics.mean(self._latencies) if self._latencies else 0,
        }
```

---

### 3.7 HealthMonitor

**File:** `backtester/data/live/health.py`

**Responsibilities:**
1. Aggregate health from all components
2. Detect stale feeds (no messages for N seconds)
3. Track metrics (message rates, latencies, reconnects)
4. Emit health events to bus (for logging/alerting)
5. Implement watchdog timer for automatic recovery

**Interface:**

```python
from dataclasses import dataclass, asdict
from typing import Literal, Callable, Awaitable, Optional
import time


@dataclass
class FeedHealth:
    """Health status for a single feed."""
    name: str
    connected: bool
    last_message_ts: Optional[int]
    messages_received: int
    messages_per_second: float
    reconnect_count: int
    consecutive_errors: int
    avg_latency_ms: float

    def is_stale(self, threshold_ms: int = 30000) -> bool:
        if self.last_message_ts is None:
            return True
        return (time.time() * 1000 - self.last_message_ts) > threshold_ms


@dataclass
class FeedSystemHealth:
    """Aggregate health for entire feed system."""
    status: Literal["healthy", "degraded", "unhealthy"]
    feeds: dict[str, FeedHealth]
    synchronizer_stats: SyncStats
    publisher_stats: dict
    uptime_s: float

    @property
    def is_healthy(self) -> bool:
        return self.status == "healthy"


class HealthMonitor:
    """
    Monitors health of all feed components.

    Features:
        - Per-feed health tracking
        - Staleness detection with configurable threshold
        - Watchdog timer: Force reconnect if stale
        - Periodic health events to bus
    """

    def __init__(
        self,
        stale_threshold_ms: int,
        health_check_interval_s: float,
        bus: Bus,
        clock: Clock,
        on_stale: Callable[[str], Awaitable[None]],  # Called when feed goes stale
    ) -> None: ...

    async def start(self) -> None:
        """Start background health check task."""
        ...

    async def stop(self) -> None: ...

    def record_message(self, feed_name: str, latency_ms: float) -> None:
        """Record message receipt for a feed."""
        ...

    def record_reconnect(self, feed_name: str) -> None:
        """Record reconnection event."""
        ...

    def record_error(self, feed_name: str) -> None:
        """Record error for a feed."""
        ...

    def clear_errors(self, feed_name: str) -> None:
        """Clear error count (on successful operation)."""
        ...

    def get_feed_health(self, feed_name: str) -> FeedHealth: ...

    def get_system_health(self) -> FeedSystemHealth: ...

    async def _health_check_loop(self) -> None:
        """Periodic health check and staleness detection."""
        while self._running:
            await asyncio.sleep(self._health_check_interval_s)

            for feed_name, health in self._feeds.items():
                if health.is_stale(self._stale_threshold_ms):
                    await self._on_stale(feed_name)

            # Emit health event to bus
            system_health = self.get_system_health()
            await self._bus.publish(
                T_LOG,
                self._clock.now(),
                LogEvent(
                    level="INFO" if system_health.is_healthy else "WARN",
                    component="HealthMonitor",
                    msg="health_check",
                    payload=asdict(system_health),
                    sim_time=self._clock.now(),
                ),
            )
```

---

### 3.8 RESTClient

**File:** `backtester/data/live/rest_client.py`

**Responsibilities:**
1. Fetch initial snapshots (order book, exchange info)
2. Historical data recovery (funding rates, klines)
3. Rate limiting with weight tracking
4. Retry with exponential backoff
5. Pagination for large result sets

**Interface:**

```python
from dataclasses import dataclass
from typing import Literal, Optional, Union, Any
import aiohttp


@dataclass
class RESTClientConfig:
    """Configuration for REST client."""
    use_testnet: bool = True
    rate_limit_buffer_pct: float = 0.2  # Stay 20% below limit
    max_retries: int = 3
    retry_base_delay_s: float = 1.0
    timeout_s: float = 30.0


class BinanceRESTClient:
    """
    Async REST client for Binance API.

    Features:
        - Automatic rate limit tracking from response headers
        - Backoff when approaching limit
        - Retry on transient errors
        - Both Spot and Futures endpoints
    """

    # Endpoints
    BASE_SPOT = "https://api.binance.com"
    BASE_SPOT_TESTNET = "https://testnet.binance.vision"
    BASE_FUTURES = "https://fapi.binance.com"
    BASE_FUTURES_TESTNET = "https://testnet.binancefuture.com"

    def __init__(self, config: RESTClientConfig) -> None: ...

    async def __aenter__(self) -> "BinanceRESTClient": ...
    async def __aexit__(self, *args) -> None: ...

    # Spot endpoints
    async def get_exchange_info(self) -> dict:
        """GET /api/v3/exchangeInfo - Symbol specs and filters."""
        ...

    async def get_depth(
        self,
        symbol: str,
        limit: Literal[5, 10, 20, 50, 100, 500, 1000, 5000] = 100,
    ) -> dict:
        """GET /api/v3/depth - Order book snapshot."""
        ...

    async def get_klines(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
    ) -> list[list]:
        """GET /api/v3/klines - Historical klines."""
        ...

    # Futures endpoints
    async def get_funding_rate_history(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
    ) -> list[dict]:
        """GET /fapi/v1/fundingRate - Historical funding rates."""
        ...

    async def get_mark_price(
        self,
        symbol: Optional[str] = None,
    ) -> Union[dict, list[dict]]:
        """GET /fapi/v1/premiumIndex - Current mark price and funding."""
        ...

    # Rate limiting
    @property
    def weight_used(self) -> int:
        """Current weight used in this minute."""
        ...

    @property
    def weight_remaining(self) -> int:
        """Estimated weight remaining before limit."""
        ...

    async def _wait_for_rate_limit(self, required_weight: int) -> None:
        """Wait if approaching rate limit."""
        ...

    # Internal
    async def _get(
        self,
        base: str,
        endpoint: str,
        params: dict,
        weight: int,
    ) -> Any:
        """Execute GET with rate limiting and retry."""
        ...
```

---

### 3.9 SymbolRegistry

**File:** `backtester/data/live/symbols.py`

**Responsibilities:**
1. Symbol normalization (exchange-specific → canonical)
2. Symbol metadata (base/quote, precision, lot size)
3. Validation (is symbol valid for venue?)
4. Mapping between spot and futures symbols

**Interface:**

```python
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


@dataclass(frozen=True)
class SymbolInfo:
    """Metadata for a trading symbol."""
    canonical: str          # e.g., "BTCUSDT"
    exchange: str           # e.g., "binance"
    market: Market          # SPOT or FUTURE
    base_asset: str         # e.g., "BTC"
    quote_asset: str        # e.g., "USDT"
    price_precision: int    # Decimal places for price
    qty_precision: int      # Decimal places for quantity
    min_qty: Decimal
    max_qty: Decimal
    min_notional: Decimal


class SymbolRegistry:
    """
    Registry for symbol normalization and metadata.

    Handles:
        - Case normalization (BTCUSDT vs btcusdt)
        - Exchange-specific quirks
        - Symbol validation
        - Metadata lookup
    """

    def __init__(self) -> None:
        self._symbols: dict[str, SymbolInfo] = {}
        self._aliases: dict[str, str] = {}  # lowercase -> canonical

    async def load_from_exchange(self, client: BinanceRESTClient) -> None:
        """Fetch and cache symbol info from exchange."""
        info = await client.get_exchange_info()
        for sym in info["symbols"]:
            # Parse filters, create SymbolInfo, register
            ...

    def register(self, info: SymbolInfo) -> None:
        """Register a symbol."""
        self._symbols[info.canonical] = info
        self._aliases[info.canonical.lower()] = info.canonical

    def normalize(self, symbol: str) -> str:
        """
        Normalize symbol to canonical form.
        Raises: ValueError if unknown symbol.
        """
        canonical = self._aliases.get(symbol.lower())
        if canonical is None:
            raise ValueError(f"Unknown symbol: {symbol}")
        return canonical

    def get_info(self, symbol: str) -> SymbolInfo:
        """Get metadata for symbol."""
        canonical = self.normalize(symbol)
        return self._symbols[canonical]

    def is_valid(self, symbol: str) -> bool:
        """Check if symbol is registered."""
        return symbol.lower() in self._aliases

    def get_all(self, market: Optional[Market] = None) -> list[SymbolInfo]:
        """Get all registered symbols, optionally filtered by market."""
        symbols = list(self._symbols.values())
        if market:
            symbols = [s for s in symbols if s.market == market]
        return symbols
```

---

### 3.10 MessageLogger (Optional)
- See whether backtester/audit/audit.py can be reused.

---

## 4. Data Types & Topics

### 4.1 New Types (to add to `backtester/types/types.py`)

```python
# --- Live Market Data Types ---

@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    """L2 order book snapshot for spread/imbalance calculations."""
    symbol: str
    bids: tuple[tuple[Decimal, Decimal], ...]  # (price, qty) descending
    asks: tuple[tuple[Decimal, Decimal], ...]  # (price, qty) ascending
    last_update_id: int
    ts_exchange: Optional[int]
    ts_recv: int

    @property
    def mid_price(self) -> Optional[Decimal]: ...

    @property
    def spread_bps(self) -> Optional[float]: ...

    @property
    def imbalance(self) -> float: ...


@dataclass(frozen=True, slots=True)
class FundingRateEvent:
    """Funding rate event for perpetual futures."""
    symbol: str
    mark_price: Decimal
    index_price: Decimal
    funding_rate: Decimal
    next_funding_time: int
    ts_exchange: int
    ts_recv: int

    @property
    def annualized_rate(self) -> float: ...


@dataclass(frozen=True, slots=True)
class TickerEvent:
    """24hr rolling window ticker."""
    symbol: str
    last_price: Decimal
    bid_price: Decimal
    ask_price: Decimal
    volume_24h: Decimal
    price_change_pct: float
    ts_recv: int
```

### 4.2 New Topics (to add to `backtester/types/topics.py`)

```python
# Live market data topics
T_ORDERBOOK = "mkt.orderbook"
T_FUNDING = "mkt.funding"
T_TICKER = "mkt.ticker"
T_TRADE = "mkt.trade"  # Optional: raw trades
```

---

## 5. Binance WebSocket Reference

### 5.1 Endpoints

| Environment | Base URL | Notes |
|-------------|----------|-------|
| **Spot Production** | `wss://stream.binance.com:9443` | Primary |
| **Spot Alternate** | `wss://stream.binance.com:443` | Firewall-friendly |
| **Spot Data-Only** | `wss://data-stream.binance.vision` | No user data |
| **Futures USD-M** | `wss://fstream.binance.com` | Perpetuals |
| **Spot Testnet** | `wss://testnet.binance.vision/ws` | Paper trading |
| **Futures Testnet** | `wss://stream.binancefuture.com` | Paper trading |

### 5.2 Stream Types

| Stream | Pattern | Update Speed | Use Case |
|--------|---------|--------------|----------|
| **Kline** | `<symbol>@kline_<interval>` | 2000ms | Primary price data |
| **Partial Depth** | `<symbol>@depth<levels>@100ms` | 100ms | Mean reversion |
| **Book Ticker** | `<symbol>@bookTicker` | Real-time | Best bid/ask |
| **Mark Price** | `<symbol>@markPrice` | 3000ms | Funding rate |
| **Agg Trade** | `<symbol>@aggTrade` | Real-time | Trade flow |

### 5.3 Connection Limits

| Limit | Value | Mitigation |
|-------|-------|------------|
| Max streams per connection | 1024 | Combined streams |
| Connection attempts | 300 / 5 min | Exponential backoff |
| Inbound messages | 5 / second | Batch subscriptions |
| Connection lifetime | 24 hours | Auto-refresh at 23h |
| Ping/Pong timeout | 60 seconds | Respond immediately |

---

## 6. File Structure

```
backtester/
└── data/
    └── live/
        ├── __init__.py
        ├── manager.py          # LiveDataFeedManager
        ├── connection.py       # ConnectionManager
        ├── router.py           # MessageRouter
        ├── handlers.py         # KlineHandler, BookHandler, FundingHandler
        ├── synchronizer.py     # FrameSynchronizer
        ├── publisher.py        # EventPublisher
        ├── health.py           # HealthMonitor, FeedHealth
        ├── rest_client.py      # BinanceRESTClient
        ├── symbols.py          # SymbolRegistry
        ├── logger.py           # MessageLogger (optional)
        └── types.py            # Candle, OrderBookSnapshot, FundingRateEvent

tests/
└── unit/
    └── data/
        └── live/
            ├── test_connection.py
            ├── test_router.py
            ├── test_handlers.py
            ├── test_synchronizer.py
            ├── test_publisher.py
            └── test_rest_client.py
└── integration/
    └── test_live_feed_e2e.py
```

---

## 7. Implementation Plan

### Phase 1: Core Infrastructure (P0) - ~3 days
- [ ] Define data types: `Candle`, `OrderBookSnapshot`, `FundingRateEvent` in `types.py`
- [ ] Add topics: `T_ORDERBOOK`, `T_FUNDING`, `T_TICKER`
- [ ] Implement `SymbolRegistry`
- [ ] Implement `ConnectionManager` with reconnection
- [ ] Implement `MessageRouter`
- [ ] Unit tests for parsing and routing

### Phase 2: Kline Feed (P0) - ~2 days
- [ ] Implement `KlineHandler`
- [ ] Implement basic `EventPublisher`
- [ ] Implement `FrameSynchronizer`
- [ ] Integration test with testnet

### Phase 3: Manager & Health (P0) - ~2 days
- [ ] Implement `LiveDataFeedManager`
- [ ] Implement `HealthMonitor`
- [ ] Graceful shutdown handling
- [ ] Health event emission

### Phase 4: Order Book & Funding (P1) - ~2 days
- [ ] Implement `BookHandler`
- [ ] Implement `FundingHandler`
- [ ] Add `on_orderbook()` / `on_funding()` to Strategy interface
- [ ] Integration tests

### Phase 5: REST & Recovery (P1) - ~2 days
- [ ] Implement `BinanceRESTClient`
- [ ] Historical funding rate fetching
- [ ] Gap detection and recovery

### Phase 6: Persistence & Polish (P2) - ~2 days
- [ ] Implement `MessageLogger`
- [ ] E2E paper trading test
- [ ] Documentation

---

## 8. Configuration Schema

### 8.1 TOML Configuration

```toml
# configs/paper_trading.toml

[live_feed]
mode = "live"  # "backtest" | "live"
use_testnet = true

[live_feed.symbols]
spot = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
futures = ["BTCUSDT", "ETHUSDT"]

[live_feed.kline]
timeframe = "1m"
sync_mode = "aligned"  # "async" | "aligned"
sync_timeout_ms = 5000

[live_feed.orderbook]
enabled = true
depth = 20
update_ms = 100

[live_feed.funding]
enabled = true

[live_feed.connection]
reconnect_delay_base_ms = 1000
reconnect_delay_max_ms = 30000
reconnect_max_attempts = 10
stale_threshold_ms = 30000

[live_feed.logging]
enabled = false
retention_days = 7
```

---

## 9. Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Concurrency model** | asyncio | Matches existing codebase, good for I/O-bound |
| **JSON parsing** | orjson | 10x faster than stdlib |
| **Numeric types** | float for OHLCV, Decimal for prices/qty | Performance vs precision |
| **Timestamps** | Store both ts_exchange and ts_recv | Critical for latency debugging |
| **Order book** | Partial depth (not full book) | Simpler, sufficient for initial use |
| **Backpressure** | Bounded queue + drop oldest | Prevents memory blowup |
| **Coalescing** | Order book only | High-frequency, only latest matters |
| **Reconnection** | Exponential backoff + jitter | Prevents thundering herd |
| **Health check** | 30s stale threshold | Balance detection speed vs false positives |

---

## 10. Dependencies

```toml
# pyproject.toml additions
[project.dependencies]
websockets = ">=12.0"
aiohttp = ">=3.9"
orjson = ">=3.9"
```

---

## Appendix A: Binance Message Examples

### Kline Message
```json
{
  "stream": "btcusdt@kline_1m",
  "data": {
    "e": "kline",
    "E": 1672515782136,
    "s": "BTCUSDT",
    "k": {
      "t": 1672515780000,
      "T": 1672515839999,
      "s": "BTCUSDT",
      "i": "1m",
      "o": "16500.00",
      "c": "16510.50",
      "h": "16515.00",
      "l": "16495.00",
      "v": "1000.5",
      "n": 100,
      "x": false,
      "q": "16505000.00"
    }
  }
}
```

### Partial Depth Message
```json
{
  "stream": "btcusdt@depth20@100ms",
  "data": {
    "lastUpdateId": 160,
    "bids": [["16500.00", "10.5"], ["16499.50", "5.2"]],
    "asks": [["16500.50", "8.3"], ["16501.00", "12.1"]]
  }
}
```

### Mark Price Message (Futures)
```json
{
  "stream": "btcusdt@markPrice",
  "data": {
    "e": "markPriceUpdate",
    "E": 1672515782136,
    "s": "BTCUSDT",
    "p": "16500.50",
    "i": "16498.00",
    "r": "0.00010000",
    "T": 1672531200000
  }
}
```
