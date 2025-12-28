"""
WebSocket Connection Manager for live data feeds.

Handles WebSocket lifecycle including:
- Connection establishment with timeout
- Exponential backoff reconnection with jitter
- Ping/pong keepalive handling
- 24-hour connection refresh (Binance limit)
- Connection-level metrics and health tracking
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

import aiohttp

from backtester.data.live.config import ConnectionConfig
from backtester.data.live.errors import ConnectionError
from backtester.data.live.types import (
    ConnectionHealth,
    ConnectionMetrics,
    ConnectionState,
)

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Manages a single WebSocket connection with automatic reconnection.

    Responsibilities:
    - WebSocket connection lifecycle (connect, reconnect, close)
    - Exponential backoff with jitter for reconnection
    - Ping/pong keepalive handling
    - 24-hour connection refresh (Binance limit)
    - Connection-level metrics tracking

    The ConnectionManager does NOT parse messages - it just delivers raw
    bytes/text to the registered callback. Message routing is handled
    by MessageRouter.

    Usage:
        async def on_message(data: dict) -> None:
            print(f"Received: {data}")

        manager = ConnectionManager(
            url="wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1m",
            config=ConnectionConfig(...),
            on_message=on_message,
        )
        await manager.connect()
        # ... later ...
        await manager.close()
    """

    def __init__(
        self,
        url: str,
        config: ConnectionConfig,
        on_message: Callable[[dict[str, Any], int], Awaitable[None]],
        on_state_change: Optional[Callable[[ConnectionState], Awaitable[None]]] = None,
        on_error: Optional[Callable[[Exception], Awaitable[None]]] = None,
        name: str = "connection",
    ) -> None:
        """
        Initialize the connection manager.

        Args:
            url: WebSocket URL to connect to
            config: Connection configuration
            on_message: Async callback for received messages (data, recv_ts_ms)
            on_state_change: Optional callback for state changes
            on_error: Optional callback for errors
            name: Name for logging purposes
        """
        self._url = url
        self._config = config
        self._on_message = on_message
        self._on_state_change = on_state_change
        self._on_error = on_error
        self._name = name

        # State
        self._state = ConnectionState.DISCONNECTED
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None

        # Tasks
        self._receive_task: Optional[asyncio.Task[None]] = None
        self._ping_task: Optional[asyncio.Task[None]] = None
        self._refresh_task: Optional[asyncio.Task[None]] = None

        # Reconnection state
        self._reconnect_attempt = 0
        self._should_reconnect = True

        # Metrics
        self._metrics = ConnectionMetrics()
        self._connected_at: Optional[datetime] = None
        self._last_message_at: Optional[datetime] = None
        self._last_error: Optional[str] = None
        self._last_error_at: Optional[datetime] = None

        # Shutdown coordination
        self._shutdown_event = asyncio.Event()

    @property
    def state(self) -> ConnectionState:
        """Current connection state."""
        return self._state

    @property
    def is_connected(self) -> bool:
        """Check if currently connected."""
        return self._state == ConnectionState.CONNECTED

    @property
    def url(self) -> str:
        """Connection URL."""
        return self._url

    @property
    def metrics(self) -> ConnectionMetrics:
        """Connection metrics."""
        return self._metrics

    async def _set_state(self, new_state: ConnectionState) -> None:
        """Update state and notify listener."""
        old_state = self._state
        self._state = new_state

        if old_state != new_state:
            logger.debug(f"[{self._name}] State: {old_state.value} -> {new_state.value}")
            if self._on_state_change:
                try:
                    await self._on_state_change(new_state)
                except Exception as e:
                    logger.warning(f"[{self._name}] State change callback error: {e}")

    async def connect(self) -> None:
        """
        Establish WebSocket connection.

        Raises:
            ConnectionError: If connection fails after all retries
        """
        if self._state in (ConnectionState.CONNECTED, ConnectionState.CONNECTING):
            logger.warning(f"[{self._name}] Already connected or connecting")
            return

        self._should_reconnect = True
        self._shutdown_event.clear()
        await self._connect_with_retry()

    async def _connect_with_retry(self) -> None:
        """Connect with exponential backoff retry logic."""
        await self._set_state(ConnectionState.CONNECTING)

        while (
            self._should_reconnect
            and self._reconnect_attempt <= self._config.max_reconnect_attempts
        ):
            try:
                await self._establish_connection()
                self._reconnect_attempt = 0
                await self._set_state(ConnectionState.CONNECTED)

                # Start background tasks
                self._receive_task = asyncio.create_task(
                    self._receive_loop(), name=f"{self._name}_receive"
                )
                self._ping_task = asyncio.create_task(self._ping_loop(), name=f"{self._name}_ping")
                self._refresh_task = asyncio.create_task(
                    self._refresh_loop(), name=f"{self._name}_refresh"
                )
                return

            except Exception as e:
                self._reconnect_attempt += 1
                self._metrics.errors += 1
                self._last_error = str(e)
                self._last_error_at = datetime.now(timezone.utc)

                if self._reconnect_attempt > self._config.max_reconnect_attempts:
                    await self._set_state(ConnectionState.DISCONNECTED)
                    raise ConnectionError(
                        f"Failed to connect after {self._config.max_reconnect_attempts} attempts",
                        url=self._url,
                        reconnect_attempt=self._reconnect_attempt,
                        component="ConnectionManager",
                    ) from e

                # Calculate backoff with jitter
                delay = self._calculate_backoff_delay()
                logger.warning(
                    f"[{self._name}] Connection failed (attempt {self._reconnect_attempt}), "
                    f"retrying in {delay:.2f}s: {e}"
                )
                await self._set_state(ConnectionState.RECONNECTING)
                await asyncio.sleep(delay)

    async def _establish_connection(self) -> None:
        """Establish the actual WebSocket connection."""
        # Create session if needed
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._config.connect_timeout_s)
            self._session = aiohttp.ClientSession(timeout=timeout)

        # Connect WebSocket
        logger.info(f"[{self._name}] Connecting to {self._url}")
        self._ws = await self._session.ws_connect(
            self._url,
            heartbeat=self._config.ping_interval_s,
            receive_timeout=self._config.ping_timeout_s * 3,
        )

        self._connected_at = datetime.now(timezone.utc)
        self._metrics.connected_at = time.monotonic()
        logger.info(f"[{self._name}] Connected successfully")

    def _calculate_backoff_delay(self) -> float:
        """Calculate exponential backoff delay with jitter."""
        base_delay = self._config.base_reconnect_delay_s
        max_delay = self._config.max_reconnect_delay_s
        jitter = self._config.reconnect_jitter

        # Exponential backoff: base * 2^attempt
        delay = base_delay * (2 ** (self._reconnect_attempt - 1))
        delay = min(delay, max_delay)

        # Add jitter: ±jitter%
        jitter_range = delay * jitter
        delay += random.uniform(-jitter_range, jitter_range)

        return float(max(0.1, delay))  # Minimum 100ms

    async def _receive_loop(self) -> None:
        """Main loop for receiving WebSocket messages."""
        if self._ws is None:
            return

        try:
            async for msg in self._ws:
                if self._shutdown_event.is_set():
                    break

                recv_ts = int(time.time() * 1000)
                self._last_message_at = datetime.now(timezone.utc)
                self._metrics.last_message_at = time.monotonic()
                self._metrics.messages_received += 1

                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        import orjson

                        data = orjson.loads(msg.data)
                        self._metrics.bytes_received += len(msg.data)
                        await self._on_message(data, recv_ts)
                    except Exception as e:
                        logger.error(f"[{self._name}] Message handling error: {e}")
                        self._metrics.errors += 1

                elif msg.type == aiohttp.WSMsgType.BINARY:
                    logger.debug(f"[{self._name}] Received binary message (ignored)")

                elif msg.type == aiohttp.WSMsgType.PING:
                    if self._ws:
                        await self._ws.pong(msg.data)
                    self._metrics.pongs_received += 1

                elif msg.type == aiohttp.WSMsgType.PONG:
                    self._metrics.pongs_received += 1

                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    logger.info(f"[{self._name}] Server closed connection")
                    break

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"[{self._name}] WebSocket error: {self._ws.exception()}")
                    self._metrics.errors += 1
                    break

        except asyncio.CancelledError:
            logger.debug(f"[{self._name}] Receive loop cancelled")
            raise
        except Exception as e:
            logger.error(f"[{self._name}] Receive loop error: {e}")
            self._last_error = str(e)
            self._last_error_at = datetime.now(timezone.utc)
            self._metrics.errors += 1

            if self._on_error:
                try:
                    await self._on_error(e)
                except Exception as cb_err:
                    logger.warning(f"[{self._name}] Error callback failed: {cb_err}")

        # Trigger reconnection if not shutting down
        if self._should_reconnect and not self._shutdown_event.is_set():
            logger.info(f"[{self._name}] Connection lost, initiating reconnect")
            self._metrics.reconnections += 1
            asyncio.create_task(self._reconnect())

    async def _ping_loop(self) -> None:
        """Send periodic pings if no messages received."""
        try:
            while not self._shutdown_event.is_set():
                await asyncio.sleep(self._config.ping_interval_s)

                if self._ws is None or self._ws.closed:
                    break

                # Check if we've received a message recently
                if self._metrics.last_message_at is not None:
                    time_since_message = time.monotonic() - self._metrics.last_message_at
                    if time_since_message < self._config.ping_interval_s:
                        continue

                # Send ping
                try:
                    await self._ws.ping()
                    self._metrics.pings_sent += 1
                    logger.debug(f"[{self._name}] Sent ping")
                except Exception as e:
                    logger.warning(f"[{self._name}] Ping failed: {e}")

        except asyncio.CancelledError:
            pass

    async def _refresh_loop(self) -> None:
        """Refresh connection before 24-hour limit."""
        try:
            await asyncio.sleep(self._config.max_connection_age_s)

            if not self._shutdown_event.is_set():
                logger.info(f"[{self._name}] Refreshing connection (24h limit approaching)")
                await self._reconnect()

        except asyncio.CancelledError:
            pass

    async def _reconnect(self) -> None:
        """Close current connection and reconnect."""
        await self._cleanup_connection()
        await self._set_state(ConnectionState.RECONNECTING)
        await self._connect_with_retry()

    async def _cleanup_connection(self) -> None:
        """Clean up current connection resources."""
        # Cancel tasks
        for task in [self._receive_task, self._ping_task, self._refresh_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._receive_task = None
        self._ping_task = None
        self._refresh_task = None

        # Close WebSocket
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None

    async def close(self) -> None:
        """Close the connection gracefully."""
        logger.info(f"[{self._name}] Closing connection")
        self._should_reconnect = False
        self._shutdown_event.set()

        await self._set_state(ConnectionState.CLOSING)
        await self._cleanup_connection()

        # Close session
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

        await self._set_state(ConnectionState.CLOSED)
        logger.info(f"[{self._name}] Connection closed")

    def get_health(self) -> ConnectionHealth:
        """Get current connection health snapshot."""
        return ConnectionHealth(
            state=self._state,
            url=self._url,
            connected_since=self._connected_at,
            last_message_at=self._last_message_at,
            reconnect_count=self._metrics.reconnections,
            message_count=self._metrics.messages_received,
            error_count=self._metrics.errors,
            last_error=self._last_error,
            last_error_at=self._last_error_at,
            latency_ms=self._metrics.avg_latency_ms,
        )
