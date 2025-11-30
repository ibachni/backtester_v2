from __future__ import annotations

import asyncio
import csv
from dataclasses import asdict
from datetime import datetime, timezone
from functools import singledispatchmethod
from pathlib import Path
from typing import Any, Optional, Protocol, Union, runtime_checkable

import orjson

from backtester.config.configs import AuditConfig, RunContext
from backtester.core.bus import Envelope
from backtester.types.aliases import AuditRecord
from backtester.types.topics import (
    T_ACCOUNT_SNAPSHOT,
    T_CANDLES,
    T_CONTROL,
    T_FILLS,
    T_LOG,
    T_ORDERS_ACK,
    T_ORDERS_CANCELED,
    T_ORDERS_REJECTED,
    T_REPORT,
    T_TRADE_EVENT,
)
from backtester.types.types import (
    Candle,
    ControlEvent,
    Fill,
    LimitOrderIntent,
    MarketOrderIntent,
    OrderAck,
    OrderIntent,
    PortfolioSnapshot,
    StopLimitOrderIntent,
    StopMarketOrderIntent,
    TradeEvent,
    ValidatedLimitOrderIntent,
    ValidatedMarketOrderIntent,
    ValidatedOrderIntent,
    ValidatedStopLimitOrderIntent,
    ValidatedStopMarketOrderIntent,
)


@runtime_checkable
class BusEvent(Protocol):
    """Typing for any event coming from the Pub/Sub bus."""

    pass


class AuditWriter:
    """
    - Ideally non blocking, since disk I/) is slow
    - Accumulate events in memory buffer and flush them in chunks
    (e.g., every 1000 events, or 500ms)
    - Granularity levels:
        Level 0: Final PnL and statistics
        Level 1: Trades (only filled trades and daily equity)
        Level 2: Orders (All Submissions, cancels, modifications)
        Level 3: Full debug (all of the above + internal calc logs and
        market snapshots)
    - Should allow for pluggable StorageBackends
    - Dual timestamping (sim and wall-clock)
    - Events must be written in the strict sequence they occured
    - UniqueIdentifiers (each Order/Trade has unique ID)
        - > Parent/Child linking (partial fills reference original Order ID)


    Achieve this:
    - Stop logging candles
    - Split the output:
        - sim_trades.csv (result analysis)
        - sim_debug.jsonl (deep-dive debugging)
        - Logging not in the modules



    Simple NDJSON audit logger. Write dict-like events to a file, one JSON per line.

    Core functionality:
    - Append-only JSONL
    - Consistent schema:

    component
        What it is: the subsystem emitting the line.
        Why: lets you filter noise (e.g., keep exec_sim INFO, strategy DEBUG).

    level
        What it is: importance/severity for filtering.
        Suggested ladder (monotone):
            DEBUG:  very chatty, step-by-step traces
            INFO: normal lifecycle events (BAR, ACK, FILL, CANCEL)
            WARN: unusual but non-fatal (dropped log lines, truncated payloads, skipped notional
            check)
            ERROR: something failed (writer I/O error, irrecoverable validation bug)

    parent_id:
        - Causal linkage, ties event to the immediately preceding event in its causal chain
        - Candle -> None
        - OrderIntent -> Candle
        - ACK / CANCEL -> OrderIntent
        - FILL -> ORDERIntent
        - For derived logs, e.g., account snapshot

    # TODO: Addition of _should_log, which compares the level by component with the provided level
    # Goal: Determine whether it is worth logging.

    Asynchronous subscriber, Synchronous writer.
    Architecture:
    [Async Bus] -> await on_event() -> [Memory Queue] -> [Worker Thread] -> [Disk IO]


    Working:
    - 1. The ProducerStrategy / exchange calls audit.on_event(event)
        - Async but no await; simply pushes object into a queue.SimpleQueue
    - 2. The Buffer (Queue): for many event quickly generated, it waits to be processed
    - 3. The consumer (Worker thread):
        - _io_worker loop runs a seperate background thread
        - Pulls an item off the queue
        - sends it to the respective stream

    Now:
    - Add all event types!

    Post MVP additions:
    - Use dictionary mapping topics to file handlers / or configuration-driven apporoach
    - Batching (wait on rows, then write some)
    - Filter for debug logs (if event.evel < self.min_level)
    - __init__ hardcoded to disc
    - Zero-copy serialization
    - Pluggable storage (SQL)
    - Reading/ parsing back into the system
    - new equity_curve.csv file (for AccountSnapshots)
    - Crash recovery (flush on signal)
    - Scalability
        - (Binary storage parquet/HDF5 -> pandas.DataFrame.to_parquet (batched)
        - if file too large, close and start debug_02.jsonl (to prevent file system errors)
    """

    def __init__(self, run_ctx: RunContext, cfg: AuditConfig) -> None:
        self._run_ctx = run_ctx
        self._cfg = cfg
        self.root = Path(cfg.log_dir) / run_ctx.run_id
        self.root.mkdir(parents=True, exist_ok=True)
        self.capture_data = cfg.capture_market_data

        # AuditWriter is a subscriber
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=10000)

        # File handles
        self._files: dict[str, Any] = {}
        self._csv_writers: dict[str, csv.DictWriter] = {}
        self._worker_task: Optional[asyncio.Task] = None

        # Obs
        self._dropped: int = 0

    def start(self) -> None:
        """Start the background writer thread."""
        loop = asyncio.get_event_loop()
        self._worker_task = loop.create_task(self._io_worker())

    def stop(self) -> None:
        """Signal worker to finish queue and close files."""
        if self._worker_task and not self._worker_task.done():
            # Enqueue sentinel
            try:
                self._queue.put_nowait(None)
            except asyncio.QueueFull:
                pass

    # --- Public Subscriber Interface (Async) ---

    async def on_event(self, event: BusEvent) -> None:
        """
        Registered callback for the Pub/Sub bus.
        Non-blocking: pushes to async queue.

        Note: Per ADR 022, this is the primary interface for components
        that use the event bus. For non-bus contexts (e.g., downloader),
        use the synchronous emit() method instead.
        """
        try:
            # put_nowait in live, put in backtester
            await self._queue.put(event)
        except asyncio.QueueFull:
            self._dropped += 1
            # Defensive: log to stderr if queue is full
            print(f"WARNING: Audit queue full, dropping event {type(event)}")

    # --- Public Direct Interface (Sync) ---

    def emit(
        self,
        *,
        component: str,
        event: str,
        level: str = "INFO",
        sim_time: Optional[int] = None,
        payload: Optional[dict[str, Any]] = None,
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        parent_id: Optional[str] = None,
    ) -> None:
        """
        Synchronous emit for non-bus contexts (e.g., downloader, standalone scripts).

        This method bypasses the async queue and writes directly to the debug log.
        Use this ONLY when you don't have access to the event bus, such as in:
        - Data ingestion pipelines (downloader)
        - Standalone utilities
        - Legacy components not yet migrated to ADR 022

        For bus-connected components, publish LogEvent to T_LOG instead.

        Parameters
        ----------
        component : str
            Subsystem name (e.g., "ingest", "exec_sim", "strategy")
        event : str
            Event name (e.g., "ORDER_DUPLICATE", "RUN_START")
        level : str
            Log level: "DEBUG", "INFO", "WARN", "ERROR"
        simple : bool
            If True, write a simplified record format
        sim_time : Optional[int]
            Simulation time in milliseconds (None for wall-clock only)
        payload : Optional[dict]
            Additional structured data
        symbol : Optional[str]
            Symbol context for filtering
        order_id : Optional[str]
            Order ID for tracing
        parent_id : Optional[str]
            Parent event ID for causal chains
        """
        record: AuditRecord = {
            "component": component,
            "event": event,
            "level": level,
            "ts_wall": datetime.now(timezone.utc).isoformat(),
            "payload": payload or {},
        }

        if sim_time is not None:
            record["sim_time"] = sim_time
        if symbol is not None:
            record["symbol"] = symbol
        if order_id is not None:
            record["order_id"] = order_id
        if parent_id is not None:
            record["parent_id"] = parent_id

        # Write directly to debug log (synchronous)
        self._write_json("debug.jsonl", record)

    # --- Internal Worker Loop (Sync) ---

    async def _io_worker(self) -> None:
        """
        Async task that handles file opening, writing, and flushing.
        Replaces the threaded worker.
        """
        self._open_streams()

        while True:
            item = await self._queue.get()

            if item is None:  # Sentinel received
                self._close_streams()
                break
            try:
                self._route_event(item)
            except Exception as e:
                print(f"AuditWriter Error: {e}")
            finally:
                self._queue.task_done()

    def _route_event(self, event: Envelope) -> None:
        """
        Decides which stream (file) the event belongs to.
        """
        # 1. LEDGER STREAM (Transactions - CSV)
        # Highly structured, used for PnL calculation.
        if event.topic == T_FILLS and isinstance(event.payload, Fill):
            self._write_json("ledger.jsonl", self.fill_to_payload(event.payload))

        elif event.topic == T_ORDERS_ACK and isinstance(event.payload, OrderAck):
            self._write_json("ledger.jsonl", self.ack_to_payload(event.payload))

        elif event.topic == T_ORDERS_CANCELED and isinstance(event.payload, OrderAck):
            self._write_json("ledger.jsonl", self.intent_to_payload(event.payload))

        elif event.topic == T_ORDERS_REJECTED and isinstance(event.payload, OrderAck):
            self._write_json("ledger.jsonl", self.intent_to_payload(event.payload))

        elif event.topic == T_TRADE_EVENT and isinstance(event.payload, TradeEvent):
            self._write_json("ledger.jsonl", self.trade_event_to_payload(event.payload))

        # 2. ACCOUNT STREAM (CSV)
        elif event.topic == T_ACCOUNT_SNAPSHOT and isinstance(event.payload, PortfolioSnapshot):
            self._write_csv("account.csv", self.portfolio_to_payload(event.payload))

        # 3. DEBUG STREAM (Context - JSONL)
        # Unstructured, used for debugging strategy logic.
        elif event.topic in (
            T_ORDERS_REJECTED,
            T_ORDERS_CANCELED,
            T_CONTROL,
            T_LOG,
        ):
            # Wrap with topic for clarity in the JSON file
            record = {
                "topic": event.topic,
                "ts_wall": datetime.now(timezone.utc).isoformat(),
                "payload": asdict(event.payload)
                if hasattr(event.payload, "__dataclass_fields__")
                else str(event.payload),
            }
            self._write_json("debug.jsonl", record)

        # 4. REPORT
        elif event.topic == T_REPORT:
            self._write_json("report.json", event.payload)

        # 5. DATA STREAM (Market Data - JSONL)
        # High volume, (self.capture_data activated)
        elif self.capture_data and event.topic == T_CANDLES and isinstance(event.payload, Candle):
            self._write_json("market_data.jsonl", asdict(event))

    # --- Writers ---

    def _write_csv(self, filename: str, data: dict) -> None:
        """Lazy-init CSV writer ensures headers are correct based on first record."""
        if filename not in self._files:
            f = (self.root / filename).open("w", newline="")
            self._files[filename] = f

            writer = csv.DictWriter(f, fieldnames=data.keys())
            writer.writeheader()
            self._csv_writers[filename] = writer

        # Write the row
        self._csv_writers[filename].writerow(data)

    def _write_json(self, filename: str, data: dict) -> None:
        if filename not in self._files:
            self._files[filename] = (self.root / filename).open("ab")

        # Use default=str to handle UUIDs, Decimals, Dates safely
        line = orjson.dumps(data, default=str)
        self._files[filename].write(line + b"\n")

    def _open_streams(self) -> None:
        # Files are opened lazily in _write methods, but we prepare directory
        pass

    def _close_streams(self) -> None:
        for f in self._files.values():
            try:
                f.flush()
                f.close()
            except Exception as e:
                print(e)

    @property
    def run_id(self) -> str:
        return self._run_ctx.run_id

    # --- Stats ---

    def get_stats(self) -> dict[str, int]:
        return {"events_dropped": self._dropped}

    # --- OrderIntent to payload ---

    def market_intent_to_payload(
        self, intent: Union[MarketOrderIntent, ValidatedMarketOrderIntent]
    ) -> dict[str, Any]:
        return {
            "reduce_only": intent.reduce_only,
            "validated": getattr(intent, "validated", None),
        } | self.abc_intent_to_payload(intent)

    def limit_intent_to_payload(
        self, intent: Union[LimitOrderIntent, ValidatedLimitOrderIntent]
    ) -> dict[str, Any]:
        return {
            "price": intent.price,
            "reduce_only": intent.reduce_only,
            "post_only": intent.post_only,
            "validated": getattr(intent, "validated", None),
        } | self.abc_intent_to_payload(intent)

    def stop_market_intent_to_payload(
        self, intent: Union[StopMarketOrderIntent, ValidatedStopMarketOrderIntent]
    ) -> dict[str, Any]:
        return {
            "stop_price": intent.stop_price,
            "reduce_only": intent.reduce_only,
            "post_only": intent.post_only,
            "validated": getattr(intent, "validated", None),
        } | self.abc_intent_to_payload(intent)

    def stop_limit_intent_to_payload(
        self, intent: Union[StopLimitOrderIntent, ValidatedStopLimitOrderIntent]
    ) -> dict[str, Any]:
        return {
            "price": intent.price,
            "stop_price": intent.stop_price,
            "reduce_only": intent.reduce_only,
            "post_only": intent.post_only,
            "validated": getattr(intent, "validated", None),
        } | self.abc_intent_to_payload(intent)

    def abc_intent_to_payload(
        self, intent: Union[OrderIntent, ValidatedOrderIntent]
    ) -> dict[str, Any]:
        return {
            "symbol": intent.symbol,
            "market": str(intent.market),
            "side": str(intent.side),
            "id": intent.id,
            "ts_utc": str(intent.ts_utc),
            "qty": str(intent.qty),
            "strategy_id": intent.strategy_id,
            "tif": str(intent.tif),
            "tags": intent.tags,
        }

    @singledispatchmethod
    def intent_to_payload(self, intent: OrderIntent) -> dict[str, Any]:
        """Fallback for unknown OrderIntent subtypes."""
        return self.abc_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: ValidatedOrderIntent) -> dict[str, Any]:
        return self.abc_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: MarketOrderIntent) -> dict[str, Any]:
        return self.market_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: ValidatedMarketOrderIntent) -> dict[str, Any]:
        return self.market_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: LimitOrderIntent) -> dict[str, Any]:
        return self.limit_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: ValidatedLimitOrderIntent) -> dict[str, Any]:
        return self.limit_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: StopMarketOrderIntent) -> dict[str, Any]:
        return self.stop_market_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: ValidatedStopMarketOrderIntent) -> dict[str, Any]:
        return self.stop_market_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: StopLimitOrderIntent) -> dict[str, Any]:
        return self.stop_limit_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: ValidatedStopLimitOrderIntent) -> dict[str, Any]:
        return self.stop_limit_intent_to_payload(intent)

    # --- Other type to payload conversion ---

    def ack_to_payload(self, ack: OrderAck) -> dict[str, Any]:
        raw_tags = getattr(ack, "tags", None)
        if isinstance(raw_tags, dict):
            tags: dict[str, Any] = dict(raw_tags)
        else:
            tags = {} if raw_tags is None else {"_raw": repr(raw_tags)}

        return {
            "intent_id": ack.intent_id,
            "strategy_id": getattr(ack, "strategy_id", None),
            "component": getattr(ack, "component", None),
            "symbol": getattr(ack, "symbol", None),
            "market": str(getattr(ack, "market", None)),
            "side": str(getattr(ack, "side", None)),
            "status": str(getattr(ack, "status", None)),
            "reason": getattr(ack, "reason", None),
            "errors": getattr(ack, "errors", None),
            "warnings": getattr(ack, "warnings", None),
            "ts_utc": getattr(ack, "ts_utc", None),
            # optional (MVP, backtest only)
            "exchange_order_id": getattr(ack, "exchange_order_id", None),
            "router_order_id": getattr(ack, "router_order_id", None),
            "venue_ts": getattr(ack, "venue_ts", None),
            "venue": getattr(ack, "venue", None),
            "seq": getattr(ack, "seq", 0),
            "tags": tags,
        }

    def candle_to_payload(self, candle: Candle) -> dict[str, Any]:
        return {
            "symbol": candle.symbol,
            "timeframe": candle.timeframe,
            "start_ms": candle.start_ms,
            "end_ms": candle.end_ms,
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
            "volume": float(candle.volume),
            "trades": candle.trades,
            "is_final": candle.is_final,
        }

    def fill_to_payload(self, fill: Fill) -> dict[str, Any]:
        return {
            "fill_id": fill.fill_id,
            "order_id": fill.order_id,
            "symbol": fill.symbol,
            "market": str(fill.market),
            "qty": str(fill.qty),
            "price": str(fill.price),
            "side": str(fill.side.value),
            "ts": fill.ts,
            "venue": fill.venue,
            "liquidity_flag": str(fill.liquidity_flag),
            "fees_explicit": str(fill.fees_explicit),
            "rebates": str(fill.rebates),
            "slippage_components": fill.slippage_components,
            "tags": list(fill.tags),
        }

    def trade_event_to_payload(self, trade: TradeEvent) -> dict[str, Any]:
        return {
            "ts": trade.ts,
            "symbol": trade.symbol,
            "side": trade.side,
            "qty": trade.qty,
            "price": trade.price,
            "fee": trade.fee,
            "venue": trade.venue,
            "liquidity_flag": trade.liquidity_flag,
            "idempotency_key": trade.idempotency_key,
        }

    def portfolio_to_payload(self, snap: PortfolioSnapshot) -> dict[str, Any]:
        return {
            "ts": snap.ts,
            "base_ccy": snap.base_ccy,
            "cash": snap.cash,
            "equity": snap.equity,
            "upnl": snap.upnl,
            "rpnl": snap.rpnl,
            "gross_exposure": snap.gross_exposure,
            "net_exposure": snap.net_exposure,
            "fees_paid": snap.fees_paid,
            "positions": snap.positions,
        }

    def control_to_payload(self, control: ControlEvent) -> dict[str, Any]:
        return {
            "type": control.type,
            "source": control.source,
            "ts_utc": control.ts_utc,
            "run_id": control.run_id,
            "details": dict(control.details) if control.details is not None else {},
        }
