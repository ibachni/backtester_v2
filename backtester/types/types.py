from __future__ import annotations

from abc import ABC
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Deque, Literal, Mapping, Optional, Tuple

from pydantic import BaseModel, ConfigDict

from backtester.types.aliases import Symbol, Tags, UnixMillis

LOG_LEVELS = {"DEBUG": 10, "INFO": 20, "WARN": 30, "ERROR": 40}

# -------- Enums --------

ZERO = Decimal("0.0")


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class Market(str, Enum):
    SPOT = "spot"
    OPTION = "option"
    FUTURE = "future"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"  # stop-market
    STOP_LIMIT = "stop_limit"
    STOP_MARKET = "stop_market"


# class AckStatus(str, Enum):
#     ACCEPTED = "accepted"
#     REJECTED = "rejected"
#     DUPLICATE = "duplicate"
#     THROTTLED = "throttled"
#     ERROR = "error"


class OrderStatus(str, Enum):
    NEW = "NEW"
    VALIDATED = "VALIDATED"
    ACK = "ACK"
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"


class _StrategyState(str, Enum):
    INITIALIZED = "initialized"
    RUNNING = "running"
    STOPPED = "stopped"


class TimeInForce(str, Enum):
    GTC = "gtc"  # good till canceled
    IOC = "ioc"  # immediate or cancel (cancel unfilled remainder)
    FOK = "fok"  # fill or kill (complete fill required)
    # MOO Market-On-Open
    # LOO Limit-On-Open
    # DTC day-til-canceled (deactivated (not canceled) at the end of the day)
    # DAY canceled if not execute by the close of trading day
    # GTD Good-til-day Good until close of the market on the date specified


class EVENTNAMES(str, Enum):
    """
    define canonical event names
    """

    ERROR = "error"
    CONFIG = "config"


class Liquidity(str, Enum):
    MAKER = "maker"
    TAKER = "taker"
    UNKNOWN = "unknown"


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event: EVENTNAMES
    ts: int  # Millis?
    run_id: int
    git_sha: str
    seed: int
    component: Any


EventType = Literal["eof", "halt", "error"]


@dataclass(frozen=True, slots=True)
class ControlEvent:
    type: EventType  # "eof" indicates end-of-stream
    source: str  # e.g., "mkt.candles"
    ts_utc: int  # emission time (ms, UTC)
    run_id: str | None = None
    details: Mapping[str, Any] | None = None


# --- Downloading ---


@dataclass(frozen=True)
class IngestStats:
    symbol: str
    timeframe: str
    days_attempted: int
    days_written: int
    days_skipped: int
    rows_written: int


# --- Errors ---


class ExecSimError(Exception):
    """Raised for Errors in the Execution Simulator"""


# --- Audit ---


@dataclass
class LogEvent:
    """
    Generic structure for unstructured logging from Strategy/Modules.
    Goes to: debug.jsonl
    """

    level: str  # DEBUG, INFO, WARN, ERROR
    component: str
    msg: str
    payload: dict[str, Any] = field(default_factory=dict)
    sim_time: Optional[int] = None
    wall_time: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# --- Candle ---


@dataclass(frozen=True)
class Candle:
    symbol: str  # e.g. "BTCUSDT"
    timeframe: str  # e.g. "1m", "3m"
    start_ms: int  # UTC ms
    end_ms: int  # UTC ms (close time)
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: Optional[int]  # optional, can be 0
    is_final: Optional[bool] = True


# --- Orders ---


@dataclass
class OrderState:
    """Simulator-internal mutable view of an order."""

    order: ValidatedOrderIntent
    remaining: Decimal
    hash: str
    order_ack: OrderAck
    status: OrderStatus = OrderStatus.ACK

    # Prioritize orders that arrive earlier
    venue_arrival_ts: int = 0  # Priority FIFO
    triggered: bool = False  # for stop orders
    created_seq: int = 0
    # deterministic fingerprint produced by validator


# --- OrderIntents ---


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderIntent(ABC):
    """
    Abstract base class
    """

    symbol: Symbol
    market: Market
    side: Side
    id: str
    ts_utc: UnixMillis
    qty: Decimal
    strategy_id: str
    tif: TimeInForce = TimeInForce.GTC
    tags: Tags = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("OrderIntent.id must be a non-empty string.")
        if self.ts_utc <= 0:
            raise ValueError("OrderIntent.ts_utc must be > 0.")
        if self.qty <= ZERO:
            raise ValueError("OrderIntent.qty must be > 0.")


@dataclass(frozen=True, slots=True, kw_only=True)
class ValidatedOrderIntent(ABC):
    """
    Abstract base class for validated orders (includes hash)
    """

    symbol: Symbol
    market: Market
    side: Side
    id: str
    ts_utc: UnixMillis
    qty: Decimal
    strategy_id: str
    hash: str
    tif: TimeInForce = TimeInForce.GTC
    tags: Tags = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("OrderIntent.id must be a non-empty string.")
        if self.ts_utc <= 0:
            raise ValueError("OrderIntent.ts_utc must be > 0.")
        if self.qty <= ZERO:
            raise ValueError("OrderIntent.qty must be > 0.")


@dataclass(frozen=True, slots=True, kw_only=True)
class MarketOrderIntent(OrderIntent):
    reduce_only: bool = False

    def __post_init__(self) -> None:
        OrderIntent.__post_init__(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class ValidatedMarketOrderIntent(ValidatedOrderIntent):
    reduce_only: bool = False
    validated: bool = True

    def __post_init__(self) -> None:
        ValidatedOrderIntent.__post_init__(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class LimitOrderIntent(OrderIntent):
    price: Decimal
    reduce_only: bool = False
    post_only: bool = False

    def __post_init__(self) -> None:
        OrderIntent.__post_init__(self)
        if self.price <= ZERO:
            raise ValueError("LimitOrderIntent.price must be > 0.")


@dataclass(frozen=True, slots=True, kw_only=True)
class ValidatedLimitOrderIntent(ValidatedOrderIntent):
    price: Decimal
    reduce_only: bool = False
    post_only: bool = False
    validated: bool = True

    def __post_init__(self) -> None:
        ValidatedOrderIntent.__post_init__(self)
        if self.price <= ZERO:
            raise ValueError("LimitOrderIntent.price must be > 0.")


@dataclass(frozen=True, slots=True, kw_only=True)
class StopMarketOrderIntent(OrderIntent):
    stop_price: Decimal
    reduce_only: bool = False
    post_only: bool = False

    def __post_init__(self) -> None:
        OrderIntent.__post_init__(self)
        if self.stop_price <= ZERO:
            raise ValueError("LimitOrderIntent.stop_price must be > 0.")


@dataclass(frozen=True, slots=True, kw_only=True)
class ValidatedStopMarketOrderIntent(ValidatedOrderIntent):
    stop_price: Decimal
    reduce_only: bool = False
    post_only: bool = False
    validated: bool = True

    def __post_init__(self) -> None:
        ValidatedOrderIntent.__post_init__(self)
        if self.stop_price <= ZERO:
            raise ValueError("LimitOrderIntent.stop_price must be > 0.")


@dataclass(frozen=True, slots=True, kw_only=True)
class StopLimitOrderIntent(OrderIntent):
    price: Decimal
    stop_price: Decimal
    reduce_only: bool = False
    post_only: bool = False

    def __post_init__(self) -> None:
        OrderIntent.__post_init__(self)
        if self.stop_price <= ZERO or self.price <= ZERO:
            raise ValueError("LimitOrderIntent.price/stop_price must be > 0.")
        if self.side == Side.BUY and self.price < self.stop_price:
            raise ValueError("BUY: StopLimitOrderIntent.price must be larger than stop_price")
        elif self.side == Side.SELL and self.price > self.stop_price:
            raise ValueError("SELL: StopLimitOrderIntent.price must be smaller than stop_price")


@dataclass(frozen=True, slots=True, kw_only=True)
class ValidatedStopLimitOrderIntent(ValidatedOrderIntent):
    price: Decimal
    stop_price: Decimal
    reduce_only: bool = False
    post_only: bool = False
    validated: bool = True

    def __post_init__(self) -> None:
        ValidatedOrderIntent.__post_init__(self)
        if self.stop_price <= ZERO or self.price <= ZERO:
            raise ValueError("LimitOrderIntent.price/stop_price must be > 0.")
        if self.side == Side.BUY and self.price < self.stop_price:
            raise ValueError("BUY: StopLimitOrderIntent.price must be larger than stop_price")
        elif self.side == Side.SELL and self.price > self.stop_price:
            raise ValueError("SELL: StopLimitOrderIntent.price must be smaller than stop_price")


# --- OrderAcknowledged ---


@dataclass
class OrderAck:
    intent_id: str  # causation: OrderIntent.id
    strategy_id: str  # strategy_id from order_intent
    component: str
    symbol: Symbol
    market: Market
    side: Side
    status: OrderStatus  # accepted | rejected | duplicate | throttled | error
    reason: Optional[str] = None  # human-readable detail
    errors: Optional[list[str]] = None  # machine-parseable ("MIN_NOTIONAL", "PRICE_BAND")
    warnings: Optional[list[str]] = None  # machine-parseable ("")

    exchange_order_id: Optional[str] = None  # set when accepted by venue/paper
    router_order_id: Optional[str] = None  # internal id minted by router/adapter

    ts_utc: UnixMillis = 0  # when the router emitted this ack (UTC ms)
    venue_ts: Optional[UnixMillis] = None  # optional venue-provided ts
    venue: Optional[str] = None  # e.g., "paper", "binance"

    seq: int = 0  # per-run monotonic sequence for ordering
    tags: Tags = field(default_factory=dict)


# --- FILL ---


@dataclass(frozen=True)
class Fill:
    fill_id: str
    order_id: str
    symbol: Symbol  # e.g. "BTCUSDT"
    market: Market  # spot | option | future
    qty: Decimal
    price: Decimal
    side: Side  # BUY | SELL
    ts: UnixMillis
    venue: str
    liquidity_flag: Liquidity
    fees_explicit: Decimal
    rebates: Decimal
    slippage_components: str
    tags: list[str]

    def __post_init__(self) -> None:
        if self.price <= ZERO:
            raise ValueError("Price must be > 0.")
        if self.side == Side.BUY and self.qty <= ZERO:
            raise ValueError("qty must be >0 for BUY")
        if self.side == Side.SELL and self.qty >= ZERO:
            raise ValueError("qty must be <0 for SELL")


# --- Symbol Details ---


@dataclass(frozen=True)
class SymbolSpec:
    """
    Tells the engine, how a given symbol can be traded (steps, limits, fees, multipliers)
    Then, reject or block orders that violate min_qty, min_notional, price bands, trading hours,
    shortability etc
    TODO: Most systems keep a registry, ctx.specs[symbol] -> SymbolSpec,
    so can be ealled when ever needed
    """

    symbol: Symbol
    base_asset: str
    quote_asset: str

    # Precision, granularity
    tick_size: Decimal
    lot_size: Decimal

    # Limits
    min_notional: Decimal
    min_qty: Decimal
    max_qty: Decimal
    price_band_low: Decimal
    price_band_high: Decimal

    # Trading
    trading: bool


# --- Account ---


@dataclass(frozen=True)
class LotClosedEvent:
    # ts: int,
    symbol: str
    qty: Decimal  # positive quantity closed
    entry_ts: int
    entry_price: Decimal
    exit_ts: int
    exit_price: Decimal
    realized_pnl: Decimal  # as computed by Account (gross or net; choose policy)
    fee_entry: Decimal = ZERO
    fee_exit: Decimal = ZERO


@dataclass
class TradeEvent:
    ts: int
    symbol: str
    side: str  # "buy" | "sell"
    qty: float
    price: float  # trade price in quote ccy
    fee: float = 0.0  # in account_ccy
    venue: Optional[str] = None
    liquidity_flag: Optional[Liquidity] = None
    # Optional idempotency key to drop duplicates (e.g., venue fill_id)
    idempotency_key: Optional[str] = None


@dataclass
class Position:
    symbol: str
    qty: Decimal = ZERO
    avg_cost: Decimal = ZERO
    last_price: Decimal = ZERO
    market_value: Decimal = ZERO
    unrealized_pnl: Decimal = ZERO
    realized_pnl: Decimal = ZERO
    # Options-specific
    kind: str = "spot"
    strike: Optional[Decimal] = None
    expiry: Optional[int] = None
    multiplier: Decimal = Decimal("1.0")
    delta: Optional[Decimal] = None

    # FIFO lots: deque of (qty, price). Long lot has +qty, short lot has -qty.
    # e.g., Long lot → qty > 0 (e.g., (+100, 150.00)); price is acquisition price for that lot
    # E.g., Short lot → qty < 0 (e.g., (-40, 156.00))

    # q_fill, p_fill, fill_ts, fees
    _lots: Deque[Tuple[Decimal, Decimal, UnixMillis, Decimal]] = field(
        default_factory=deque, repr=False
    )

    def clear_lots(self) -> None:
        self._lots.clear()

    def _apply_mark(self, price: Decimal) -> None:
        self.last_price = price
        self.market_value = self.qty * price
        self.unrealized_pnl = (price - self.avg_cost) * self.qty

    def _recalc_from_lots(self) -> None:
        """Recompute qty and signed avg_cost from lots"""
        total_qty = sum((q for q, *_ in self._lots), ZERO)
        self.qty = total_qty
        if total_qty == ZERO:
            self.avg_cost = ZERO
        else:
            notional = sum((q * px for q, px, *_ in self._lots), ZERO)
            self.avg_cost = notional / total_qty

        self.market_value = self.last_price * self.qty
        self.unrealized_pnl = (self.last_price - self.avg_cost) * self.qty


@dataclass(frozen=True)
class PositionView:
    symbol: str
    qty: Decimal
    avg_cost: Decimal
    last_price: Decimal
    market_value: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    kind: str = "spot"
    strike: Optional[Decimal] = None
    expiry: Optional[int] = None
    multiplier: Decimal = Decimal("1.0")
    delta: Optional[Decimal] = None


@dataclass(frozen=True)
class FloatPositionView:
    symbol: str
    qty: float
    avg_cost: float
    last_price: float
    market_value: float
    unrealized_pnl: float
    realized_pnl: float
    kind: str = "spot"
    strike: Optional[float] = None
    expiry: Optional[int] = None
    multiplier: float = 1.0
    delta: Optional[float] = None


@dataclass(frozen=True)
class PortfolioSnapshot:
    ts: UnixMillis
    base_ccy: str  # e.g., "USDC"
    cash: Decimal
    equity: Decimal  # cash + sum(position market value) - fees
    upnl: Decimal  # unrealized PnL
    rpnl: Decimal  # realized PnL (cumulative)
    gross_exposure: Decimal  # sum of absolute value of all open positions
    net_exposure: Decimal  # signed sum of market values
    fees_paid: Decimal
    positions: tuple[PositionView, ...] = field(default_factory=tuple)
    # margin_used, leverage,....


@dataclass(frozen=True)
class FloatPortfolioSnapshot:
    ts: UnixMillis
    base_ccy: str  # e.g., "USDC"
    cash: float
    equity: float  # cash + sum(position market value) - fees
    upnl: float  # unrealized PnL
    rpnl: float  # realized PnL (cumulative)
    gross_exposure: float  # sum of absolute value of all open positions
    net_exposure: float  # signed sum of market values
    fees_paid: float
    positions: tuple[FloatPositionView, ...] = field(default_factory=tuple)


# -------- Live Market Data Types --------


@dataclass(frozen=True, slots=True)
class OrderBookLevel:
    """Single price level in an order book."""

    price: float
    qty: float


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    """L2 order book snapshot for spread/imbalance calculations."""

    symbol: str
    ts_exchange: int  # Exchange timestamp (Unix ms)
    ts_recv: int  # Local receive timestamp (Unix ms)
    bids: tuple[OrderBookLevel, ...]  # Best bid first (descending by price)
    asks: tuple[OrderBookLevel, ...]  # Best ask first (ascending by price)
    last_update_id: int  # Binance sequence number for consistency

    @property
    def best_bid(self) -> Optional[float]:
        """Best bid price, or None if empty."""
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        """Best ask price, or None if empty."""
        return self.asks[0].price if self.asks else None

    @property
    def mid_price(self) -> Optional[float]:
        """Mid price, or None if either side is empty."""
        if self.bids and self.asks:
            return (self.bids[0].price + self.asks[0].price) / 2
        return None

    @property
    def spread(self) -> Optional[float]:
        """Absolute spread, or None if either side is empty."""
        if self.bids and self.asks:
            return self.asks[0].price - self.bids[0].price
        return None

    @property
    def spread_bps(self) -> Optional[float]:
        """Spread in basis points relative to mid price."""
        mid = self.mid_price
        spread = self.spread
        if mid and spread and mid > 0:
            return (spread / mid) * 10000
        return None

    def imbalance(self) -> Optional[float]:
        """
        Order book imbalance: (bid_qty - ask_qty) / (bid_qty + ask_qty).
        Returns value in [-1, 1], or None if book is empty.
        """
        if not self.bids or not self.asks:
            return None
        bid_qty = sum(lvl.qty for lvl in self.bids)
        ask_qty = sum(lvl.qty for lvl in self.asks)
        total = bid_qty + ask_qty
        if total == 0:
            return None
        return (bid_qty - ask_qty) / total


@dataclass(frozen=True, slots=True)
class FundingRateEvent:
    """Funding rate event for perpetual futures."""

    symbol: str
    funding_rate: float  # e.g., 0.0001 for 0.01%
    mark_price: float
    index_price: float
    next_funding_time: int  # Unix ms
    ts_exchange: int  # Exchange timestamp (Unix ms)
    ts_recv: int  # Local receive timestamp (Unix ms)

    def annualized_rate(self, funding_interval_hours: int = 8) -> float:
        """
        Convert funding rate to annualized rate.
        Binance funds every 8 hours (3x daily).
        """
        periods_per_year = (365 * 24) / funding_interval_hours
        return self.funding_rate * periods_per_year


@dataclass(frozen=True, slots=True)
class TickerEvent:
    """24hr rolling window ticker for a symbol."""

    symbol: str
    price_change: float
    price_change_percent: float
    weighted_avg_price: float
    last_price: float
    last_qty: float
    open_price: float
    high_price: float
    low_price: float
    volume: float
    quote_volume: float
    open_time: int  # Unix ms
    close_time: int  # Unix ms
    ts_recv: int  # Local receive timestamp (Unix ms)


@dataclass(frozen=True, slots=True)
class LiveCandle:
    """
    Candlestick from live WebSocket stream.
    Extends base Candle with live-specific metadata.
    """

    symbol: str
    timeframe: str
    start_ms: int  # Kline open time (UTC ms)
    end_ms: int  # Kline close time (UTC ms)
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trades: int
    is_final: bool  # Whether this kline is closed
    ts_recv: int  # Local receive timestamp (Unix ms)

    def to_candle(self) -> Candle:
        """Convert to standard Candle for compatibility with existing code."""
        return Candle(
            symbol=self.symbol,
            timeframe=self.timeframe,
            start_ms=self.start_ms,
            end_ms=self.end_ms,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            trades=self.trades,
            is_final=self.is_final,
        )
