from __future__ import annotations

from abc import ABC
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Deque, Literal, Mapping, Optional, Tuple

from pydantic import BaseModel, ConfigDict

# TODO Additional Validations

# -------- Aliases (clarify intent) --------
UnixMillis = int
UnixMicros = int
Symbol = str
Timeframe = str  # e.g., "1s","5s","1m","3m","5m","1h","1d"
Tags = dict[str, Any]
AuditRecord = dict[str, Any]

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


class AckStatus(str, Enum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    DUPLICATE = "duplicate"
    THROTTLED = "throttled"
    ERROR = "error"


class OrderStatus(str, Enum):
    NEW = "NEW"
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


# --- Strategy


@dataclass(frozen=True)
class StrategyInfo:
    name: str  # stable identifier
    version: str = "0.1.0"
    description: str = ""


# --- Orders ---


@dataclass
class OrderState:
    """Simulator-internal mutable view of an order."""

    order: ValidatedOrderIntent
    remaining: Decimal
    hash: str
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


# --- OrderIntent (Legacy) ---


@dataclass(frozen=True, slots=True)
class OrderIntentOld(OrderIntent):
    """
    Market: No price, or stop_price
    Limit: Price is mandatory; post_only is optional
    Stop-Market: Stop_price mandatory; convert into market order
    Stop-Limit: Stop-price (trigger) and price (Limit the order converts)
    TODO TS MISSING
    """

    symbol: Symbol
    market: Market
    side: Side
    id: str
    type: OrderType = OrderType.MARKET
    price: Optional[float] = None
    stop_price: Optional[float] = None
    reduce_only: bool = False
    post_only: bool = False
    tif: TimeInForce = TimeInForce.GTC
    client_id: Optional[str] = None
    tags: Tags = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.qty <= 0:
            raise ValueError("OrderIntent.qty must be > 0.")
        if self.type in (OrderType.LIMIT, OrderType.STOP_LIMIT) and (self.price is None):
            raise ValueError("Limit/StopLimit orders require price.")
        if self.type in (OrderType.STOP, OrderType.STOP_LIMIT, OrderType.STOP_MARKET) and (
            self.stop_price is None
        ):
            raise ValueError("Stop/StopLimit/StopMarket orders require stop_price.")


# --- OrderAcknowledged ---


@dataclass
class OrderAck:
    intent_id: str  # causation: OrderIntent.id
    strategy_id: str  # strategy_id from order_intent
    component: str
    symbol: Symbol
    market: Market
    side: Side
    status: AckStatus  # accepted | rejected | duplicate | throttled | error
    reason_code: Optional[str] = None  # machine-parseable (e.g., "MIN_NOTIONAL", "PRICE_BAND")
    reason: Optional[str] = None  # human-readable detail

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
    tick_size: float
    lot_size: float
    min_notional: float
    price_band_low: Optional[float] = None
    price_band_high: Optional[float] = None


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
