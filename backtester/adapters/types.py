"""
define canonical types
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal, Mapping, Optional

from pydantic import BaseModel, ConfigDict

# -------- Aliases (clarify intent) --------
UnixMillis = int
UnixMicros = int
Symbol = str
Timeframe = str  # e.g., "1s","5s","1m","3m","5m","1h","1d"

# -------- Enums --------


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"  # stop-market
    STOP_LIMIT = "stop_limit"


class TimeInForce(str, Enum):
    GTC = "gtc"  # good till canceled
    IOC = "ioc"  # immediate or cancel
    FOK = "fok"  # fill or kill


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


class Topics:
    BAR = "bar"


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


Tags = dict[str, Any]


@dataclass(frozen=True, slots=True)
class OrderIntent:
    symbol: Symbol
    side: Side
    type: OrderType = OrderType.MARKET
    qty: float = 0.0
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
        if self.type in (OrderType.STOP, OrderType.STOP_LIMIT) and (self.stop_price is None):
            raise ValueError("Stop/StopLimit orders require stop_price.")


class OrderAck:
    client_id: str
    ts: UnixMillis
    accepted: bool
    # causation_id: str  # Linking to the OrderIntent
    reason: Optional[str] = None
    exchange_order_id: Optional[str] = None


class Fill:
    pass


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


# -------- Portfolio & accounting --------


@dataclass(frozen=True)
class Position:
    """
    TODO: Think about additional information
    """

    symbol: Symbol
    qty: float  # signed (long > 0, short < 0); spot MVP usually ≥ 0
    avg_price: float  # average entry price in quote currency


@dataclass(frozen=True)
class PortfolioSnapshot:
    ts: UnixMillis
    base_ccy: str  # e.g., "USDT"
    cash: float
    equity: float  # cash + sum(position market value) - fees
    upnl: float  # unrealized PnL
    rpnl: float  # realized PnL (cumulative)
    positions: tuple[Position, ...] = field(default_factory=tuple)
