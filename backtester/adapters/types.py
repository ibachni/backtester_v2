"""
define canonical types
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict

# -------- Aliases (clarify intent) --------
UnixMillis = int
UnixMicros = int
Symbol = str
Timeframe = str  # e.g., "1s","5s","1m","3m","5m","1h","1d"


class EVENTNAMES(str, Enum):
    """
    define canonical event names
    """

    ERROR = "error"
    CONFIG = "config"


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event: EVENTNAMES
    ts: int  # Millis?
    run_id: int
    git_sha: str
    seed: int
    component: Any


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


class OrderIntent:
    pass


class OrderAck:
    pass


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
