from __future__ import annotations

import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal

from backtester.types.types import (
    Fill,
    Liquidity,
    Side,
)

# --- ABC ---


class FeeModel(ABC):
    """
    Abstract fee model interface.
    Implementations return the fee for a given Fill in the account's base currency.
    """

    @abstractmethod
    def fee(self, fill: Fill) -> float:
        """
        Compute the fee for this fill (>= 0.0), in base currency (quote for spot).
        Implementations should be pure and deterministic.
        """
        raise NotImplementedError


# --- Utils ---


def fee_from_bps(notional: float, bps: float) -> float:
    """
    Helper for bps-based models.
    notional >= 0, bps in 'basis points' (1 bps = 0.01%).
    """
    n = float(notional)
    b = float(bps)
    if n <= 0.0 or b <= 0.0:
        return 0.0
    return n * (b / 10_000.0)


def _notional(fill: Fill) -> float:
    # Safe notional (never negative)
    p = float(fill.price)
    q = abs(float(fill.qty))
    n = p * q
    return n if n > 0.0 else 0.0


# --- Implementations ---


@dataclass(frozen=True)
class ZeroFees(FeeModel):
    """Always returns zero fee (useful for dry runs)."""

    def fee(self, fill: Fill) -> float:
        return 0.0


@dataclass(frozen=True)
class FixedBps(FeeModel):
    """
    Single bps rate for all fills (maker/taker agnostic).
    Example: FixedBps(bps=10)  # 0.10% on notional
    """

    bps: float = 0.0

    def fee(self, fill: Fill) -> float:
        return fee_from_bps(_notional(fill), self.bps)


@dataclass(frozen=True)
class MakerTakerBps(FeeModel):
    """
    Maker/Taker bps schedule.
    - maker_bps is applied if fill.liquidity == Liquidity.MAKER
    - taker_bps otherwise (TAKER or UNKNOWN)
    Example: MakerTakerBps(maker_bps=8, taker_bps=10)  # 0.08% vs 0.10%
    """

    maker_bps: float = 0.0
    taker_bps: float = 0.0

    def fee(self, fill: Fill) -> float:
        bps = self.maker_bps if fill.liquidity_flag == Liquidity.MAKER else self.taker_bps
        return fee_from_bps(_notional(fill), bps)


# --- Other Models ---


@dataclass(frozen=True)
class SlippageModel:
    """
    Static slippage applied to reference price (bar close in MVP).
    bps: basis points (1 bps = 0.01%). BUY pays up, SELL receives down.
    Returns the adjusted price.
    """

    bps: float = 0.0

    def apply(self, side: Side, ref_price: Decimal) -> Decimal:
        if self.bps == 0.0:
            return ref_price
        m = Decimal(self.bps) / Decimal(10_000.0)
        if side == Side.BUY:
            return (ref_price) * (Decimal(1.0) + m)
        else:
            return Decimal(ref_price) * (Decimal(1.0) - m)


class FillModel:
    pass


class ImpactModel:
    pass


@dataclass(frozen=True)
class LatencyModel:
    """
    Currently not in use for.
    """

    seed: int = 1
    random.seed(seed)
    random_bariers: tuple[int, int] = field(default=(10, 200))  # Latency in ms

    def random_latency(self) -> int:
        return random.randrange(self.random_bariers[0], self.random_bariers[1], 5)


class RoutingModel:
    pass
