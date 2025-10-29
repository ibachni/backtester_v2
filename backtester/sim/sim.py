import random
from dataclasses import dataclass, field

from backtester.adapters.types import Side

"""
Design choices:
    - Liquidity and fill modeling: based on data
        - Bar-based ()
            - Market: fill at open/close/mid/next bar open \pm slippage
            - Limit: fill if limit crosses bar range; price = limit or better
            - Stops: trigger if bar touches stop; convert to market / limit per type
            - Volume participation: cap fills to a friction of bar volume
        - quote-based (l1)
        - order-book (l2)
"""

# --------------------------------------------------------------------------------------
# Slippage model (MVP: fixed bps over the reference price)
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class SlippageModel:
    """
    Static slippage applied to reference price (bar close in MVP).
    bps: basis points (1 bps = 0.01%). BUY pays up, SELL receives down.
    Returns the adjusted price.
    """

    bps: float = 0.0

    def apply(self, side: Side, ref_price: float) -> float:
        if self.bps == 0.0:
            return float(ref_price)
        m = float(self.bps) / 10_000.0
        if side == Side.BUY:
            return float(ref_price) * (1.0 + m)
        else:
            return float(ref_price) * (1.0 - m)


class FillModel:
    pass


class ImpactModel:
    pass


class FeeModel:
    pass


class LatencyModel:
    random_bariers: tuple[int, int] = field(default=(10, 200))

    def random_latency(self) -> int:
        return random.randrange(self.random_bariers[0], self.random_bariers[1], 5)


class RoutingModel:
    pass


class ExecutionSimulator:
    """
    Responsibilities:
        - Accept Orders (validate, timestamp, route)
        - Simulate market interaction -> queueing, matching, partial fills, rejections, cancels,
        amendments
        - Produce fills -> price, size, time, fees, slippage, venue
        - Update portfolio state ->
        - Emit events -> Order status transitions, trade prints, risk/capacity alerts

    """
