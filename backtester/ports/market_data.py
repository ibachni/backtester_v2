"""MarketDataProvider Port Interface.

References: ADR-001, ADR-010.

Contract: Stream bar data (historical or simulated real-time) for a symbol.
"""

from __future__ import annotations

from typing import Iterable, Protocol


class Bar:  # minimal placeholder type
    __slots__ = ("ts", "open", "high", "low", "close", "volume")

    def __init__(
        self, ts: int, open: float, high: float, low: float, close: float, volume: float
    ) -> None:
        self.ts = ts
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume


class MarketDataProvider(Protocol):
    def stream_bars(self, symbol: str) -> Iterable[Bar]:
        """Yield bars in strictly increasing timestamp order (UTC)."""
        ...
