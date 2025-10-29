from __future__ import annotations

from abc import ABC, abstractmethod

from backtester.adapters.types import Fill

# from backtester.core.types import Fill, Liquidity


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
