"""PortfolioStore Port Interface.

References: ADR-001 (Determinism).

Contract: Load & persist portfolio state (positions, cash, metrics).
"""

from __future__ import annotations

from typing import Any, Protocol


class PortfolioStore(Protocol):
    def load(self) -> Any: ...
    def save(self, portfolio: Any) -> None: ...
