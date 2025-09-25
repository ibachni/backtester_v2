"""RiskEngine Port Interface.

References: ADR-018 (Risk rails & safety).

Contract: Pre-trade checks return a decision struct.
"""

from __future__ import annotations

from typing import Any, Protocol


class RiskDecision:  # placeholder
    def __init__(self, allow: bool, reason_code: str | None = None):
        self.allow = allow
        self.reason_code = reason_code


class RiskEngine(Protocol):
    def pre_trade_check(self, order: Any, portfolio: Any) -> RiskDecision: ...
