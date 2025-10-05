"""Clock Port Interface.

References: ADR-001 (Determinism), ADR-011 (Single-threaded backtests).

Contract: Provides current deterministic UTC timestamp for a run.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        """Return current UTC time (datetime, tz-aware).
        Should be monotonic non-decreasing within a run.
        """
        ...
