"""
now() provides the canonical notion of time for the whole engine. sleep_until() and sleep>_for()
provide a way to pause until a boundary or deadline. This module is used by the
 - Enginge (ctx) to strategies and to polling loops
 - DataFeed (REST polling) to fetch new bars, sleep_until_next_boundary
 - Risk, to evaluate daily windows, using now()
 - Execution stamps client order IDs and timeouts with now()
"""

from abc import ABC
from typing import Final

Millis = int  # Milliseconds since epoch
Nanos = int

# -------- Exceptions -----------------------------------------------------------


class ClockError(RuntimeError):
    """Custom runtime error, raised when a clock operation would violate its invariants
    (e.g., going backward).
    """


# -------- Utilities (timeframe parsing & alignment) ---------------------------

_TIME_UNITS_MS: Final[dict[str, int]] = {
    "ms": 1,
    "s": 1000,
    "m": 60_000,  # by convention, one can divide groups of digits for easier reading
    "h": 3_600_000,
    "d": 86_400_000,
}

# -------- Interface -----------------------------------------------------------


class Clock(ABC):
    def now(self) -> Millis:
        return 1

    def now_ns(self) -> Nanos:
        return 1

    async def sleep_until(self) -> None:
        pass

    async def sleep_for(self, delta_ms: Millis) -> None:
        pass

    async def sleep_until_next_boundary(self, interval_ms: Millis) -> Millis:
        return 1


# -------- RealtimeClock -------------------------------------------------------


class RealtimeClock(Clock):
    pass


class SimClock(Clock):
    pass
