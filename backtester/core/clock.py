"""
now() provides the canonical notion of time for the whole engine. sleep_until() and sleep>_for()
provide a way to pause until a boundary or deadline. This module is used by the
 - Enginge (ctx) to strategies and to polling loops
 - DataFeed (REST polling) to fetch new bars, sleep_until_next_boundary
 - Risk, to evaluate daily windows, using now()
 - Execution stamps client order IDs and timeouts with now()
"""

import asyncio
import math
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Final, Optional

# type alias (at runtime equivalent to int)
# Anything coming from the outside world (config files, APIs) should be
# parsed and checked before it becomes a Millis.
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


def parse_timeframe(tf: str) -> Millis:
    """
    Parse timeframe strings like '1s', '3m', '1h', '1d' into milliseconds.
    Raises ValueError on unknown units, empty quantities, or non-digits, or non-positive values.

    # TODO: Decimals (e.g., 1.5s) are not supported in MVP!

    Valid units: ms, s, m, h, d. Raises ValueError for invalid inputs.
    """
    # 1. seperate num and unit
    unit = ("ms", "s", "m", "h", "d")

    tf = tf.strip().lower()

    for u in unit:
        if tf.endswith(u):
            prefix = tf[: -len(u)].strip()
            if not prefix or not prefix.isdigit():
                raise ValueError("clock.parse_timeframe(): quantity None or not digit")
            quantity = int(prefix)
            if quantity <= 0:
                raise ValueError("clock.parse_timeframe(): quantity must be positive")
            return quantity * _TIME_UNITS_MS[u]
    raise ValueError("clock.parse_timeframe(): Invalid timeframe: ")


def align_forward(ts_ms: Millis, interval_ms: Millis, offset_ms: Millis = 0) -> Millis:
    """
    Find the smallest timestamp that is greater or equal to the input timestamp ts_ms.
    The grid is defined by interval_ms, which defines the time between grid timestamps.
    offset_ms offsets the boundaries by the indicated amount.
    """
    if interval_ms <= 0:
        raise ValueError("align_forward: interval_ms mut be >0")

    # 1. get time relative to the grids origin: ts_ms - offset_ms
    # 2. divid by interval_ms to find how many intervals have passed
    # 3. Use math.ceil to roung up, so that reuslt is always at or after ts_ms
    n = math.ceil((ts_ms - offset_ms) / interval_ms)
    return offset_ms + n * interval_ms


# -------- Interface -----------------------------------------------------------


class Clock(ABC):
    """
    Engine-wide time source interface.

    All timestamps are UTC epoch milliseconds (int). Implementations must be
    monotonic non-decreasing within a run.
    """

    def now(self) -> Millis:
        """Current time in UTC epoch milliseconds (monotonic within the run)."""
        raise NotImplementedError

    def now_ns(self) -> Nanos:
        """High-resolution timestamp in nanoseconds (derived)."""
        return int(self.now()) * 1_000_000

    async def sleep_until(self, ts_ms: Millis) -> None:
        """
        Block (await) until the clock reaches ts_ms.

        RealtimeClock: actually sleeps.
        SimClock: advances its internal time to ts_ms and yields control once.
        """
        raise NotImplementedError

    async def sleep_for(self, delta_ms: Millis) -> None:
        """Convenience helper: sleep for a relative duration (>= 0)."""
        pass

    # use property method to make it immutable.
    # Cannot be accessed like traditional attribute, but only be called.

    @property
    @abstractmethod
    def is_realtime(self) -> bool:
        """True for RealtimeClock; False for SimClock."""
        raise NotImplementedError

    async def sleep_until_next_boundary(
        self, interval_ms: Millis, *, offset_ms: Millis, extra_delay_ms: Millis
    ) -> Millis:
        """
        Sleep until the next alignment boundary and (optionally) a small extra delay.
        Returns the boundary timestamp it targeted (without the extra delay).
        Exchanges sometimes need a few hundred ms after the boundary to mark a bar final.

        Workflow:
            - Compute target = align_forward(self.now(), interval_ms, offset_ms)
            - Sleep until target + extra_delay_ms
            - Return the boundary time (target), not the delayed arrival time.
        """
        target = align_forward(self.now(), interval_ms, offset_ms)
        if extra_delay_ms < 0:
            raise ValueError("Clock(ABC): extra_delay_ms must be non-negative")
        await asyncio.sleep(target + extra_delay_ms)
        return target


# -------- RealtimeClock -------------------------------------------------------


@dataclass
class RealtimeClock(Clock):
    """
    Realtime clock that is robust to system time changes.

    It anchors to the wall-clock at construction and then advances using
    time.monotonic(). This makes `now()` monotonic within the run even if
    the OS clock is adjusted by NTP or manual tinkering.

    param sleep_chunk_ms: The duration (in milliseconds) to sleep in each chunk.
    _t0_wall_ms: The wall-clock time at the start (in milliseconds).
    _t0_mono: The monotonic counter. time.monotonic captures the elapsed time since start.

    This is used by paper & live models and the live ingest loop, polls at exact bar boundaries.
    Also, any place with deadlines.
    """

    # for chunked sleeping (to let check kill-switch often)
    sleep_chunck_ms: int = 500

    _t0_wall_ms: Optional[Millis] = None
    _t0_mono: Optional[float] = None
    _last_ms: Optional[float] = None

    def __post_init__(self) -> None:
        """
        Initialize the RealtimeClock by capturing the current wall-clock and monotonic times.
        """
        self._t0_wall_ms = int(time.time() * 1000)  # time since Unix Epoch (UTC)
        self._t0_mono = time.monotonic()
        self.sleep_chunck_ms = max(5, int(self.sleep_chunck_ms))

    @property
    def is_realtime(self) -> bool:
        # This clock follows real time
        return True

    def now(self) -> Millis:
        """
        Returns the internal time, calculated using the wall time and elapsed time
        """
        # Ensure clock was initialized in __post_init__
        if self._t0_wall_ms is None or self._t0_mono is None:
            raise ClockError("Clock not properly initialized")
        elapsed_ms = int((time.monotonic() - self._t0_mono) * 1000)
        self._last_ms = self._t0_wall_ms + elapsed_ms
        return self._last_ms

    async def sleep_until(self, ts_ms: Millis) -> None:
        """
        Sleep in small chunks up to ts_ms to keep the loop responsive.
        """
        while True:
            remaining = ts_ms - self.now()
            if remaining <= 0:
                return
            chunck = min(remaining, self.sleep_chunck_ms)
            await asyncio.sleep(chunck / 1000.0)


# -------- SimClock ------------------------------------------------------------


class SimClock(Clock):
    """
    Deterministic, manually-advanced clock for backtests.

    The engine drives the clock by calling `advance_to()` (or `sleep_until()` which
    internally advances). All advances must be forward (monotonic).

    This is used by the backtesting engine. When it reads the next bar at T, it advances
    the clock (clock.advance_to(T)) (or await sleep_until(T)), publishes the event, then runs
    the stragegy callback.
    """

    def __init__(self, start_ms: Millis):
        self.start_ms: Millis = start_ms
        self._current_ms: Optional[Millis] = start_ms

    def __post_init__(self) -> None:
        if self.start_ms < 0:
            raise ValueError("start_ms must be >= 0")
        self.current_ms = int(self.start_ms)

    @property
    def is_realtime(self) -> bool:
        return False

    def now(self) -> Millis:
        if self._current_ms is None:
            raise ClockError("SimClock: _current_ms is not initialized")
        return self._current_ms

    def advance_to(self, ts_ms: Millis) -> Millis:
        """
        Move the simulated time forward to exactly ts_ms.

        Returns the new current time. Raises ClockError on backward moves.
        """
        if ts_ms < self.now():
            raise ClockError(f"SimCLock: cannot go backwards: {ts_ms} < {self.now()}")
        self._current_ms = ts_ms
        return self._current_ms

    def advance_by(self, delta_ms: Millis) -> Millis:
        """
        Move the simulated time forward by delta_ms (>= 0).
        """
        if delta_ms < 0:
            raise ClockError(f"SimClock: cannot go backwards, delta_ms < 0: {delta_ms}")
        return self.advance_to(self.now() + int(delta_ms))

    async def sleep_until(self, ts_ms: Millis) -> None:
        """
        In simulation, 'sleep' is just time travel: set the time and yield control once.
        """
        self.advance_to(ts_ms)
        # Let the loop switch to other coroutines
        await asyncio.sleep(0)
