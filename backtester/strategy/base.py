"""
Goal: define the strategy interface
"""

import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, Optional, Sequence

from backtester.adapters.types import Candle, Fill, OrderIntent, Timeframe
from backtester.core.ctx import Context

# --- Exceptions ---


class StrategyError(Exception):
    """Recoverable strategy error (engine can continue or quarantine the strategy)."""


class StrategyPanic(Exception):
    """Non-recoverable; engine should disable this strategy instance cleanly."""


# ---


@dataclass(frozen=True)
class StrategyInfo:
    name: str  # stable identifier
    version: str = "0.1.0"
    description: str = ""


class _StrategyState(str, Enum):
    INITIALIZED = "initialized"
    RUNNING = "running"
    STOPPED = "stopped"


# --- Base strategy contract ---


class Strategy(ABC):
    """
    key principles:
        - event-driven (on_start, on_candle, on_fill, on_timer, on_end)
        - side-effect free: returns OrderIntent[]; engine does all I/O
        - Deterministic
        - Small, tagged outputs; engine enforces constraints, risk, exec

    The ctx (context) exposes read-only views
        - clock, portfolio, positions, risk_limits, fees, lot_sizes, logger
        and helpers, like order_factory

    Additions:
    - Output guardrails: max_intents_per_event: int = 5
    """

    def __init__(self, params: Mapping[str, Any], *, info: Optional[StrategyInfo] = None) -> None:
        # read-only (immutable view) of a dictionary/mapping (when tried, TypeError)
        self._params = MappingProxyType(dict(params))
        self._params_hash = self._stable_params_hash(self._params)
        self._info = info or StrategyInfo(name=self.__class__.__name__.lower())
        self._mode: Optional[str] = None  # Initialized, running, stopped
        # self._cooldown_until_ms: int = 0  # Cooldown gate in UTC ms; 0 means no cooldown

    @property
    def info(self) -> StrategyInfo:
        return self._info

    @property
    def params(self) -> Mapping[str, Any]:
        return self._params

    @property
    def params_hash(self) -> str:
        return self._params_hash

    # --- data needs / subscriptions --- (optional hooks; no implementation enforced)

    def subscriptions(self) -> Sequence[str]:
        """
        List of topics this strategy wants to subscribe to.
        Declares the event feeds the engine should wire from the bus.
        To be kept statis during a run; engine resolves once at startup
        """
        return ()

    def symbols(self) -> Sequence[str]:
        """
        list of symbols this strategy wants to trade.
        """
        return ()

    def timeframe(self) -> Sequence[Timeframe]:
        """
        The bar timeframes the strategy consumes
        """
        return ()

    def warmup_bars(self) -> int:
        """
        number of bars to warm up before the strayegy can produce isgnals
        """
        return 0

    # --- Lifecycle hooks (called by the engine)

    def on_start(self, ctx: Context) -> list[OrderIntent]:
        """
        Called once at the start of the strategy. Sometime immediate actions (intent) are required.
        """
        return []

    @abstractmethod  # required for each instance: Otherwise TypeError
    def on_candle(self, ctx: Context, candle: Candle) -> list[OrderIntent]:
        """
        Primary decision hook. Required.
        When: For every final bar published on topics subscribed to.
        What to do:
            - Update rolling features, make decisions, build Order Intents
        Conventions:
            - Never round price/qty here; the engine enforces consraints consistently
        """

    def on_fill(self, ctx: Context, fill: Fill) -> Optional[list[OrderIntent]]:
        """
        When: After an order intent results in an execution (sim or live)
        What to do:
            - Update any fill-sensitive state (e.g., average entry price in strategy logic,
            not accounting!)
            - Optionally, emit follow-up intents

        """

    def on_timer(self, ctx: Context, ts: float) -> list[OrderIntent]:
        """
        Engine triggers at scheduled boundaries, e.g., every 1 hour or at midnight.
        What to do:
            - Time-based housekeeping (rebalance, risk tightening), or cross-bar checks
            - must be idempotent for the same ts
        """
        return []

    # def on_lifecycle(self, ctx, event): Expiries, listings, corporate actions

    def on_end(self, ctx: Context) -> None:
        """
        Finalize and dump diagnostics.
            - End-of-run hosekeeping: emit final metrics to ctx.metrics
            - log summary, verify invariants
            - No I/O; no orders here
        """

    # ---- Observability / debugging

    def snapshot_state(self) -> dict[str, Any]:
        """
        Capture the current state of the strategy for debugging and observability
        Output: small, JSON-safe dict , reflecting current internal state (e.g.,
            "last_signal_ts", "cooldown_until"...)
        """
        return {
            "params_hash": self.params_hash,
            # etc.
        }

    # --- parameter validation hook (optional to override) --

    def log_event(self, ctx: Context, level: str, msg: str, **fields: Any) -> None:
        base = {
            "strategy": self.info.name,
            "version": self.info.version,
            "run_id": ctx.run_id,
            "params_hash": self.params_hash,
            "mode": self._mode or "unknown",
        }

        base.update(fields)
        line = msg + " " + " ".join(f"{k}={v}" for k, v in base.items())
        lvl = (level or "info").lower()
        if lvl == "debug":
            ctx.log.debug(line)
        elif lvl == "warning" or lvl == "warn":
            ctx.log.warning(line)
        elif lvl == "error":
            ctx.log.error(line)
        else:
            ctx.log.info(line)

    # --- Internals ----

    @staticmethod
    def _stable_params_hash(params: Mapping[str, Any]) -> str:
        try:
            blob = json.dumps(params, sort_keys=True, seperators=(",", ":"), default=str).encode(
                "utf-8"
            )
        except Exception:
            items = sorted((k, str(v)) for k, v in dict(params).items())
            blob = json.dumps(items, seperators=(",", ":"), default=str).encode("utf-8")
        h = hashlib.sha1(blob).hexdigest()
        return h[:10]
