# core/ctx.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import Callable, Mapping, Optional, Sequence

from backtester.core.clock import Clock
from backtester.types.aliases import Symbol, UnixMillis
from backtester.types.types import PortfolioSnapshot, SymbolSpec
from backtester.utils.utility import get_symbol_spec

"""
- What does the Context provider do?
    - consisten snapshot of the world at a given sim step
    - Provide high level APIs instead of raw engine internals
        - Position in AAPL
        - Give me last 60 days of prices
        - Clean interface for reading state

- Main components
    - clock now
    - sim metadata, seed
    - Market data
        - Spot data
        - historical data
    - Portfolio & account
        - get position per symbol
        - account snapshot (cash, equity, margin used, margin available, urpnl, rpnl)
        - Open orders
    - Convenience
        - Logging (with message)
        - record metric

- Key design choices
    - in place update vs object each step
    - read only


"""


class ReadOnlyPortfolio:
    """
    Narrow, read-only view exposed to strategies via ctx.portfolio.
    Backed by adapter callables so we don’t leak engine internals.
    fn for function.
    """

    def __init__(
        self,
        *,
        # a function, that returns a Portfolio Snapshot when returned.
        snapshot_fn: Callable[[], Optional[PortfolioSnapshot]],
        position_qty_fn: Callable[[Symbol], Optional[Decimal]],
        equity_base_fn: Callable[[], Decimal],
    ) -> None:
        self._snapshot_fn = snapshot_fn
        self._position_qty_fn = position_qty_fn
        self._equity_base_fn = equity_base_fn

    def snapshot(self) -> Optional[PortfolioSnapshot]:
        return self._snapshot_fn()

    def position_qty(self, symbol: Symbol) -> Optional[Decimal]:
        return self._position_qty_fn(symbol)

    def equity_base(self) -> Decimal:
        return self._equity_base_fn()


class MetricsSink:
    """
    Minimal metrics collector for the MVP.
    - inc(name, by): counters
    - gauge(name, value): last-value gauges
    - observe(name, value): summary (count/sum/min/max)
        - track distribution of values, not just points
        - useful to analyze performance characteristics
            - execution times, slippage, signal strength etc.
    Thread-safe enough for single-process asyncio usage.
    """

    def __init__(self) -> None:
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}
        self._summaries: dict[str, dict[str, float]] = {}

    def inc(self, name: str, by: float = 1.0) -> None:
        self._counters[name] = self._counters[name] + by

    def gauge(self, name: str, value: float) -> None:
        self._gauges[name] = float(value)

    def observe(self, name: str, value: float) -> None:
        s = self._summaries.get(name)
        if s is None:
            s = {"count": 0.0, "sum": 0.0, "min": float("inf"), "max": float("-inf")}
            self._summaries[name] = s
        s["count"] += 1.0
        s["sum"] += value
        if value < s["min"]:
            s["min"] = value
        if value > s["max"]:
            s["max"] = value

    # --- exports ---

    def counters(self) -> Mapping[str, float]:
        return MappingProxyType(self._counters)

    def gauges(self) -> Mapping[str, float]:
        return MappingProxyType(self._gauges)

    def summaries(self) -> Mapping[str, Mapping[str, float]]:
        return MappingProxyType({k: MappingProxyType(v) for k, v in self._summaries.items()})


class SymbolSpecs:
    """
    Read-only registry of per-symbol trading constraints.
    """

    def __init__(self, symbols: list[Symbol]) -> None:
        self._specs = get_symbol_spec(symbols)

    def get(self, symbol: Symbol) -> Optional[SymbolSpec]:
        return self._specs.get(symbol)

    def require(self, symbol: Symbol) -> SymbolSpec:
        spec = self._specs.get(symbol)
        if spec is None:
            raise KeyError(f"SymbolSpec not found for {symbol!r}")
        return spec

    def symbols(self) -> Sequence[Symbol]:
        """
        Returns a sequence of all registered symbol names.
        """
        return tuple(self._specs.keys())


@dataclass(frozen=True)
class Context:
    """
    Read-only handle passed to strategies.
    Provides:
      - clock.now() (canonical UTC ms)
      - portfolio (RO)
      - symbol specs (RO)
      - rng (seeded per run/strategy)
      - metrics sink
      - logger
      - params (resolved, read-only mapping)
      - run_id (for tagging logs/metrics/artifacts)

       The ctx (context) exposes read-only views
        - clock, portfolio, positions, risk_limits, fees, lot_sizes, logger
        and helpers, like order_factory
    """

    clock: Clock
    portfolio: ReadOnlyPortfolio
    specs: SymbolSpecs

    # To be added
    # audit: AuditWriter
    # bus: Bus
    # base_ccy: str
    # rng: random.Random = field(repr=False)
    # metrics: MetricsSink
    # log: logging.Logger
    # # Global configuration parameters that apply across the backtest run
    # # - base currency, slippage, commission, max position, max leverage etc. etc.
    # params: Mapping[str, Any] = field(default_factory=dict, repr=False)
    # run_id: str = "run-unknown"

    # ----- convenience methods -----

    def now(self) -> UnixMillis:
        return self.clock.now()

    def symbol_spec(self, symbol: Symbol) -> SymbolSpec:
        """Get constraints for a symbol (tick, lot, min notional, bands)."""
        return self.specs.require(symbol)

    # ----- factory helpers (create instance of the class) -----
    def make_context(
        self,
        *,
        clock: Clock,
        portfolio_snapshot_fn: Callable[[], Optional[PortfolioSnapshot]],
        position_qty_fn: Callable[[Symbol], Optional[Decimal]],
        equity_base_fn: Callable[[], Decimal],
        symbols: list[Symbol],
    ) -> Context:
        # Portfolio
        ro_portfolio = ReadOnlyPortfolio(
            snapshot_fn=portfolio_snapshot_fn,
            position_qty_fn=position_qty_fn,
            equity_base_fn=equity_base_fn,
        )

        # Specs
        ro_specs = SymbolSpecs(symbols)

        return Context(
            clock=clock,
            portfolio=ro_portfolio,
            specs=ro_specs,
        )
