from dataclasses import dataclass
from typing import Mapping

from backtester.core.clock import SimClock


@dataclass(frozen=True)
class BacktestReport:
    """Lightweight run artifacts for programmatic consumption."""

    snapshot: (
        tuple  # tuple[PortfolioSnapshot, ...]  # a tuple of PortfolioSnapshots of length 0 or more
    )
    fills: tuple[object, ...]  # backtester.core.types.Fill but avoid circular import here
    acks: tuple  # tuple[OrderAck, ...]
    stats: Mapping[str, int]


class BacktestEngine:
    """
    Deterministic, close-driven backtest engine.
    Walks bars, calls the strategy, routes orders, updates accounting

    Typical wiring:
      eng = BacktestEngine(clock, feed, strategies, account, exec_sim, risk_engine, specs)
      report = eng.run()


    Working:
    - 1. Init
        - Load config & seed RNG (instruments, venues, fee tables, slippage model, margin rules,
        borrow liimits, options specs, trading calendar, time zone, random seed (for determinism)
        - ingest and normalize data (Prics depending on strategy; align to common schema:
        timestamps, symbol ids, corporate action adjusted fields, roll calendars for futures/perps
        if used)
        - warmup window (preload indicators/ML features; verify no look-ahead)
        - create core subsystems
            - Clock/Calendar
            - event bus/ priority queue
            - strategy (stateful, with on_data / on_timer hooks)
            - Broker/exchange sim (order book or bar-based fill model)
            - Portfolio & risk (positions, cash, margin, leverage, borrow)
            - Accounting: PnL, performance, metrics, tearsheets
            - logging / telemetry (orders, fills, risk breaches, warnings)
            - Persistence / Checkpointing (optional for long runs)
    - 2. Event Loop
        2.1. Advance Clock: pop the next timestamped event from the queue (market data, timer,
        order status, corporate actions, expiry, funding etc.; skip non trading intervals
        2.2. Inject market / exogenous events (at this timestamp)
            - market data
            - venue status
            - corporate actions
            - options lifecycle (new listings, series rolls, expirations, assignment/exercise)
            - Funding / borrow
        2.3. Mark-to-market: updare mid/last/close marks for all open positions; recompute
        greeks with current IV/rates/spot/refresh portfolio equity, VaR, margin usage
        2.4.


    MVP
    1. Data Loader & Calendar
    Load OHLCV Bars (adjusted for splits; for equities: include splits and cash dividends at
    minimum)
    2. Clock & main loop (iterate bar by bar): trigger on_bar(data): strategy returns desired
    target weights/ positions
    3. Order Generation: convert target position to market order at next bar open (or close)
    with size rounding
    4. Simple broker & fills: bar-based fill modell:
        - Market orders fill at next bars open
        - limit order fill if price traded through the limit
    Transaction costs: fixed bps commission + bps slippage proportional to notional; no rebates
    5. Portfolio & accounting:
        - Positions (qty, avg cost), cash, equity, unrealized/realized PnL
        - Daliy metrics: equity curve, drawdown, return, turnover, Sharpe
    6. Corporate actions (skip)
    7. Options
        - Price options off provided close prices; exercise at expiry automatically;
        - ignore early exercise and complex margin; ignore greeks
    8. Persistence & outputs
        - CSV / Parquet logs: orders, fills, portfolio, daily performance
        - Basic plots: equity curve, drawdown
        - config file + random seed

    """

    def __init__(
        self,
        clock: SimClock,
        feed,
        strategies,
        account,
        exec_sim,
        risk_engine,
        specs,
        run_id,
        rng_seed,
        audit,
    ) -> None:
        self._clock = clock
        self._feed = feed
        self._strategies = strategies
        self._account = account
        self._exec = exec_sim
        self._risk = risk_engine
        self._specs = specs
        self._run_id = run_id
        self._rng_seed = rng_seed
        self._audit = audit

        # Observability
        # self._acks: list[OrderAck] = []
        # self._fills: list[object] = []
        # self._snaps: list[PortfolioSnapshot] = []
        self._bars: int = 0
        self._intents_in: int = 0
        self._intents_out: int = 0

        # Follow-ups from on_fill to be executed on the next bar
        # self._next_bar_intents: list[OrderIntent] = []

        # Strategy routing and warmup
        # self._strat_symbols: dict[Strategy, set[str]] = {}
        # self._strat_tfs: dict[Strategy, set[str]] = {}
        self._strat_warmup: dict[tuple[int, str, str], int] = {}

        # Per-frame reference prices for risk (symbol -> ref price)
        self._ref_prices: dict[str, float] = {}

    def run(self) -> BacktestReport:
        for s in self._strategies:
            # 1. collect collect all smbols
            pass

        return BacktestReport(
            snapshot=(),
            fills=(),
            acks=(),
            stats={},
        )
