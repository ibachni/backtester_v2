"""
Goals (MVP)
- Metrics from portolio snapshots/trade events
- Equity curve, risk/return summaries at configurable sampling
- Spot + Options
- Side-effect free: pure calculators over inputs, deterministic & reproducible

Inputs
    - Portfolio Snaoshot
    - Position
    - TradeEvent
    - Pricing
    - Calendar and Clock

Outputs:
    - Equity curve
    - period_returns
    - risk_summary
    - trade_summary
    - exposure_series
    - attribution

Working:
    - Receive Portfolio Snapshots
    - Snaps to clock
    - converts the equity curve into returns
    - streams returns into risk/drawdown stats (tracking exposure and PnL components)
    - Returns timeseries (for plot) and a summary (comparing runs)

"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from math import isfinite, log, sqrt
from typing import Optional, Tuple

from backtester.core.clock import parse_timeframe
from backtester.types.types import (
    ZERO,
    Candle,
    FloatPortfolioSnapshot,
    FloatPositionView,
    LotClosedEvent,
    PortfolioSnapshot,
    PositionView,
    TradeEvent,
)

# --- Utilities ---


def _annualization_factor(bar_millseconds: int) -> float:
    seconds_per_year = 365 * 24 * 3600
    bar_seconds = max(1, bar_millseconds) / 1000
    # 365 days for crypto; continuous clock
    return seconds_per_year / bar_seconds


def snapshot_to_float(snap: PortfolioSnapshot) -> FloatPortfolioSnapshot:
    out = FloatPortfolioSnapshot(
        ts=snap.ts,
        base_ccy=snap.base_ccy,
        cash=float(snap.cash),
        equity=float(snap.equity),
        upnl=float(snap.upnl),
        rpnl=float(snap.rpnl),
        gross_exposure=float(snap.gross_exposure),
        net_exposure=float(snap.net_exposure),
        fees_paid=float(snap.fees_paid),
        positions=positionview_to_float(snap.positions),
    )
    return out


def positionview_to_float(
    positions: Tuple[PositionView, ...],
) -> Tuple[FloatPositionView, ...]:
    """Convert Decimal-backed PositionView -> FloatPositionView (single boundary)."""
    out: list[FloatPositionView] = []
    for p in positions:
        out.append(
            FloatPositionView(
                symbol=p.symbol,
                qty=float(p.qty),
                avg_cost=float(p.avg_cost),
                last_price=float(p.last_price),
                market_value=float(p.market_value),
                unrealized_pnl=float(p.unrealized_pnl),
                realized_pnl=float(p.realized_pnl),
                kind=p.kind,
                strike=float(p.strike) if p.strike is not None else None,
                expiry=p.expiry,
                multiplier=float(p.multiplier),
                delta=float(p.delta) if p.delta is not None else None,
            )
        )
    return tuple(out)


# --- Metrics Classes ---


class Sampler:
    """
    Problem:
    - Multiple portfolio updates between bars (do not compute metrics for every one)
    (not always calculate risk etc. on it)
    - Compute metrics only every x bars and not on each bar!

    Working:
    - Stores the last bar and snapshots
    - Emits both the last bar and last snapshots

    Problem of this version: internal ts drift; better: Use clock as SSOT
    # TODO Use Clock instead!
    # TODO Gap Filling
    # TODO Reordering
    """

    def __init__(self, trading_interval: str, metrics_interval: str) -> None:
        self.trading_interval: str = trading_interval
        self.trading_mseconds: int = parse_timeframe(trading_interval)
        self.metrics_interval: str = metrics_interval
        self.metrics_mseconds: int = parse_timeframe(metrics_interval)

        # Next time (ms) at which we should emit a sample
        self._next_emit_ts: Optional[int] = None
        # Latest seen bar and snapshot
        self._last_bar: Optional[Candle] = None
        self._last_snapshot: Optional[FloatPortfolioSnapshot] = None

        # Emission tracking: emit_ts -> version count (avoid duplicates)
        self._emitted_version: dict[int, int] = {}

        # Alignment tolerance (ms)
        self._tolerance_ms: int = 50

    def on_bar(self, bar: Candle) -> None:
        """
        Record the latest bar and initialize the emit schedule if needed.
        """
        self._last_bar = bar
        if self._next_emit_ts is None:
            # Start emitting from the first bar boundary + metrics step
            self._next_emit_ts = bar.end_ms + self.metrics_mseconds

    def on_snapshot(self, snap: PortfolioSnapshot) -> None:
        """
        Record the latest snapshot. This does not emit by itself.
        """
        fsnap = snapshot_to_float(snap=snap)
        self._last_snapshot = fsnap

    def snapshot_update(
        self, snap: PortfolioSnapshot
    ) -> Optional[Tuple[Candle, FloatPortfolioSnapshot]]:
        """
        Optionally returns a (bar, snapshot) pair when the snapshot reaches or
        passes the next emit timestamp and the latest bar aligns within tolerance.
        """
        # Update latest snapshot first
        self.on_snapshot(snap)

        if self._next_emit_ts is None:
            # Need at least one bar to set the schedule
            return None
        if self._last_snapshot is None or self._last_bar is None:
            return None

        emit_ts = self._next_emit_ts

        # Not yet time to emit
        if self._last_snapshot.ts < emit_ts - self._tolerance_ms:
            return None

        # Align bar and snapshot within tolerance
        bar_ts = self._last_bar.end_ms
        snap_ts = self._last_snapshot.ts
        if bar_ts < snap_ts - self._tolerance_ms or bar_ts > snap_ts + self._tolerance_ms:
            pass
            # raise ValueError("Performance Sampler: Snapshot and bar misaligned")

        # Avoid duplicate emission for the same emit_ts
        if self._emitted_version.get(emit_ts, 0) > 0:
            return None

        pair = (self._last_bar, self._last_snapshot)

        # Mark emitted and roll schedule forward in fixed steps
        self._emitted_version[emit_ts] = 1
        while self._next_emit_ts is not None and self._next_emit_ts <= snap_ts:
            self._next_emit_ts += self.metrics_mseconds

        return pair


# --- Metric calculators ---


class ReturnCalculator:
    def __init__(self, kind: str = "log") -> None:
        assert kind in {"log", "simple"}
        self.kind = kind
        self.prev_equity: Optional[float] = None
        self.series: list[Tuple[int, float]] = []

        # TODO equity decimal to be verified

    def update(self, ts: int, equity: float) -> None:
        """
        Needs to be update in case of leverage/funding
        Exchange to float
        """

        # First observation or invalid baselines → set baseline and skip emitting a return
        if (
            self.prev_equity is None
            or not isfinite(equity)
            or equity <= 0.0
            or self.prev_equity <= 0.0
        ):
            self.prev_equity = equity
            return

        if self.kind == "log":
            r = log(max(1e-12, equity) / max(1e-12, self.prev_equity))
        else:
            r = (equity - self.prev_equity) / max(1e-12, self.prev_equity)
        self.series.append((ts, r))
        self.prev_equity = equity


class RiskStats:
    """
    Key API: Update(r) per return
    Internals: Welford's algorithm (to compute Variance); annualization derived from bar_seconds
    Assumptions: risk_free default 0 in MVP
    """

    def __init__(self, bar_millseconds: int, risk_free: float = 0.0) -> None:
        self.n: int = 0
        self.mean: float = 0.0
        self.M2: float = 0.0
        self.risk_free = risk_free
        self.ann_factor = _annualization_factor(bar_millseconds)

    def update(self, r: float) -> None:
        self.n += 1
        delta = r - self.mean
        self.mean += delta / self.n
        delta2 = r - self.mean
        self.M2 += delta * delta2

    @property
    def variance(self) -> float:
        if self.n < 2:
            return 0.0
        return self.M2 / (self.n - 1)

    @property
    def vol_ann(self) -> float:
        return sqrt(max(0.0, self.variance) * self.ann_factor)

    @property
    def sharpe(self) -> float:
        # risk_free applied per-period ~ 0 for crypto MVP
        if self.vol_ann == 0:
            return 0.0
        mean_ann = self.mean * self.ann_factor
        return (mean_ann - self.risk_free) / self.vol_ann


class DrawdownTracker:
    """
    Does what is advertised.
    """

    def __init__(self) -> None:
        self.peak = float("-inf")
        self.max_dd = 0.0
        self.series: list[Tuple[int, float]] = []  # (ts, dd)

    def update(self, ts: int, equity: float) -> None:
        if equity > self.peak:
            self.peak = equity
        dd = 0.0 if self.peak <= 0 else (equity / self.peak - 1.0)
        if dd < self.max_dd:
            self.max_dd = dd
        self.series.append((ts, dd))


class ExposureTracker:
    """
    Track portfolio exposure over time (monitor leverage and directional bias).
    """

    def __init__(self) -> None:
        self.series: list[Tuple[int, float, float, Optional[float]]] = []

    @staticmethod
    def _sum_values(positions: tuple[FloatPositionView, ...]) -> Tuple[float, float]:
        gross = sum(abs(p.market_value) for p in positions)
        net = sum(p.market_value for p in positions)
        return gross, net

    @staticmethod
    def _delta_exposure(positions: tuple[FloatPositionView, ...]) -> Optional[float]:
        deltas = [p.delta * p.multiplier for p in positions if p.delta is not None]
        if not deltas:
            return None
        return sum(deltas)

    def update(self, ts: int, equity: float, positions: tuple[FloatPositionView, ...]) -> None:
        gross_val, net_val = self._sum_values(positions)
        # total leverage (notional per unit equity)
        gross = 0.0 if equity == 0 else gross_val / equity
        net = 0.0 if equity == 0 else net_val / equity
        delta = self._delta_exposure(positions)
        self.series.append((ts, gross, net, delta))


class PnLTracker:
    def __init__(self) -> None:
        self.series: list[Tuple[int, float, float]] = []  # (ts, realized, unrealized)

    def update(self, ts: int, positions: tuple[FloatPositionView, ...]) -> None:
        realized = sum(p.realized_pnl for p in positions)
        unrealized = sum(p.unrealized_pnl for p in positions)
        self.series.append((ts, realized, unrealized))


class TradeStats:
    """
    Aggregates both raw trade prints and round-trip closures.
    """

    _EPS_EQUITY = Decimal("1e-9")

    def __init__(self, net_of_fees: bool = False) -> None:
        self.round_trips: int = 0
        self.wins: int = 0
        self.losses: int = 0
        self._gross_profit: Decimal = ZERO
        self._gross_loss: Decimal = ZERO
        self._hold_ms_sum: float = 0.0
        self._hold_samples: int = 0
        self._closed_pnls: list[Decimal] = []
        self._net = net_of_fees

        self.total_trades: int = 0
        self.turnover_notional: Decimal = ZERO
        self.fees_total: Decimal = ZERO
        self._equity_accum: Decimal = ZERO
        self._equity_samples: int = 0

    def update(self, event: object, equity: Optional[float] = None) -> None:
        """
        Route events to the appropriate tracker.
        """
        if isinstance(event, LotClosedEvent):
            self.update_from_closure(event)
        elif isinstance(event, TradeEvent):
            self.update_from_trade(event, equity)
        else:
            raise TypeError(f"Unsupported trade event type: {type(event)!r}")

    def update_from_trade(self, trade: TradeEvent, equity: Optional[float] = None) -> None:
        """
        Track turnover/fees on every executed trade.
        """
        self.total_trades += 1
        price = Decimal(str(trade.price))
        qty = Decimal(str(trade.qty))
        self.turnover_notional += abs(price * qty)
        if trade.fee:
            self.fees_total += Decimal(str(trade.fee))
        if equity is not None:
            equity_dec = Decimal(str(equity))
            if equity_dec > self._EPS_EQUITY:
                self._equity_accum += equity_dec
                self._equity_samples += 1

    def update_from_closure(self, e: LotClosedEvent) -> None:
        pnl = e.realized_pnl
        if self._net:
            pnl -= e.fee_entry + e.fee_exit

        self.round_trips += 1
        self._closed_pnls.append(pnl)

        if pnl > ZERO:
            self.wins += 1
            self._gross_profit += pnl
        elif pnl < ZERO:
            self.losses += 1
            self._gross_loss += -pnl

        hold = max(0, e.exit_ts - e.entry_ts)
        self._hold_ms_sum += float(hold)
        self._hold_samples += 1

    def summary(self, avg_equity: Optional[float] = None) -> dict[str, float]:
        win_rate = (self.wins / self.round_trips) if self.round_trips else 0.0
        avg_win = float(self._gross_profit / self.wins) if self.wins else 0.0
        avg_loss = float(self._gross_loss / self.losses) if self.losses else 0.0
        payoff = (avg_win / avg_loss) if self.losses and avg_loss > 0 else 0.0
        profit_factor = (
            float(self._gross_profit / self._gross_loss) if self._gross_loss > ZERO else 0.0
        )
        avg_hold_ms = self._hold_ms_sum / self._hold_samples if self._hold_samples > 0 else 0.0

        turnover = float(self.turnover_notional)
        fees = float(self.fees_total)
        equity_denominator: Optional[float] = None
        if avg_equity is not None and avg_equity > float(self._EPS_EQUITY):
            equity_denominator = avg_equity
        elif self._equity_samples:
            equity_denominator = float(self._equity_accum / self._equity_samples)
        turnover_ratio = turnover / equity_denominator if equity_denominator else 0.0

        return {
            "round_trips": float(self.round_trips),
            "wins": float(self.wins),
            "losses": float(self.losses),
            "win_rate": float(win_rate),
            "avg_win": float(avg_win),
            "avg_loss": float(avg_loss),
            "payoff": float(payoff),
            "profit_factor": float(profit_factor),
            "avg_hold_ms": float(avg_hold_ms),
            "num_trades": float(self.total_trades),
            "turnover": turnover,
            "fees": fees,
            "turnover_ratio": float(turnover_ratio),
        }


# --- Configuration ---


@dataclass
class MetricsConfig:
    trading_interval: str
    metrics_interval: Optional[str] = None
    base_ccy: str = "USDC"
    returns_kind: str = "log"  # "log" | "simple"
    risk_free: float = 0.0

    def __post_init__(self) -> None:
        if self.metrics_interval is None:
            self.metrics_interval = self.trading_interval


# --- Orchestration ---


class MetricsEngine:
    def __init__(self, cfg: MetricsConfig = MetricsConfig("5min")) -> None:
        self.cfg = cfg
        metrics_interval = cfg.metrics_interval or cfg.trading_interval
        self.sampler = Sampler(
            trading_interval=cfg.trading_interval, metrics_interval=metrics_interval
        )
        self.returns = ReturnCalculator(cfg.returns_kind)
        self.risk = RiskStats(self.sampler.trading_mseconds, cfg.risk_free)
        self.dd = DrawdownTracker()
        self.expo = ExposureTracker()
        self.pnl = PnLTracker()
        self.tradestats = TradeStats()

        self.equity_curve: list[tuple[int, float]] = []
        self._equity_sum: float = 0.0
        self.equity_count: int = 0

    # --- API ---

    def on_bar(self, bar: Candle) -> None:
        self.sampler.on_bar(bar)

    def on_snapshot(self, snap: PortfolioSnapshot) -> None:
        emissions = self.sampler.snapshot_update(snap)
        if emissions is not None:
            self._emit_bars(emissions[0].end_ms, emissions[1])
            self.equity_curve.append((snap.ts, emissions[1].equity))

    def on_trade(self, tr: TradeEvent, account_equity: Optional[float] = None) -> None:
        self.tradestats.update(tr, account_equity)

    def on_lot_closed(self, lot_closed: LotClosedEvent) -> None:
        self.tradestats.update_from_closure(e=lot_closed)

    # --- Internals ---

    def _emit_bars(self, ts: int, snap: FloatPortfolioSnapshot) -> None:
        equity = snap.equity
        self._equity_sum += equity
        self.equity_count += 1

        # returns/risk
        self.returns.update(ts, equity)
        if self.returns.series:
            self.risk.update(self.returns.series[-1][1])

        # drawdown, exposure, pnl
        self.dd.update(ts, equity)
        self.expo.update(ts, equity, snap.positions)
        self.pnl.update(ts, snap.positions)

    # ---- outputs ----
    def summary(self) -> dict[str, float]:
        eq0 = self.equity_curve[0][1] if self.equity_curve else 0.0
        eqT = self.equity_curve[-1][1] if self.equity_curve else 0.0
        n_periods = max(1, len(self.returns.series))
        ann_factor = _annualization_factor(self.sampler.trading_mseconds)

        cagr = 0.0
        if eq0 > 0 and eqT > 0:
            cagr = (eqT / eq0) ** (ann_factor / n_periods) - 1.0

        avg_equity = self._equity_sum / self.equity_count if self.equity_count else None
        trade_summ = self.tradestats.summary(avg_equity)

        return {
            "CAGR": cagr,
            "AnnVol": self.risk.vol_ann,
            "Sharpe": self.risk.sharpe,
            "MaxDD": self.dd.max_dd,
            "Bars": float(n_periods),
            **trade_summ,
        }

    def timeseries(self) -> dict[str, list[Tuple]]:
        return {
            "equity_curve": self.equity_curve,
            "period_returns": self.returns.series,
            "drawdown": self.dd.series,
            "exposure": self.expo.series,
            "pnl": self.pnl.series,
        }
