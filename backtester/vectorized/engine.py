"""
Vectorized Backtesting Engine for Research.

This module provides:
- CostModel: Fee + slippage configuration for backtest simulation
- BacktestResult: Lightweight result container with metrics and optional equity curve
- VectorizedEngine: Core engine computing equity curves using column-wise operations

Design Principles (per vectorized.md spec):
- ZERO LOOPS: All computation via Polars expressions
- Trade at next bar (position = signal.shift(1))
- Deterministic cost model (fixed bps for fees + slippage)
- Minimal output: metrics dict + optional aligned DataFrame
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

import polars as pl

# -----------------------------------------------------------------------------
# Cost Model
# -----------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CostModel:
    """
    Simple cost model for research backtesting.

    Total cost per trade = (fee_bps + slippage_bps) * turnover
    where turnover = abs(position_change).

    Args:
        fee_bps: Trading fee in basis points (1 bps = 0.01%).
        slippage_bps: Estimated slippage in basis points.

    Example:
        >>> cost = CostModel(fee_bps=10, slippage_bps=5)  # 0.15% round-trip
    """

    fee_bps: float = 0.0
    slippage_bps: float = 0.0

    @property
    def total_bps(self) -> float:
        """Total cost in basis points."""
        return self.fee_bps + self.slippage_bps

    @property
    def total_frac(self) -> float:
        """Total cost as a fraction (for calculations)."""
        return self.total_bps / 10_000.0

    def cost_id(self) -> str:
        """Unique identifier for this cost model."""
        return f"fee{self.fee_bps:.1f}_slip{self.slippage_bps:.1f}"


# -----------------------------------------------------------------------------
# Backtest Result
# -----------------------------------------------------------------------------


@dataclass(slots=True)
class BacktestResult:
    """
    Lightweight backtest result container.

    Attributes:
        metrics: Dictionary of scalar performance metrics.
        params: Strategy parameters used for this run.
        aligned_df: Optional DataFrame with signal, position, returns columns.
    """

    metrics: dict[str, float | int | None]
    params: dict[str, Any] = field(default_factory=dict)
    aligned_df: pl.DataFrame | None = None

    @property
    def sharpe(self) -> float | None:
        """Annualized Sharpe ratio."""
        return self.metrics.get("sharpe")

    @property
    def total_return(self) -> float | None:
        """Total cumulative return."""
        return self.metrics.get("total_return")

    @property
    def max_drawdown(self) -> float | None:
        """Maximum drawdown (as positive fraction)."""
        return self.metrics.get("max_drawdown")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "metrics": self.metrics,
            "params": self.params,
        }


# -----------------------------------------------------------------------------
# Vectorized Engine
# -----------------------------------------------------------------------------


class VectorizedEngine:
    """
    Vectorized backtesting engine for research.

    Computes equity curves and performance metrics using column-wise operations.
    NO LOOPS - all computation is done via Polars expressions.

    Logic Flow:
        1. Signal: 1 (long), -1 (short), 0 (neutral)
        2. Position: signal.shift(1) - trade at next bar
        3. Asset Return: (close[t] / close[t-1]) - 1
        4. Gross Return: position * asset_return
        5. Turnover: abs(position - position.shift(1))
        6. Cost: turnover * cost_model.total_frac
        7. Net Return: gross_return - cost
        8. Equity: cumulative product of (1 + net_return)

    Example:
        >>> engine = VectorizedEngine(cost_model=CostModel(fee_bps=10, slippage_bps=5))
        >>> result = engine.run(lf_with_signals)
        >>> print(f"Sharpe: {result.sharpe:.2f}")
    """

    # Column name constants
    COL_SIGNAL = "signal"
    COL_POSITION = "position"
    COL_ASSET_RETURN = "asset_return"
    COL_GROSS_RETURN = "gross_return"
    COL_TURNOVER = "turnover"
    COL_COST = "cost"
    COL_NET_RETURN = "net_return"
    COL_EQUITY = "equity"

    def __init__(
        self,
        *,
        cost_model: CostModel | None = None,
        price_col: str = "close",
        signal_col: str = "signal",
        time_col: str = "end_ms",
        annualization_factor: float = 252.0,
    ) -> None:
        """
        Initialize the vectorized engine.

        Args:
            cost_model: Fee and slippage model. Defaults to zero costs.
            price_col: Column name for price data (default: "close").
            signal_col: Column name for signals (default: "signal").
            time_col: Column name for timestamps (default: "end_ms").
            annualization_factor: Factor for annualizing metrics (default: 252 for daily).
        """
        self.cost_model = cost_model or CostModel()
        self.price_col = price_col
        self.signal_col = signal_col
        self.time_col = time_col
        self.annualization_factor = annualization_factor

    def run(
        self,
        lf: pl.LazyFrame,
        *,
        return_aligned_df: bool = False,
        params: dict[str, Any] | None = None,
    ) -> BacktestResult:
        """
        Run vectorized backtest on a LazyFrame with signals.

        Args:
            lf: LazyFrame with price and signal columns.
            return_aligned_df: If True, include aligned DataFrame in result.
            params: Strategy parameters to store in result.

        Returns:
            BacktestResult with metrics and optional aligned DataFrame.

        Raises:
            ValueError: If required columns are missing.
        """
        schema = lf.collect_schema()
        self._validate_schema(schema)

        # Build computation pipeline
        lf = self._add_position(lf)
        lf = self._add_returns(lf)
        lf = self._add_costs(lf)
        lf = self._add_equity(lf)

        # Collect and compute metrics
        df = lf.collect()
        metrics = self._compute_metrics(df)

        # Build result
        aligned_df = df if return_aligned_df else None
        return BacktestResult(
            metrics=metrics,
            params=params or {},
            aligned_df=aligned_df,
        )

    def _validate_schema(self, schema: pl.Schema) -> None:
        """Validate that required columns exist."""
        required = [self.price_col, self.signal_col]
        missing = [col for col in required if col not in schema]
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

    def _add_position(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Add position column: trade at next bar (shift signal by 1).

        Position represents the actual held position at each bar,
        which is the signal from the previous bar.
        """
        return lf.with_columns(
            pl.col(self.signal_col).shift(1).fill_null(0).alias(self.COL_POSITION)
        )

    def _add_returns(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Add asset return and gross strategy return columns.

        Asset Return = (price[t] / price[t-1]) - 1
        Gross Return = position[t] * asset_return[t]
        """
        return lf.with_columns(
            # Asset return: simple percentage return
            (pl.col(self.price_col) / pl.col(self.price_col).shift(1) - 1.0)
            .fill_null(0.0)
            .alias(self.COL_ASSET_RETURN),
        ).with_columns(
            # Gross strategy return: position * asset return
            (pl.col(self.COL_POSITION) * pl.col(self.COL_ASSET_RETURN)).alias(
                self.COL_GROSS_RETURN
            ),
        )

    def _add_costs(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Add turnover, cost, and net return columns.

        Turnover = abs(position[t] - position[t-1])
        Cost = turnover * total_cost_fraction
        Net Return = gross_return - cost
        """
        cost_frac = self.cost_model.total_frac

        return lf.with_columns(
            # Turnover: absolute position change (0 to 2 for full reversal)
            (pl.col(self.COL_POSITION) - pl.col(self.COL_POSITION).shift(1))
            .abs()
            .fill_null(0.0)
            .alias(self.COL_TURNOVER),
        ).with_columns(
            # Cost: turnover * cost fraction
            (pl.col(self.COL_TURNOVER) * cost_frac).alias(self.COL_COST),
            # Net return: gross - cost
            (pl.col(self.COL_GROSS_RETURN) - pl.col(self.COL_TURNOVER) * cost_frac).alias(
                self.COL_NET_RETURN
            ),
        )

    def _add_equity(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Add equity curve column.

        Equity = cumulative product of (1 + net_return)
        """
        return lf.with_columns(
            (1.0 + pl.col(self.COL_NET_RETURN)).cum_prod().alias(self.COL_EQUITY)
        )

    def _compute_metrics(self, df: pl.DataFrame) -> dict[str, float | int | None]:
        """
        Compute performance metrics from the backtest DataFrame.

        Returns a dictionary with:
        - sharpe: Annualized Sharpe ratio
        - sortino: Annualized Sortino ratio
        - total_return: Total cumulative return
        - cagr: Compound annual growth rate
        - max_drawdown: Maximum drawdown
        - volatility: Annualized volatility
        - calmar: Calmar ratio (CAGR / max_drawdown)
        - avg_turnover: Average daily turnover
        - trade_count: Number of position changes
        - n_bars: Total number of bars
        - n_bars_in_market: Bars with non-zero position
        """
        n_bars = len(df)
        if n_bars == 0:
            return self._empty_metrics()

        # Extract series for calculations
        net_returns = df[self.COL_NET_RETURN]
        equity = df[self.COL_EQUITY]
        turnover = df[self.COL_TURNOVER]
        position = df[self.COL_POSITION]

        # Basic statistics - cast to float for type safety
        mean_ret = float(net_returns.mean())
        std_ret = float(net_returns.std())

        # Handle edge cases
        if math.isnan(mean_ret) or math.isnan(std_ret):
            return self._empty_metrics()

        # Sharpe ratio (annualized)
        sharpe = (mean_ret / std_ret) * math.sqrt(self.annualization_factor) if std_ret > 0 else 0.0

        # Sortino ratio (downside deviation)
        downside_returns = net_returns.filter(net_returns < 0)
        downside_std_raw = downside_returns.std() if len(downside_returns) > 1 else None
        downside_std = float(downside_std_raw) if downside_std_raw is not None else None
        sortino = (
            (mean_ret / downside_std) * math.sqrt(self.annualization_factor)
            if downside_std and downside_std > 0
            else None
        )

        # Total return
        final_equity = equity[-1] if len(equity) > 0 else 1.0
        total_return = final_equity - 1.0 if final_equity is not None else 0.0

        # CAGR (assuming annualization_factor bars per year)
        years = n_bars / self.annualization_factor
        cagr = (
            (final_equity ** (1.0 / years) - 1.0)
            if final_equity is not None and final_equity > 0 and years > 0
            else 0.0
        )

        # Maximum drawdown
        max_drawdown = self._compute_max_drawdown(equity)

        # Volatility (annualized)
        volatility = std_ret * math.sqrt(self.annualization_factor) if std_ret else 0.0

        # Calmar ratio
        calmar = cagr / max_drawdown if max_drawdown > 0 else None

        # Turnover statistics
        avg_turnover = float(turnover.mean())
        trade_count = int(turnover.filter(turnover > 0).len())

        # Bars in market
        n_bars_in_market = int(position.filter(position != 0).len())

        return {
            "sharpe": round(sharpe, 4) if sharpe else None,
            "sortino": round(sortino, 4) if sortino else None,
            "total_return": round(total_return, 6) if total_return else None,
            "cagr": round(cagr, 6) if cagr else None,
            "max_drawdown": round(max_drawdown, 6) if max_drawdown else None,
            "volatility": round(volatility, 6) if volatility else None,
            "calmar": round(calmar, 4) if calmar else None,
            "avg_turnover": round(avg_turnover, 6) if avg_turnover else None,
            "trade_count": trade_count,
            "n_bars": n_bars,
            "n_bars_in_market": n_bars_in_market,
        }

    def _compute_max_drawdown(self, equity: pl.Series) -> float:
        """
        Compute maximum drawdown from equity series.

        Max Drawdown = max((peak - equity) / peak)
        """
        if len(equity) == 0:
            return 0.0

        # Convert to numpy for cummax (not available as Series method)
        eq_arr = equity.to_numpy()
        import numpy as np

        # Running maximum (peak)
        peak = np.maximum.accumulate(eq_arr)

        # Drawdown at each point
        drawdown = (peak - eq_arr) / np.where(peak > 0, peak, 1.0)

        return float(np.nanmax(drawdown)) if len(drawdown) > 0 else 0.0

    def _empty_metrics(self) -> dict[str, float | int | None]:
        """Return empty metrics dictionary."""
        return {
            "sharpe": None,
            "sortino": None,
            "total_return": None,
            "cagr": None,
            "max_drawdown": None,
            "volatility": None,
            "calmar": None,
            "avg_turnover": None,
            "trade_count": 0,
            "n_bars": 0,
            "n_bars_in_market": 0,
        }


# -----------------------------------------------------------------------------
# Convenience Functions
# -----------------------------------------------------------------------------


def quick_backtest(
    lf: pl.LazyFrame,
    *,
    fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
    price_col: str = "close",
    signal_col: str = "signal",
) -> dict[str, float | int | None]:
    """
    Quick backtest returning only metrics dictionary.

    Convenience function for optimization loops where only metrics are needed.

    Args:
        lf: LazyFrame with price and signal columns.
        fee_bps: Trading fee in basis points.
        slippage_bps: Slippage in basis points.
        price_col: Price column name.
        signal_col: Signal column name.

    Returns:
        Dictionary of performance metrics.

    Example:
        >>> metrics = quick_backtest(lf, fee_bps=10)
        >>> print(f"Sharpe: {metrics['sharpe']}")
    """
    engine = VectorizedEngine(
        cost_model=CostModel(fee_bps=fee_bps, slippage_bps=slippage_bps),
        price_col=price_col,
        signal_col=signal_col,
    )
    result = engine.run(lf, return_aligned_df=False)
    return result.metrics


__all__ = [
    "BacktestResult",
    "CostModel",
    "VectorizedEngine",
    "quick_backtest",
]
