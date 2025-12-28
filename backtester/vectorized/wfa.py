"""
Walk-Forward Analysis (WFA) for Vectorized Backtesting.

This module provides:
- TimeWindow: Single train/test window definition
- TimeSlicer: Generate rolling or anchored train/test windows
- WFAResult: Container for walk-forward analysis results
- WalkForwardValidator: Orchestrate train/test optimization cycles

Design Principles (per vectorized.md spec):
- Rolling: Train window moves forward (constant size) - adapts to recent regimes
- Anchored: Train window grows (start date fixed) - accumulates more history
- Warmup buffer: Test slice starts before cutoff so indicators are valid
- Result stitching: Concatenate OOS equity curves into master performance series
"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeAlias

import polars as pl

from backtester.vectorized.engine import CostModel, VectorizedEngine
from backtester.vectorized.optimizer import ParamOptimizer

logger = logging.getLogger(__name__)

# Type aliases
ParamSet: TypeAlias = dict[str, Any]
SignalGenerator: TypeAlias = Callable[[pl.DataFrame, ParamSet], pl.LazyFrame]


# -----------------------------------------------------------------------------
# Time Window
# -----------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TimeWindow:
    """
    A single train/test window for walk-forward analysis.

    Attributes:
        train_start: Start of training (in-sample) period.
        train_end: End of training period (exclusive).
        test_start: Start of testing (out-of-sample) period.
        test_end: End of testing period (exclusive).
        warmup_start: Start of warmup buffer (for indicator calculation).
        window_id: Sequential identifier for this window.
    """

    train_start: dt.datetime
    train_end: dt.datetime
    test_start: dt.datetime
    test_end: dt.datetime
    warmup_start: dt.datetime
    window_id: int

    def __post_init__(self) -> None:
        if self.train_end <= self.train_start:
            raise ValueError("train_end must be after train_start")
        if self.test_end <= self.test_start:
            raise ValueError("test_end must be after test_start")
        if self.warmup_start > self.test_start:
            raise ValueError("warmup_start cannot be after test_start")

    @property
    def train_days(self) -> int:
        """Number of days in training period."""
        return (self.train_end - self.train_start).days

    @property
    def test_days(self) -> int:
        """Number of days in testing period."""
        return (self.test_end - self.test_start).days

    @property
    def warmup_days(self) -> int:
        """Number of days in warmup buffer."""
        return (self.test_start - self.warmup_start).days

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "window_id": self.window_id,
            "train_start": self.train_start.isoformat(),
            "train_end": self.train_end.isoformat(),
            "test_start": self.test_start.isoformat(),
            "test_end": self.test_end.isoformat(),
            "warmup_start": self.warmup_start.isoformat(),
            "train_days": self.train_days,
            "test_days": self.test_days,
        }


# -----------------------------------------------------------------------------
# Time Slicer
# -----------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TimeSlicer:
    """
    Generate train/test windows for walk-forward analysis.

    Supports two modes:
    - Rolling: Train window moves forward (constant size). Good for adapting
      to recent market regimes.
    - Anchored: Train window grows (start date fixed). Good for accumulating
      more historical data over time.

    Args:
        train_days: Number of days in each training period.
        test_days: Number of days in each testing period.
        warmup_days: Number of days before test_start for indicator warmup.
        anchored: If True, use anchored mode (growing train window).

    Example:
        >>> slicer = TimeSlicer(train_days=365, test_days=30, warmup_days=50)
        >>> windows = list(slicer.generate(start, end))
    """

    train_days: int
    test_days: int
    warmup_days: int = 0
    anchored: bool = False

    def __post_init__(self) -> None:
        if self.train_days <= 0:
            raise ValueError("train_days must be positive")
        if self.test_days <= 0:
            raise ValueError("test_days must be positive")
        if self.warmup_days < 0:
            raise ValueError("warmup_days cannot be negative")

    @property
    def min_days_required(self) -> int:
        """Minimum number of days required for at least one window."""
        return self.train_days + self.test_days

    def generate(
        self,
        start: dt.datetime,
        end: dt.datetime,
    ) -> Iterator[TimeWindow]:
        """
        Generate train/test windows for the given date range.

        Args:
            start: Start of the full date range.
            end: End of the full date range.

        Yields:
            TimeWindow objects for each train/test period.

        Raises:
            ValueError: If date range is too short for even one window.
        """
        total_days = (end - start).days
        if total_days < self.min_days_required:
            raise ValueError(
                f"Date range ({total_days} days) too short. "
                f"Need at least {self.min_days_required} days "
                f"(train={self.train_days}, test={self.test_days})."
            )

        train_delta = dt.timedelta(days=self.train_days)
        test_delta = dt.timedelta(days=self.test_days)
        warmup_delta = dt.timedelta(days=self.warmup_days)

        window_id = 0
        anchor_start = start

        if self.anchored:
            # Anchored mode: train always starts at anchor_start
            train_start = anchor_start
            cutoff = train_start + train_delta

            while cutoff + test_delta <= end:
                test_start = cutoff
                test_end = cutoff + test_delta
                warmup_start = max(test_start - warmup_delta, train_start)

                yield TimeWindow(
                    train_start=train_start,
                    train_end=cutoff,
                    test_start=test_start,
                    test_end=test_end,
                    warmup_start=warmup_start,
                    window_id=window_id,
                )

                window_id += 1
                cutoff = test_end  # Move cutoff forward by test_days
        else:
            # Rolling mode: train window slides forward
            train_start = anchor_start
            cutoff = train_start + train_delta

            while cutoff + test_delta <= end:
                test_start = cutoff
                test_end = cutoff + test_delta
                warmup_start = max(test_start - warmup_delta, train_start)

                yield TimeWindow(
                    train_start=train_start,
                    train_end=cutoff,
                    test_start=test_start,
                    test_end=test_end,
                    warmup_start=warmup_start,
                    window_id=window_id,
                )

                window_id += 1
                # Slide train window forward by test_days
                train_start = train_start + test_delta
                cutoff = train_start + train_delta

    def count_windows(self, start: dt.datetime, end: dt.datetime) -> int:
        """Count how many windows will be generated for the date range."""
        return sum(1 for _ in self.generate(start, end))


# -----------------------------------------------------------------------------
# WFA Window Result
# -----------------------------------------------------------------------------


@dataclass(slots=True)
class WFAWindowResult:
    """
    Result for a single walk-forward window.

    Attributes:
        window: The time window definition.
        best_params: Optimal parameters found during training.
        train_metrics: Metrics from training (in-sample).
        test_metrics: Metrics from testing (out-of-sample).
        test_equity: Equity curve for the test period (optional).
    """

    window: TimeWindow
    best_params: ParamSet
    train_metrics: dict[str, float | int | None]
    test_metrics: dict[str, float | int | None]
    test_equity: pl.DataFrame | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            **self.window.to_dict(),
            "best_params": self.best_params,
            "train_metrics": self.train_metrics,
            "test_metrics": self.test_metrics,
        }


# -----------------------------------------------------------------------------
# WFA Result
# -----------------------------------------------------------------------------


@dataclass(slots=True)
class WFAResult:
    """
    Container for walk-forward analysis results.

    Attributes:
        window_results: Results for each train/test window.
        stitched_equity: Combined out-of-sample equity curve.
        aggregate_metrics: Overall performance metrics across all OOS periods.
    """

    window_results: list[WFAWindowResult]
    stitched_equity: pl.DataFrame | None = None
    aggregate_metrics: dict[str, float | int | None] = field(default_factory=dict)

    @property
    def n_windows(self) -> int:
        """Number of walk-forward windows."""
        return len(self.window_results)

    @property
    def all_params(self) -> list[ParamSet]:
        """All best parameters across windows."""
        return [wr.best_params for wr in self.window_results]

    def to_dataframe(self) -> pl.DataFrame:
        """
        Convert window results to a Polars DataFrame for analysis.

        Returns:
            DataFrame with one row per window, containing params and metrics.
        """
        if not self.window_results:
            return pl.DataFrame()

        rows = []
        for wr in self.window_results:
            row = {
                "window_id": wr.window.window_id,
                "train_start": wr.window.train_start,
                "train_end": wr.window.train_end,
                "test_start": wr.window.test_start,
                "test_end": wr.window.test_end,
                **{f"param_{k}": v for k, v in wr.best_params.items()},
                **{f"train_{k}": v for k, v in wr.train_metrics.items()},
                **{f"test_{k}": v for k, v in wr.test_metrics.items()},
            }
            rows.append(row)

        return pl.DataFrame(rows)


# -----------------------------------------------------------------------------
# Walk-Forward Validator
# -----------------------------------------------------------------------------


class WalkForwardValidator:
    """
    Orchestrate walk-forward analysis for strategy validation.

    Walk-forward analysis validates robustness by:
    1. Optimizing parameters on training (in-sample) data
    2. Testing with best params on out-of-sample data
    3. Rolling the window forward and repeating
    4. Stitching all out-of-sample results into a master equity curve

    Example:
        >>> validator = WalkForwardValidator(
        ...     train_days=365,
        ...     test_days=30,
        ...     warmup_days=50,
        ...     cost_model=CostModel(fee_bps=10),
        ... )
        >>> result = validator.run(
        ...     data=price_data,
        ...     signal_generator=my_signal_fn,
        ...     param_ranges={"window": [10, 20, 50]},
        ... )
        >>> print(f"OOS Sharpe: {result.aggregate_metrics['sharpe']:.2f}")
    """

    def __init__(
        self,
        *,
        train_days: int,
        test_days: int,
        warmup_days: int = 0,
        anchored: bool = False,
        cost_model: CostModel | None = None,
        price_col: str = "close",
        signal_col: str = "signal",
        time_col: str = "end_ms",
        rank_by: str = "sharpe",
        n_jobs: int = -1,
    ) -> None:
        """
        Initialize the walk-forward validator.

        Args:
            train_days: Number of days in each training period.
            test_days: Number of days in each testing period.
            warmup_days: Number of days before test_start for indicator warmup.
            anchored: If True, use anchored mode (growing train window).
            cost_model: Fee and slippage model for backtests.
            price_col: Column name for price data.
            signal_col: Column name for signals.
            time_col: Column name for timestamps (ms epoch).
            rank_by: Metric to rank optimization results.
            n_jobs: Number of parallel jobs for optimization (-1 = all cores).
        """
        self.slicer = TimeSlicer(
            train_days=train_days,
            test_days=test_days,
            warmup_days=warmup_days,
            anchored=anchored,
        )
        self.cost_model = cost_model or CostModel()
        self.price_col = price_col
        self.signal_col = signal_col
        self.time_col = time_col
        self.rank_by = rank_by
        self.n_jobs = n_jobs

    def run(
        self,
        data: pl.LazyFrame | pl.DataFrame,
        signal_generator: SignalGenerator,
        param_ranges: dict[str, Sequence[Any]],
        *,
        start: dt.datetime | None = None,
        end: dt.datetime | None = None,
        n_random: int | None = None,
        seed: int | None = None,
        return_equity: bool = False,
    ) -> WFAResult:
        """
        Run walk-forward analysis.

        Args:
            data: Price data as LazyFrame or DataFrame.
            signal_generator: Function(df, params) -> LazyFrame with signals.
            param_ranges: Dictionary of parameter name -> list of values.
            start: Start date for analysis (inferred from data if None).
            end: End date for analysis (inferred from data if None).
            n_random: If provided, use random search with this many samples.
            seed: Random seed for reproducibility.
            return_equity: If True, store equity curves for each window.

        Returns:
            WFAResult with per-window results and stitched OOS equity.
        """
        # Pre-collect data once
        if isinstance(data, pl.LazyFrame):
            logger.debug("Collecting LazyFrame for WFA")
            df = data.collect()
        else:
            df = data

        # Infer date range from data if not provided
        start_dt, end_dt = self._infer_date_range(df, start, end)

        # Generate windows
        windows = list(self.slicer.generate(start_dt, end_dt))
        if not windows:
            raise ValueError("No valid windows generated for the date range")

        logger.info(
            f"Starting WFA: {len(windows)} windows, "
            f"train={self.slicer.train_days}d, test={self.slicer.test_days}d"
        )

        # Process each window
        window_results: list[WFAWindowResult] = []
        for window in windows:
            result = self._process_window(
                df=df,
                window=window,
                signal_generator=signal_generator,
                param_ranges=param_ranges,
                n_random=n_random,
                seed=seed,
                return_equity=return_equity,
            )
            window_results.append(result)

            logger.debug(
                f"Window {window.window_id}: "
                f"train_sharpe={result.train_metrics.get('sharpe', 'N/A'):.2f}, "
                f"test_sharpe={result.test_metrics.get('sharpe', 'N/A'):.2f}"
            )

        # Stitch OOS equity curves
        stitched = self._stitch_equity_curves(window_results, df) if return_equity else None

        # Compute aggregate metrics
        aggregate = self._compute_aggregate_metrics(window_results)

        return WFAResult(
            window_results=window_results,
            stitched_equity=stitched,
            aggregate_metrics=aggregate,
        )

    def _infer_date_range(
        self,
        df: pl.DataFrame,
        start: dt.datetime | None,
        end: dt.datetime | None,
    ) -> tuple[dt.datetime, dt.datetime]:
        """Infer date range from data if not provided."""
        if start is not None and end is not None:
            return start, end

        # Get min/max timestamps from data
        time_stats = df.select(
            pl.col(self.time_col).min().alias("min_ts"),
            pl.col(self.time_col).max().alias("max_ts"),
        ).row(0)

        min_ts, max_ts = time_stats

        if start is None:
            start = dt.datetime.fromtimestamp(min_ts / 1000, tz=dt.timezone.utc)
        if end is None:
            end = dt.datetime.fromtimestamp(max_ts / 1000, tz=dt.timezone.utc)

        return start, end

    def _process_window(
        self,
        df: pl.DataFrame,
        window: TimeWindow,
        signal_generator: SignalGenerator,
        param_ranges: dict[str, Sequence[Any]],
        n_random: int | None,
        seed: int | None,
        return_equity: bool,
    ) -> WFAWindowResult:
        """Process a single train/test window."""
        # Slice training data
        train_df = self._slice_data(df, window.train_start, window.train_end)

        # Optimize on training data
        optimizer = ParamOptimizer(
            cost_model=self.cost_model,
            price_col=self.price_col,
            signal_col=self.signal_col,
            n_jobs=self.n_jobs,
        )

        # Use sequential mode for single-threaded execution (avoids joblib dependency)
        if self.n_jobs == 1:
            opt_result = optimizer.run_sequential(
                data=train_df,
                signal_generator=signal_generator,
                param_ranges=param_ranges,
                n_random=n_random,
                seed=seed,
                rank_by=self.rank_by,
            )
        else:
            opt_result = optimizer.run(
                data=train_df,
                signal_generator=signal_generator,
                param_ranges=param_ranges,
                n_random=n_random,
                seed=seed,
                rank_by=self.rank_by,
            )

        best_params = opt_result.best_params or {}
        train_metrics = opt_result.best_metrics or {}

        # Slice test data (including warmup buffer)
        test_df = self._slice_data(df, window.warmup_start, window.test_end)

        # Run backtest on test data with best params
        engine = VectorizedEngine(
            cost_model=self.cost_model,
            price_col=self.price_col,
            signal_col=self.signal_col,
            time_col=self.time_col,
        )

        lf_with_signals = signal_generator(test_df, best_params)
        test_result = engine.run(lf_with_signals, return_aligned_df=return_equity)

        # Truncate warmup period from results
        test_metrics = test_result.metrics
        test_equity = None

        if return_equity and test_result.aligned_df is not None:
            test_equity = self._truncate_warmup(
                test_result.aligned_df,
                window.test_start,
            )

        return WFAWindowResult(
            window=window,
            best_params=best_params,
            train_metrics=train_metrics,
            test_metrics=test_metrics,
            test_equity=test_equity,
        )

    def _slice_data(
        self,
        df: pl.DataFrame,
        start: dt.datetime,
        end: dt.datetime,
    ) -> pl.DataFrame:
        """Slice DataFrame by date range."""
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)

        return df.filter((pl.col(self.time_col) >= start_ms) & (pl.col(self.time_col) < end_ms))

    def _truncate_warmup(
        self,
        df: pl.DataFrame,
        test_start: dt.datetime,
    ) -> pl.DataFrame:
        """Remove warmup period from test results."""
        test_start_ms = int(test_start.timestamp() * 1000)
        return df.filter(pl.col(self.time_col) >= test_start_ms)

    def _stitch_equity_curves(
        self,
        results: list[WFAWindowResult],
        original_df: pl.DataFrame,
    ) -> pl.DataFrame | None:
        """
        Stitch out-of-sample equity curves into a single continuous series.

        Re-indexes equity to be continuous (compounds across stitches).
        """
        equity_dfs = []
        cumulative_factor = 1.0

        for wr in results:
            if wr.test_equity is None:
                continue

            # Get the equity column
            eq_df = wr.test_equity.select([self.time_col, "equity"])

            if eq_df.is_empty():
                continue

            # Get first and last equity values
            first_equity = eq_df.row(0)[1]
            last_equity = eq_df.row(-1)[1]

            if first_equity is None or first_equity == 0:
                continue

            # Scale equity to continue from previous cumulative level
            scaled_df = eq_df.with_columns(
                (pl.col("equity") / first_equity * cumulative_factor).alias("equity")
            )

            equity_dfs.append(scaled_df)

            # Update cumulative factor for next window
            if last_equity is not None and first_equity != 0:
                cumulative_factor *= last_equity / first_equity

        if not equity_dfs:
            return None

        return pl.concat(equity_dfs).sort(self.time_col)

    def _compute_aggregate_metrics(
        self,
        results: list[WFAWindowResult],
    ) -> dict[str, float | int | None]:
        """
        Compute aggregate metrics across all out-of-sample windows.

        Returns average metrics weighted by test period length.
        """
        if not results:
            return {}

        # Collect test metrics from all windows
        sharpes = []
        total_returns = []
        max_drawdowns = []

        for wr in results:
            if wr.test_metrics.get("sharpe") is not None:
                sharpes.append(wr.test_metrics["sharpe"])
            if wr.test_metrics.get("total_return") is not None:
                total_returns.append(wr.test_metrics["total_return"])
            if wr.test_metrics.get("max_drawdown") is not None:
                max_drawdowns.append(wr.test_metrics["max_drawdown"])

        aggregate: dict[str, float | int | None] = {
            "n_windows": len(results),
        }

        if sharpes:
            aggregate["avg_sharpe"] = sum(sharpes) / len(sharpes)
            aggregate["min_sharpe"] = min(sharpes)
            aggregate["max_sharpe"] = max(sharpes)

        if total_returns:
            # Compound total returns across windows
            compounded = 1.0
            for ret in total_returns:
                compounded *= 1.0 + ret
            aggregate["total_return"] = compounded - 1.0
            aggregate["avg_return"] = sum(total_returns) / len(total_returns)

        if max_drawdowns:
            aggregate["worst_drawdown"] = max(max_drawdowns)
            aggregate["avg_drawdown"] = sum(max_drawdowns) / len(max_drawdowns)

        return aggregate


__all__ = [
    "TimeWindow",
    "TimeSlicer",
    "WFAWindowResult",
    "WFAResult",
    "WalkForwardValidator",
]
