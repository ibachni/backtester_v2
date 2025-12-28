"""
Parameter Optimization Engine for Vectorized Backtesting.

This module provides:
- ParamGrid: Generate parameter combinations for grid/random search
- OptimizationResult: Container for optimization results with score table
- ParamOptimizer: High-speed parallel optimization over strategy parameters

Design Principles (per vectorized.md spec):
- Pre-collect data ONCE before parallel loop (zero-copy sharing via threading)
- Use joblib.Parallel with threading backend (Polars/NumPy release GIL)
- Workers return only metrics dict (minimize IPC data transfer)
- Robust error handling: failed params return None, don't crash optimization
"""

from __future__ import annotations

import itertools
import logging
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeAlias

import polars as pl

from backtester.vectorized.engine import CostModel, VectorizedEngine

logger = logging.getLogger(__name__)

# Type aliases
ParamSet: TypeAlias = dict[str, Any]
MetricsDict: TypeAlias = dict[str, float | int | None]
SignalGenerator: TypeAlias = Callable[[pl.DataFrame, ParamSet], pl.LazyFrame]


# -----------------------------------------------------------------------------
# Parameter Grid Generation
# -----------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ParamGrid:
    """
    Parameter grid generator for optimization.

    Supports both grid search (all combinations) and random search
    (sampled subset of combinations).

    Example:
        >>> grid = ParamGrid({"ma_fast": [5, 10, 20], "ma_slow": [50, 100, 200]})
        >>> list(grid.generate())  # 9 combinations
        >>> list(grid.generate(n_random=5))  # 5 random samples
    """

    param_ranges: dict[str, Sequence[Any]]

    def __post_init__(self) -> None:
        if not self.param_ranges:
            raise ValueError("param_ranges cannot be empty")
        for name, values in self.param_ranges.items():
            if not values:
                raise ValueError(f"Parameter '{name}' has empty range")

    @property
    def grid_size(self) -> int:
        """Total number of combinations in full grid."""
        size = 1
        for values in self.param_ranges.values():
            size *= len(values)
        return size

    @property
    def param_names(self) -> tuple[str, ...]:
        """Parameter names in consistent order."""
        return tuple(self.param_ranges.keys())

    def generate(
        self, *, n_random: int | None = None, seed: int | None = None
    ) -> Iterable[ParamSet]:
        """
        Generate parameter combinations.

        Args:
            n_random: If provided, sample this many random combinations.
                      If None, generate full grid (all combinations).
            seed: Random seed for reproducibility (only used with n_random).

        Yields:
            Dictionary of parameter name -> value for each combination.
        """
        names = self.param_names
        all_values = [self.param_ranges[name] for name in names]

        if n_random is None:
            # Full grid search
            for combo in itertools.product(*all_values):
                yield dict(zip(names, combo, strict=True))
        else:
            # Random search
            import random

            rng = random.Random(seed)
            grid_size = self.grid_size

            if n_random >= grid_size:
                # If requesting more samples than grid size, return full grid
                yield from self.generate(n_random=None)
                return

            # Sample indices without replacement
            indices = rng.sample(range(grid_size), n_random)
            all_combos = list(itertools.product(*all_values))

            for idx in indices:
                yield dict(zip(names, all_combos[idx], strict=True))


def generate_grid(param_ranges: dict[str, Sequence[Any]]) -> list[ParamSet]:
    """
    Convenience function to generate full parameter grid.

    Args:
        param_ranges: Dictionary of parameter name -> list of values.

    Returns:
        List of all parameter combinations as dictionaries.

    Example:
        >>> grid = generate_grid({"window": [10, 20], "threshold": [0.5, 1.0]})
        >>> len(grid)  # 4 combinations
    """
    return list(ParamGrid(param_ranges).generate())


# -----------------------------------------------------------------------------
# Optimization Result
# -----------------------------------------------------------------------------


@dataclass(slots=True)
class OptimizationResult:
    """
    Container for optimization results.

    Attributes:
        results: List of (params, metrics) tuples for successful runs.
        failed: List of params that failed during optimization.
        best_params: Parameters with best score (based on rank_by metric).
        best_metrics: Metrics for the best parameter set.
        score_table: Polars DataFrame with all results for analysis.
    """

    results: list[tuple[ParamSet, MetricsDict]]
    failed: list[ParamSet] = field(default_factory=list)
    rank_by: str = "sharpe"
    rank_descending: bool = True

    @property
    def n_successful(self) -> int:
        """Number of successful parameter evaluations."""
        return len(self.results)

    @property
    def n_failed(self) -> int:
        """Number of failed parameter evaluations."""
        return len(self.failed)

    @property
    def best_params(self) -> ParamSet | None:
        """Parameters with best score."""
        if not self.results:
            return None
        return self._get_sorted_results()[0][0]

    @property
    def best_metrics(self) -> MetricsDict | None:
        """Metrics for best parameter set."""
        if not self.results:
            return None
        return self._get_sorted_results()[0][1]

    def _get_sorted_results(self) -> list[tuple[ParamSet, MetricsDict]]:
        """Sort results by ranking metric."""

        def sort_key(item: tuple[ParamSet, MetricsDict]) -> float:
            val = item[1].get(self.rank_by)
            if val is None:
                return float("-inf") if self.rank_descending else float("inf")
            return float(val)

        return sorted(self.results, key=sort_key, reverse=self.rank_descending)

    def top_n(self, n: int = 10) -> list[tuple[ParamSet, MetricsDict]]:
        """Get top N results by ranking metric."""
        return self._get_sorted_results()[:n]

    def to_dataframe(self) -> pl.DataFrame:
        """
        Convert results to Polars DataFrame for analysis.

        Returns:
            DataFrame with columns for each parameter and metric.
        """
        if not self.results:
            return pl.DataFrame()

        rows = []
        for params, metrics in self.results:
            row = {**params, **metrics}
            rows.append(row)

        return pl.DataFrame(rows)

    def score_table(self, top_n: int | None = None) -> pl.DataFrame:
        """
        Get score table sorted by ranking metric.

        Args:
            top_n: If provided, return only top N results.

        Returns:
            Sorted DataFrame with parameters and metrics.
        """
        df = self.to_dataframe()
        if df.is_empty():
            return df

        # Sort by ranking metric
        df = df.sort(self.rank_by, descending=self.rank_descending, nulls_last=True)

        if top_n is not None:
            df = df.head(top_n)

        return df


# -----------------------------------------------------------------------------
# Worker Function
# -----------------------------------------------------------------------------


def _run_single_backtest(
    df: pl.DataFrame,
    params: ParamSet,
    signal_generator: SignalGenerator,
    engine: VectorizedEngine,
) -> tuple[ParamSet, MetricsDict] | None:
    """
    Worker function: run one backtest with given parameters.

    This function is designed for parallel execution. It:
    - Takes a pre-collected DataFrame (zero-copy in threading mode)
    - Generates signals using the provided generator function
    - Runs backtest and returns only metrics (minimal IPC)
    - Returns None on failure (robust error handling)

    Args:
        df: Pre-collected price data DataFrame.
        params: Parameter dictionary for this run.
        signal_generator: Function to generate signals from data + params.
        engine: VectorizedEngine instance for backtesting.

    Returns:
        Tuple of (params, metrics) on success, None on failure.
    """
    try:
        # Generate signals with these parameters
        lf_with_signals = signal_generator(df, params)

        # Run backtest
        result = engine.run(lf_with_signals, params=params)

        return (params, result.metrics)

    except Exception as e:
        logger.warning(f"Backtest failed for params {params}: {e}")
        return None


# -----------------------------------------------------------------------------
# Parameter Optimizer
# -----------------------------------------------------------------------------


class ParamOptimizer:
    """
    High-speed parameter optimizer for vectorized backtesting.

    Uses joblib.Parallel with threading backend for efficient parallel
    execution. All threads share the same memory pointer to the DataFrame
    (zero-copy), avoiding the overhead of process-based parallelism.

    Example:
        >>> def signal_gen(df, params):
        ...     return df.lazy().with_columns(
        ...         pl.when(pl.col("close") > pl.col("close").rolling_mean(params["window"]))
        ...         .then(1).otherwise(-1).alias("signal")
        ...     )
        >>>
        >>> optimizer = ParamOptimizer(cost_model=CostModel(fee_bps=10))
        >>> result = optimizer.run(
        ...     data=my_lazyframe,
        ...     signal_generator=signal_gen,
        ...     param_ranges={"window": [10, 20, 50, 100]},
        ... )
        >>> print(result.best_params)
    """

    def __init__(
        self,
        *,
        cost_model: CostModel | None = None,
        price_col: str = "close",
        signal_col: str = "signal",
        n_jobs: int = -1,
        verbose: int = 0,
    ) -> None:
        """
        Initialize the parameter optimizer.

        Args:
            cost_model: Fee and slippage model for backtests.
            price_col: Column name for price data.
            signal_col: Column name for signals.
            n_jobs: Number of parallel jobs (-1 = all cores).
            verbose: Verbosity level for joblib (0 = silent).
        """
        self.cost_model = cost_model or CostModel()
        self.price_col = price_col
        self.signal_col = signal_col
        self.n_jobs = n_jobs
        self.verbose = verbose

    def run(
        self,
        data: pl.LazyFrame | pl.DataFrame,
        signal_generator: SignalGenerator,
        param_ranges: dict[str, Sequence[Any]],
        *,
        n_random: int | None = None,
        seed: int | None = None,
        rank_by: str = "sharpe",
        rank_descending: bool = True,
    ) -> OptimizationResult:
        """
        Run parallel parameter optimization.

        Args:
            data: Price data as LazyFrame or DataFrame.
            signal_generator: Function(df, params) -> LazyFrame with signals.
            param_ranges: Dictionary of parameter name -> list of values.
            n_random: If provided, use random search with this many samples.
            seed: Random seed for reproducibility (with n_random).
            rank_by: Metric to rank results by (default: "sharpe").
            rank_descending: Sort descending (True = higher is better).

        Returns:
            OptimizationResult with all results and best parameters.
        """
        # Pre-collect data ONCE (critical optimization)
        if isinstance(data, pl.LazyFrame):
            logger.debug("Collecting LazyFrame before optimization loop")
            df = data.collect()
        else:
            df = data

        # Generate parameter grid
        grid = ParamGrid(param_ranges)
        param_list = list(grid.generate(n_random=n_random, seed=seed))

        logger.info(
            f"Starting optimization: {len(param_list)} parameter sets, " f"{self.n_jobs} jobs"
        )

        # Create engine (shared across threads)
        engine = VectorizedEngine(
            cost_model=self.cost_model,
            price_col=self.price_col,
            signal_col=self.signal_col,
        )

        # Run parallel optimization
        results = self._run_parallel(df, param_list, signal_generator, engine)

        # Separate successful and failed runs
        successful = []
        failed = []
        for params, outcome in zip(param_list, results, strict=True):
            if outcome is not None:
                successful.append(outcome)
            else:
                failed.append(params)

        if failed:
            logger.warning(f"{len(failed)} parameter sets failed during optimization")

        return OptimizationResult(
            results=successful,
            failed=failed,
            rank_by=rank_by,
            rank_descending=rank_descending,
        )

    def _run_parallel(
        self,
        df: pl.DataFrame,
        param_list: list[ParamSet],
        signal_generator: SignalGenerator,
        engine: VectorizedEngine,
    ) -> list[tuple[ParamSet, MetricsDict] | None]:
        """
        Run backtests in parallel using joblib.

        Uses threading backend for zero-copy DataFrame sharing.
        """
        from joblib import Parallel, delayed

        # Use threading backend - all threads share the same DataFrame
        # Polars and NumPy release the GIL, so this is efficient
        results = Parallel(n_jobs=self.n_jobs, backend="threading", verbose=self.verbose)(
            delayed(_run_single_backtest)(df, params, signal_generator, engine)
            for params in param_list
        )

        return list(results)

    def run_sequential(
        self,
        data: pl.LazyFrame | pl.DataFrame,
        signal_generator: SignalGenerator,
        param_ranges: dict[str, Sequence[Any]],
        *,
        n_random: int | None = None,
        seed: int | None = None,
        rank_by: str = "sharpe",
        rank_descending: bool = True,
    ) -> OptimizationResult:
        """
        Run sequential parameter optimization (for debugging/profiling).

        Same interface as run(), but executes serially without joblib.
        Useful for debugging signal generators or profiling single runs.
        """
        # Pre-collect data
        if isinstance(data, pl.LazyFrame):
            df = data.collect()
        else:
            df = data

        # Generate parameter grid
        grid = ParamGrid(param_ranges)
        param_list = list(grid.generate(n_random=n_random, seed=seed))

        # Create engine
        engine = VectorizedEngine(
            cost_model=self.cost_model,
            price_col=self.price_col,
            signal_col=self.signal_col,
        )

        # Run sequentially
        successful = []
        failed = []
        for params in param_list:
            result = _run_single_backtest(df, params, signal_generator, engine)
            if result is not None:
                successful.append(result)
            else:
                failed.append(params)

        return OptimizationResult(
            results=successful,
            failed=failed,
            rank_by=rank_by,
            rank_descending=rank_descending,
        )


__all__ = [
    "ParamGrid",
    "OptimizationResult",
    "ParamOptimizer",
    "generate_grid",
]
