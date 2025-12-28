"""
Vectorized Strategy Module for Research Backtesting.

This module provides:
- IndicatorFactory: Pure Polars expressions for standard technical indicators
- StrategyBase: Base class for research strategies with Numba JIT support

Design Principles (per vectorized.md spec):
- Use Polars expressions for stateless indicators (SMA, RSI, returns)
- Use Numba JIT for stateful/recursive logic (trailing stops, adaptive indicators)
- Handle multiple symbols via `.over('symbol')` grouping
- Maintain Float32 consistency for Numba compatibility
- Fill nulls before passing data to Numba kernels
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TypeVar

import numpy as np
import polars as pl

# Type alias for Numba JIT-compiled functions
# Signature: (*np.ndarray) -> np.ndarray (1D output matching input length)
NumbaKernel = Callable[..., np.ndarray]

# Signal rule: (condition expression, value when true)
SignalRule = tuple[pl.Expr, int | float]

T = TypeVar("T", bound=np.generic)

_NUMPY_DTYPE_MAP: dict[pl.DataType, type[np.generic]] = {
    pl.Float32(): np.float32,
    pl.Float64(): np.float64,
    pl.Int32(): np.int32,
    pl.Int64(): np.int64,
}


def _normalize_over(over: str | Sequence[str] | None) -> list[str] | None:
    """Normalize over parameter to a list of column names or None."""
    if over is None:
        return None
    if isinstance(over, str):
        return [over]
    return list(over)


def _as_expr(col: str | pl.Expr) -> pl.Expr:
    """Convert column name to expression if needed."""
    return pl.col(col) if isinstance(col, str) else col


class IndicatorFactory:
    """
    Technical indicator helpers using pure Polars expressions.

    All methods return `pl.Expr` objects that can be composed in LazyFrame
    pipelines. Use the `over` parameter for multi-symbol DataFrames.

    Example:
        >>> lf = lf.with_columns(
        ...     IndicatorFactory.sma("close", 20, over="symbol").alias("sma_20"),
        ...     IndicatorFactory.rsi("close", 14, over="symbol").alias("rsi_14"),
        ... )
    """

    # -------------------------------------------------------------------------
    # Moving Averages
    # -------------------------------------------------------------------------

    @staticmethod
    def sma(
        col: str | pl.Expr,
        window: int,
        *,
        min_samples: int | None = None,
        over: str | Sequence[str] | None = None,
    ) -> pl.Expr:
        """
        Simple Moving Average.

        Args:
            col: Column name or expression.
            window: Rolling window size.
            min_samples: Minimum observations required (default: window).
            over: Group by columns for multi-symbol data.

        Returns:
            Polars expression for the SMA.
        """
        expr = _as_expr(col).rolling_mean(window, min_samples=min_samples)
        over_cols = _normalize_over(over)
        return expr.over(over_cols) if over_cols else expr

    @staticmethod
    def ema(
        col: str | pl.Expr,
        span: int,
        *,
        min_samples: int | None = None,
        over: str | Sequence[str] | None = None,
    ) -> pl.Expr:
        """
        Exponential Moving Average.

        Uses span to compute alpha = 2 / (span + 1).

        Args:
            col: Column name or expression.
            span: EMA span (e.g., 12, 26 for MACD).
            min_samples: Minimum observations required.
            over: Group by columns for multi-symbol data.

        Returns:
            Polars expression for the EMA.
        """
        expr = _as_expr(col).ewm_mean(span=span, min_samples=min_samples or 1)
        over_cols = _normalize_over(over)
        return expr.over(over_cols) if over_cols else expr

    # -------------------------------------------------------------------------
    # Returns & Changes
    # -------------------------------------------------------------------------

    @staticmethod
    def returns(
        col: str | pl.Expr,
        *,
        over: str | Sequence[str] | None = None,
    ) -> pl.Expr:
        """
        Simple percentage returns: (price[t] / price[t-1]) - 1.

        Args:
            col: Column name or expression (typically close price).
            over: Group by columns for multi-symbol data.

        Returns:
            Polars expression for returns.
        """
        expr = _as_expr(col).pct_change()
        over_cols = _normalize_over(over)
        return expr.over(over_cols) if over_cols else expr

    @staticmethod
    def log_returns(
        col: str | pl.Expr,
        *,
        over: str | Sequence[str] | None = None,
    ) -> pl.Expr:
        """
        Logarithmic returns: ln(price[t] / price[t-1]).

        Preferred for multi-period compounding and statistical analysis.

        Args:
            col: Column name or expression.
            over: Group by columns for multi-symbol data.

        Returns:
            Polars expression for log returns.
        """
        base = _as_expr(col)
        expr = base.log() - base.shift(1).log()
        over_cols = _normalize_over(over)
        return expr.over(over_cols) if over_cols else expr

    @staticmethod
    def forward_returns(
        col: str | pl.Expr,
        periods: int = 1,
        *,
        over: str | Sequence[str] | None = None,
    ) -> pl.Expr:
        """
        Forward-looking returns (for IC calculation and signal evaluation).

        CRITICAL: Only use for research/signal evaluation, NOT for trading signals.

        Args:
            col: Column name or expression.
            periods: Forward periods (1 = next bar return).
            over: Group by columns for multi-symbol data.

        Returns:
            Polars expression for forward returns.
        """
        base = _as_expr(col)
        expr = base.shift(-periods) / base - 1.0
        over_cols = _normalize_over(over)
        return expr.over(over_cols) if over_cols else expr

    # -------------------------------------------------------------------------
    # Volatility
    # -------------------------------------------------------------------------

    @staticmethod
    def rolling_std(
        col: str | pl.Expr,
        window: int,
        *,
        min_samples: int | None = None,
        over: str | Sequence[str] | None = None,
    ) -> pl.Expr:
        """
        Rolling standard deviation.

        Args:
            col: Column name or expression.
            window: Rolling window size.
            min_samples: Minimum observations required.
            over: Group by columns for multi-symbol data.

        Returns:
            Polars expression for rolling std.
        """
        expr = _as_expr(col).rolling_std(window, min_samples=min_samples)
        over_cols = _normalize_over(over)
        return expr.over(over_cols) if over_cols else expr

    @staticmethod
    def atr(
        high: str | pl.Expr,
        low: str | pl.Expr,
        close: str | pl.Expr,
        window: int = 14,
        *,
        min_samples: int | None = None,
        over: str | Sequence[str] | None = None,
    ) -> pl.Expr:
        """
        Average True Range - volatility indicator.

        TR = max(high - low, abs(high - prev_close), abs(low - prev_close))
        ATR = SMA(TR, window)

        Args:
            high: High price column.
            low: Low price column.
            close: Close price column.
            window: ATR period (default: 14).
            min_samples: Minimum observations required.
            over: Group by columns for multi-symbol data.

        Returns:
            Polars expression for ATR.
        """
        h = _as_expr(high)
        l = _as_expr(low)  # noqa: E741
        c = _as_expr(close)
        prev_close = c.shift(1)

        tr = pl.max_horizontal(
            h - l,
            (h - prev_close).abs(),
            (l - prev_close).abs(),
        )
        over_cols = _normalize_over(over)
        if over_cols:
            tr = tr.over(over_cols)

        expr = tr.rolling_mean(window, min_samples=min_samples)
        return expr.over(over_cols) if over_cols else expr

    # -------------------------------------------------------------------------
    # Oscillators & Momentum
    # -------------------------------------------------------------------------

    @staticmethod
    def rsi(
        col: str | pl.Expr,
        window: int = 14,
        *,
        min_samples: int | None = None,
        over: str | Sequence[str] | None = None,
    ) -> pl.Expr:
        """
        Relative Strength Index.

        RSI = 100 - (100 / (1 + RS))
        RS = avg_gain / avg_loss (using SMA)

        Args:
            col: Column name or expression (typically close).
            window: RSI period (default: 14).
            min_samples: Minimum observations required.
            over: Group by columns for multi-symbol data.

        Returns:
            Polars expression for RSI (0-100 scale).
        """
        base = _as_expr(col)
        delta = base.diff()
        gain = delta.clip(lower_bound=0)
        loss = (-delta).clip(lower_bound=0)

        over_cols = _normalize_over(over)
        if over_cols:
            gain = gain.over(over_cols)
            loss = loss.over(over_cols)

        avg_gain = gain.rolling_mean(window, min_samples=min_samples)
        avg_loss = loss.rolling_mean(window, min_samples=min_samples)

        if over_cols:
            avg_gain = avg_gain.over(over_cols)
            avg_loss = avg_loss.over(over_cols)

        rs = avg_gain / avg_loss
        expr = pl.when(avg_loss == 0).then(100.0).otherwise(100.0 - (100.0 / (1.0 + rs)))
        return expr

    @staticmethod
    def momentum(
        col: str | pl.Expr,
        periods: int = 10,
        *,
        over: str | Sequence[str] | None = None,
    ) -> pl.Expr:
        """
        Price momentum: current price / price N periods ago.

        Args:
            col: Column name or expression.
            periods: Lookback period.
            over: Group by columns for multi-symbol data.

        Returns:
            Polars expression for momentum ratio.
        """
        base = _as_expr(col)
        expr = base / base.shift(periods)
        over_cols = _normalize_over(over)
        return expr.over(over_cols) if over_cols else expr

    @staticmethod
    def roc(
        col: str | pl.Expr,
        periods: int = 10,
        *,
        over: str | Sequence[str] | None = None,
    ) -> pl.Expr:
        """
        Rate of Change: ((price - price_n) / price_n) * 100.

        Args:
            col: Column name or expression.
            periods: Lookback period.
            over: Group by columns for multi-symbol data.

        Returns:
            Polars expression for ROC (percentage).
        """
        base = _as_expr(col)
        prev = base.shift(periods)
        expr = ((base - prev) / prev) * 100.0
        over_cols = _normalize_over(over)
        return expr.over(over_cols) if over_cols else expr

    # -------------------------------------------------------------------------
    # Trend Indicators
    # -------------------------------------------------------------------------

    @staticmethod
    def macd(
        col: str | pl.Expr,
        fast: int = 12,
        slow: int = 26,
        *,
        over: str | Sequence[str] | None = None,
    ) -> pl.Expr:
        """
        MACD Line: EMA(fast) - EMA(slow).

        For the signal line, apply EMA(9) to the MACD output.

        Args:
            col: Column name or expression.
            fast: Fast EMA period (default: 12).
            slow: Slow EMA period (default: 26).
            over: Group by columns for multi-symbol data.

        Returns:
            Polars expression for MACD line.
        """
        base = _as_expr(col)
        over_cols = _normalize_over(over)

        ema_fast = base.ewm_mean(span=fast)
        ema_slow = base.ewm_mean(span=slow)

        if over_cols:
            ema_fast = ema_fast.over(over_cols)
            ema_slow = ema_slow.over(over_cols)

        return ema_fast - ema_slow

    @staticmethod
    def bollinger_bands(
        col: str | pl.Expr,
        window: int = 20,
        num_std: float = 2.0,
        *,
        min_samples: int | None = None,
        over: str | Sequence[str] | None = None,
    ) -> tuple[pl.Expr, pl.Expr, pl.Expr]:
        """
        Bollinger Bands: (middle, upper, lower).

        Args:
            col: Column name or expression.
            window: SMA window (default: 20).
            num_std: Number of standard deviations (default: 2.0).
            min_samples: Minimum observations required.
            over: Group by columns for multi-symbol data.

        Returns:
            Tuple of (middle_band, upper_band, lower_band) expressions.
        """
        base = _as_expr(col)
        over_cols = _normalize_over(over)

        sma = base.rolling_mean(window, min_samples=min_samples)
        std = base.rolling_std(window, min_samples=min_samples)

        if over_cols:
            sma = sma.over(over_cols)
            std = std.over(over_cols)

        middle = sma
        upper = sma + (std * num_std)
        lower = sma - (std * num_std)

        return middle, upper, lower

    # -------------------------------------------------------------------------
    # Cross-Sectional (for multi-asset research)
    # -------------------------------------------------------------------------

    @staticmethod
    def rank(
        col: str | pl.Expr,
        *,
        over: str | Sequence[str] | None = None,
        descending: bool = False,
    ) -> pl.Expr:
        """
        Cross-sectional rank (percentile rank within group).

        Essential for signal normalization in multi-asset research.

        Args:
            col: Column name or expression.
            over: Group by columns (e.g., timestamp for cross-sectional rank).
            descending: If True, highest value gets rank 1.

        Returns:
            Polars expression for rank (as float, normalized 0-1).
        """
        base = _as_expr(col)
        rank_expr = base.rank(method="average", descending=descending)
        over_cols = _normalize_over(over)

        if over_cols:
            rank_expr = rank_expr.over(over_cols)
            count_expr = base.count().over(over_cols)
        else:
            count_expr = base.count()

        # Normalize to [0, 1]
        return (rank_expr - 1) / (count_expr - 1)

    @staticmethod
    def zscore(
        col: str | pl.Expr,
        *,
        over: str | Sequence[str] | None = None,
    ) -> pl.Expr:
        """
        Cross-sectional z-score: (x - mean) / std.

        Standardizes signals for comparability across assets.

        Args:
            col: Column name or expression.
            over: Group by columns (e.g., timestamp for cross-sectional zscore).

        Returns:
            Polars expression for z-score.
        """
        base = _as_expr(col)
        over_cols = _normalize_over(over)

        if over_cols:
            mean = base.mean().over(over_cols)
            std = base.std().over(over_cols)
        else:
            mean = base.mean()
            std = base.std()

        return (base - mean) / std

    @staticmethod
    def rolling_zscore(
        col: str | pl.Expr,
        window: int,
        *,
        min_samples: int | None = None,
        over: str | Sequence[str] | None = None,
    ) -> pl.Expr:
        """
        Rolling z-score: (x - rolling_mean) / rolling_std.

        Time-series standardization for mean-reversion signals.

        Args:
            col: Column name or expression.
            window: Rolling window size.
            min_samples: Minimum observations required.
            over: Group by columns for multi-symbol data.

        Returns:
            Polars expression for rolling z-score.
        """
        base = _as_expr(col)
        over_cols = _normalize_over(over)

        rolling_mean = base.rolling_mean(window, min_samples=min_samples)
        rolling_std = base.rolling_std(window, min_samples=min_samples)

        if over_cols:
            rolling_mean = rolling_mean.over(over_cols)
            rolling_std = rolling_std.over(over_cols)

        return (base - rolling_mean) / rolling_std

    # -------------------------------------------------------------------------
    # Utility / Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def shift(
        col: str | pl.Expr,
        periods: int = 1,
        *,
        over: str | Sequence[str] | None = None,
    ) -> pl.Expr:
        """
        Shift/lag a column by N periods.

        Positive periods = lag (look back), negative = lead (look forward).

        Args:
            col: Column name or expression.
            periods: Number of periods to shift.
            over: Group by columns for multi-symbol data.

        Returns:
            Polars expression for shifted values.
        """
        expr = _as_expr(col).shift(periods)
        over_cols = _normalize_over(over)
        return expr.over(over_cols) if over_cols else expr

    @staticmethod
    def diff(
        col: str | pl.Expr,
        periods: int = 1,
        *,
        over: str | Sequence[str] | None = None,
    ) -> pl.Expr:
        """
        Difference between current and N periods ago.

        Args:
            col: Column name or expression.
            periods: Diff period.
            over: Group by columns for multi-symbol data.

        Returns:
            Polars expression for differenced values.
        """
        expr = _as_expr(col).diff(n=periods)
        over_cols = _normalize_over(over)
        return expr.over(over_cols) if over_cols else expr


class StrategyBase:
    """
    Base class for vectorized research strategies.

    Subclass this to implement custom strategies. Override `add_indicators`
    to compute features and `add_signals` to generate trading signals.

    Features:
    - Pure Polars for stateless indicators (fast, parallelized)
    - Numba JIT bridge for stateful/recursive logic
    - Multi-symbol support via `over` grouping
    - Debug mode returns intermediate features for inspection

    Example:
        >>> class MySMAStrategy(StrategyBase):
        ...     def add_indicators(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        ...         return lf.with_columns(
        ...             IndicatorFactory.sma("close", 20, over="symbol").alias("sma_20"),
        ...             IndicatorFactory.sma("close", 50, over="symbol").alias("sma_50"),
        ...         )
        ...
        ...     def add_signals(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        ...         return lf.with_columns(
        ...             self.combine_signals([
        ...                 (pl.col("sma_20") > pl.col("sma_50"), 1),  # Long
        ...                 (pl.col("sma_20") < pl.col("sma_50"), -1), # Short
        ...             ])
        ...         )
    """

    # Optional: set class-level Numba kernel and output column name
    numba_kernel: NumbaKernel | None = None
    numba_output: str | None = None

    def __init__(self, *, debug: bool = False) -> None:
        """
        Initialize strategy.

        Args:
            debug: If True, `run()` returns (output, features) tuple.
        """
        self.debug = debug

    def add_indicators(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Add technical indicators to the LazyFrame.

        Override this method to add your strategy's features.
        Use IndicatorFactory methods for standard indicators.

        Args:
            lf: Input LazyFrame with OHLCV data.

        Returns:
            LazyFrame with indicator columns added.
        """
        return lf

    def add_signals(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Generate trading signals from indicators.

        Override this method to implement your signal logic.
        Use `combine_signals()` for rule-based signal generation.

        Signal convention:
        - 1 = Long
        - -1 = Short
        - 0 = Neutral/Flat

        Args:
            lf: LazyFrame with indicators.

        Returns:
            LazyFrame with 'signal' column added.
        """
        return lf

    def run(
        self,
        lf: pl.LazyFrame,
        *,
        debug: bool | None = None,
    ) -> pl.LazyFrame | tuple[pl.LazyFrame, pl.LazyFrame]:
        """
        Execute the strategy pipeline.

        Args:
            lf: Input LazyFrame with price data.
            debug: Override instance debug setting.

        Returns:
            LazyFrame with signals. If debug=True, returns
            (output_with_signals, intermediate_features) tuple.
        """
        features = self.add_indicators(lf)
        out = self.add_signals(features)

        if debug is None:
            debug = self.debug
        return (out, features) if debug else out

    def apply_numba_logic(
        self,
        lf: pl.LazyFrame,
        col_inputs: Sequence[str],
        *,
        kernel: NumbaKernel | None = None,
        output_col: str | None = None,
        input_dtype: pl.DataType | None = pl.Float32(),
        return_dtype: pl.DataType | None = pl.Float32(),
        fill_value: float | int | None = 0.0,
        over: str | Sequence[str] | None = None,
    ) -> pl.LazyFrame:
        """
        Apply a Numba JIT-compiled function to LazyFrame columns.

        This bridges Polars and Numba for recursive/stateful logic that
        cannot be expressed as pure vectorized operations (e.g., trailing
        stops, path-dependent indicators).

        Args:
            lf: Input LazyFrame.
            col_inputs: Column names to pass to the kernel.
            kernel: Numba JIT function. Signature: (*arrays) -> 1D array.
                   Falls back to self.numba_kernel if not provided.
            output_col: Name for the output column.
                       Falls back to self.numba_output if not provided.
            input_dtype: Cast inputs to this dtype (default: Float32).
            return_dtype: Output column dtype (default: Float32).
            fill_value: Replace nulls with this value before Numba (default: 0.0).
                       Set to None to keep nulls (they become NaN).
            over: Group by columns for per-symbol processing.

        Returns:
            LazyFrame with the new output column.

        Raises:
            ImportError: If numba is not installed.
            ValueError: If kernel, output_col, or required columns are missing.

        Example:
            >>> import numba
            >>> @numba.njit
            ... def trailing_stop(price: np.ndarray, stop_pct: float) -> np.ndarray:
            ...     n = len(price)
            ...     result = np.empty(n, dtype=np.float32)
            ...     peak = price[0]
            ...     for i in range(n):
            ...         if price[i] > peak:
            ...             peak = price[i]
            ...         result[i] = peak * (1 - stop_pct)
            ...     return result
            ...
            >>> lf = strategy.apply_numba_logic(
            ...     lf, ["close"], kernel=trailing_stop, output_col="stop_level"
            ... )
        """
        try:
            import numba  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "numba is required for apply_numba_logic. " "Install with: pip install numba"
            ) from exc

        if not col_inputs:
            raise ValueError("col_inputs must include at least one column")

        selected_kernel = kernel or self.numba_kernel
        if selected_kernel is None:
            raise ValueError(
                "A numba kernel must be provided either via the 'kernel' argument "
                "or by setting self.numba_kernel"
            )

        out_col = output_col or self.numba_output
        if not out_col:
            raise ValueError("output_col is required for numba output")

        schema = lf.collect_schema()
        missing = [col for col in col_inputs if col not in schema]
        if missing:
            raise ValueError(f"Missing columns for numba logic: {sorted(missing)}")

        # Build expressions for input columns with casting/filling
        exprs: list[pl.Expr] = []
        for col in col_inputs:
            expr = pl.col(col)
            if fill_value is not None:
                expr = expr.fill_null(fill_value)
            if input_dtype is not None:
                expr = expr.cast(input_dtype)
            exprs.append(expr)

        np_dtype = _NUMPY_DTYPE_MAP.get(return_dtype) if return_dtype is not None else None

        def _batch_fn(cols: Sequence[pl.Series]) -> pl.Series:
            # Convert to contiguous numpy arrays for Numba performance
            arrays = [np.ascontiguousarray(col.to_numpy()) for col in cols]
            result = selected_kernel(*arrays)
            arr = np.asarray(result, dtype=np_dtype) if np_dtype is not None else np.asarray(result)

            if arr.ndim != 1:
                raise ValueError(f"Numba kernel must return a 1D array, got shape {arr.shape}")
            if len(arr) != len(cols[0]):
                raise ValueError(
                    f"Numba kernel output length ({len(arr)}) must match "
                    f"input length ({len(cols[0])})"
                )
            return pl.Series(arr)

        expr = pl.map_batches(exprs, _batch_fn, return_dtype=return_dtype)
        over_cols = _normalize_over(over)
        if over_cols:
            expr = expr.over(over_cols)
        return lf.with_columns(expr.alias(out_col))

    @staticmethod
    def combine_signals(
        rules: Sequence[SignalRule],
        *,
        default: int | float = 0,
        alias: str = "signal",
    ) -> pl.Expr:
        """
        Combine multiple signal rules into a single signal column.

        Rules are evaluated in order; the first matching condition wins.

        Args:
            rules: List of (condition, value) tuples. Conditions are pl.Expr.
            default: Value when no condition matches (default: 0).
            alias: Output column name (default: "signal").

        Returns:
            Polars expression producing the signal column.

        Example:
            >>> signal = StrategyBase.combine_signals([
            ...     (pl.col("rsi") < 30, 1),   # Oversold -> Long
            ...     (pl.col("rsi") > 70, -1),  # Overbought -> Short
            ... ])
        """
        if not rules:
            return pl.lit(default).alias(alias)

        expr = pl.when(rules[0][0]).then(rules[0][1])
        for condition, value in rules[1:]:
            expr = expr.when(condition).then(value)
        return expr.otherwise(default).alias(alias)


__all__ = [
    "IndicatorFactory",
    "NumbaKernel",
    "SignalRule",
    "StrategyBase",
]
