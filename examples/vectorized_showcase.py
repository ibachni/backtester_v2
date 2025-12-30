#!/usr/bin/env python3
"""
Vectorized Backtesting Module - End-to-End Showcase

This script demonstrates the complete functionality of the vectorized
backtesting module, from data generation through walk-forward analysis.

Since we may not have real data available, we generate synthetic OHLCV
data for demonstration purposes.

Usage:
    python examples/vectorized_showcase.py
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import numpy as np
import polars as pl

from backtester.vectorized import (
    CostModel,
    IndicatorFactory,
    ParamGrid,
    ParamOptimizer,
    StrategyBase,
    TimeSlicer,
    VectorizedEngine,
    WalkForwardValidator,
    quick_backtest,
)

# =============================================================================
# PART 1: Synthetic Data Generation
# =============================================================================


def generate_synthetic_ohlcv(
    n_bars: int = 5000,
    start_date: dt.datetime | None = None,
    timeframe_ms: int = 3600000,  # 1 hour in milliseconds
    initial_price: float = 100.0,
    volatility: float = 0.02,
    trend: float = 0.0001,
    seed: int = 42,
) -> pl.DataFrame:
    """
    Generate synthetic OHLCV data for backtesting demonstrations.

    Args:
        n_bars: Number of bars to generate.
        start_date: Starting datetime (defaults to 2 years ago).
        timeframe_ms: Bar duration in milliseconds.
        initial_price: Starting price.
        volatility: Daily volatility (annualized ~0.32 for 0.02).
        trend: Drift per bar (small positive = uptrend).
        seed: Random seed for reproducibility.

    Returns:
        Polars DataFrame with OHLCV columns.
    """
    np.random.seed(seed)

    if start_date is None:
        start_date = dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc)

    # Generate timestamps
    start_ms = int(start_date.timestamp() * 1000)
    timestamps = np.arange(n_bars) * timeframe_ms + start_ms

    # Generate returns using geometric Brownian motion
    returns = np.random.normal(trend, volatility, n_bars)
    prices = initial_price * np.exp(np.cumsum(returns))

    # Generate OHLC from close prices with some noise
    close = prices
    high = close * (1 + np.abs(np.random.normal(0, volatility / 2, n_bars)))
    low = close * (1 - np.abs(np.random.normal(0, volatility / 2, n_bars)))
    open_price = np.roll(close, 1)
    open_price[0] = initial_price

    # Ensure high >= max(open, close) and low <= min(open, close)
    high = np.maximum(high, np.maximum(open_price, close))
    low = np.minimum(low, np.minimum(open_price, close))

    # Generate volume (correlated with volatility)
    base_volume = 1000000
    volume = base_volume * (1 + np.abs(returns) * 10) * np.random.uniform(0.5, 1.5, n_bars)

    return pl.DataFrame(
        {
            "start_ms": timestamps - timeframe_ms,
            "end_ms": timestamps,
            "open": open_price.astype(np.float32),
            "high": high.astype(np.float32),
            "low": low.astype(np.float32),
            "close": close.astype(np.float32),
            "volume": volume.astype(np.float32),
            "symbol": ["BTCUSDT"] * n_bars,
            "timeframe": ["1h"] * n_bars,
        }
    )


# =============================================================================
# PART 2: Strategy Definitions
# =============================================================================


class SMACrossoverStrategy(StrategyBase):
    """
    Simple Moving Average Crossover Strategy.

    Goes long when fast SMA crosses above slow SMA,
    goes short when fast SMA crosses below slow SMA.
    """

    def __init__(
        self,
        fast_period: int = 20,
        slow_period: int = 50,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.fast_period = fast_period
        self.slow_period = slow_period

    def add_indicators(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(
            IndicatorFactory.sma("close", self.fast_period).alias("sma_fast"),
            IndicatorFactory.sma("close", self.slow_period).alias("sma_slow"),
        )

    def add_signals(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(
            self.combine_signals(
                [
                    (pl.col("sma_fast") > pl.col("sma_slow"), 1),  # Long
                    (pl.col("sma_fast") < pl.col("sma_slow"), -1),  # Short
                ]
            )
        )


class RSIMeanReversionStrategy(StrategyBase):
    """
    RSI Mean Reversion Strategy.

    Goes long when RSI is oversold (below lower threshold),
    goes short when RSI is overbought (above upper threshold).
    """

    def __init__(
        self,
        rsi_period: int = 14,
        oversold: int = 30,
        overbought: int = 70,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.rsi_period = rsi_period
        self.oversold = oversold
        self.overbought = overbought

    def add_indicators(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(
            IndicatorFactory.rsi("close", self.rsi_period).alias("rsi"),
        )

    def add_signals(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(
            self.combine_signals(
                [
                    (pl.col("rsi") < self.oversold, 1),  # Oversold → Long
                    (pl.col("rsi") > self.overbought, -1),  # Overbought → Short
                ]
            )
        )


class BollingerBandStrategy(StrategyBase):
    """
    Bollinger Band Mean Reversion Strategy.

    Goes long when price touches lower band,
    goes short when price touches upper band.
    """

    def __init__(
        self,
        window: int = 20,
        num_std: float = 2.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.window = window
        self.num_std = num_std

    def add_indicators(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        middle, upper, lower = IndicatorFactory.bollinger_bands(
            "close",
            self.window,
            self.num_std,
        )
        return lf.with_columns(
            middle.alias("bb_middle"),
            upper.alias("bb_upper"),
            lower.alias("bb_lower"),
        )

    def add_signals(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(
            self.combine_signals(
                [
                    (pl.col("close") < pl.col("bb_lower"), 1),  # Below lower → Long
                    (pl.col("close") > pl.col("bb_upper"), -1),  # Above upper → Short
                ]
            )
        )


# =============================================================================
# PART 3: Main Demonstration
# =============================================================================


def main() -> None:
    print("=" * 70)
    print("VECTORIZED BACKTESTING MODULE - END-TO-END SHOWCASE")
    print("=" * 70)

    # -------------------------------------------------------------------------
    # Step 1: Generate Synthetic Data
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("STEP 1: Data Generation")
    print("=" * 70)

    df = generate_synthetic_ohlcv(
        n_bars=8760,  # ~1 year of hourly data
        start_date=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
        volatility=0.015,
        trend=0.00005,  # Slight upward drift
        seed=42,
    )

    print(f"\nGenerated {len(df)} bars of synthetic OHLCV data")
    print(f"Date range: {df['end_ms'].min()} to {df['end_ms'].max()}")
    print(f"Price range: ${df['close'].min():.2f} - ${df['close'].max():.2f}")
    print("\nSample data:")
    print(df.head(5))

    lf = df.lazy()

    # -------------------------------------------------------------------------
    # Step 2: Demonstrate IndicatorFactory
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("STEP 2: Technical Indicators (IndicatorFactory)")
    print("=" * 70)

    lf_with_indicators = lf.with_columns(
        # Moving Averages
        IndicatorFactory.sma("close", 20).alias("sma_20"),
        IndicatorFactory.ema("close", 12).alias("ema_12"),
        # Oscillators
        IndicatorFactory.rsi("close", 14).alias("rsi_14"),
        IndicatorFactory.momentum("close", 10).alias("momentum_10"),
        # Volatility
        IndicatorFactory.atr("high", "low", "close", 14).alias("atr_14"),
        IndicatorFactory.rolling_std("close", 20).alias("volatility_20"),
        # Returns
        IndicatorFactory.returns("close").alias("returns"),
        IndicatorFactory.log_returns("close").alias("log_returns"),
    )

    df_indicators = lf_with_indicators.collect()
    print("\nIndicators added:")
    print(
        df_indicators.select(
            ["end_ms", "close", "sma_20", "ema_12", "rsi_14", "atr_14", "returns"]
        ).tail(10)
    )

    # -------------------------------------------------------------------------
    # Step 3: Quick Backtest with VectorizedEngine
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("STEP 3: Single Backtest (VectorizedEngine)")
    print("=" * 70)

    # Create strategy and generate signals
    strategy = SMACrossoverStrategy(fast_period=20, slow_period=50)
    lf_signals_result = strategy.run(lf, debug=False)
    # Type assertion: debug=False guarantees LazyFrame return (not tuple)
    assert isinstance(lf_signals_result, pl.LazyFrame)
    lf_signals = lf_signals_result

    # Create engine with cost model
    cost_model = CostModel(fee_bps=10, slippage_bps=5)  # 0.15% round-trip
    engine = VectorizedEngine(
        cost_model=cost_model,
        price_col="close",
        signal_col="signal",
        annualization_factor=8760,  # Hourly data
    )

    # Run backtest
    result = engine.run(lf_signals, return_aligned_df=True)

    print("\n--- SMA Crossover Strategy Results ---")
    print(f"Parameters: fast={strategy.fast_period}, slow={strategy.slow_period}")
    print(f"Cost Model: {cost_model.total_bps} bps total")
    print("\nPerformance Metrics:")
    print(f"  Sharpe Ratio:  {result.sharpe or 0:.4f}")
    print(f"  Total Return:  {(result.total_return or 0) * 100:.2f}%")
    print(f"  Max Drawdown:  {(result.max_drawdown or 0) * 100:.2f}%")
    print(f"  CAGR:          {(result.metrics.get('cagr') or 0) * 100:.2f}%")
    print(f"  Volatility:    {(result.metrics.get('volatility') or 0) * 100:.2f}%")
    print(f"  Calmar Ratio:  {result.metrics.get('calmar') or 0:.4f}")
    print(f"  Trade Count:   {result.metrics.get('trade_count', 0)}")
    print(f"  Bars in Market:{result.metrics.get('n_bars_in_market', 0)}")

    # Show aligned dataframe sample
    if result.aligned_df is not None:
        print("\nAligned DataFrame (last 5 rows):")
        print(
            result.aligned_df.select(
                ["end_ms", "close", "signal", "position", "net_return", "equity"]
            ).tail(5)
        )

    # -------------------------------------------------------------------------
    # Step 4: Compare Multiple Strategies
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("STEP 4: Strategy Comparison")
    print("=" * 70)

    strategies = [
        ("SMA Crossover (20/50)", SMACrossoverStrategy(fast_period=20, slow_period=50)),
        ("SMA Crossover (10/30)", SMACrossoverStrategy(fast_period=10, slow_period=30)),
        ("RSI Mean Reversion", RSIMeanReversionStrategy(rsi_period=14, oversold=30, overbought=70)),
        ("Bollinger Bands", BollingerBandStrategy(window=20, num_std=2.0)),
    ]

    results_comparison = []
    for name, strat in strategies:
        lf_with_signals = strat.run(lf)
        metrics = quick_backtest(lf_with_signals, fee_bps=10, slippage_bps=5)
        results_comparison.append(
            {
                "Strategy": name,
                "Sharpe": metrics.get("sharpe"),
                "Return (%)": (metrics.get("total_return") or 0) * 100,
                "MaxDD (%)": (metrics.get("max_drawdown") or 0) * 100,
                "Trades": metrics.get("trade_count"),
            }
        )

    df_comparison = pl.DataFrame(results_comparison)
    print("\nStrategy Comparison:")
    print(df_comparison)

    # -------------------------------------------------------------------------
    # Step 5: Parameter Optimization
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("STEP 5: Parameter Optimization")
    print("=" * 70)

    def sma_signal_generator(data: pl.DataFrame, params: dict) -> pl.LazyFrame:
        """Signal generator for SMA crossover optimization."""
        fast = params["fast_period"]
        slow = params["slow_period"]

        return (
            data.lazy()
            .with_columns(
                pl.col("close").rolling_mean(fast).alias("sma_fast"),
                pl.col("close").rolling_mean(slow).alias("sma_slow"),
            )
            .with_columns(
                pl.when(pl.col("sma_fast") > pl.col("sma_slow"))
                .then(1)
                .when(pl.col("sma_fast") < pl.col("sma_slow"))
                .then(-1)
                .otherwise(0)
                .alias("signal")
            )
        )

    # Define parameter grid
    from collections.abc import Sequence

    param_ranges: dict[str, Sequence[Any]] = {
        "fast_period": [5, 10, 20, 30],
        "slow_period": [40, 60, 80, 100],
    }

    grid = ParamGrid(param_ranges)
    print(f"\nParameter grid size: {grid.grid_size} combinations")

    # Run optimization
    optimizer = ParamOptimizer(
        cost_model=CostModel(fee_bps=10, slippage_bps=5),
        n_jobs=1,  # Sequential for demo (use -1 for parallel)
    )

    opt_result = optimizer.run_sequential(
        data=lf,
        signal_generator=sma_signal_generator,
        param_ranges=param_ranges,
        rank_by="sharpe",
    )

    print("\nOptimization Results:")
    print(f"  Successful runs: {opt_result.n_successful}")
    print(f"  Failed runs:     {opt_result.n_failed}")
    print(f"\nBest Parameters: {opt_result.best_params}")
    print("Best Metrics:")
    if opt_result.best_metrics:
        print(f"  Sharpe:      {opt_result.best_metrics.get('sharpe', 0):.4f}")
        print(f"  Return:      {(opt_result.best_metrics.get('total_return') or 0) * 100:.2f}%")
        print(f"  Max DD:      {(opt_result.best_metrics.get('max_drawdown') or 0) * 100:.2f}%")

    # Show score table
    print("\nTop 5 Parameter Combinations:")
    score_table = opt_result.score_table(top_n=5)
    if not score_table.is_empty():
        print(
            score_table.select(
                ["fast_period", "slow_period", "sharpe", "total_return", "max_drawdown"]
            )
        )

    # -------------------------------------------------------------------------
    # Step 6: Walk-Forward Analysis
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("STEP 6: Walk-Forward Analysis")
    print("=" * 70)

    # Create time slicer to see windows
    slicer = TimeSlicer(
        train_days=180,  # 6 months training
        test_days=30,  # 1 month testing
        warmup_days=30,  # 30 days warmup for indicators
        anchored=False,  # Rolling window
    )

    # Get date range from data
    df_collected = lf.collect()
    min_ts = int(df_collected["end_ms"].min())
    max_ts = int(df_collected["end_ms"].max())
    start_dt = dt.datetime.fromtimestamp(min_ts / 1000, tz=dt.timezone.utc)
    end_dt = dt.datetime.fromtimestamp(max_ts / 1000, tz=dt.timezone.utc)

    total_days = (end_dt - start_dt).days
    print(f"\nData period: {start_dt.date()} to {end_dt.date()} ({total_days} days)")
    print(
        f"Window config: train={slicer.train_days}d, "
        f"test={slicer.test_days}d, warmup={slicer.warmup_days}d"
    )

    try:
        n_windows = slicer.count_windows(start_dt, end_dt)
        print(f"Expected windows: {n_windows}")
    except ValueError as e:
        print(f"Note: {e}")
        print("Adjusting window sizes for available data...")
        slicer = TimeSlicer(train_days=120, test_days=20, warmup_days=20, anchored=False)
        n_windows = slicer.count_windows(start_dt, end_dt)
        print(f"Adjusted expected windows: {n_windows}")

    # Run Walk-Forward Analysis
    validator = WalkForwardValidator(
        train_days=slicer.train_days,
        test_days=slicer.test_days,
        warmup_days=slicer.warmup_days,
        anchored=False,
        cost_model=CostModel(fee_bps=10, slippage_bps=5),
        rank_by="sharpe",
        n_jobs=1,
    )

    # Use smaller parameter grid for WFA
    wfa_param_ranges: dict[str, Sequence[Any]] = {
        "fast_period": [10, 20],
        "slow_period": [50, 80],
    }

    wfa_result = validator.run(
        data=lf,
        signal_generator=sma_signal_generator,
        param_ranges=wfa_param_ranges,
        return_equity=False,  # Don't store equity curves
    )

    print("\n--- Walk-Forward Analysis Results ---")
    print(f"Windows completed: {wfa_result.n_windows}")

    if wfa_result.aggregate_metrics:
        agg = wfa_result.aggregate_metrics
        print("\nAggregate OOS Metrics:")
        print(f"  Avg Sharpe:      {agg.get('avg_sharpe', 0):.4f}")
        print(f"  Min Sharpe:      {agg.get('min_sharpe', 0):.4f}")
        print(f"  Max Sharpe:      {agg.get('max_sharpe', 0):.4f}")
        print(f"  Total Return:    {(agg.get('total_return') or 0) * 100:.2f}%")
        print(f"  Worst Drawdown:  {(agg.get('worst_drawdown') or 0) * 100:.2f}%")

    # Show per-window results
    wfa_df = wfa_result.to_dataframe()
    if not wfa_df.is_empty():
        print("\nPer-Window Results:")
        cols_to_show = [
            "window_id",
            "train_start",
            "test_start",
            "param_fast_period",
            "param_slow_period",
            "train_sharpe",
            "test_sharpe",
        ]
        # Filter to columns that exist
        available_cols = [c for c in cols_to_show if c in wfa_df.columns]
        print(wfa_df.select(available_cols))

    # -------------------------------------------------------------------------
    # Step 7: Summary
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("SHOWCASE COMPLETE")
    print("=" * 70)

    print(
        """
This showcase demonstrated:

1. DATA GENERATION
   - Synthetic OHLCV data with realistic properties

2. INDICATOR FACTORY
   - SMA, EMA, RSI, ATR, Bollinger Bands
   - Returns and log returns calculations

3. STRATEGY CLASSES
   - StrategyBase subclassing
   - Signal combination helpers

4. VECTORIZED ENGINE
   - Zero-loop backtesting
   - Cost model integration
   - Full metrics computation

5. PARAMETER OPTIMIZATION
   - Grid generation
   - Parallel optimization (threading backend)
   - Score tables and ranking

6. WALK-FORWARD ANALYSIS
   - Rolling train/test windows
   - Out-of-sample validation
   - Aggregate metrics

For production use:
- Replace synthetic data with ParquetLoader
- Use n_jobs=-1 for parallel optimization
- Implement custom strategies with Numba for complex logic
- Export results to Parquet for analysis
"""
    )


if __name__ == "__main__":
    main()
