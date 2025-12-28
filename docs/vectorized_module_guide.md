# Vectorized Backtesting Module - Usage Guide

This guide covers the **vectorized** backtesting module, a high-speed research backtesting framework optimized for rapid strategy iteration, parameter optimization, and walk-forward analysis.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Core Concepts](#2-core-concepts)
3. [Module Components](#3-module-components)
4. [Research Workflow](#4-research-workflow)
5. [API Reference](#5-api-reference)
6. [Best Practices](#6-best-practices)
7. [Post-MVP Roadmap](#7-post-mvp-roadmap)

---

## 1. Overview

The vectorized module provides a **research-first** backtesting framework designed for:

- **Speed**: Zero-loop execution using Polars column-wise operations
- **Scalability**: Lazy evaluation with filter pushdowns for large datasets
- **Iteration**: Rapid parameter optimization with parallel execution
- **Robustness**: Walk-forward analysis for out-of-sample validation

### Key Design Principles

| Principle | Implementation |
|-----------|----------------|
| **ZERO LOOPS** | All computation via Polars expressions |
| **Lazy First** | Use `LazyFrame` until the last moment |
| **Next Bar Execution** | `position = signal.shift(1)` |
| **Float32 Consistency** | Numba-compatible data types |
| **Thread-Safe Parallelism** | Threading backend with zero-copy sharing |

### Technology Stack

- **Polars** (v1.0+): Primary data manipulation
- **Numba** (v0.60+): JIT compilation for iterative logic
- **Parquet**: Partitioned data storage (Year/Month)
- **Joblib**: Parallel parameter optimization

---

## 2. Core Concepts

### 2.1 Signal Convention

Signals represent the **desired position** at each bar:

| Signal | Meaning |
|--------|---------|
| `1` | Long position |
| `-1` | Short position |
| `0` | Flat/neutral (no position) |

### 2.2 Execution Timing

Trades execute on the **next bar** after the signal:

```
Bar t:   Signal generated (based on data available at t)
Bar t+1: Trade executed at bar open/close
```

This is implemented as `position = signal.shift(1)`.

### 2.3 Return Calculation

```
Asset Return    = (Price[t] / Price[t-1]) - 1
Gross Return    = Position[t] × Asset Return[t]
Turnover        = |Position[t] - Position[t-1]|
Cost            = Turnover × (Fee + Slippage)
Net Return      = Gross Return - Cost
Equity          = Cumulative Product of (1 + Net Return)
```

---

## 3. Module Components

### 3.1 Data Loader (`data_loader.py`)

Efficiently loads partitioned parquet files using Polars LazyFrames.

```python
from backtester.vectorized import ParquetLoader, DataSpec, DateRange
import datetime as dt
from pathlib import Path

# Define what data to load
spec = DataSpec(
    base_dir=Path("/data/crypto"),
    exchange="binance",
    market="futures",
    symbols=("BTCUSDT",),
    timeframe="1h",
    date_range=DateRange(
        start=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
        end=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
    ),
)

# Load as LazyFrame
loader = ParquetLoader()
lf = loader.load_dataset(spec)

# Optional: resample to different timeframe
lf_daily = loader.resample_data(lf, "1d")
```

**Key Features:**
- Hive partitioning support (`/year=2023/month=01/`)
- Early filter pushdowns (date/symbol filtering before collect)
- Automatic Float64→Float32 conversion for Numba compatibility
- Debug mode with `LoadStats` for observability

### 3.2 Strategy Module (`strategy.py`)

Provides indicator calculations and signal generation.

#### IndicatorFactory (Pure Polars)

```python
from backtester.vectorized import IndicatorFactory
import polars as pl

# Add indicators to a LazyFrame
lf = lf.with_columns(
    # Moving averages
    IndicatorFactory.sma("close", 20).alias("sma_20"),
    IndicatorFactory.ema("close", 12).alias("ema_12"),

    # Oscillators
    IndicatorFactory.rsi("close", 14).alias("rsi"),
    IndicatorFactory.macd("close", 12, 26).alias("macd"),

    # Volatility
    IndicatorFactory.atr("high", "low", "close", 14).alias("atr"),
    IndicatorFactory.rolling_std("close", 20).alias("volatility"),

    # Returns
    IndicatorFactory.returns("close").alias("returns"),
    IndicatorFactory.log_returns("close").alias("log_returns"),
)
```

**Available Indicators:**
- Moving Averages: `sma`, `ema`
- Oscillators: `rsi`, `momentum`, `roc`, `macd`
- Volatility: `rolling_std`, `atr`, `bollinger_bands`
- Returns: `returns`, `log_returns`, `forward_returns`
- Cross-sectional: `rank`, `zscore`, `rolling_zscore`
- Utility: `shift`, `diff`

#### StrategyBase (Custom Strategies)

```python
from backtester.vectorized import StrategyBase, IndicatorFactory
import polars as pl

class SMAcrossover(StrategyBase):
    def __init__(self, fast: int = 20, slow: int = 50, **kwargs):
        super().__init__(**kwargs)
        self.fast = fast
        self.slow = slow

    def add_indicators(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(
            IndicatorFactory.sma("close", self.fast).alias("sma_fast"),
            IndicatorFactory.sma("close", self.slow).alias("sma_slow"),
        )

    def add_signals(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(
            self.combine_signals([
                (pl.col("sma_fast") > pl.col("sma_slow"), 1),   # Long
                (pl.col("sma_fast") < pl.col("sma_slow"), -1),  # Short
            ])
        )

# Usage
strategy = SMAcrossover(fast=10, slow=50)
lf_with_signals = strategy.run(lf)
```

#### Numba JIT Bridge (For Iterative Logic)

For path-dependent calculations (trailing stops, etc.):

```python
import numba
import numpy as np

@numba.njit
def trailing_stop(price: np.ndarray, stop_pct: float) -> np.ndarray:
    n = len(price)
    result = np.empty(n, dtype=np.float32)
    peak = price[0]
    for i in range(n):
        if price[i] > peak:
            peak = price[i]
        result[i] = peak * (1 - stop_pct)
    return result

class TrailingStopStrategy(StrategyBase):
    numba_kernel = staticmethod(trailing_stop)
    numba_output = "stop_level"

    def add_indicators(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return self.apply_numba_logic(
            lf, ["close"],
            kernel=lambda price: trailing_stop(price, 0.05),
            output_col="stop_level"
        )
```

### 3.3 Vectorized Engine (`engine.py`)

The core backtesting engine computing equity curves and metrics.

```python
from backtester.vectorized import VectorizedEngine, CostModel

# Define cost model
cost = CostModel(fee_bps=10, slippage_bps=5)  # 0.15% round-trip

# Create engine
engine = VectorizedEngine(
    cost_model=cost,
    price_col="close",
    signal_col="signal",
    annualization_factor=252.0,  # Daily data
)

# Run backtest
result = engine.run(lf_with_signals, return_aligned_df=True)

# Access results
print(f"Sharpe Ratio: {result.sharpe:.2f}")
print(f"Total Return: {result.total_return:.2%}")
print(f"Max Drawdown: {result.max_drawdown:.2%}")

# Full metrics dictionary
print(result.metrics)
# {
#   'sharpe': 1.45,
#   'sortino': 2.10,
#   'total_return': 0.234,
#   'cagr': 0.156,
#   'max_drawdown': 0.089,
#   'volatility': 0.165,
#   'calmar': 1.75,
#   'avg_turnover': 0.023,
#   'trade_count': 156,
#   'n_bars': 8760,
#   'n_bars_in_market': 6543,
# }
```

**Quick Backtest Convenience Function:**

```python
from backtester.vectorized import quick_backtest

metrics = quick_backtest(lf_with_signals, fee_bps=10, slippage_bps=5)
print(f"Sharpe: {metrics['sharpe']:.2f}")
```

### 3.4 Parameter Optimizer (`optimizer.py`)

High-speed parallel optimization over parameter grids.

```python
from backtester.vectorized import ParamOptimizer, CostModel, generate_grid
import polars as pl

# Define signal generator function
def signal_generator(df: pl.DataFrame, params: dict) -> pl.LazyFrame:
    fast, slow = params["fast"], params["slow"]
    return df.lazy().with_columns(
        pl.col("close").rolling_mean(fast).alias("sma_fast"),
        pl.col("close").rolling_mean(slow).alias("sma_slow"),
    ).with_columns(
        pl.when(pl.col("sma_fast") > pl.col("sma_slow"))
        .then(1)
        .when(pl.col("sma_fast") < pl.col("sma_slow"))
        .then(-1)
        .otherwise(0)
        .alias("signal")
    )

# Run optimization
optimizer = ParamOptimizer(
    cost_model=CostModel(fee_bps=10),
    n_jobs=-1,  # Use all cores
)

result = optimizer.run(
    data=lf,  # Can be LazyFrame or DataFrame
    signal_generator=signal_generator,
    param_ranges={
        "fast": [5, 10, 20, 30],
        "slow": [50, 100, 150, 200],
    },
    rank_by="sharpe",
)

# Results
print(f"Best params: {result.best_params}")
print(f"Best Sharpe: {result.best_metrics['sharpe']:.2f}")

# Full score table
df_results = result.score_table(top_n=10)
print(df_results)
```

**Grid Generation:**

```python
from backtester.vectorized import ParamGrid

# Full grid (all combinations)
grid = ParamGrid({"x": [1, 2, 3], "y": [10, 20]})
print(f"Grid size: {grid.grid_size}")  # 6

# Random search (for large parameter spaces)
combos = list(grid.generate(n_random=100, seed=42))
```

### 3.5 Walk-Forward Analysis (`wfa.py`)

Validate strategy robustness with rolling train/test windows.

```python
from backtester.vectorized import WalkForwardValidator, CostModel
import datetime as dt

validator = WalkForwardValidator(
    train_days=365,       # 1 year training
    test_days=30,         # 1 month testing
    warmup_days=50,       # 50 days for indicator warmup
    anchored=False,       # Rolling window (vs. anchored)
    cost_model=CostModel(fee_bps=10),
    rank_by="sharpe",
    n_jobs=-1,
)

wfa_result = validator.run(
    data=lf,
    signal_generator=signal_generator,
    param_ranges={"fast": [10, 20], "slow": [50, 100]},
    return_equity=True,
)

# Analyze results
print(f"Windows: {wfa_result.n_windows}")
print(f"Avg OOS Sharpe: {wfa_result.aggregate_metrics['avg_sharpe']:.2f}")
print(f"Total OOS Return: {wfa_result.aggregate_metrics['total_return']:.2%}")
print(f"Worst Drawdown: {wfa_result.aggregate_metrics['worst_drawdown']:.2%}")

# Per-window analysis
df_windows = wfa_result.to_dataframe()
print(df_windows.select([
    "window_id",
    "train_sharpe",
    "test_sharpe",
    "param_fast",
    "param_slow"
]))
```

**Window Modes:**

| Mode | Description | Use Case |
|------|-------------|----------|
| **Rolling** | Train window slides forward (constant size) | Adapting to recent regimes |
| **Anchored** | Train window grows (start date fixed) | Accumulating more history |

---

## 4. Research Workflow

### 4.1 Typical Research Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. DATA LOADING                                                 │
│    Load partitioned parquet → LazyFrame with early filters      │
├─────────────────────────────────────────────────────────────────┤
│ 2. INDICATOR CALCULATION                                        │
│    Pure Polars expressions for standard indicators              │
│    Numba JIT for path-dependent logic                           │
├─────────────────────────────────────────────────────────────────┤
│ 3. SIGNAL GENERATION                                            │
│    Combine indicators into trading signals (-1, 0, +1)          │
├─────────────────────────────────────────────────────────────────┤
│ 4. QUICK BACKTEST                                               │
│    Run single backtest to validate logic                        │
├─────────────────────────────────────────────────────────────────┤
│ 5. PARAMETER OPTIMIZATION                                       │
│    Grid/random search over parameter space                      │
├─────────────────────────────────────────────────────────────────┤
│ 6. WALK-FORWARD ANALYSIS                                        │
│    Validate robustness with out-of-sample testing               │
├─────────────────────────────────────────────────────────────────┤
│ 7. ANALYSIS & REPORTING                                         │
│    Review metrics, equity curves, parameter stability           │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Complete Example

```python
import datetime as dt
from pathlib import Path
import polars as pl

from backtester.vectorized import (
    ParquetLoader, DataSpec, DateRange,
    IndicatorFactory, StrategyBase,
    VectorizedEngine, CostModel,
    ParamOptimizer,
    WalkForwardValidator,
)

# ============================================================
# Step 1: Load Data
# ============================================================
spec = DataSpec(
    base_dir=Path("/data/crypto"),
    exchange="binance",
    market="futures",
    symbols=("BTCUSDT",),
    timeframe="1h",
    date_range=DateRange(
        start=dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc),
        end=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
    ),
)

loader = ParquetLoader()
lf = loader.load_dataset(spec)

# ============================================================
# Step 2: Define Strategy
# ============================================================
class RSIMomentum(StrategyBase):
    def __init__(self, rsi_period: int = 14, threshold: int = 30, **kwargs):
        super().__init__(**kwargs)
        self.rsi_period = rsi_period
        self.threshold = threshold

    def add_indicators(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(
            IndicatorFactory.rsi("close", self.rsi_period).alias("rsi"),
        )

    def add_signals(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(
            self.combine_signals([
                (pl.col("rsi") < self.threshold, 1),              # Oversold → Long
                (pl.col("rsi") > (100 - self.threshold), -1),     # Overbought → Short
            ])
        )

# ============================================================
# Step 3: Quick Validation Backtest
# ============================================================
strategy = RSIMomentum(rsi_period=14, threshold=30)
lf_signals = strategy.run(lf)

engine = VectorizedEngine(cost_model=CostModel(fee_bps=10, slippage_bps=5))
result = engine.run(lf_signals)
print(f"Initial Sharpe: {result.sharpe:.2f}")

# ============================================================
# Step 4: Parameter Optimization
# ============================================================
def signal_gen(df: pl.DataFrame, params: dict) -> pl.LazyFrame:
    strategy = RSIMomentum(**params)
    return strategy.run(df.lazy())

optimizer = ParamOptimizer(
    cost_model=CostModel(fee_bps=10, slippage_bps=5),
    n_jobs=-1,
)

opt_result = optimizer.run(
    data=lf,
    signal_generator=signal_gen,
    param_ranges={
        "rsi_period": [7, 14, 21, 28],
        "threshold": [20, 25, 30, 35],
    },
)

print(f"Best params: {opt_result.best_params}")
print(f"Best Sharpe: {opt_result.best_metrics['sharpe']:.2f}")

# ============================================================
# Step 5: Walk-Forward Validation
# ============================================================
validator = WalkForwardValidator(
    train_days=365,
    test_days=30,
    warmup_days=50,
    cost_model=CostModel(fee_bps=10, slippage_bps=5),
)

wfa = validator.run(
    data=lf,
    signal_generator=signal_gen,
    param_ranges={
        "rsi_period": [7, 14, 21],
        "threshold": [25, 30, 35],
    },
    return_equity=True,
)

print(f"\n=== Walk-Forward Results ===")
print(f"Windows: {wfa.n_windows}")
print(f"Avg OOS Sharpe: {wfa.aggregate_metrics.get('avg_sharpe', 0):.2f}")
print(f"Total Return: {wfa.aggregate_metrics.get('total_return', 0):.2%}")
```

---

## 5. API Reference

### 5.1 Data Contracts

| Class | Description |
|-------|-------------|
| `DateRange` | Start/end datetime range |
| `DataSpec` | Complete data loading specification |
| `LoadStats` | Debug statistics for data loading |

### 5.2 Core Components

| Class | Description |
|-------|-------------|
| `ParquetLoader` | Load partitioned parquet files |
| `IndicatorFactory` | Technical indicator expressions |
| `StrategyBase` | Base class for custom strategies |
| `VectorizedEngine` | Core backtesting engine |
| `CostModel` | Fee and slippage configuration |
| `BacktestResult` | Backtest output container |

### 5.3 Optimization

| Class | Description |
|-------|-------------|
| `ParamGrid` | Parameter grid generator |
| `ParamOptimizer` | Parallel parameter optimizer |
| `OptimizationResult` | Optimization results container |

### 5.4 Walk-Forward Analysis

| Class | Description |
|-------|-------------|
| `TimeWindow` | Single train/test window |
| `TimeSlicer` | Window generator (rolling/anchored) |
| `WalkForwardValidator` | WFA orchestrator |
| `WFAResult` | WFA results container |
| `WFAWindowResult` | Per-window results |

---

## 6. Best Practices

### 6.1 Performance Tips

1. **Keep LazyFrames lazy** until the last moment
2. **Pre-collect data once** before optimization loops
3. **Use Float32** for Numba compatibility
4. **Fill nulls** before passing to Numba kernels
5. **Use threading backend** for parallel optimization

### 6.2 Multi-Symbol Data

Use the `over` parameter for per-symbol calculations:

```python
lf = lf.with_columns(
    IndicatorFactory.sma("close", 20, over="symbol").alias("sma_20"),
    IndicatorFactory.rsi("close", 14, over="symbol").alias("rsi"),
)
```

### 6.3 Avoiding Common Pitfalls

| Pitfall | Solution |
|---------|----------|
| NaN in Numba loops | Use `fill_null()` before Numba |
| Slow optimization | Pre-collect data, use threading |
| Wrong execution timing | Position = signal.shift(1) |
| Mixed dtypes | Cast to Float32 consistently |
| Look-ahead bias | Use `forward_returns` only for research |

---

## 7. Post-MVP Roadmap

*Ordered by importance for professional-grade research.*

---

### 7.1 Statistical Rigor & Overfitting Protection ⭐⭐⭐

**Why Critical:** Without these, any backtest results are suspect. The #1 cause of strategy failure is overfitting.

#### Deflated Sharpe Ratio (DSR)
Adjust Sharpe for multiple testing and non-normality:
```python
@dataclass
class RobustnessMetrics:
    raw_sharpe: float
    deflated_sharpe: float      # Adjusted for trials
    haircut_sharpe: float       # Harvey-Liu-Zhu adjustment
    p_value: float              # Probability of being lucky
    trials_equivalent: int      # Effective number of trials
```

**Implementation:**
- Track number of parameter combinations tested
- Apply Bailey-López de Prado deflation formula
- Report confidence intervals on all metrics

#### Multiple Hypothesis Testing Corrections
| Method | Use Case |
|--------|----------|
| Bonferroni | Conservative, few tests |
| Benjamini-Hochberg (FDR) | Many signals, control false discovery |
| Holm-Bonferroni | Step-down, more power than Bonferroni |

#### Bootstrap Confidence Intervals
```python
def bootstrap_sharpe(returns: np.ndarray, n_bootstrap: int = 10000) -> dict:
    """Return mean, std, and percentile confidence intervals."""
    return {
        "mean": 1.45,
        "std": 0.23,
        "ci_5": 1.05,
        "ci_95": 1.82,
    }
```

#### Combinatorial Purged Cross-Validation (CPCV)
- Avoid leakage between train/test folds
- Embargo periods to handle autocorrelation
- Multiple test paths for robust OOS estimation

---

### 7.2 Data Quality & Integrity ⭐⭐⭐

**Why Critical:** Garbage in, garbage out. Professional research requires clean, validated data.

#### Survivorship Bias Handling
```python
@dataclass
class UniverseSnapshot:
    """Point-in-time universe membership."""
    as_of_date: dt.datetime
    symbols: frozenset[str]
    delisted: frozenset[str]    # Removed since last snapshot
    added: frozenset[str]       # Added since last snapshot

class UniverseManager:
    def get_universe(self, as_of: dt.datetime) -> frozenset[str]:
        """Return tradeable symbols as of date (no look-ahead)."""

    def with_survivorship_free(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Filter to only symbols that existed at each timestamp."""
```

#### Data Validation Pipeline
| Check | Action |
|-------|--------|
| Gap detection | Log missing bars, optionally fill |
| Outlier detection | Flag/remove price spikes (>N std) |
| OHLC consistency | Ensure High ≥ Open,Close ≥ Low |
| Timestamp monotonicity | Reject out-of-order data |
| Volume anomalies | Flag zero-volume or extreme spikes |

#### Corporate Actions (Equities)
- Split adjustments (backward/forward)
- Dividend adjustments (total return vs price return)
- Point-in-time adjustment factors

#### Data Versioning & Checksums
```python
@dataclass
class DataManifest:
    version: str
    sha256: str
    row_count: int
    date_range: DateRange
    symbols: tuple[str, ...]
    created_at: dt.datetime
```

---

### 7.3 Risk Analytics & Stress Testing ⭐⭐⭐

**Why Critical:** Understanding tail risk and factor exposures separates research from gambling.

#### Tail Risk Metrics
```python
@dataclass
class RiskMetrics:
    # Standard
    volatility: float
    max_drawdown: float

    # Tail risk
    var_95: float               # Value at Risk (95%)
    var_99: float               # Value at Risk (99%)
    cvar_95: float              # Conditional VaR / Expected Shortfall

    # Higher moments
    skewness: float
    kurtosis: float

    # Drawdown analysis
    avg_drawdown: float
    drawdown_duration_avg: int  # bars
    drawdown_duration_max: int
    ulcer_index: float          # Pain index
```

#### Factor Exposure Analysis
```python
class FactorModel:
    """Decompose returns into factor exposures."""

    def fit(self, strategy_returns: pl.Series, factors: pl.DataFrame) -> FactorResult:
        """
        factors: DataFrame with columns like 'mkt_rf', 'smb', 'hml', 'mom'
        Returns alpha, betas, R², and residual returns.
        """

@dataclass
class FactorResult:
    alpha: float                # Unexplained return (annualized)
    alpha_t_stat: float         # Statistical significance
    betas: dict[str, float]     # Factor loadings
    r_squared: float            # Variance explained
    residual_vol: float         # Idiosyncratic risk
```

#### Scenario & Stress Testing
```python
class StressTester:
    # Historical scenarios
    SCENARIOS = {
        "covid_crash": DateRange("2020-02-19", "2020-03-23"),
        "fed_pivot_2022": DateRange("2022-01-01", "2022-06-30"),
        "ftx_collapse": DateRange("2022-11-01", "2022-11-15"),
        "luna_crash": DateRange("2022-05-01", "2022-05-15"),
    }

    def run_scenario(self, lf: pl.LazyFrame, scenario: str) -> ScenarioResult:
        """Run strategy through historical stress period."""

    def hypothetical_shock(self,
        lf: pl.LazyFrame,
        price_shock: float,      # e.g., -0.20 for 20% drop
        vol_multiplier: float,   # e.g., 3.0 for 3x normal vol
    ) -> ScenarioResult:
        """Simulate hypothetical market conditions."""
```

#### Correlation Regime Detection
- Rolling correlation with benchmark
- Regime classification (risk-on, risk-off, crisis)
- Beta stability analysis

---

### 7.4 Performance Attribution ⭐⭐

**Why Important:** Understand *why* a strategy makes money to assess sustainability.

#### Return Decomposition
```python
@dataclass
class Attribution:
    # Gross/net breakdown
    gross_return: float
    transaction_costs: float
    slippage: float
    net_return: float

    # Timing vs selection (Brinson)
    allocation_effect: float    # Being in right assets
    selection_effect: float     # Picking winners within assets
    interaction_effect: float

    # Factor attribution
    factor_returns: dict[str, float]  # Return from each factor
    alpha_return: float               # Unexplained return

    # Long/short breakdown
    long_return: float
    short_return: float
```

#### Trade-Level Analysis
```python
@dataclass
class TradeStats:
    total_trades: int
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float        # Gross profit / Gross loss
    avg_holding_period: float   # bars

    # By time
    returns_by_hour: dict[int, float]
    returns_by_weekday: dict[int, float]
    returns_by_month: dict[int, float]

    # By regime
    returns_by_vol_regime: dict[str, float]
```

#### Rolling Performance Windows
- Rolling Sharpe (30d, 90d, 1y)
- Rolling alpha/beta vs benchmark
- Rolling hit rate and profit factor

---

### 7.5 Signal Research & Analysis ⭐⭐

#### Signal Leaderboard
Generate a DataFrame ranking signals by predictive power:

| Signal Name | IC Mean | IC Sharpe | Autocorr | Quantile Spread | Decay Half-Life |
|-------------|---------|-----------|----------|-----------------|-----------------|
| RSI_Divergence | 0.04 | 0.6 | 0.85 | 1.2% | 5 bars |
| Momentum_12M | 0.02 | 0.3 | 0.98 | 0.5% | 20 bars |

**Metrics:**
- **IC Mean**: Information Coefficient (Spearman rank correlation)
- **IC Sharpe**: Mean IC / Std Dev of IC
- **Autocorrelation**: Signal persistence (higher = slower decay)
- **Quantile Spread**: Top vs Bottom decile returns
- **Decay Half-Life**: Bars until IC drops to 50%

#### Alphalens-Style Deep Dive

1. **Quantile Bucket Returns** (Monotonicity Check)
   - Bar chart of returns by signal decile
   - Must be monotonic for robust signals

2. **IC Decay Curve** (Horizon Check)
   ```python
   ic_by_horizon = {
       "1d": 0.05,
       "5d": 0.04,
       "10d": 0.03,
       "20d": 0.01,
   }
   ```
   - Determines optimal trading frequency

3. **Regime Conditioning**
   ```python
   ic_by_regime = {
       "overall": 0.03,
       "bull_market": 0.05,
       "bear_market": -0.02,
       "high_vol": 0.01,
       "low_vol": 0.04,
   }
   ```

4. **Signal Correlation Matrix**
   - Identify redundant signals
   - Build uncorrelated signal ensembles

5. **Turnover Analysis**
   - Signal autocorrelation by lag
   - Estimated trading costs at various thresholds

---

### 7.6 Portfolio Construction ⭐⭐

#### Position Sizing Methods
```python
class PositionSizer:
    @staticmethod
    def equal_weight(signals: pl.Series) -> pl.Series:
        """1/N allocation to all non-zero signals."""

    @staticmethod
    def volatility_target(
        signals: pl.Series,
        volatility: pl.Series,
        target_vol: float = 0.10
    ) -> pl.Series:
        """Scale positions to target portfolio volatility."""

    @staticmethod
    def kelly_criterion(
        signals: pl.Series,
        win_rate: float,
        win_loss_ratio: float,
        fraction: float = 0.5,  # Half-Kelly for safety
    ) -> pl.Series:
        """Optimal growth sizing (use fractional Kelly)."""

    @staticmethod
    def risk_parity(
        signals: pl.DataFrame,  # Multi-asset
        covariance: np.ndarray,
    ) -> pl.Series:
        """Equal risk contribution across assets."""
```

#### Constraints & Limits
```python
@dataclass
class PortfolioConstraints:
    max_position: float = 0.10      # Max 10% per asset
    max_sector: float = 0.30        # Max 30% per sector
    max_gross_exposure: float = 2.0 # 200% gross (100L/100S)
    max_net_exposure: float = 0.20  # ±20% net
    min_assets: int = 10            # Minimum diversification
    max_turnover: float = 5.0       # Annual turnover limit
```

#### Optimization Targets
| Method | Objective | Use Case |
|--------|-----------|----------|
| Max Sharpe | Maximize risk-adjusted return | Standard optimization |
| Min Variance | Minimize portfolio volatility | Defensive positioning |
| Risk Parity | Equal risk contribution | Balanced diversification |
| Max Diversification | Maximize diversification ratio | Reduce concentration |
| Mean-CVaR | Maximize return per tail risk | Tail-risk aware |

---

### 7.7 Experiment Tracking & Reproducibility ⭐⭐

#### Research Experiment Logging
```python
@dataclass
class ExperimentLog:
    experiment_id: str
    timestamp: dt.datetime
    git_sha: str

    # Data
    data_spec: DataSpec
    data_checksum: str

    # Strategy
    strategy_class: str
    parameters: dict[str, Any]

    # Results
    metrics: dict[str, float]
    equity_curve_path: Path

    # Metadata
    runtime_seconds: float
    notes: str

class ExperimentTracker:
    def log_run(self, ...) -> ExperimentLog:
        """Log experiment with full reproducibility info."""

    def compare_runs(self, ids: list[str]) -> pl.DataFrame:
        """Compare metrics across experiments."""

    def reproduce(self, experiment_id: str) -> BacktestResult:
        """Re-run experiment from logged parameters."""
```

#### Artifact Storage
- Equity curves (parquet)
- Trade logs (parquet)
- Parameter grids and results
- Signal diagnostics snapshots

#### Notebook Integration
```python
# Auto-capture notebook state
tracker.log_notebook(
    notebook_path="research/sma_study.ipynb",
    cell_outputs=True,
    environment="requirements.txt",
)
```

---

### 7.8 Enhanced Cost Models ⭐

#### Market Impact Models
```python
class MarketImpactModel:
    @staticmethod
    def linear_impact(
        trade_size: float,
        avg_daily_volume: float,
        impact_coefficient: float = 0.1,
    ) -> float:
        """Simple linear impact: cost = coef × (size/ADV)"""

    @staticmethod
    def square_root_impact(
        trade_size: float,
        avg_daily_volume: float,
        volatility: float,
        impact_coefficient: float = 0.5,
    ) -> float:
        """Almgren-Chriss style: cost = coef × σ × √(size/ADV)"""
```

#### Capacity Estimation
```python
def estimate_capacity(
    strategy_turnover: float,
    universe_adv: pl.Series,      # Average daily volume per asset
    max_participation: float = 0.01,  # Max 1% of daily volume
) -> float:
    """Estimate maximum strategy AUM before returns degrade."""
```

#### Realistic Cost Components
| Component | Model |
|-----------|-------|
| Spread | Bid-ask from data or estimate |
| Commission | Tiered by exchange/broker |
| Slippage | POV or square-root model |
| Market Impact | Temporary + permanent |
| Funding | Overnight/margin rates |
| Borrow Cost | For short positions |

---

### 7.9 Multi-Asset & Cross-Sectional ⭐

#### Universe Management
```python
class Universe:
    def __init__(self, symbols: list[str], sector_map: dict[str, str] = None):
        self.symbols = symbols
        self.sector_map = sector_map or {}

    def filter_by_liquidity(self, min_adv: float) -> Universe:
        """Filter to liquid assets only."""

    def filter_by_sector(self, sectors: list[str]) -> Universe:
        """Filter to specific sectors."""
```

#### Cross-Sectional Signals
```python
# Rank assets by momentum, go long top decile, short bottom
lf = lf.with_columns(
    IndicatorFactory.rank("momentum_20d", over="date").alias("momentum_rank"),
).with_columns(
    pl.when(pl.col("momentum_rank") >= 0.9).then(1)
      .when(pl.col("momentum_rank") <= 0.1).then(-1)
      .otherwise(0)
      .alias("signal")
)
```

#### Pairs/Stat-Arb Framework
```python
class PairsAnalyzer:
    def find_cointegrated_pairs(
        self,
        price_matrix: pl.DataFrame,
        p_value_threshold: float = 0.05,
    ) -> list[tuple[str, str, float]]:
        """Find cointegrated pairs with hedge ratios."""

    def calculate_spread(
        self,
        pair: tuple[str, str],
        hedge_ratio: float,
    ) -> pl.Series:
        """Calculate normalized spread for pair."""
```

---

### 7.10 Visualization & Reporting ⭐

#### Standard Report Suite
| Chart | Purpose |
|-------|---------|
| Cumulative returns (log scale) | Strategy vs benchmark |
| Underwater plot | Drawdown over time |
| Rolling Sharpe | Performance stability |
| Monthly returns heatmap | Seasonality patterns |
| Return distribution | Fat tails, skewness |
| Rolling correlation | Regime changes |

#### Interactive Dashboards
- Parameter sensitivity heatmaps
- WFA window-by-window breakdown
- Trade-level drill-down

#### Export Formats
```python
class ReportGenerator:
    def to_html(self, result: BacktestResult, path: Path) -> None:
        """Generate standalone HTML report."""

    def to_pdf(self, result: BacktestResult, path: Path) -> None:
        """Generate PDF report."""

    def to_tearsheet(self, result: BacktestResult) -> pl.DataFrame:
        """Generate quantstats-style tearsheet data."""
```

---

### 7.11 Storage & Caching

- Parquet output format with canonical schema
- Cache by `data_version + universe + frequency`:
  - Asset returns matrix
  - Basic features (vol, momentum, ranks)
  - Covariance matrices
  - Transaction cost inputs
- Incremental updates for rolling windows

---

### 7.12 Advanced Features (Long-Term)

| Feature | Description | Priority |
|---------|-------------|----------|
| **GPU Acceleration** | cuDF/RAPIDS for large universes | Medium |
| **Multi-Timeframe Signals** | Resample in Polars, align back | Medium |
| **Signal Diagnostic Streams** | Reason codes, stop levels, regime labels | Low |
| **Live Trading Bridge** | Connect research signals to execution | Low |
| **Synthetic Data Generation** | Monte Carlo for stress testing | Medium |
| **Bayesian Optimization** | Smart parameter search | Medium |
| **Ensemble Methods** | Signal combination optimization | Medium |
| **Alternative Data Integration** | Sentiment, on-chain, etc. | Low |

---

## Appendix: Metrics Glossary

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| **Sharpe** | Mean(R) / Std(R) × √AF | Risk-adjusted returns |
| **Sortino** | Mean(R) / DownsideStd × √AF | Downside risk-adjusted |
| **Max Drawdown** | Max((Peak - Equity) / Peak) | Worst peak-to-trough |
| **CAGR** | (Final/Initial)^(1/years) - 1 | Annualized growth |
| **Calmar** | CAGR / Max Drawdown | Return per unit risk |
| **Volatility** | Std(R) × √AF | Annualized std dev |
| **Turnover** | Mean(|Position Change|) | Trading activity |

*AF = Annualization Factor (252 for daily, 365×24 for hourly)*

---

*Last updated: December 2024*
