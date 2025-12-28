# Backtester V2: Event-Driven Quantitative Trading Engine

[![CI](https://github.com/ibachni/backtester_v2/actions/workflows/ci.yml/badge.svg)](https://github.com/ibachni/backtester_v2/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)

## 1. Overview
Backtester V2 is a a high-fidelity, **event-driven** trading engine built for **crypto derivatives** (Spot, Futures, Options), however, the architecture is built to be asset-agnostic. Core features include a **deterministic** "tick-and-drain" event loop that guarantees reproducibility across runs. The backtester combines grandular tick-lvel simulation with high-throughput using Polars and Parquet (Throughput: ~68,000 events/second on M4 Pro, see full analysis under 6. Performance & Benchmarks.)

## 2. Core Architecture: The "Tick-and-Drain" Loop
Unlike vectorized backtesters that prioritize speed over fidelity, Backtester V2 uses a strict **Tick-and-Drain** event loop ([ADR 021](adr/021-micro-batch.md)).
1. **Micro-Batching:** Time advances only when the event queue for time $T$ is fully drained.
2. **Causality:** Guarantees that a signal generated at $T$ cannot be acted upon until $T+\Delta$.
3. **Determinism:** A run with the same config, data, and seed produces the exact same ledger, regardless of thread scheduling.

(This approach lacks realism to simulate high-frequency trading. However, this type of trading is out of scope of this software project.)

## 3. Engineering Standards & ADRs
Major architectural decisions are documented to track trade-offs and context:
- [ADR 001: Determinism by Default](adr/001-determinism-by-default.md)
- [ADR 018: Risk Rails & Pre-Trade Checks](adr/018-risk-rails-pretrade-and-halt.md)
- [ADR 021: Micro-Batch Architecture](adr/021-micro-batch.md)

### Code Quality & Rigor
- **Type Safety:** Fully typed codebase using Python type hints and `mypy` for static analysis.
- **Code Quality:** Enforced via `ruff` (linting/formatting) and `pre-commit` hooks.

## 4. Key Features
### Market Microstructure Simulation
- **Execution Models:** Supports **Slippage** and Market Impact models (current: Fixed BPS) defined in ([`SlippageModel`](backtester/sim/sim_models.py)).
- **Fee Structures:** Native support for Maker/Taker tiers and fixed BPS fee models ([`FeeModel`](backtester/sim/sim_models.py)).

### Pre-Trade Risk Engine
- **Risk Rails:** Enforces exchange constraints like Max Notional, Lot Size, and Tick Size via [`SymbolSpecs`](backtester/risk/order_validation.py).
- **Safety:** Fail-closed design with global halt capabilities.

### Data Pipeline
Built on **Polars** and **Parquet** for high-throughput ingestion of tick-level data.
- **Multi-Asset Support:** Native handling of Spot, Futures, and Options.
- **Sanitization:** Automated checks for negative prices, broken OHLC invariants, and gaps.
- **Storage:** Partitioned Parquet datasets (`year=YYYY/symbol.parquet`) for efficient querying.

## 5. System Architecture
The engine follows a **Modular Monolith** architecture where distinct domains (Strategy, Risk, Execution, Account) are decoupled and communicate exclusively via an in-memory **Event Bus**.

### Event Bus Topology
Components publish and subscribe to typed topics, ensuring strict separation of concerns.
- **Strategy:** Subscribes to `T_CANDLES`, publishes `T_ORDERS_INTENT`.
- **Risk:** Intercepts intents, validates them, and publishes `T_ORDERS_SANITIZED`.
- **Execution:** Matches sanitized orders against market data and publishes `T_FILLS`.
- **Account:** Tracks state and publishes `T_ACCOUNT_SNAPSHOT`.

<img src="docs/images/architecture_mermaid.png" alt="Architecture Mermaid Flow Chart" width="600"/>

Figure 1: Mermaid chart of the Event Flow. The red lines represent a standard flow, from tick to fill.

For a complete registry of topics and payloads, see [**Bus Topology & Module I/O**](docs/bus_topology.md).

## 6. Performance & Benchmarks
While event-driven engines are inherently slower than vectorized simulations, Backtester V2 is optimized for high-throughput research iterations.

The following benchmarks measure the "Stress Test" configuration: a high-frequency churn strategy ($T=1m$) processing tick-level state updates with full risk validation enabled.

| Metric | Result | Notes |
| :--- | :--- | :--- |
| **Throughput** | **67,976 events/sec** | Includes signal generation, risk checks, and filling |
| **Order Latency** | **~1,084 orders/sec** | Full lifecycle (Intent $\to$ Risk $\to$ Match $\to$ Fill) |
| **Total Runtime** | **12 min 18 sec** | Processed 6 years (2192 days) of M1 Data |
| **Events Processed** | **50,189,938** | Total distinct bus messages handled |
| **Drift** | **0ms** | Strict event ordering guarantees zero look-ahead |

*Hardware: MacBook M4 Pro, running on Python 3.12.*


## 7. Getting Started
### Prerequisites
- **Python 3.11+**
- **pip**

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/nicolas/backtester_v2.git
   cd backtester_v2
   ```

2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -e ".[dev]"
   ```

3. Verify installation:
   ```bash
   bt --help
   ```

### Running a Backtest
To execute a backtest, you need a **Backtest Configuration** (engine settings `backtest.toml`) and a **Strategy Configuration** (algo parameters, e.g., `sma_extended.toml`), located in the `configs/` folder.

```bash
# Example usage
bt --strategy sma_extended --symbols BTCUSDT ETHUSDT
```

**Console Output**: The engine provides a CLI dashboard summarizing the simulation parameters and final performance report (Tear Sheet).

<img src="docs/images/backtest_screenshot.png" alt="Backtest Performance Report" width="600"/>

Figure 2: Terminal output demonstrating a sanity check run using a basic SMA strategy on 4h data.

Results (logs, metrics, audit trails) are automatically logged to the runs/ directory for further analysis.

## 8. Data Ingestion
Before running a backtest, you need to download historical data. Currently, this is done via a Python script using the `DownloaderFiles`, `ParsingSanitizing`, and `WritingParquet` components, see [Downloading](docs/downloading_data.md) for an example.


## 9. Strategy Implementation
Strategies implement the [`Strategy`](backtester/strategy/base.py) abstract base class. The core logic resides in two main event hooks:

- **`on_candle(self, candle: Candle) -> list[OrderIntent]`**: Called for every new candle. This is where you update indicators, check signals, and return a list of `OrderIntent` (e.g., `MarketOrderIntent`, `LimitOrderIntent`) to be executed.
- **`on_fill(self, fill: Fill) -> list[OrderIntent]`**: Called whenever an order is filled. Use this to manage position state or issue follow-up orders (e.g., stop-loss/take-profit).

The engine handles all state management, order routing, and time-travel safety. Strategies are designed to be **side-effect free** regarding the external world — they only emit intents.

For a complete reference implementation, see [`SMAExtendedStrategy`](backtester/strategy/sma_extended.py), which demonstrates:
- Parameter management
- Rolling window calculations (SMA)
- State tracking (positions, cooldowns)
- Risk controls (daily loss limits)



## 10. Project Roadmap - Up Next
- **Crash Recovery:** Enable restart on crash by hydrating state from disk to avoid losing long-running simulations.
- **Parameter Optimization (Optuna):** Integrate grid/Random/Optuna-based optimization to search parameter spaces efficiently.
- **Vectorized Backtester:** Provide a fast, low-fidelity engine for quick initial screening before event-driven runs.
- **Live Execution Adapter:** Connect the engine to exchange APIs for Paper and Live trading using the same core abstractions.
- **Short Selling & Margin:** Add robust support for margin, funding rates, and borrowing costs for realistic derivatives strategies.
- **Microstructure Signals** Add Support for L2/L3 data (predictive power on much shorter time horizon).


## 11. Feature Backlog

### Core Engine & Engineering
*Focus on the "tick-and-drain" loop, execution fidelity, and system stability.*
- [ ] **Global Kill/Flatten Mechanism:** Safety switch to close all positions immediately ([ADR 002](adr/002-safety-fail-closed.md)).
- [ ] **Short Selling & Margin:** Support for margin management, funding rates, and borrowing costs.
- [ ] **Latency Modelling:** Configurable oracle to inject delays between signal and execution.
- [ ] **Order Book Reconstruction:** Support for L2 snapshots and L3 incremental updates for true liquidity modeling.
- [ ] **Crash Recovery:** Enable restart on crash by hydrating state from disk.
- [ ] **UX Improvements:** Banner in stdout when `allow-net=True`.

### Idea Generation & Alpha Research
*Tools for rapid prototyping and signal discovery.*
- [ ] **Expression-Based Strategies:** Simple parser for formulas (e.g., `bt --formula "cross_over(sma(close, 50), sma(close, 200))"`).
- [ ] **Alpha Library:** Repository of pure signal functions (return -1 to 1) to decouple signal generation from execution logic.
- [ ] **Universe Selection Framework:** Dynamic asset selection (e.g., "Top 10 coins by volume > 200d MA").
- [ ] **Information Coefficient (IC) Analysis:** Vectorized tool to measure correlation between signals and forward returns over time.
- [ ] **Factor Analysis / Alpha Decay:** Track signal performance over years to detect decay.
- [ ] **ML Integration:** Native support for XGBoost, LightGBM, CatBoost models.
- [ ] **Statistical Arbitrage Tools:** Cointegration tests (Engle-Granger, Johansen) and spread modeling for pairs trading.
- [ ] **Dynamic State Estimation:** Kalman Filters for adaptive hedge ratios and trend following without lag.
- [ ] **Regime Detection:** Hidden Markov Models (HMM) to classify market states (e.g., Trending vs Mean Reverting).
- [ ] **Microstructure Signals:** Order Flow Imbalance (OFI) and effective spread estimators.

### Data & Analytics
*Visualization, reporting, and data management.*
- [ ] **Parameter Optimization:** Grid search and genetic algorithm runners (with Optuna integration).
- [ ] **Performance Attribution:** Decompose returns into Beta (market), Sector (crypto), and Alpha (skill).
- [ ] **Trade-Entry Heatmap:** Visualize buy/sell markers on charts, color-coded by PnL.
- [ ] **2D Parameter Grid:** Heatmap visualization of strategy performance across two parameters (e.g., Period vs Stop Loss).
- [ ] **Reason Codes:** Log specific reasons when a strategy *declines* a trade (e.g., "Risk Limit", "Low Volatility").
- [ ] **Historical Data Splitter:** Helpers for Train/Test/Walk-Forward splits.
- [ ] **Vectorized Backtester:** Lightweight engine for initial quick analysis before event-driven simulation.
- [ ] **Alternative Data:** Support for Funding Rates, Open Interest, and On-Chain metrics.
- [ ] **Transaction Cost Analysis (TCA):** Compare execution prices against Arrival Price and VWAP benchmarks.

### Robustness & Validation
*Ensuring strategies are not overfitted.*
- [ ] **Combinatorial Purged Cross Validation (CPCV):** Advanced splitting method to prevent look-ahead bias.
- [ ] **Deflated Sharpe Ratio:** Adjust metrics based on the number of trials to penalize p-hacking.
- [ ] **Synthetic Data Generation:** Test against GAN/Bootstrap generated price paths.
- [ ] **Market Regime Classifier:** Tag periods as Bull/Bear/Crab to filter performance metrics.
- [ ] **Monte Carlo Simulation:** Shuffle trade returns to estimate tail risk.
- [ ] **Noise Injection:** Stress test with widened spreads, dropped ticks, and timestamp jitter.
- [ ] **Sensitivity Analysis:** 3D surface plots to ensure parameters sit on a "plateau" of stability, not a "peak".

### Portfolio & Capital Management
- [ ] **Dynamic Position Sizing:** Kelly Criterion and Volatility Targeting.
- [ ] **Correlation Constraints:** Limit exposure to highly correlated assets.
- [ ] **Breakeven AUM Calculator:** Determine maximum capital capacity before alpha degrades.
- [ ] **Portfolio Optimization:** Solvers for Mean-Variance Optimization and Hierarchical Risk Parity (HRP).

### Live Trading & Infrastructure
- [ ] **Live Execution Adapter:** Connect to exchange APIs for Paper and Live trading.
- [ ] **Algo Execution:** TWAP, VWAP, and POV (Percentage of Volume) logic to minimize market impact.
- [ ] **Distributed Backtesting:** Parallel execution for large-scale parameter sweeps.
- [ ] **Web Dashboard:** UI for monitoring active backtests and results.
- [ ] **Experiment Tracking:** Database to log results, parameters, and artifacts.

## Note:
- **Testing** The goal is to stabilize the core API. The existing Unit and property-based tests (via Hypothesis) are being migrated from v1 to ensure invariant preservation across the new event loop.
