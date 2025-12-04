# Backtester V2: Event-Driven Quantitative Trading Engine

<!-- Add Badges here: CI Status, Code Coverage, Python Version, License -->

## 1. Overview
Backtester V2 is an attempt for a high-fidelity, **event-driven** trading engine built for **crypto derivatives** (Spot, Futures, Options), however, the architecture is built to be asset-agnostic. Core features include a **deterministic** "tick-and-drain" event loop that guarantees reproducibility across runs. The backtester combines grandular tick-lvel simulation with decently a high-throughput using Polars and Parquet.

## 2. Key Features
### Simulation Fidelity
- **Execution Models:** Supports **Slippage** and Market Impact models (current: Fixed BPS) defined in ([`SlippageModel`](backtester/sim/sim_models.py)).
- **Fee Structures:** Native support for Maker/Taker tiers and fixed BPS fee models ([`FeeModel`](backtester/sim/sim_models.py)).

### Architecture & Performance
- **Tick-and-Drain Loop:** A strict, deterministic event loop ([ADR 021](adr/021-micro-batch.md)) that guarantees reproducibility by resolving all events at time $T$ before advancing.
- **High-Throughput Data:** Leverages **Polars** and **Parquet** for efficient storage and querying of tick-level data.
- **Async Core:** Built on Python's `asyncio` for non-blocking I/O and responsive signal handling.

### Risk Management
- **Pre-Trade Checks:** Enforces exchange constraints like Max Notional, Lot Size, and Tick Size via [`SymbolSpecs`](backtester/risk/order_validation.py).

### Data Pipeline
historical data downloader (`backtester/data/downloader.py`) supporting:
- **Sources:** Binance (Spot, Futures, Options).
- **Integrity:** Automatic validation (negative prices, H/L checks) and sanitization.
- **Storage:** Parquet files partitioned by day with a central manifest.


## 3. System Architecture
The engine follows a **Modular Monolith** architecture where distinct domains (Strategy, Risk, Execution, Account) are decoupled and communicate exclusively via an in-memory **Event Bus**.

### Event Bus Topology
Components publish and subscribe to typed topics, ensuring strict separation of concerns.
- **Strategy:** Subscribes to `T_CANDLES`, publishes `T_ORDERS_INTENT`.
- **Risk:** Intercepts intents, validates them, and publishes `T_ORDERS_SANITIZED`.
- **Execution:** Matches sanitized orders against market data and publishes `T_FILLS`.
- **Account:** Tracks state and publishes `T_ACCOUNT_SNAPSHOT`.

For a complete registry of topics and payloads, see [**Bus Topology & Module I/O**](docs/bus_topology.md).

## 4. Getting Started
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

Results (logs, metrics, audit trails) will be saved to the `runs/` directory by default.

## 5. Data Ingestion
Before running a backtest, you need to download historical data. Currently, this is done via a Python script using the `DownloaderFiles`, `ParsingSanitizing`, and `WritingParquet` components.

The following script demonstrates how to download Spot, Futures, and Options data from Binance, parse it, and save it to a local Parquet dataset.

```python
from pathlib import Path
import datetime as dt
from backtester.config.configs import DownloaderConfig, RunContext
from backtester.data.downloader import DownloaderFiles, ParsingSanitizing, WritingParquet

# Configure your data directory
BASE_DIR = Path("/Users/name/data/crypto_data/parquet")
LOG_DIR = Path("runs")

def run_e2e(symbol: str, market: str, interval: str | None, start: dt.datetime, end: dt.datetime):
    # 1) Config and services
    run_ctx = RunContext(run_id=f"DL-{market}-{symbol}", seed=42, git_sha="HEAD", allow_net=True)
    cfg = DownloaderConfig(base_dir=BASE_DIR, log_dir=LOG_DIR, download_run_id=run_ctx.run_id)
    dl = DownloaderFiles(run_ctx=run_ctx, cfg=cfg)

    # 2) Download → RawBytes[]
    # For options, interval is ignored by the dataset; pass any string (e.g., "1h") or None.
    tf = interval or "1m"
    blobs = dl.download_files(symbol, market, tf, start, end)
    print(f"Downloaded: {len(blobs)} blobs")

    # 3) Parse & sanitize → [SanitizedBatch]
    ps = ParsingSanitizing(audit=dl._audit)
    batches, parse_report = ps.parse_archives(blobs)
    print("Parse report:", parse_report)

    # 4) Write Parquet (+ manifest)
    writer = WritingParquet(cfg=cfg, audit=dl._audit)
    write_report = writer.write_batches(batches, strict=False)
    print("Write report:", write_report)

# ---- Usage Examples ----

# 1. Spot (ETHUSDC)
run_e2e(
    symbol="ETHUSDC",
    market="spot",
    interval="1m",
    start=dt.datetime(2024, 9, 1, tzinfo=dt.timezone.utc),
    end=dt.datetime(2024, 10, 1, tzinfo=dt.timezone.utc),
)

# 2. Futures (BTCUSDT)
run_e2e(
    symbol="BTCUSDT",
    market="futures",
    interval="1m",
    start=dt.datetime(2024, 9, 1, tzinfo=dt.timezone.utc),
    end=dt.datetime(2024, 10, 1, tzinfo=dt.timezone.utc),
)

# 3. Options (ETHUSDT)
run_e2e(
    symbol="ETHUSDT",
    market="option",
    interval=None,
    start=dt.datetime(2023, 8, 1, tzinfo=dt.timezone.utc),
    end=dt.datetime(2023, 8, 5, tzinfo=dt.timezone.utc),
)
```

Data will be saved to `BASE_DIR` in a partitioned Parquet format (e.g., `year=YYYY/symbol-tf-YYYY-MM.parquet`).

## 6. Strategy Implementation
Strategies implement the [`Strategy`](backtester/strategy/base.py) abstract base class. The core logic resides in two main event hooks:

- **`on_candle(self, candle: Candle) -> list[OrderIntent]`**: Called for every new candle. This is where you update indicators, check signals, and return a list of `OrderIntent` (e.g., `MarketOrderIntent`, `LimitOrderIntent`) to be executed.
- **`on_fill(self, fill: Fill) -> list[OrderIntent]`**: Called whenever an order is filled. Use this to manage position state or issue follow-up orders (e.g., stop-loss/take-profit).

The engine handles all state management, order routing, and time-travel safety. Strategies are designed to be **side-effect free** regarding the external world — they only emit intents.

For a complete reference implementation, see [`SMAExtendedStrategy`](backtester/strategy/sma_extended.py), which demonstrates:
- Parameter management
- Rolling window calculations (SMA)
- State tracking (positions, cooldowns)
- Risk controls (daily loss limits)

## 7. Engineering Standards
In this project, I tried to adhere to the following engineering standards:

- **ADRs (Architecture Decision Records):** All major architectural decisions are documented in the `adr/` folder.
- **Determinism:** Every run is seeded and logged with the Git SHA. The "Tick-and-Drain" loop ensures that `Run(Config + Data + Seed)` always yields the exact same result.
- **Type Safety:** Fully typed codebase using Python type hints and `mypy` for static analysis.
- **Code Quality:** Enforced via `ruff` (linting/formatting) and `pre-commit` hooks.

## 8. Project Roadmap

### Data & Analytics
- [ ] **Parameter Optimization:** Grid search and genetic algorithm runners for strategy tuning.
- [ ] **Historical Data Splitter** Helpers to split historical data into optimization & walk forward

### Live Trading & Infrastructure
- [ ] **Live Execution Adapter:** "Paper Trading" and "Live Trading" modes connecting to real exchange APIs.
- [ ] **Distributed Backtesting:** Parallel execution of backtests for large-scale parameter sweeps.
- [ ] **Web Dashboard:** A lightweight UI for monitoring active backtests and visualizing results.

## Closing Remarks
- **Testing** Due to my inexperience my designs kept changing wherefore I removed almost all tests. Tests will come back as soon as a stable interface is reached.
