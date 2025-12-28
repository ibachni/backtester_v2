## 1. Goals
A **high-speed research backtesting module** optimized for rapid iteration, including
- Parameter Optimzation Engine
- Built-in Walk-forward analysis (WFA)
## 2. System Architecture and Stack
- **Primary Framework:** `polars` (v1.0+) for data manipulation.
- **JIT Compilation:** `numba` (v0.60+) for non-vectorizable logic (e.g., path-dependent trailing stops).
- **Data Format:** Parquet (Partitioned by Year/Month).
- **Parallelism:**
	- Use joblib.Parallel(backend='threading') for parameter search, with a thread cap.
	- Inside Numba kernels, disable parallel=True unless you benchmark benefit; set threading layer and thread counts to avoid oversubscription.
	- Document thread control (e.g., NUMBA_NUM_THREADS, OMP_NUM_THREADS).
## 3. Modules
#### 3.1. Data & I/O (`data_loader.py`)
- Goal: Efficiently load partitioned parquet files using LazyFrames to avoid OOM errors during research.
- **Class:** `ParquetLoader`
- **Methods:**
	- `load_dataset(source_path: str, date_range: tuple)`:
	    - Must use `pl.scan_parquet()`.
	    - Must support Hive partitioning (e.g., `/data/symbol=BTC/year=2023/month=01/*.parquet`).
	    - **Requirement:** Apply filter pushdowns immediately (filter by date/symbol before collecting).
	    - **Output:** Returns a `pl.LazyFrame`.
	- `resample_data(lazy_frame, timeframe)`:
	    - Use `upsample` or `group_by_dynamic` for converting tick/minute data to research timeframes.
	- Components
		- DataSpec (the contract: defines what to load)
		- PolarsScanLayer (lazy query builder)
			- pl.scan_parquet (hive_partitioning=True)
			- Filter by start and end immediately
			- Always use **LazyFrame** until the last moment (lets Polars optimize).
			- Keep the scan minimal
		- Schema & dtype normalization
			- Normalize data for downstream numba/vectorized
			- Cast floats to Float32 (for speed)
			- Ensure timestamp is ms
			- Sort by timestamp immediately
		- Timeframe resampler
			- Convert data (e.g., 1 min) to analysis timeframe (e.g., 1 hours or 1 day)
			- Logic: Use `group_by_dynamic()` or `upsample()`.
- Notes:
	- Data in parquet, partitioned by month (default) or day
	- Use existing utility functions and data loaders if possible
#### 3.2. Strategy & Signal (`strategy.py`)
- **Goal:** Hybrid calculation engine. Use Polars expressions for standard indicators and Numba for complex iterative logic. It must handle two types of logic:
	1. **Stateless (Vectorized):** Standard indicators (SMA, RSI, returns) where row t does not depend on the _output_ of row t−1.
	2. **Stateful (Iterative/Recursive):** Logic where the current state depends on the previous state (e.g., Trailing Stops, SuperTrend, exponential moving averages with variable alpha).
- **Class:** `StrategyBase`
- **Methods:**
    - `add_indicators(lf: pl.LazyFrame)`:
        - Pure Polars expressions (e.g., `pl.col("close").rolling_mean(n)`).
    - `apply_numba_logic(lf: pl.LazyFrame, col_inputs: list)`:
        - **Spec:** Extract columns as NumPy arrays inside a custom function.
        - **Spec:** Pass arrays to a `@numba.jit(nopython=True)` function.
        - **Spec:** Return result as a `pl.Series` via `map_batches`.
        - _Use Case:_ Trailing stop-losses, regime-dependent sizing.
    - Components:
	    - The Indicator Factory (Pure Polars)
			- **Role:** Calculate standard technical indicators using Polars Expressions.
			- **Mechanism:**
			    - Use `pl.col().rolling_mean()`, `pl.col().diff()`, etc.
			    - Prefer `pl.Expr` over custom Python functions whenever possible. Polars executes these in Rust with parallelism.
		- The JIT Bridge (`map_batches` + Numba)
			- **Role:** Handle recursive logic that requires a loop.
			- **Mechanism:**
			    - Use `pl.map_batches()` to pass a column (Series) to a Python function.
			    - Inside that function, convert the Series to a **NumPy array**.
			    - Pass the array to a `@numba.njit` optimized function.
			    - Return the result as a Polars Series.
			- Note
				- **Trade at next bar close**
		- The Signal Combiner
			- **Role:** Combine indicators into a single "Target Position" or "Signal" column.
			- **Mechanism:**
			    - Use `pl.when(condition).then(1).otherwise(0)` chains.
			    - This logic remains inside the LazyFrame graph.
		- Notes
			- **Handling Nulls:** use `fill_null()` before passing data to Numba. NaNs in Numba loops can propagate unexpectedly or cause errors if not handled explicitly."
			- **Float32 Consistency:** "Ensure the Numba function signature expects `float32` arrays if the Data Loader casts to `Float32`. Mismatched types trigger expensive casting."
			- **Groups:** If the LazyFrame contains multiple symbols, use `.over('symbol')` or `group_by` context for the indicators so that the moving average of 'BTC' doesn't bleed into 'ETH'."
			- Keep a stable interface so later you can swap indicator engine or add GPU, etc.
			- Provide a debug mode returning the feature DataFrame for inspection.
		- Future additions (non MVP!!!)
			- Multi-asset features
			- Multi-timeframe signals (resample in polars + align back)
			- Signal diagnostic streams (reason codes, stop levels, regime labels)

#### 3.3. Vectorized Engine (`engine.py`)
**Goal:** Compute the equity curve and performance metrics efficiently using column-wise operations. (loops are forbidden here).
**Output:** A lightweight `LazyFrame` with the equity curve, and a dictionary of scalar metrics (Sharpe, Drawdown).
**Intended workflow**:
- **Generate Factors:** Compute signals (vectors) for the whole universe.
- **Calculate IC:** Run the "Leaderboard" code. Filter out anything with IC < 0.01.
- **Check Monotonicity:** For the survivors, look at the Quantile Bar Chart.
Implementation:
- **Class:** `VectorizedEngine`
- **Logic Flow:**
    1. **Signal Calculation:** `signal = 1` (long), `-1` (short), `0` (neutral).
    2. **Position Shift:** `position = signal.shift(1)` (to simulate trade execution on next bar).
    3. **Returns:** `strategy_return = position * asset_return`.
	    - Logic:
		    - `Market Return` = `(Price[t] / Price[t-1]) - 1`
			- `Gross Strategy Return` = `Position[t] * Market Return[t]`
			- `Net Return` = `Gross Strategy Return - Cost`
			- `Equity Curve` = Cumulative Product of `(1 + Net Return)`.
    4. **Cost Model:** Apply fixed slippage/fees
	    - **Turnover:** `abs(position - position.shift(1))` represents the volume traded (0 to 200% turnover per bar).
		- **Cost:** `Turnover * (Trading Fee + Slippage estimate)`.
		- See existing fee and slippage models
    5. **Metrics (Lazy):** Construct expressions for Sharpe, Max Drawdown, and CAGR.
	    - Sharpe, Sortino, Max Drawdown
	    - Compute equity series lazily via expressions, then select only max drawdown scalar. You still “collect,” but only scalars, not the full series.
	    - Logic: Use polars aggregations (std, mean, min) on the calculated returns column.
- **Output:**
	- Identity & reproducability
		- Run id
		- strategy version
		- data version (dataset hash, vendor + asof)
		- frequency, start, end
		- cost_model_id
		- params
	- Cache by **data_version + universe + frequency**:
		- asset returns matrix
		- basic features (vol, momentum, ranks, z-scores)
		- masks (tradability, borrow, corporate actions filters)
		- transaction cost inputs (spread, ADV)
	- Robustness
		- n_nans
		- n_days_in_market
	- A "Aligned DataFrame", containing:
		- Raw Signal
		- Discretized Position
		- Asset returns
		- Strategy Returns
		- Transactions costs
		- Strategy returns (net)
	- Metrics dictionary
		- Sharpe, max drawdown, CAGR, turnover, trade count, total return, volatility, sortino, Calmar, Stability: $R^2$ of the equity curve vs a straight line
	- Avg win/loss, profitfactor, winrate, costshare
	- Trade log:
		- Goal: reconstruct distinct trades from position vectory
		- **Logic:** Identify where Positiont​=Positiont−1​.
		- **Output:** A list of dictionaries containing `{Entry Time, Exit Time, Duration, PnL %, Entry Signal Strength}`.
	- Visualization outputs:
		- Log-scale cumulative returns: compare strategy (net) vs benchmark
		- Underwater plot (drawdown)
		- Rolling sharpe/beta
	- Parameter cube: Hyperparameter sensitivity:
		- Heatmap matrix: x (parameter a), y (parameter b), color: Sharpe
- Storage formats:
	- Use Parquet
	- `StrategyName_AssetClass_TimeFrame_Hash.parquet`
	- Use a **single canonical schema** for metrics (stable column names)
	- Keep `params` both as JSON _and_ flattened columns for quick filtering
- Notes
	- ZERO LOOPS! We rely entirely on Polars expressions to compute performance across millions of rows in milliseconds.
	- Trade at next bar open
	- **MUST KEEP INTERFACE COMPATIBLE WITH LATER ADDITION OF A RESEARCH MODULE (see below)**
- Non MVP, latter additions:
	- Multi-asset portfolio simulation (vector of qty per asset)
	- Intrabar stop/limit using high/low, partial fills
	- Funding/borrow, leverage/margin rules

#### 3.4 Parameter Optimization Engine (`optimizer.py`)
**Goal:** High-speed grid/random search over the `StrategyCore`.
- **Class:** `ParamOptimizer`
- **Methods:**
    - `generate_grid(param_ranges: dict)`: Creates a list of permutation dictionaries.
    - `run_parallel(dataset: pl.LazyFrame, strategy_class, param_grid)`:
        - **Spec:** Use `joblib.Parallel` or `ProcessPoolExecutor`.
        - **Constraint:**
	        - Pre-collect the minimum needed dataset once in the main thread and reuse it across threads (zero-copy).
			- Avoid passing LazyFrame to each worker to prevent N× disk reads and redundant optimization plans.
			- Threading backend is fine because Polars/NumPy release the GIL; ensure read-only usage per thread.
        - Workers instantiate the engine locally and return only metrics (minimizing IPC data transfer
- components
	- Grid generator
		- **Role:** Turn ranges of parameters into a testable list.
		- **Logic:** Use `itertools.product` to create a Cartesian product of all inputs.
		    - _Input:_ `{'ma_window': [10, 20], 'stop': [0.01, 0.05]}`
		    - _Output:_ `[{'ma_window': 10, 'stop': 0.01}, ...]`
	- Parallel Executor (joblib)
		- **Role:** Distribute the backtest jobs.
		- **Critical optimization:** Use `backend='threading'`.
		    - _Process-based (default):_ Requires copying the dataset to every core. Slow start-up, high RAM usage.
		    - _Thread-based:_ All threads share the same memory pointer to the Polars DataFrame. Zero-copy.
	- The Worker Function (The "Unit of Work")
		- **Role:** A standalone function that runs one single backtest.
		- **Constraint:** It must **never** return the full equity curve (DataFrame). It should only return a lightweight dictionary (metrics + params) to keep the main thread unblocked.
- Note
	- Supports
		- **Grid search** (small spaces)
		- **Random search** (large spaces)
	- Prefer **Numba `prange`** inside a single process (often faster than Python multiprocessing)
	- **Robustness.** If a specific parameter set crashes (e.g., `window=0`), the worker should return `None` or a "failure" dict rather than crashing the whole optimization run.
	- **Pre-collection of Data** **I/O Bottleneck Prevention.** We call `collect()` on the data _before_ the loop starts. We do not want 100 threads trying to read the Parquet file from disk simultaneously.
#### 3.6. Walk-Forward Analysis (`wfa.py`)
**Goal:** Validate robustness by rolling training/testing windows.
- **Class:** `WalkForwardValidator`
- **Logic:**
    - Define `train_window_months` and `test_window_months`.
    - **Loop:**
        1. Slice `LazyFrame` for Training Period (In-Sample).
        2. Run `ParamOptimizer` to find top params.
        3. Slice `LazyFrame` for Testing Period (Out-of-Sample).
        4. Run `VectorizedEngine` using top params on Test slice.
    - **Stitching:** Concatenate all Out-of-Sample equity curves into a single master performance series.
- Components
	- The Time Slicer
		- **Role:** Generate start/end timestamps for every window.
		- **Logic:**
		    - Define `train_period` (e.g., '365d') and `test_period` (e.g., '30d').
		    - Shift the window forward by `test_period` in each iteration.
		    - **Types:**
		        - _Rolling:_ Train window moves forward (constant size). Good for adapting to recent regimes.
		        - _Anchored:_ Train window grows (start date fixed). Good for accumulating more history.
	- The Orchestrator (The Loop)
		- **Role:** Manage the complexity of the re-optimization cycle.
		- **Workflow:**
		    1. **Slice Train:** Extract data tstart​ to tcutoff​.
		    2. **Optimize:** Run **Module D** on Train slice → Get `Best_Params`.
		    3. **Slice Test:** Extract data tcutoff​ to tend​.
		    4. **Warm-up Handling:** _Crucial Step._ The Test slice must actually start slightly _before_ tcutoff​ (the "warmup buffer") so that indicators (like a 200-day MA) are valid on the very first day of the testing period.
		    5. **Execute:** Run **Module C** (Engine) on Test slice using `Best_Params`.
		- The Result Stitcher
			- **Role:** Combine the fragmented Test results.
			- **Logic:**
			    - Truncate the "warmup" portion of the Test results.
			    - Append the valid PnL to the Master OOS DataFrame.
			    - Re-index the equity curve so it is continuous (compound returns correctly across stitches).

#### 3.7 Other Notes
- Convert Polars → numpy once per dataset/window
- Keep arrays **float32 contiguous** (`np.ascontiguousarray`)
- Use `cache=True` so repeated runs reuse compiled artifacts

## 4. MVP acceptance criteria (quick checklist)
- Loads date ranges from **monthly/day partitioned Parquet** using Polars scan + filters
- Runs a full backtest with **Numba JIT** kernels end-to-end
- Parameter Optimization: grid + random, returns best params + score table
- Walk-forward: produces per-window train/test results in Parquet
- Costs included (fee + slippage bps), deterministic execution timing

## 5. Non-goals (MVP)
- Full order lifecycle (limit/stop/partial fills)
- Multi-asset portfolio constraints beyond “single symbol or small set”
- Live/paper integration (this module is research-mode)

## 6. Later Additions
### The Workflow (Research Module)
- Keep this module separate from execution Engine.
1. The "Signal Leaderboard"
	- Generate a DataFrame where the index is the list of final names and columns are "predictive vital signs"
	- Goal: sort and filter thousands of ideas instantly.
	- Output table should look like this:
	| **Signal Name**  | **IC Mean** | **IC Sharpe (ICIR)** | **Autocorrelation** | **Quantile Spread** |
	| ---------------- | ----------- | -------------------- | ------------------- | ------------------- |
	| `RSI_Divergence` | 0.04        | 0.6                  | 0.85                | 1.2%                |
	| `Momentum_12M`   | 0.02        | 0.3                  | 0.98                | 0.5%                |
	| `MeanRev_Fast`   | -0.01       | -0.1                 | 0.10                | -0.2%               |

	- Metrics
		- IC Mean (information coefficient) (rank correlation (spearman))
		- IC Sharpe (mean ic/ std dev of IC)
		- Autocorrelation
		- **Quantile Spread:** The difference in returns between your Top 10% ranked assets and Bottom 10%. If this is flat, your signal has no discriminatory power.
2. The alphalens style deep-dive:
	- Once a signal passes the Leaderboard filter, you generate a visual "Tear Sheet" for that specific signal. Do not look at an equity curve yet; look at the **discriminatory structure**.
	- **A. Quantile Bucket Returns (The Monotonicity Check)** Group your asset universe into buckets (e.g., Deciles 1–10) based on signal strength. Plot the mean forward return for each bucket.
		- **The Output:** A bar chart.
		- **The Efficiency Test:** It **must** be monotonic. If Bucket 10 > Bucket 9 > ... > Bucket 1, the signal is robust. If the bars are random heights, the signal is noise, even if the backtest makes money.
	- **B. IC Decay (The Horizon Check)** Calculate the IC for different forward horizons (1-day, 5-day, 10-day, 20-day returns).
		- **The Output:** A line graph of IC over time.
		- **The Efficiency Test:** This tells you the **alpha decay**. If the signal dies after 2 days but you trade weekly, you will lose money. This output tells you exactly what frequency to trade.
3. The Dictionary of "Regimes"
	- A single number often hides when a strategy fails. Your output dictionary should slice performance by market regime.
	- You immediately see _conditional_ probability. You might realize, "This isn't a bad signal; it's just a Bull Market only signal."
```python
results = {
    "Overall_IC": 0.03,
    "Bull_Market_IC": 0.05,       # Signal works great when SPY > 200 SMA
    "Bear_Market_IC": -0.02,      # Signal fails in downtrends
    "High_Vol_IC": 0.01           # Signal ignores volatility
}
```
4. Correlation Matrix of Signals
	- If you are researching multiple signals, your output **must** include a correlation matrix of the signals themselves (not the returns).
	- **The Output:** A heatmap showing correlation between `Signal_A` and `Signal_B`.
	- **Efficiency Hack:** If `Signal_A` and `Signal_B` have 0.8 correlation, **delete one.** You are researching the same thing twice. You want a portfolio of uncorrelated signals.
