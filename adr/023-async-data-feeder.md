# 023. Async Data Feeder Pipeline

Date: 2025-11-22
Status: Accepted

## Context

The current data pipeline (`CandleSource` -> `ResampleIter` -> `BarFeed`) is implemented using synchronous Python iterators. However, the `BacktestEngine` runs on an `asyncio` event loop.

This architecture presents two significant problems:
1.  **Blocking I/O**: Reading Parquet files (even if fast) blocks the main event loop, preventing other async tasks (like signal listeners or keep-alive heartbeats) from running during data loading.
2.  **Non-Deterministic Logging**: The `Bus.publish` method is `async`. To log from within the synchronous `BarFeed` or `Source`, we currently have to use `asyncio.create_task` (fire-and-forget) or skip logging entirely if no loop is running. This leads to race conditions where logs might appear out of order or be lost if the program terminates before the task completes.

## Decision

We will refactor the entire data pipeline to use **Async Generators** (`async def __aiter__`, `async for`).

This allows us to:
1.  `await` I/O operations (or at least yield control to the loop via `await asyncio.sleep(0)` during CPU-bound sync I/O).
2.  `await self._bus.publish(...)` for logs, ensuring the log is accepted by the bus before processing continues.
3.  Prepare the system for live trading sources (WebSockets), which are inherently asynchronous.

## Technical Specification

### 1. Abstract Source (`backtester/data/source.py`)

Change `CandleSource` to inherit from `AsyncIterable[Candle]`.

```python
class CandleSource(AsyncIterable[Candle]):
    def __aiter__(self) -> AsyncIterator[Candle]:
        raise NotImplementedError
```

### 2. Parquet Source (`backtester/data/source.py`)

Update `ParquetCandleSource` to implement `__aiter__`.
- Since `pyarrow` is synchronous, we must manually yield control to the event loop to prevent starving other tasks.
- **Action**: Insert `await asyncio.sleep(0)` between batch reads or file opens.
- **Action**: Change `_emit_log` to be `async` and `await` the bus publication.

### 3. Resampler (`backtester/data/resampler.py`)

Update `ResampleIter` to handle async sources.
- **Input**: `it: AsyncIterator[Candle]`
- **Method**: `async def __aiter__(self)` and `async def __anext__(self)`.
- **Logic**: Replace `next(self.it)` with `await anext(self.it)`.

### 4. Bar Feed (`backtester/data/feeder.py`)

Refactor `BarFeed` to be an asynchronous orchestrator.
- **Method**: `async def iter_frames(self) -> AsyncIterator[...]`.
- **Heap Management**: The methods `_prime_heap`, `_pull_first`, and `_advance_sub` must become `async` because they need to pull data from the async sources (`await anext(sub.it)`).
- **Logging**: Update `_emit_log` to `async` and `await` calls.

### 5. Engine Integration (`backtester/core/backtest_engine.py`)

Update the main loop in `feed_candles`:

```python
# Old
for t_close, frame in self._feed.iter_frames():
    ...

# New
async for t_close, frame in self._feed.iter_frames():
    ...
```

## Consequences

### Positive
- **Non-blocking**: The event loop remains responsive.
- **Reliable Logging**: Logs are guaranteed to be published in order.
- **Future-proof**: Compatible with async WebSocket clients for live trading.

### Negative
- **Complexity**: Introduces `async/await` syntax throughout the data stack.
- **Refactoring**: Requires touching core data classes (`Source`, `Resampler`, `Feeder`).
