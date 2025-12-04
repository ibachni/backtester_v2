# ADR 021: Micro-Batch (Tick-and-Drain) Architecture

## Status
Accepted

## Context
In the previous version, an event-driven architecture, the `BacktestEngine` and `Bus` operated in a "free-running" mode. The Engine would push market data as fast as possible, and subscribers (Strategies, Risk, Execution) would process events concurrently.

This introduced **non-deterministic race conditions**:
1.  **Time Leakage:** The Engine could advance the clock to $T+1$ while a Strategy was still calculating signals for $T$.
2.  **Order Shuffle:** If multiple strategies emitted orders at $T$, the order in which they reached the Execution Simulator depended on asyncio scheduling jitter, not business logic.
3.  **Impossible Debugging:** Replaying a crash was impossible because the exact interleaving of tasks varied between runs.

We need a guarantee that **Time T is fully resolved**—meaning all resulting signals, orders, fills, and accounting updates are finished—before **Time T+1** begins.

## Decision
We will implement a **Micro-Batch (Tick-and-Drain)** architecture.

### 1. The "Tick-and-Drain" Loop
The `BacktestEngine` will no longer be a simple producer. It will act as a synchronous orchestrator of an asynchronous system:

1.  **Tick (Publish):** The Engine sets the clock to $T$ and publishes `MARKET_DATA`.
2.  **Drain (Barrier):** The Engine immediately calls `await bus.wait_until_idle()`.
3.  **Advance:** Only when the bus is confirmed idle (queue depth 0 AND all tasks marked done) does the Engine proceed to $T+1$.

### 2. The "Consume" Pattern (The Lock)
For the Barrier to work, the Bus must know when a subscriber is *truly* done with a message.
- **Old Way:** `msg = await queue.get()` followed immediately by `queue.task_done()`. This is incorrect because the task is marked "done" before the Strategy has actually processed it or emitted downstream orders.
- **New Way:** We introduce a `consume()` context manager.
    ```python
    async with sub.consume() as msg:
        # Processing...
        # Emitting orders...
    # task_done() is called HERE, automatically
    ```
This holds the "processing lock" on the Bus. The Engine's `wait_until_idle()` will block as long as *any* subscriber is inside a `consume()` block.

### 3. API Changes

#### Deprecated / Discouraged Methods
The following methods are **unsafe** for deterministic backtesting because they encourage "fire-and-forget" consumption or eager acknowledgement:
- `__aiter__` / `async for msg in sub`: **DANGEROUS**. It acknowledges the message before the loop body executes.
- `__anext__`: **DANGEROUS**. Same reason.
- `get()`: **Manual**. Requires the user to manually call `task_done()`, which is error-prone.
- `poll()`: **Manual**. Same reason.

#### New Standard Method
- `Subscription.consume() -> AsyncContextManager[Envelope]`: The **ONLY** approved way to consume events in the backtest loop.

## Detailed Flow

1.  **Engine** publishes `Candle(T=100)`. Bus Queue Size: 1.
2.  **Engine** awaits `bus.wait_until_idle()`. Blocks.
3.  **Strategy** calls `async with sub.consume()`.
    - Pops `Candle`. Queue Size: 0.
    - `unfinished_tasks`: 1 (The Candle is "in flight").
4.  **Strategy** logic runs. Decides to buy.
5.  **Strategy** publishes `OrderIntent`.
    - Order Queue Size: 1.
    - `unfinished_tasks`: 2 (Candle + Order).
6.  **Strategy** exits `consume()` block.
    - Candle `task_done()`. `unfinished_tasks`: 1.
7.  **Engine** wakes up? **NO.** `unfinished_tasks` is 1 (The Order).
8.  **OrderRouter** picks up `OrderIntent`.
    - Pops Order. Queue Size: 0.
    - `unfinished_tasks`: 1.
9.  **OrderRouter** processes. Exits `consume()`.
    - Order `task_done()`. `unfinished_tasks`: 0.
10. **Engine** wakes up. Advances to $T=101$.

## Consequences

### Positive
+   **Determinism:** Two runs with the same seed and data will produce bit-identical logs and artifacts.
+   **Causality:** It is physically impossible to process an event from $T+1$ before $T$ is finished.
+   **Observability:** If the system hangs, we know exactly which message is "stuck" (the one currently in a `consume` block).

### Negative
-   **Throughput:** We lose the ability to pipeline $T+1$ preparation while $T$ is finalizing. The speed is limited by the critical path of the slowest tick.
-   **Boilerplate:** Strategies must use the `async with` syntax.
-   **Deadlocks:** If a subscriber waits for a future event inside a `consume` block, the system will deadlock (because the Engine is waiting for the subscriber to finish). **Rule: Never await a future event inside a consume block.**
