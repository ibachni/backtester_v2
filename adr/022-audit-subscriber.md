# ADR 022: Audit as a Subscriber

## Status
Accepted

## Context
Currently, the `AuditWriter` is a core dependency injected into almost every component (Strategy, ExecutionSimulator, BarFeed, etc.). Components log their activities by directly calling `audit.emit(...)`.

This approach has several drawbacks:
1.  **Coupling**: Every component depends on `AuditWriter`.
2.  **Synchronous/Blocking**: While `emit` can be made async, direct method calls often lead to blocking I/O or complex threading within the writer to avoid slowing down the simulation loop.
3.  **Inconsistency**: Some data flows through the Bus (Orders, Candles), while other data (logs, debug info) flows directly to Audit.
4.  **Refactoring Friction**: Changing how we store logs requires touching every component.

## Decision
We will invert the dependency. `AuditWriter` will transition from being a service called by components to being a **Subscriber** on the Event Bus.

1.  **Audit as Subscriber**: `AuditWriter` will subscribe to relevant topics on the `Bus` (e.g., `mkt.candles`, `orders.*`, `account.*`).
2.  **LogEvent**: We introduce a generic `LogEvent` structure and a dedicated topic (e.g., `log.event`) for unstructured logging (DEBUG/INFO messages).
3.  **Publishing Logs**: Instead of calling `audit.emit()`, components will publish `LogEvent` messages to the Bus.
4.  **Persistence**: The `AuditWriter` listens to these events and persists them to the appropriate storage (JSONL, CSV, etc.) based on the topic and event type.

## Detailed Design

### 1. LogEvent Structure
Defined in `backtester/types/types.py`:

```python
@dataclass
class LogEvent:
    level: str          # "DEBUG", "INFO", "WARN", "ERROR"
    component: str      # e.g., "strategy", "sim"
    msg: str            # Human readable message
    payload: dict       # Structured context
    sim_time: int | None
    wall_time: str
```

### 2. Topic
A new topic `log.event` will be registered on the Bus.

### 3. AuditWriter Logic
The `AuditWriter` implements the subscriber interface (or simply attaches a callback via `bus.subscribe`).
-   **Input**: `Envelope` from the Bus.
-   **Processing**:
    -   Route `T_FILLS`, `T_ORDERS_*` to `ledger.jsonl`.
    -   Route `T_ACCOUNT_SNAPSHOT` to `metrics.csv`.
    -   Route `log.event` and debug topics to `debug.jsonl`.
    -   Route `T_CANDLES` to `market_data.jsonl` (optional).

## Migration Guide

### Before (Legacy)
```python
# In a component
self.audit.emit(
    event="STRATEGY_SIGNAL",
    component="strategy",
    payload={"signal": 1.5},
    sim_time=self.clock.now()
)
```

### After (New)
```python
# In a component
from backtester.types.types import LogEvent

event = LogEvent(
    level="INFO",
    component="strategy",
    msg="Signal generated",
    payload={"event": "STRATEGY_SIGNAL", "signal": 1.5},
    sim_time=self.clock.now()
)
await self.bus.publish("log.event", self.clock.now(), event)
```

### Refactoring Steps
1.  Ensure `LogEvent` is available in `types.py`.
2.  Ensure `AuditWriter` is subscribed to `log.event` in the engine setup.
3.  Find usages of `audit.emit` using grep/search.
4.  Replace with `bus.publish("log.event", ...)` or specific typed events if applicable.
5.  Remove `audit` dependency from components that only used it for logging.

## Consequences
### Positive
-   **Decoupling**: Components only need the `Bus`.
-   **Unified Flow**: All system state changes and logs are visible on the Bus, enabling easier replay and debugging.
-   **Async I/O**: The Bus handles buffering and delivery, allowing the `AuditWriter` to write to disk in a separate thread/task without blocking the core loop (if configured with appropriate backpressure).

### Negative
-   **Bus Traffic**: High-frequency logging increases bus pressure. We must ensure the Bus and `AuditWriter` can handle the throughput (using `BackpressurePolicy.DROP_NEWEST` for debug logs if necessary).
-   **Boilerplate**: Publishing an event is slightly more verbose than a method call. Helper methods or a wrapper logger can mitigate this.
