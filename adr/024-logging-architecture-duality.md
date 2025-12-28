# ADR 024: Logging Architecture – Dual-Layer Approach

## Status
Accepted

## Date
2025-01-06

## Context
The backtester system requires logging at two distinct levels:

1. **Infrastructure logging**: Low-level operational events (connection state, reconnection attempts, health monitoring, performance metrics) that are essential for debugging and operations.

2. **Domain event logging**: Business-significant events (trades, fills, orders, signals, account snapshots) that form the audit trail and enable strategy analysis.

The question arose during live data module implementation: should we use a single logging mechanism (AuditWriter via Bus) for all events, or maintain separation between infrastructure and domain concerns?

## Decision
We adopt a **dual-layer logging architecture**:

### Layer 1: Standard Python Logger (Infrastructure)
Used for:
- WebSocket connection lifecycle events
- Reconnection attempts with exponential backoff
- Heartbeat ping/pong operations
- Thread/task lifecycle
- Performance timing
- Verbose debug traces

**Characteristics:**
- Fire-and-forget semantics (no await required)
- Low overhead for high-frequency events
- Configurable via standard logging config
- Output to console/file as configured
- Not persisted to audit trail

### Layer 2: Bus-Based LogEvent (Domain Events)
Used for:
- Trading signals generated
- Order placement/cancellation
- Fill execution
- Account state changes
- Health status changes (degraded/unhealthy transitions)
- Critical connection failures affecting trading

**Characteristics:**
- Published to `T_LOG` topic via Bus
- Captured by AuditWriter subscriber
- Persisted to `debug.jsonl`
- Part of reproducible audit trail
- Enables downstream analysis/alerting

### Bridge: Critical Infrastructure Events
Some infrastructure events warrant persistence for post-mortem analysis. These are **bridged** to the Bus via a helper method:

```python
async def _emit_log(self, level: str, msg: str, payload: dict | None = None) -> None:
    log_event = LogEvent(
        level=level,
        component="ComponentName",
        msg=msg,
        payload=payload or {},
        sim_time=self._clock.now()
    )
    await self._bus.publish(topic=T_LOG, ts_utc=self._clock.now(), payload=log_event)
```

**Events to bridge:**
- Connection established/lost (affects data availability)
- Feed health transitions (HEALTHY → DEGRADED → UNHEALTHY)
- Manager start/stop lifecycle
- Unrecoverable errors

## Consequences

### Positive
- **Separation of concerns**: Infrastructure noise doesn't pollute domain audit
- **Performance**: High-frequency infrastructure logging doesn't incur Bus overhead
- **Flexibility**: Each layer configurable independently
- **Clarity**: Clear distinction between "operational" and "business" events
- **Auditability**: Critical events still reach audit trail via bridging

### Negative
- **Two mechanisms**: Developers must choose appropriate layer
- **Potential duplication**: Critical events logged both ways (acceptable for important events)

## Alternatives Considered

### 1. All Events via Bus
Rejected because:
- High-frequency ping/pong events would flood the Bus
- Audit trail becomes noisy with infrastructure details
- Performance overhead for non-critical events
- Not all infrastructure events need persistence

### 2. All Events via Logger
Rejected because:
- Loses integration with audit trail
- Harder to analyze domain events programmatically
- No downstream subscriber capability (alerting, metrics)

### 3. Single Configurable Logger with Multiple Handlers
Considered but adds complexity:
- Would require custom handler routing logic
- Still need async Bus integration for domain events
- Blurs the clear infrastructure/domain boundary

## Implementation Guidelines

### When to use standard logger:
```python
self._logger.debug("Ping sent, expecting pong within %d ms", timeout)
self._logger.info("Connection established to %s", url)
self._logger.warning("Reconnection attempt %d/%d", attempt, max_retries)
```

### When to bridge to Bus:
```python
await self._emit_log("INFO", "Feed connected", {"exchange": "binance", "stream": "btcusdt"})
await self._emit_log("WARNING", "Feed degraded", {"reason": "stale_data", "staleness_sec": 15})
await self._emit_log("ERROR", "Feed unhealthy", {"reason": "connection_lost"})
```

### Decision criteria:
| Criterion | Logger | Bridge to Bus |
|-----------|--------|---------------|
| Affects trading decisions | No | Yes |
| Needed for post-mortem | No | Yes |
| High frequency (>1/sec) | Yes | No |
| Part of audit trail | No | Yes |
| Operational debugging only | Yes | No |

## Related ADRs
- ADR 003: Observability Foundation
- ADR 006: Event-Driven Core Loop
- ADR 022: Audit as a Subscriber
