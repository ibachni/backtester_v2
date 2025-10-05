# ADR 010: Event-Driven Core Loop (In-Process Bus)

Status: Accepted
Date: 2025-09-23
Context Version: 1.0

## Context
A linear "for each bar do everything" loop tightly couples sequencing and makes replay / instrumentation harder. External message brokers (Kafka/Rabbit) add infra overhead and timing nondeterminism. An in-process event bus offers decoupling while preserving determinism and simplicity.

## Decision
Adopt an in-process typed event bus. Canonical event progression example:
`BarReady → IndicatorsComputed → StrategySignal → OrderIntent → OrderSubmitted → Fill → MetricsFlush`.

Each event is a lightweight dataclass containing: `event_type`, `ts_utc`, `payload`, `seq`.

## Consequences
+ Clear separation of responsibilities; easier targeted testing of each stage.
+ Natural insertion points for logging, metrics, and replay.
− Slight indirection vs a direct function chain.

## Alternatives Considered
*Linear loop*: Less flexible for instrumentation.
*External queue*: Overkill for local deterministic runs.

## Revisit Criteria
- Cross-process scaling needed (multiple strategies sharing a feed).
- Need to buffer across slow consumers.
- Distributed ingestion from multiple venues concurrently.

## Implementation Notes
- Provide an `EventBus` with `publish(event)` and `subscribe(event_type, handler)` APIs.
- Maintain a monotonically increasing sequence number for ordering invariants.
- Replay utility rehydrates events from persisted log into the bus.

## Related
Interfaces ADR (004), Determinism (001), Modular Monolith (009).
