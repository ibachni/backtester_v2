# ADR 010: Event-Driven Core Loop (In-Process Bus)

Status: Accepted
Date: 2025-09-23
Context Version: 1.0

## Context
An in-process event bus offers decoupling while preserving determinism and simplicity.

## Decision
Adopt an in-process typed event bus. Canonical event progression example:
`BarReady → IndicatorsComputed → StrategySignal → OrderIntent → OrderSubmitted → Fill → MetricsFlush`.

Each event is a lightweight dataclass containing: `event_type`, `ts_utc`, `payload`, `seq`.

## Consequences
+ Clear separation of responsibilities; easier targeted testing of each stage.
+ Natural insertion points for logging, metrics, and replay.
- Difficult to debug and maintaining proper sequences.

## Alternatives Considered
*Non-in-memory queue*: Drags performance.

## Revisit Criteria
- performance constraints

## Implementation Notes
- Provide an `EventBus` with `publish(event)` and `subscribe(event_type, handler)` APIs.
- Maintain a monotonically increasing sequence number for ordering invariants.
