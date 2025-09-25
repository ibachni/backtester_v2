# ADR 009: Modular Monolith over Microservices

Status: Accepted
Date: 2025-09-23
Context Version: 1.0

## Context
Early-stage quantitative backtesting platform with limited concurrency demands. Microservices introduce operational overhead (deployment, version skew, inter-service latency) and complicate determinism (distributed clocks, network retries) without proportional benefit at current scale.

## Decision
Adopt a single-process, single-repo modular monolith. Enforce boundaries via explicit interface packages (`backtester/ports/`) and adapter subpackages. Core domain modules communicate through in-process events (see Event-Driven Core Loop ADR) rather than network RPC.

Interfaces (initial set): `DataFeed`, `Broker`, `Storage`, `Clock`, `Strategy`.

## Consequences
+ Minimal ops overhead; easy step-through debugging; simpler deterministic replay.
+ Shared memory speeds handoff between components.
− Harder horizontal scaling across machines; potential contention if hot loops expand.

## Alternatives Considered
*Early microservices*: Rejected due to premature complexity.
*Hybrid (monolith + sidecar)*: Deferred until a clear performance bottleneck emerges.

## Revisit Criteria
- Need to process multiple independent venues concurrently with CPU saturation.
- Profiling shows contention not solvable with intra-process concurrency primitives.
- Requirement for language heterogeneity.

## Implementation Notes
- Keep adapter dependencies isolated; avoid leaking heavy libs into core.
- Use static type checks to ensure `ports` imports never depend on adapter packages (acyclic).
- Consider enforcing with a lightweight import graph test.

## Related
Referenced by Small Slices (ADR 003) and Interfaces (ADR 004).
