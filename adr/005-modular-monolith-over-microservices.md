# ADR 009: Modular Monolith over Microservices

Status: Accepted
Date: 2025-09-23
Context Version: 1.1

## Context
Early-stage quantitative backtesting platform with limited concurrency demands. Microservices introduce operational overhead (deployment, version skew, inter-service latency) and complicate determinism (distributed clocks, network retries) without proportional benefit at current scale.

## Decision
Adopt a single-process, single-repo modular monolith. Enforce boundaries via explicit module separation (`core`, `data`, `sim`, `strategy`, `audit`). Core domain modules communicate through in-process events (see Event-Driven Core Loop ADR) rather than network RPC.

Key Components: `BarFeed`, `ExecutionSimulator`, `AuditWriter`, `Clock`, `Strategy`.

## Consequences
+ Minimal ops overhead; easy step-through debugging; simpler deterministic replay.
+ Shared memory speeds handoff between components.
− Harder horizontal scaling across machines; potential contention if hot loops expand.

## Alternatives Considered
-

## Revisit Criteria
- Performance demands increase

## Implementation Notes
- Keep module dependencies isolated; avoid leaking implementation details across boundaries.
- Use static type checks to ensure acyclic dependencies.
