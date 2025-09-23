# ADR 004: Interfaces Before Implementation

Status: Accepted  
Date: 2025-09-23  
Context Version: 1.0

## Context
Backtesting systems frequently suffer from tight coupling when concrete classes are built before stable boundaries exist. Establishing interfaces early preserves flexibility for alternative data feeds, execution models, and strategy paradigms.

## Decision
1. Define abstract protocols (Python `Protocol` or ABC) for core roles: `MarketDataFeed`, `ExecutionSimulator/Broker`, `Strategy`, `MetricsSink`.  
2. Commit interface modules under `backtester/ports/` prior to implementing adapters.  
3. Breaking changes to published interfaces require an ADR + migration notes section.  
4. Implementations live under `backtester.adapters` or domain subpackages, never inside `ports/`.  
5. Tests for adapters import only interfaces, not concrete siblings (enforces decoupling).

## Consequences
+ Enhanced swap-ability / mocking.  
+ Encourages contract-driven development and early review of boundaries.  
− Slight upfront design effort before shipping behavior.

## Alternatives Considered
*Code first then refactor to interfaces*: Risk of leaky abstractions & retrofitting cost.

## Implementation Notes
- Start with minimal surface (only methods required by first strategy slice).  
- Expand interfaces via additive methods with default no-op where sensible.

## Related
Principle 4 in `AGENTS.md`.

## Clarifications (2025-09-23)
The Event-Driven Core Loop ADR formalizes event sequencing; interface definitions should avoid leaking queue mechanics (no direct dependency on an event bus type—use callbacks or method returns that generate events). Idempotent client order ID policy (future ADR) implies Broker/Execution interfaces MUST surface `client_order_id` explicitly for reconciliation.
