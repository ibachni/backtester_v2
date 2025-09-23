# ADR 006: Test Pyramid with Property-Based Emphasis

Status: Accepted  
Date: 2025-09-23  
Context Version: 1.0

## Context
Accuracy of backtesting hinges on invariant preservation (e.g., cash + position value changes match fills). Traditional example-based tests miss corner cases. Property-based tests can catch subtle accounting or ordering defects.

## Decision
1. Use `pytest` for all tests; property tests via `hypothesis`.  
2. Emphasize unit + property tests first; integration & e2e only for cross-component behavior.  
3. Each new financial invariant (e.g., no negative shares unless shorting enabled) encoded as a property test early.  
4. E2E parity tests limited to a small curated set to maintain speed.  
5. Failing property tests block merges; flakiness treated as determinism violation.

## Consequences
+ Higher defect detection early.  
+ Encourages explicit invariants and clearer domain model.  
− Requires discipline crafting generators / strategies.

## Alternatives Considered
*Rely solely on example tests*: Higher latent defect risk.

## Implementation Notes
- Shared strategies/generators placed under `tests/property_strategies/`.  
- Seed stabilization for reproducible counterexamples (persist Hypothesis seeds).  
- Consider future caching of failing seeds.

## Related
Principle 6 in `AGENTS.md`.
