# ADR 018: Risk Rails – Pre-Trade Checks & Global Halt

Status: Accepted
Date: 2025-09-23
Context Version: 1.0

## Context
Unbounded order submission or runaway loss conditions can degrade account capital and distort backtest comparability. Early enforcement prevents invalid states rather than relying on post-trade remediation.

## Decision
Implement pre-trade risk checks applied to each `OrderIntent`: notional cap per order, daily realized loss limit, max gross exposure, and position concentration limit. A global `halt/flatten` mechanism cancels pending intents and issues offsetting orders for open positions (subject to safety mode). Exit orders bypass certain caps to ensure positions can be reduced.

## Consequences
+ Limits catastrophic errors; enforces consistent assumptions across runs.
+ Facilitates clean fail-fast instead of progressive corruption.
− Requires careful ordering to avoid blocking protective exits.

## Alternatives Considered
*Post-trade monitoring only*: Slower detection & larger blast radius.
*External risk service*: Overkill at current scale.

## Revisit Criteria
- Introduction of leverage/margin or derivatives.
- Multi-strategy portfolio with shared capital pool.

## Implementation Notes
- Maintain a `RiskState` updated on each Fill & snapshot.
- Pre-trade check returns either `Allowed`, `Modified`, or `Rejected` with reason.
- Emit metric & structured log on every rejection/halt.

## Related
Safety (002), Single Venue Scope (013), Determinism (001), Event Loop (010).
