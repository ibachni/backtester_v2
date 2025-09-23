# ADR 015: Execution Model Without Partial Fills (V1)

Status: Accepted  
Date: 2025-09-23  
Context Version: 1.0

## Context
Modeling partial fills introduces a more complex order state machine (PARTIALLY_FILLED, CANCEL_PENDING, etc.). For early parity and simplified accounting, treating fills as atomic reduces edge cases while still enabling realistic slippage.

## Decision
Represent each order lifecycle as either FILLED (one aggregated fill) or CANCELED/REJECTED. If the live venue returns intermediate partials, the adapter aggregates until terminal state, then emits a single Fill event.

## Consequences
+ Simpler strategy logic & engine bookkeeping.  
+ Easier reconciliation (idempotent client order IDs).  
− Loses insight into intra-order execution quality and queue dynamics.

## Alternatives Considered
*Full partial fill lifecycle now*: Deferred to reduce initial complexity.

## Revisit Criteria
- Strategies depending on execution pacing or TWAP/VWAP requiring partial state.  
- Thin liquidity trading where partials materially affect PnL modeling.

## Implementation Notes
- Fill event contains cumulative executed quantity = requested quantity.  
- Slippage model operates on final fill not incremental pieces.

## Related
Idempotent Order IDs (016 future), Slippage Model (017 future), Risk Rails (011 concept).
