# ADR 016: Idempotent Client Order IDs & Reconciliation

Status: Accepted  
Date: 2025-09-23  
Context Version: 1.0

## Context
Network retries, adapter restarts, and transient broker errors can cause duplicate order submissions if identification is opaque. Deterministic replay also needs stable identifiers to align intents with fills.

## Decision
Generate deterministic `client_order_id` pattern: `STRAT-<symbol>-<ts>-<nonce>` where `<ts>` = integer UTC epoch seconds (or logical event seq) and `<nonce>` a short monotonic per-run counter. Broker adapter includes this in submission; retries reuse same ID. Reconciler on startup queries live/paper broker (if allowed) and maps orphan fills or open orders.

## Consequences
+ Prevents duplicate unintended orders on retry.  
+ Enables idempotent reconciliation and crash recovery.  
− Requires namespace discipline across strategies if multi-process emerges.

## Alternatives Considered
*Opaque broker IDs only*: Harder dedupe & replay alignment.  
*UUID v4 random IDs*: Non-deterministic.

## Revisit Criteria
- Multi-process strategies share an account (add process UUID component).  
- Introduction of cross-venue routing.

## Implementation Notes
- Maintain per-run `OrderIdGenerator` object seeded deterministically.  
- Persist last nonce in run manifest for continuity if mid-run snapshot restore.

## Related
Execution Model (015), Event Loop (010), Determinism (001).
