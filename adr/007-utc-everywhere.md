# ADR 007: UTC Everywhere & Strict Time Ordering

Status: Accepted  
Date: 2025-09-23  
Context Version: 1.0

## Context
Inconsistent timezone handling introduces subtle look-ahead or ordering bugs. Financial data may include local exchange times; normalization needed for cross-asset strategies.

## Decision
1. All internal timestamps stored/compared as timezone-aware UTC `datetime`.  
2. Data adapters normalize incoming bars/trades to UTC on ingestion.  
3. Event queue enforces strictly increasing (timestamp, sequence) ordering; ties resolved by insertion order or deterministic priority mapping.  
4. Any detected retrograde timestamp raises an exception (fail-fast).  
5. User strategies receive already-normalized data; they must not handle timezone conversions themselves.

## Consequences
+ Prevents subtle ordering and daylight-saving anomalies.  
+ Simplifies multi-market aggregation logic.  
− Slight cost to convert on ingest.

## Alternatives Considered
*Store naive datetimes*: Risky & ambiguous.  
*Per-exchange timezone handling downstream*: Spreads complexity.

## Implementation Notes
- Provide `normalize_to_utc(dt, tz_source)` helper.  
- Add invariant test feeding out-of-order bars expecting failure.

## Related
Principle 7 in `AGENTS.md`.

## Clarifications (2025-09-23)
REST polling decision (future ADR) emphasizes consistent timestamp normalization on each poll cycle; single venue scope (future ADR) reduces complexity of cross-venue calendar alignment in V1. When multi-venue arrives, this ADR still holds—adapters must provide already-normalized UTC bars and trades before entering the event queue.
