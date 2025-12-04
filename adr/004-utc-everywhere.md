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
4. Any detected missaligned timestamp are recorded and not ingested (fail-fast).
5. User strategies receive already-normalized data; they must not handle timezone conversions themselves.

## Consequences
+ Prevents subtle ordering and daylight-saving anomalies.
+ Simplifies multi-market aggregation logic.
− Slight cost to convert on ingest.

## Alternatives Considered
-

## Implementation Notes
-
