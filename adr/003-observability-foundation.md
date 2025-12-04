# ADR 005: Observability Foundation

Status: Accepted
Date: 2025-09-23
Context Version: 1.0

## Context
Strategy evaluation and engine debugging require consistent logging and metrics.

## Decision
1. Introduce a lightweight structured logger (JSON lines) writing to `runs/<run_id>/<file>.log`.
2. Emit: timestamp (UTC), run_id, event_type, payload hash, sequence number.
3. Metrics aggregated in-memory, flushed periodically (configurable) to `runs/<run_id>/metrics.csv` (format may evolve).

## Consequences
+ Enables debugging & analytics.
+ Standard format supports tooling later (dashboards).
− Slight runtime overhead.

## Alternatives Considered
*Ad-hoc print logging*: Not machine-friendly.

## Implementation Notes
- Use monotonic sequence numbers to detect gaps.
- Keep payload lean
