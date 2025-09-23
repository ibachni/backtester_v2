# ADR 005: Observability Foundation

Status: Accepted  
Date: 2025-09-23  
Context Version: 1.0

## Context
Strategy evaluation and engine debugging require consistent logging and metrics. Without structured observability primitives, post-run analysis becomes ad-hoc and brittle.

## Decision
1. Introduce a lightweight structured logger (JSON lines) writing to `runs/<run_id>/events.log`.  
2. Emit: timestamp (UTC), run_id, event_type, payload hash, sequence number.  
3. Metrics aggregated in-memory, flushed periodically (configurable) to `runs/<run_id>/metrics.parquet` (format may evolve).  
4. Each run has a manifest file `runs/<run_id>/manifest.json` containing config digest, git SHA, seeds, data sources.  
5. Provide a replay utility to reconstruct event stream deterministically for debugging.

## Consequences
+ Enables time-travel debugging & post-hoc analytics.  
+ Standard format supports tooling later (dashboards).  
− Slight runtime overhead for serialization.

## Alternatives Considered
*Ad-hoc print logging*: Not machine-friendly.  
*Heavy observability stack early (OpenTelemetry)*: Overkill for initial slices.

## Implementation Notes
- Use monotonic sequence numbers to detect gaps.  
- Keep payload lean; large blobs (raw bars) not duplicated if already sourced.

## Related
Principle 5 in `AGENTS.md`.

## Clarifications (2025-09-23)
The expanded Observability ADR (later numbering) constrains metric surface to a minimal counter/timer set (`order_rtt`, `bars_lag`, `error_rate`, `pnl_intraday`) plus structured logs and alerts. This foundational ADR remains concerned with *formats & run manifest*; the later ADR governs *which* metrics and alert conditions are required.
