# ADR 014: Storage Format – Parquet History, Append-Only Logs, Snapshots

Status: Accepted
Date: 2025-09-23
Context Version: 1.0

## Context
Historical bar data benefits from columnar compression & predicate pushdown. Operational events (orders, fills, strategy decisions) require durable append semantics and replay support. Full DB solutions (DuckDB, SQLite) add dependencies and migration concerns prematurely.

## Decision
- Historical bars stored as Parquet under `data/history/<symbol>/<resolution>.parquet`.
- Operational event log: JSONL append-only `runs/<run_id>/events.log`.
- Periodic state snapshots (positions, equity curve) as compact JSON in `runs/<run_id>/snapshots/seq-<n>.json`.
- Crash recovery: load last snapshot then replay subsequent events from log.

## Consequences
+ Portable & inspectable artifacts; minimal tooling required.
+ Deterministic replay path well-defined.
− No concurrent writers; manual compaction if logs grow large.

## Alternatives Considered
*DuckDB for everything*: Strong candidate later for ad-hoc analytics.
*SQLite*: Row-oriented; less optimal for column scans of bar data.

## Revisit Criteria
- Need multi-run aggregated analytics requiring SQL.
- Performance profiling shows Parquet IO bottleneck.

## Implementation Notes
- Use stable schema version field in snapshots.
- Provide a manifest entry listing Parquet files & hashes for reproducibility.

## Related
Observability (005), Determinism (001), Modular Monolith (009).
