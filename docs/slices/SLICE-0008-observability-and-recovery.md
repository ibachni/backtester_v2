---
id: SLICE-0008
title: Observability & Recovery
status: planned
ticket: tickets/BT-0008.json
created: 2025-09-23
owner: nicolas
depends_on: [SLICE-0000, SLICE-0007]
targets: [backtester/core/logging.py, backtester/core/metrics.py, backtester/core/replay.py]
contracts: [Logger/Telemetry, SnapshotStore, RunManifestStore]
ads: [ADR 005, ADR 014, ADR 018, ADR 020]
risk: medium
---

# Observability & Recovery

## Goal
Provide structured logs, minimal metrics, snapshots, and crash replay tooling with basic alert conditions.

## Why
Ensures debuggability, post-mortem analysis, and confidence in resilience before scaling complexity.

## Scope
- JSONL structured logging (fields: run_id, ts_utc, component, level, msg, context fields).
- Metrics aggregator emitting: `bars_lag`, `strategy_tick_latency`, `order_rtt`, `error_rate`, `risk_block_rate`, `pnl_intraday`.
- Snapshots on position change & periodic cadence (open orders, cash, positions).
- WAL (append-only decision/order log) enabling deterministic replay.
- Replay command: `bt replay --from snapshot --wal path` validates idempotency & state match.
- Alerts (logged): bar gap, high order error rate, daily loss halt.

## Out of Scope
External monitoring stacks (Prometheus, Grafana), rich dashboards.

## Deliverables
- Logging module + metrics collector.
- Snapshot writer/loader & replay utility.
- CLI replay command.

## Acceptance Criteria
- Simulated crash mid-submit recovers with no duplicate orders.
- All metrics emitted; alert conditions log entries when induced.
- End-of-run summary prints basic performance & location of artifacts.

## Test Plan
- Unit: snapshot round-trip, replay idempotency, metric computation.
- Integration: induced crash & recovery scenario.

## Risks / Mitigations
- Log bloat → rotation & compression.

## Follow-ups
- Add run-level summary HTML report later.
