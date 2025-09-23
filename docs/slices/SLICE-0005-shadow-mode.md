---
id: SLICE-0005
title: Shadow Mode Runtime
status: planned
ticket: tickets/BT-0005.json
created: 2025-09-23
owner: nicolas
depends_on: [SLICE-0004]
targets: [backtester/adapters/null_router.py, backtester/core/shadow_runner.py]
contracts: [OrderRouter, MarketDataProvider, RiskEngine]
ads: [ADR 010, ADR 012, ADR 018, ADR 020]
risk: medium
---

# Shadow Mode Runtime

## Goal
Soak-test the live pipeline safely by computing signals and risk-evaluating orders without submitting them.

## Why
Validates runtime stability, latency SLOs, and risk logic under real-time conditions before touching a live venue.

## Scope
- `NullOrderRouter` capturing proposed orders & blocked reasons.
- Full live pipeline reuse: data → strategy → risk → (null) router → logs/metrics/snapshots.
- Metrics: `bars_lag`, `strategy_tick_latency`, `order_proposed_rate`, `risk_block_rate`, `error_rate`.
- Alerts (log-level): bar gap, error rate spike, daily loss halt.
- Compressed daily logs & manifest rotation.

## Out of Scope
Exchange auth, order placement, reconciliation logic.

## Deliverables
- Null router adapter & shadow runner harness.
- Metrics emission + log enrichment.
- CLI: `bt shadow --config ...`.

## Acceptance Criteria
- 7-day continuous run w/ zero missed bars (or logged within tolerance) and stable memory footprint.
- Latency SLOs: p95 ≤ 500 ms, p99 ≤ 900 ms decision latency.
- Non-zero proposed and blocked order metrics.
- Crash/restart resumes consistent state from snapshot.

## Test Plan
- Unit: null router capture; alert condition triggers.
- Integration: simulated long-running loop (time-compressed) verifying metrics.

## Risks / Mitigations
- Log volume growth → compression & rotation daily.

## Follow-ups
- Add anomaly detection on latency distribution.
