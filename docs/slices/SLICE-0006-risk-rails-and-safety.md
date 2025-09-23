---
id: SLICE-0006
title: Risk Rails & Safety Expansion
status: planned
ticket: tickets/BT-0006.json
created: 2025-09-23
owner: nicolas
depends_on: [SLICE-0003, SLICE-0005]
targets: [backtester/risk/rails.py, backtester/cli/commands/halt.py]
contracts: [RiskEngine, OrderRouter]
ads: [ADR 002, ADR 018]
risk: medium
---

# Risk Rails & Safety Expansion

## Goal
Enforce portfolio and per-order guardrails pre-trade and provide operator kill-switch controls.

## Why
Prevents catastrophic losses or runaway order submission; ensures safe live/paper operation.

## Scope
- Pre-trade rules: allowed symbols, per-order notional cap, max open positions, daily realized loss limit.
- Evaluation order deterministic; first failure halts evaluation and returns reason code.
- Operator commands: `bt halt`, `bt flatten` (close all positions then halt), manual resume flag.
- Metrics: per-rule counters, last block reason, daily loss tracker.
- Config toggles for rules by environment.

## Out of Scope
Leverage/margin, shorting, sector exposure modeling.

## Deliverables
- Risk rails implementation + reason code enum.
- CLI commands for halt/flatten.
- Logging integration for each block event.

## Acceptance Criteria
- Violating orders blocked with stable reason codes & metrics incremented.
- Kill-switch halts new orders ≤1s; flatten closes all then sets halt state.
- Simulated breach scenarios produce expected block metrics in shadow/paper modes.

## Test Plan
- Unit: each rule edge cases; kill-switch timing.
- Integration: scenario run with forced breaches.

## Risks / Mitigations
- Ordering dependency between rules → document evaluation order & keep simple.

## Follow-ups
- Extend to leverage & exposure buckets later.
