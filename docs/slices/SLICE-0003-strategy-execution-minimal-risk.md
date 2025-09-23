---
id: SLICE-0003
title: Strategy & Execution (Slippage + Minimal Risk)
status: planned
ticket: tickets/BT-0003.json
created: 2025-09-23
owner: nicolas
depends_on: [SLICE-0002]
targets: [backtester/strategy/base.py, backtester/sim/slippage.py, backtester/risk/minimal.py]
contracts: [Strategy, OrderRouter, RiskEngine, PortfolioStore]
ads: [ADR 001, ADR 015, ADR 017, ADR 018]
risk: medium
---

# Strategy & Execution (Slippage + Minimal Risk)

## Goal
Expose a stable strategy API with deterministic execution, basic costs, and minimal pre-trade risk rules reused later in paper/live modes.

## Why
Allows real strategy iteration while keeping execution semantics aligned across environments.

## Scope
- Strategy API: `on_start(ctx)`, `on_bar(ctx, symbol, bar) -> list[OrderIntent]`, `on_fill(ctx, fill)`, `on_stop(ctx)`.
- Context object exposing `state` (positions, cash), `now_utc`, submission helper, config access.
- Slippage model: fixed spread or simple impact placeholder; fees/commissions cost model.
- Risk rules: allowed symbols, per-order notional cap, max open positions, daily loss limit (hard stop).
- Aggregated fills (no partial lifecycle yet).
- Structured logs for blocked orders including reason codes.

## Out of Scope
Advanced exposure metrics, leverage, borrow/short logic.

## Deliverables
- Base strategy class + example strategy (buy-and-hold or moving average cross placeholder).
- Slippage & cost model modules.
- Minimal risk engine implementation.

## Acceptance Criteria
- Deterministic output (orders & P&L) across repeated runs with same inputs.
- Risk rule violations block orders and emit reason-coded log entries.
- Config toggles costs on/off; P&L reflects fees when enabled.

## Test Plan
- Unit: limit cross logic, slippage applied, each risk rule.
- Integration: example strategy full run deterministic.

## Risks / Mitigations
- API churn → keep method set minimal; extend additively.

## Follow-ups
- Expand cost model to POV-based slippage (later slice ties to ADR 017).
