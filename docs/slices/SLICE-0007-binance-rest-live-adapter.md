---
id: SLICE-0007
title: Binance REST Live Adapter (Tiny Orders)
status: planned
ticket: tickets/BT-0007.json
created: 2025-09-23
owner: nicolas
depends_on: [SLICE-0006]
targets: [backtester/adapters/binance, backtester/ports/order_router.py]
contracts: [OrderRouter, MarketDataProvider, RiskEngine]
ads: [ADR 012, ADR 013, ADR 015, ADR 016]
risk: high
---

# Binance REST Live Adapter (Tiny Orders)

## Goal
Submit tiny real orders via Binance REST using the same state machine and risk path as paper, with robust idempotency & reconciliation.

## Why
Validates real venue integration with minimal financial exposure before scaling.

## Scope
- Auth/signing via `SecretsProvider`; clock skew guard.
- Order placement with venue filter validation (LOT_SIZE, MIN_NOTIONAL, PRICE_FILTER) and early rejection on violation.
- Idempotent `client_order_id`; retryable vs terminal error classification; exponential backoff.
- Reconciliation: poll open orders & account snapshot on startup + periodic sync; aggregate venue partial fills.
- Safety: tiny order sizing flag; auto-halt after repeated failures.

## Out of Scope
WebSocket streaming, derivatives, advanced order types beyond market/limit GTC.

## Deliverables
- Binance adapter modules (data + orders as needed for MVP).
- Enhanced contract tests for OrderRouter against sandbox.
- CLI: `bt live --adapter binance --tiny --config cfg.yml`.

## Acceptance Criteria
- Orders fill end-to-end with correct state transitions & no duplicates after crash.
- Rate limit handling validated (throttled path test) without hot loops.
- p95 order RTT ≤ 500 ms, p99 ≤ 900 ms (light load).

## Test Plan
- Unit: filter mapping, error classification.
- Integration: sandbox order lifecycle, restart reconciliation.

## Risks / Mitigations
- Rate limit surprises → respect headers & jittered backoff.

## Follow-ups
- Add WebSocket feed when sub-minute latency required.
