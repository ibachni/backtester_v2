---
id: SLICE-0004
title: Paper Trading Parity
status: planned
ticket: tickets/BT-0004.json
created: 2025-09-23
owner: nicolas
depends_on: [SLICE-0003]
targets: [backtester/adapters/paper, backtester/ports/market_data.py, backtester/ports/order_router.py]
contracts: [MarketDataProvider, OrderRouter, PortfolioStore, RiskEngine]
ads: [ADR 012, ADR 013, ADR 015, ADR 016, ADR 017, ADR 018]
risk: medium
---

# Paper Trading Parity

## Goal
Run live 1-minute bars through a paper broker path identical to simulation, validating parity before integrating real venue adapters.

## Why
Ensures strategy & risk logic behave identically in a live-timed context without financial risk.

## Scope
- REST polling live data provider with backoff & lag metric (`bars_lag`).
- Paper order router implementing state machine (new/submitted/filled/canceled/rejected) with idempotent `client_order_id`.
- Fill policy mirrors simulation (market @ close ± slippage; limit on intra-bar cross).
- Persistent portfolio & open orders; restart restores state.
- Global halt/flatten; dry-run mode for auditing.
- Telemetry metrics: `bars_lag`, `strategy_tick_latency`, `orders_submitted`, `orders_blocked_risk`.

## Out of Scope
Real exchange connectivity, advanced alerting dashboards.

## Deliverables
- Paper data feed + router adapters.
- Persistence layer for portfolio state.
- CLI: `bt paper --config ...`.

## Acceptance Criteria
- Median paper vs replayed sim P&L diff ≤ 5 bps on same-day data.
- No missed bars over 7-day soak (gaps logged if any).
- Idempotent order IDs survive restart without duplication.
- Latency SLO: p95 decision ≤ 500 ms, p99 ≤ 900 ms.

## Test Plan
- Unit: polling gap detection, order state transitions.
- Integration: parity test comparing sim vs paper run.

## Risks / Mitigations
- REST jitter → monitor `bars_lag`; configurable poll interval guard.

## Follow-ups
- Introduce WebSocket adapter (future slice when sub-minute required).
