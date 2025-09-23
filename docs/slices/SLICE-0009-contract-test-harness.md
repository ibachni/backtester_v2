---
id: SLICE-0009
title: Contract Test Harness
status: planned
ticket: tickets/BT-0009.json
created: 2025-09-23
owner: nicolas
depends_on: [SLICE-0000, SLICE-0008]
targets: [tests/contract, backtester/ports]
contracts: [MarketDataProvider, OrderRouter, RiskEngine, SnapshotStore, PortfolioStore, Clock, EventBus]
ads: [ADR 004, ADR 010, ADR 016, ADR 018]
risk: medium
---

# Contract Test Harness

## Goal
Automated gating tests ensuring every adapter/store honors its published port contract before enabling real trading.

## Why
Prevents integration drift and enforces behavioral parity across simulation, paper, and live adapters.

## Scope
- MarketDataProvider tests: chronological UTC history, explicit gap surfacing, live bar monotonicity & lag metric.
- OrderRouter tests: idempotent submit, cancel semantics, retryable vs terminal error classification, aggregated partial fills.
- RiskEngine tests: rule triggers & reason codes, blocking semantics.
- Snapshot/Portfolio store tests: save/restore byte-equivalent state.
- Clock/EventBus tests: deterministic publish order, sequence monotonicity.
- CLI entry: `bt contract-tests [--adapter paper|binance-sandbox]`.
- Summary report (JUnit/JSON) stored as CI artifact.

## Out of Scope
Performance/stress testing, fuzz of third-party APIs.

## Deliverables
- Shared contract test harness utilities.
- Adapter-specific fixture data / mock responses.
- CI job invoking contract suite.

## Acceptance Criteria
- All targeted ports pass contract suite locally & in CI.
- Failing behavior surfaces clear diff / reason code.

## Test Plan
- Unit: isolated harness utilities.
- Integration: full contract suite run against paper adapter.

## Risks / Mitigations
- Test flakiness (timing) → use deterministic clocks & fixture data.

## Follow-ups
- Add performance budget checks per adapter later.
