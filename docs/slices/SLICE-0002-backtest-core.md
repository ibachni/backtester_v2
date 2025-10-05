---
id: SLICE-0002
title: Backtest Core (Single Symbol)
status: planned
ticket: tickets/BT-0002.json
created: 2025-09-23
owner: nicolas
depends_on: [SLICE-0000, SLICE-0001]
targets: [backtester/core/engine.py, backtester/ports/market_data.py, backtester/sim/matcher.py]
contracts: [Clock, EventBus, MarketDataProvider, OrderRouter, PortfolioStore, SnapshotStore]
ads: [ADR 001, ADR 010, ADR 011, ADR 014]
risk: medium
---

# Backtest Core (Single Symbol)

## Goal
Deterministic 1-minute bar backtest pipeline for one symbol with accurate accounting and auditable artifacts.

## Why
Establishes the foundational execution + accounting loop onto which strategy, risk, and live features layer.

## Scope
- Simulated UTC clock stepping through Parquet bar dataset.
- Parquet reader enforcing monotonic timestamps & no overlaps (strict mode) or configurable gap handling (fill-forward/ignore).
- Event pipeline: bar → strategy.on_bar → order intents → risk (minimal placeholder) → matcher → fill → accounting.
- Matcher: market orders fill at bar close ± slippage; limits fill on cross using intra-bar high/low.
- Accounting: FIFO lots, realized & unrealized P&L, equity curve, fees post-fill.
- Outputs: `trades.csv`, `orders.csv`, `equity.csv`, bar cache copy, structured logs, manifest.
- CLI: `bt backtest --config cfg.yml [--from YYYY-MM-DD --to YYYY-MM-DD] [--strict]`.
- Performance: month of 1m bars completes <5s on laptop-class HW.

## Out of Scope
Multi-symbol, tick data, partial fill lifecycle, derivatives.

## Deliverables
- Engine loop implementation.
- Parquet bar ingestion with QC.
- Matcher & basic slippage placeholder (constant or zero for now).
- Accounting module & CSV writers.

## Acceptance Criteria
- Two identical runs produce identical `trades.csv` and `equity.csv` hashes.
- Gap/duplicate bars logged; strict mode aborts with error, permissive modes recorded in manifest.
- Equity curve matches expected P&L for controlled test scenario.

## Test Plan
- Unit: bar ordering & QC; limit cross logic; P&L FIFO; slippage application.
- Integration: end-to-end deterministic run fixture.
- Property: price monotonicity violation triggers failure in strict mode.

## Risks / Mitigations
- Performance regressions → add timing assertion test.
- Hidden nondeterminism → isolate RNG seed in slippage component.

## Follow-ups
- Introduce multi-symbol support in later slice.
