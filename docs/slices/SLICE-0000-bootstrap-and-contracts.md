---
id: SLICE-0000
title: Bootstrap & Contracts
status: planned
ticket: tickets/BT-0000.json
created: 2025-09-23
owner: nicolas
depends_on: []
targets: [backtester/ports, backtester/cli, ci/]
contracts: [Clock, EventBus, MarketDataProvider, OrderRouter, RiskEngine, SnapshotStore, RunManifestStore, PortfolioStore, ConfigProvider, SecretsProvider]
ads: [ADR 001, ADR 004, ADR 009, ADR 010, ADR 011]
risk: low
---

# Bootstrap & Contracts

## Goal
Establish the minimal runnable skeleton and all core port interfaces so subsequent slices build on stable contracts.

## Why
Locks boundaries early (Interfaces-before-Implementation principle) and enables parallel later slices without churn.

## Scope
- Repository scaffold & packaging (single top-level package `backtester`).
- CLI stub `bt` with subcommands: `backtest`, `shadow`, `paper`, `live` (no-op pipelines).
- Port interfaces (`Protocol`/ABC): `Clock`, `EventBus`, `SnapshotStore`, `RunManifestStore`, `MarketDataProvider`, `OrderRouter`, `RiskEngine`, `PortfolioStore`, `Logger/Telemetry` (initial facade), `ConfigProvider`, `SecretsProvider`.
- CI pipeline: run unit tests, type checks (mypy/pyright), lint (ruff/flake8), formatting (black/ruff format). 
- ADR references in each port module docstring.

## Out of Scope
Concrete adapters (exchange, paper broker), advanced metrics, real slippage model.

## Deliverables
- Package layout under `backtester/` with empty or skeletal modules.
- `bt --help` shows four subcommands.
- Each port has a unit test ensuring import & basic instantiation or abstract method presence.
- CI config files committed and green.

## Acceptance Criteria
- `bt backtest --noop` produces a run directory with `run_manifest.json` and basic structured log.
- All ports import without runtime errors; type checking passes.
- < 300 LOC net new (excluding CI YAML & boilerplate).

## Test Plan
- Unit: CLI argument parsing; import tests; manifest write smoke test.
- Property: (future slices) for determinism—not required here.

## Risks / Mitigations
- Over-engineering early interfaces → Keep minimal surface; extend additively.
- CI flakiness → Start with deterministic checks only.

## Follow-ups
- Expand telemetry facade into concrete structured logger (Slice 0008).
