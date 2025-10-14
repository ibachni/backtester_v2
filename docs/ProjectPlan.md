# Backtester V2 – Project Plan

> Living document summarizing core goals, architectural decisions (ADRs), and delivery slices. Updated as slices progress.

## 0. Overall TODOS

TODO: Decide how to log: logging vs. my own module.

## 1. Vision & Scope
Provide a deterministic, event-driven quantitative backtesting and lightweight live/paper trading platform optimized for:
- Reproducible research (bitwise or hash-equivalent outputs given same code + config + data + seed).
- Safe progression from simulation → shadow → paper → tiny live.
- Extensibility via well-defined ports/adapters while retaining a single-process modular monolith (ADR 009).
- Minimal operational overhead (no external infra required for core loop, storage, or observability in V1).

Initial asset scope: single venue, single spot asset class (ADR 013). Multi-venue, derivatives, leverage, and order book microstructure explicitly deferred.

## 2. Core Architectural Tenets
| Principle | Summary | Key ADRs |
|-----------|---------|----------|
| Determinism | Same inputs ⇒ identical outputs (manifest, trades, equity) | ADR 001, 011, 016 |
| Modular Monolith | Single process with strict interface boundaries (`ports/`) | ADR 009, 004 |
| Event-Driven Loop | In-process bus sequencing typed events | ADR 010 |
| Safety & Risk | Fail-closed modes + pre-trade risk rails + kill switch | ADR 002, 018, 006 (slices) |
| Observability | Structured logs + minimal metrics + replay capability | ADR 005, 020, 014 |
| Progressive Realism | Simulation → paper parity → shadow → small live | Slices 0002–0007 |
| Small Slices | ≤400 LOC incremental evolution | ADR 003 |

## 3. Domain Model & Key Interfaces
Initial interfaces codified in Slice 0000 (Bootstrap & Contracts):
- Market & Time: `Clock`, `EventBus`
- Data & Execution: `MarketDataProvider`, `OrderRouter`
- State & Persistence: `PortfolioStore`, `SnapshotStore`, `RunManifestStore`
- Risk & Controls: `RiskEngine`
- Strategy Layer: `Strategy` API (Slice 0003)
- Configuration & Secrets: `ConfigProvider`, `SecretsProvider`
- Telemetry: `Logger/Telemetry` facade (expanded in Slice 0008)

Design choices:
- All ports reside in `backtester/ports/`; adapters under `backtester/adapters/` (enforces decoupling per ADR 004).
- Deterministic single-threaded processing guarantees strict event ordering (ADR 011).
- Idempotent `client_order_id` across all order flows (ADR 016) supports crash-safe reconciliation.

## 4. Data Flow (High-Level)
1. Clock advances (sim) or live data poll returns new bar.
2. `MarketDataProvider` publishes `BarReady` event.
3. Strategy `on_bar` emits zero or more `OrderIntent` objects.
4. `RiskEngine` evaluates each intent (rules: symbol allow-list, notional cap, max positions, daily loss).
5. Approved intents sent to `OrderRouter` (sim matcher / paper / live adapter).
6. Execution produces Fill event; accounting updates portfolio state & emits metrics.
7. Logging & metrics modules persist events, snapshots, and counters.
8. On shutdown (or periodic), snapshot + manifest finalized; contract & parity tests validate behavior.

Replay: load latest snapshot → replay WAL/JSONL event log to reconstruct identical state (ADR 014, 020).

## 5. Storage & Artifacts
- Historical bars: Parquet (ADR 014) under deterministic path hashed in manifest.
- Run artifacts: `runs/<run_id>/` containing `manifest.json`, `events.log` (JSONL), `orders.csv`, `trades.csv`, `equity.csv`, snapshots directory.
- Logs compressed/rotated for long shadow/paper runs (Slice 0005, 0008).
- Snapshots represent positions, cash, open orders, and config hash for fast recovery.

## 6. Execution & Slippage Model
V1 execution (ADR 015, 017):
- Market orders fill at bar close ± deterministic slippage (spread + participation-of-volume model in later slice).
- Limit orders fill if intra-bar high/low crosses limit; price chosen side-consistently.
- Partial venue fills aggregated into a single logical Fill event (simplifies state machine; ADR 015).
- Costs applied post-fill via pluggable `CostModel` (commissions, fees, slippage parameters).

## 7. Risk & Safety
Two layers:
- Mode Safety (ADR 002): backtest/paper/live gating; explicit flags for network/live enablement.
- Pre-Trade Risk Rails (ADR 018 / Slice 0006): order-level and portfolio constraints; reason-coded blocks; global `halt` & `flatten` commands. Exit orders bypass certain blocks to ensure safe position reduction.

Kill-switch semantics: halt stops new intents immediately; flatten submits closures then sets halt state.

## 8. Observability & Recovery
- Structured JSONL logs with normalized UTC timestamps (ADR 007).
- Minimal metrics set: `bars_lag`, `strategy_tick_latency`, `order_rtt`, `error_rate`, `risk_block_rate`, `pnl_intraday` (ADR 020).
- Alerts emitted as structured log entries (bar gaps, order error rate threshold, daily loss halt).
- Replay tool validates deterministic reconstruction; mismatches flag potential nondeterminism regressions.

## 9. Configuration & Secrets
Layered precedence (ADR 019 / Slice 0001): defaults < file < env < CLI flags.
Secrets never logged; manifest stores only redacted references + hash of effective config.
`RunManifestStore` centralizes storage of git SHA, config hash, seeds, environment info (ADR 001).

## 10. Live Progression Path
| Stage | Slice | Purpose | Key Criteria |
|-------|-------|---------|--------------|
| Simulation | 0002-0003 | Deterministic loop + strategy & risk | Hash-stable outputs, rule enforcement |
| Shadow | 0005 | Soak-test latency & stability | p95/p99 latency SLO; no missed bars |
| Paper | 0004 | Live timing + parity safety | P&L diff ≤5 bps vs sim; idempotent orders |
| Tiny Live | 0007 | Real venue integration | Tiny orders; safe reconciliation |
| Observability | 0008 | Debug & replay | Crash-safe recovery; alerts work |
| Gating | 0009 | Enforce contracts | All adapters pass test harness |

## 11. Testing Strategy
- Unit & property tests emphasize invariants: strictly increasing timestamps, FIFO P&L consistency, idempotent order submission (ADR 006 philosophy).
- Contract Test Harness (Slice 0009) provides programmable gates for adapters.
- Determinism tests compare artifact hashes across duplicate runs.
- Parity tests: sim vs paper P&L drift threshold.

## 12. Performance Targets (Initial)
- Single-symbol 1-month 1m-bar backtest <5s local (Slice 0002).
- Live decision latency: p95 ≤ 500 ms, p99 ≤ 900 ms (Slices 0004–0005).
- Order round-trip (paper/live tiny): p95 ≤ 500 ms (Slice 0007).
Profiling added when targets missed; micro-optimizations deferred until baseline met.

## 13. Risks & Mitigations
| Risk | Impact | Mitigation |
|------|--------|-----------|
| Interface churn | Rework & test fragility | Lock minimal ports early (Slice 0000) & additive evolution |
| Hidden nondeterminism | Invalid results, flaky tests | Centralized RNG seeding, snapshot + replay validation |
| Latency regressions | Missed fills or parity gaps | Metrics & alerting + contract/perf tests |
| Adapter divergence | Paper vs live mismatch | Contract tests (Slice 0009), parity criteria |
| Log/metric bloat | Disk growth, slower runs | Rotation/compression, minimal metric surface |

## 14. Roadmap (Slice Sequencing)
1. SLICE-0000: Skeleton & ports.
2. SLICE-0001: Config & secrets, manifest enrichment.
3. SLICE-0002: Core deterministic backtest + accounting.
4. SLICE-0003: Strategy API + slippage & minimal risk.
5. SLICE-0004: Paper parity (REST polling).
6. SLICE-0005: Shadow mode soak.
7. SLICE-0006: Expanded risk rails & operator controls.
8. SLICE-0007: Binance tiny live adapter.
9. SLICE-0008: Observability, snapshots, replay.
10. SLICE-0009: Contract harness gating.

## 15. Future (Explicitly Deferred)
- Multi-symbol & portfolio allocation logic.
- Partial fill lifecycle & advanced execution algorithms (TWAP/VWAP, slicing).
- WebSocket streaming, order book / L2 modeling.
- Multi-venue routing & cross-venue risk hedging.
- Derivatives (futures, margin, funding rates) & leverage.
- Advanced exposure & risk buckets (sector, factor).
- External observability stack (Prometheus/Grafana) & dashboards.
- SQL analytics layer (DuckDB/SQLite) for multi-run analysis.

## 16. Change Control
Any change impacting locked ports, storage formats, determinism guarantees, or slice acceptance criteria requires a new or amended ADR and update to this plan. Minor wording corrections may be direct edits referencing commit SHA.

## 17. References
- ADR Directory: `/adr/` (001–020)
- Slices: `/docs/slices/`
- Contract Harness Slice: `SLICE-0009`
- Determinism & Single Threading: ADR 001, ADR 011
- Observability: ADR 005, ADR 014, ADR 020

---
_Revision: 2025-09-23 (initial synthesis). Update as slices transition status._

# Backtester V2 – Project Plan

> Living document summarizing core goals, architectural decisions (ADRs), and delivery slices. Updated as slices progress.

## 1. Vision & Scope
Provide a deterministic, event-driven quantitative backtesting and lightweight live/paper trading platform optimized for:
- Reproducible research (bitwise or hash-equivalent outputs given same code + config + data + seed).
- Safe progression from simulation → shadow → paper → tiny live.
- Extensibility via well-defined ports/adapters while retaining a single-process modular monolith (ADR 009).
- Minimal operational overhead (no external infra required for core loop, storage, or observability in V1).

Initial asset scope: single venue, single spot asset class (ADR 013). Multi-venue, derivatives, leverage, and order book microstructure explicitly deferred.

## 2. Core Architectural Tenets
| Principle | Summary | Key ADRs |
|-----------|---------|----------|
| Determinism | Same inputs ⇒ identical outputs (manifest, trades, equity) | ADR 001, 011, 016 |
| Modular Monolith | Single process with strict interface boundaries (`ports/`) | ADR 009, 004 |
| Event-Driven Loop | In-process bus sequencing typed events | ADR 010 |
| Safety & Risk | Fail-closed modes + pre-trade risk rails + kill switch | ADR 002, 018, 006 (slices) |
| Observability | Structured logs + minimal metrics + replay capability | ADR 005, 020, 014 |
| Progressive Realism | Simulation → paper parity → shadow → small live | Slices 0002–0007 |
| Small Slices | ≤400 LOC incremental evolution | ADR 003 |

## 3. Domain Model & Key Interfaces
Initial interfaces codified in Slice 0000 (Bootstrap & Contracts):
- Market & Time: `Clock`, `EventBus`
- Data & Execution: `MarketDataProvider`, `OrderRouter`
- State & Persistence: `PortfolioStore`, `SnapshotStore`, `RunManifestStore`
- Risk & Controls: `RiskEngine`
- Strategy Layer: `Strategy` API (Slice 0003)
- Configuration & Secrets: `ConfigProvider`, `SecretsProvider`
- Telemetry: `Logger/Telemetry` facade (expanded in Slice 0008)

Design choices:
- All ports reside in `backtester/ports/`; adapters under `backtester/adapters/` (enforces decoupling per ADR 004).
- Deterministic single-threaded processing guarantees strict event ordering (ADR 011).
- Idempotent `client_order_id` across all order flows (ADR 016) supports crash-safe reconciliation.

## 4. Data Flow (High-Level)
1. Clock advances (sim) or live data poll returns new bar.
2. `MarketDataProvider` publishes `BarReady` event.
3. Strategy `on_bar` emits zero or more `OrderIntent` objects.
4. `RiskEngine` evaluates each intent (rules: symbol allow-list, notional cap, max positions, daily loss).
5. Approved intents sent to `OrderRouter` (sim matcher / paper / live adapter).
6. Execution produces Fill event; accounting updates portfolio state & emits metrics.
7. Logging & metrics modules persist events, snapshots, and counters.
8. On shutdown (or periodic), snapshot + manifest finalized; contract & parity tests validate behavior.

Replay: load latest snapshot → replay WAL/JSONL event log to reconstruct identical state (ADR 014, 020).

## 5. Storage & Artifacts
- Historical bars: Parquet (ADR 014) under deterministic path hashed in manifest.
- Run artifacts: `runs/<run_id>/` containing `manifest.json`, `events.log` (JSONL), `orders.csv`, `trades.csv`, `equity.csv`, snapshots directory.
- Logs compressed/rotated for long shadow/paper runs (Slice 0005, 0008).
- Snapshots represent positions, cash, open orders, and config hash for fast recovery.

## 6. Execution & Slippage Model
V1 execution (ADR 015, 017):
- Market orders fill at bar close ± deterministic slippage (spread + participation-of-volume model in later slice).
- Limit orders fill if intra-bar high/low crosses limit; price chosen side-consistently.
- Partial venue fills aggregated into a single logical Fill event (simplifies state machine; ADR 015).
- Costs applied post-fill via pluggable `CostModel` (commissions, fees, slippage parameters).

## 7. Risk & Safety
Two layers:
- Mode Safety (ADR 002): backtest/paper/live gating; explicit flags for network/live enablement.
- Pre-Trade Risk Rails (ADR 018 / Slice 0006): order-level and portfolio constraints; reason-coded blocks; global `halt` & `flatten` commands. Exit orders bypass certain blocks to ensure safe position reduction.

Kill-switch semantics: halt stops new intents immediately; flatten submits closures then sets halt state.

## 8. Observability & Recovery
- Structured JSONL logs with normalized UTC timestamps (ADR 007).
- Minimal metrics set: `bars_lag`, `strategy_tick_latency`, `order_rtt`, `error_rate`, `risk_block_rate`, `pnl_intraday` (ADR 020).
- Alerts emitted as structured log entries (bar gaps, order error rate threshold, daily loss halt).
- Replay tool validates deterministic reconstruction; mismatches flag potential nondeterminism regressions.

## 9. Configuration & Secrets
Layered precedence (ADR 019 / Slice 0001): defaults < file < env < CLI flags.
Secrets never logged; manifest stores only redacted references + hash of effective config.
`RunManifestStore` centralizes storage of git SHA, config hash, seeds, environment info (ADR 001).

## 10. Live Progression Path
| Stage | Slice | Purpose | Key Criteria |
|-------|-------|---------|--------------|
| Simulation | 0002-0003 | Deterministic loop + strategy & risk | Hash-stable outputs, rule enforcement |
| Shadow | 0005 | Soak-test latency & stability | p95/p99 latency SLO; no missed bars |
| Paper | 0004 | Live timing + parity safety | P&L diff ≤5 bps vs sim; idempotent orders |
| Tiny Live | 0007 | Real venue integration | Tiny orders; safe reconciliation |
| Observability | 0008 | Debug & replay | Crash-safe recovery; alerts work |
| Gating | 0009 | Enforce contracts | All adapters pass test harness |

## 11. Testing Strategy
- Unit & property tests emphasize invariants: strictly increasing timestamps, FIFO P&L consistency, idempotent order submission (ADR 006 philosophy).
- Contract Test Harness (Slice 0009) provides programmable gates for adapters.
- Determinism tests compare artifact hashes across duplicate runs.
- Parity tests: sim vs paper P&L drift threshold.

## 12. Performance Targets (Initial)
- Single-symbol 1-month 1m-bar backtest <5s local (Slice 0002).
- Live decision latency: p95 ≤ 500 ms, p99 ≤ 900 ms (Slices 0004–0005).
- Order round-trip (paper/live tiny): p95 ≤ 500 ms (Slice 0007).
Profiling added when targets missed; micro-optimizations deferred until baseline met.

## 13. Risks & Mitigations
| Risk | Impact | Mitigation |
|------|--------|-----------|
| Interface churn | Rework & test fragility | Lock minimal ports early (Slice 0000) & additive evolution |
| Hidden nondeterminism | Invalid results, flaky tests | Centralized RNG seeding, snapshot + replay validation |
| Latency regressions | Missed fills or parity gaps | Metrics & alerting + contract/perf tests |
| Adapter divergence | Paper vs live mismatch | Contract tests (Slice 0009), parity criteria |
| Log/metric bloat | Disk growth, slower runs | Rotation/compression, minimal metric surface |

## 14. Roadmap (Slice Sequencing)
1. SLICE-0000: Skeleton & ports.
2. SLICE-0001: Config & secrets, manifest enrichment.
3. SLICE-0002: Core deterministic backtest + accounting.
4. SLICE-0003: Strategy API + slippage & minimal risk.
5. SLICE-0004: Paper parity (REST polling).
6. SLICE-0005: Shadow mode soak.
7. SLICE-0006: Expanded risk rails & operator controls.
8. SLICE-0007: Binance tiny live adapter.
9. SLICE-0008: Observability, snapshots, replay.
10. SLICE-0009: Contract harness gating.

## 15. Future (Explicitly Deferred)
- Multi-symbol & portfolio allocation logic.
- Partial fill lifecycle & advanced execution algorithms (TWAP/VWAP, slicing).
- WebSocket streaming, order book / L2 modeling.
- Multi-venue routing & cross-venue risk hedging.
- Derivatives (futures, margin, funding rates) & leverage.
- Advanced exposure & risk buckets (sector, factor).
- External observability stack (Prometheus/Grafana) & dashboards.
- SQL analytics layer (DuckDB/SQLite) for multi-run analysis.

## 16. Change Control
Any change impacting locked ports, storage formats, determinism guarantees, or slice acceptance criteria requires a new or amended ADR and update to this plan. Minor wording corrections may be direct edits referencing commit SHA.

## 17. References
- ADR Directory: `/adr/` (001–020)
- Slices: `/docs/slices/`
- Contract Harness Slice: `SLICE-0009`
- Determinism & Single Threading: ADR 001, ADR 011
- Observability: ADR 005, ADR 014, ADR 020

---
_Revision: 2025-09-23 (initial synthesis). Update as slices transition status._
