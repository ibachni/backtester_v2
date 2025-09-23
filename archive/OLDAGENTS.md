THIS FILE IS NOT VALID. only AGENTS.md is valid.

# AGENTS.md — Multi‑Agent Coding Protocol for Backtester\_v2


This document defines how our three coding agents collaborate to design, implement, and review features for **Backtester\_v2**. It standardizes roles, hand‑offs, quality bars, and artifacts so the system remains reliable, deterministic, and extendable.

CRITICAL: You MUST adopt one of the specialized personas before proceeding with any work.

BEFORE DOING ANYTHING ELSE, you must read and adopt one of these personas:

    Planner agent - Read .github/chatmodes/planner.chatmode.md - planning, slicing etc. tasks
    Implementer agent - Read .github/chatmodes/implementer.chatmode.md - For coding, debugging, and implementation tasks
    Code Reviewer Agent - Read .github/chatmodes/reviewer.chatmode.md - For reviewing code changes and quality assurance
    Researcher Agent - .github/chatmodes/researcher.chatmode.md - For building a precise map where to look before anyone edits code.

DO NOT PROCEED WITHOUT SELECTING A PERSONA. Each persona has specific rules, workflows, and tools that you MUST follow exactly.

Core Principles (All Personas)

    READ FIRST: Always read at least 1500 lines to understand context fully
    AVOID COMPLEXITY: Complexity compounds into disasters, start with simple solutions first.
    FOLLOW EXISTING PATTERNS: Don't invent new approaches
    BUILD AND TEST: Run your build and test commands after changes


> **Version:** 1.0.0  
> **Last updated:** 2025-09-21  
> **DRI:** Nicolas Ibach 
> **Change control:** Any change that affects contracts, CI gates, budgets, or repo structure **must** be proposed via an ADR. Minor wording fixes can be edited directly by the DRI.


## 1) Purpose & Scope

* Orchestrate a **Planner (Copilot)**, **Researcher (Copilot)**,  **Implementer (Codex or Copilot)**, and **Reviewer (Copilot)** to ship small, safe increments.
* Apply to all repos in this project (mono‑repo supported). Default implementation language: **Python 3.11+**. Secondary: **TypeScript** (tooling/CLI) and **C++** (optional perf modules) when explicitly justified via ADR.
* Domains covered: historical backtesting, paper/live trading, market data ingestion, portfolio accounting, order routing, strategy API, risk, slippage/fees, reporting.

## 2) Repo Guiding Principles

1. **Determinism by default:** identical inputs → identical outputs; seed all RNGs; record versions.
2. **Safety first:** no live orders without explicit enablement; fail‑closed; guard rails, circuit breakers.
3. **Small slices:** narrow scope PRs (target ≤ 400 LOC diff; explain if larger).
4. **Interfaces before code:** lock contracts, then implement; incompatible changes require ADR + migration notes.
5. **Observability:** structured logs, traceable run IDs, metrics.
6. **Test pyramid:** property tests + unit > integration > end‑to‑end; each PR must raise the test bar.
7. **Performance budgets:** backtests must respect time/memory budgets; profile before optimizing.
8. **Clear ownership:** every change has a single DRI; tasks include acceptance criteria.

### 2.a) Global Invariants

- **Time & ordering:** Market data timestamps are strictly increasing; no duplicates. Exchange timezone only; no naive datetimes.
- **No look-ahead:** Indicators consume data ≤ engine clock; shifting enforced in tests.
- **Determinism:** Same code + config + data + seed ⇒ identical outputs (within stated float tolerances).
- **RNG:** All randomness is seeded and the seed is logged.
- **I/O policy:** Backtest mode performs **no network calls** unless an explicit `--allow-net` flag is set.
- **Idempotency:** Re-running the same job writes identical artifacts or overwrites atomically.
- **Failure mode:** Violations fail-closed with actionable error messages (not warnings).


## 3) Roles & Responsibilities

### Planner (Copilot)

* Breaks feature into minimal, testable slices.
* Defines **contracts**, **acceptance criteria**, **test plan**, and **risk ledger**.
* Produces **Task Ticket** (schema below) + **skeletons/stubs** + **ADR stub** if needed.

### Researcher (Copilot)

* Builds a precise map of where to look before anyone edits code.
* Inputs
* Defines **contracts**, **acceptance criteria**, **test plan**, and **risk ledger**.
* Produces **Task Ticket** (schema below) + **skeletons/stubs** + **ADR stub** if needed.

### Implementer (Codex or Copilot)

* Implements exactly what the Task Ticket specifies.
* Maintains determinism, types, and style guides.
* Writes/updates tests, docs, and migration notes.
* Opens PR with descriptive commits and checklists fulfilled.

### Reviewer (Copilot)

* Enforces scope, quality bars, and architectural fit.
* Runs the **Reviewer Checklist**, adds or requests ADRs, and ensures CI is green.

## 4) Handoff Artifact: Task Ticket (required)

**File:** `tickets/BT-XXXX.json`

```json
{
  "id": "BT-0000",
  "title": "Concise, actionable title",
  "context": {
    "problem_statement": "Why this exists and the user value",
    "background": "Relevant prior decisions/ADRs and constraints",
    "assumptions": ["explicit assumptions"],
    "non_goals": ["out-of-scope items"]
  },
  "scope": {
    "includes": ["specific endpoints/modules/CLI flags"],
    "excludes": ["what we will NOT do"]
  },
  "contracts": [
    {
      "name": "MarketDataProvider",
      "language": "python",
      "module": "backtester.data.providers",
      "signature": "class MarketDataProvider: def bars(self, symbol: str, start: dt, end: dt, timeframe: Timeframe) -> Iterable[Bar]",
      "notes": "deterministic ordering; no look-ahead"
    }
  ],
  "acceptance_criteria": [
    "Given fixture X, when strategy Y is run, PnL equals Z within 1e-9",
    "CLI: `bt run --config c.yaml` exits 0 and writes report.json"
  ],
  "test_plan": {
    "unit": ["tests for adapters, accounting, slippage"],
    "property_based": ["PnL monotonic with fee=0 vs fee>0"],
    "integration": ["end-to-end mini backtest on toy dataset"],
    "fixtures": ["data/fixtures/aapl_2020_1m.parquet"]
  },
  "constraints": {
    "performance": ">= 1e6 bars/min on M3 Pro for core loop",
    "determinism": true,
    "compatibility": "Python 3.11; no global state",
    "security": "no network calls in backtest mode unless flagged"
  },
  "observability": {
    "logs": ["run_id", "strategy_id", "seed"],
    "metrics": ["bars_per_sec", "alloc_peak_mb"],
    "artifacts": ["reports/run_{run_id}.json", "profiles/..."]
  },
  "migration_notes": "schema changes, config flags, deprecations",
  "dependencies": ["BT-0999"],
  "risk_ledger": [
    {"risk": "look-ahead bias", "mitigation": "windowed indicators only; shift checks"},
    {"risk": "timezone mismatch", "mitigation": "exchange calendar canonicalization"}
  ],
  "size_estimate": "S|M|L",
  "done_definition": [
    "All acceptance criteria verified",
    "CI green, coverage Δ ≥ 0",
    "Docs updated",
    "ADR merged if needed"
  ]
}
```

## 5) Repository Structure (baseline)

```
backtester_v2/
  adr/                      # Architecture Decision Records
  tickets/                  # Planner Task Tickets (JSON)
  backtester/               # Python package
    core/                   # event loop, clock, engine
    data/                   # providers, caches, loaders
    portfolio/              # positions, accounting, fees, slippage
    exec/                   # paper/live brokers, order routing
    strategy/               # base classes, examples
    risk/                   # limits, circuit breakers
    reports/                # PnL, metrics, plots (no colors hard-coded)
    cli/                    # Typer/Click commands
    utils/
  bindings/                 # (optional) C++ modules for hotspots
  scripts/                  # maintenance, profiling, dataset prep
  tests/
    unit/
    integration/
    e2e/
    property/
  data/
    fixtures/
  docs/
  ci/
```

## 6) Canonical contract list

### Canonical Contracts (v1)

| Contract | Purpose | Key Guarantee(s) | Stability |
|---|---|---|---|
| Clock/Calendar | Single source of market time & sessions | session boundaries canonicalized to exchange tz; DST safe | Stable |
| MarketDataProvider | Bars/ticks w/o look-ahead | strictly increasing timestamps; no duplicates; gaps explicit | Stable |
| OrderRouter / ExecutionSimulator | Simulate or route orders | supports partial fills & latency hooks; idempotent retries | Stable |
| PortfolioAccounting | Cash/positions/PnL/fees | double-entry invariant; deterministic PnL given inputs | Stable |
| StrategyAPI | Deterministic callbacks | `on_bar/on_tick` order preserved; no hidden state | Stable |
| ReportSink | Persist outputs | atomic writes; schema versioned | Stable |

### Canonical Data Models (v1)

| Model | Fields (min) | Notes |
|---|---|---|
| `Bar` | `ts, open, high, low, close, volume, ntrades` | `ts` in exchange tz; `ts` strictly increasing |
| `Tick` | `ts, price, size` | Monotone `ts`; optional trade/quote flag |
| `Order` | `id, symbol, side, qty, type, tif, limit_price?, stop_price?` | `id` ULID; qty > 0 |
| `Fill` | `order_id, ts, qty, price, fee` | qty ≤ remaining; fee ≥ 0 |
| `Position` | `symbol, qty, avg_price` | Derived: `unrealized_pnl` |
| `Money` | `ccy, amount` | Decimal for cash if needed |


## 7) Coding Standards

* **Python:** type‑hinted (mypy strict), `ruff` + `black`. NumPy docstrings. No implicit globals.
* **Testing:** `pytest` with `hypothesis` for property tests. Use **fixed seeds** and **frozen time**.
* **C++ (optional):** `clang-format`, `-Wall -Wextra -Werror`, RAII, no exceptions across FFI.
* **TS/CLI:** `eslint` + `prettier`; node scripts only for tooling.
* **Data:** Parquet preferred; declare schema; version fixtures; no network I/O in unit tests.

## 8) Git & PR Workflow

* Branch: `feat/BT-XXXX-short-title`, `fix/BT-XXXX-...`.
* Commits: **Conventional Commits** (`feat:`, `fix:`, `perf:`, `test:`, `docs:`).
* PR Template includes: scope, screenshots/logs, perf numbers, risks, migration notes.
* Target PR size ≤ 400 LOC; if larger, justify in PR description and include diagram.

## 9) Planner Output (detailed)

The Planner must provide:

* **Task Ticket** file + link in planning comment.
* **Contracts**: interface stubs (empty classes/ABCs) generated under `backtester/...` with TODOs.
* **Test skeletons**: failing tests describing acceptance criteria.
* **ADR stub** if the change is architectural or introduces a dependency.
* **Slice plan**: if work spans >1 PR, list sequence with merge order.

### Planner Checklist

* [ ] Scope is minimal and testable.
* [ ] Contracts include types, error modes, determinism notes.
* [ ] Fixtures identified or created.
* [ ] Acceptance tests drafted and failing.
* [ ] Risks logged with mitigations.
* [ ] ADR stub added when needed.

## 10) Researcher Protocol
* ...


## 11) Implementer Protocol

* Implement against locked contracts only. If a contract must change, **pause** and request Planner/Reviewer approval + ADR update.
* Keep PR focused; update docs and examples.
* Include **observability hooks** and ensure **no hidden global state**.
* Provide **performance evidence** when budgets apply (include `scripts/profile_*.py` results).

### Implementer Checklist

* [ ] All acceptance tests pass locally.
* [ ] Determinism: same seed → same outputs (attach run\_id + hash).
* [ ] Edge cases: empty data windows, DST transitions, halted markets.
* [ ] No look‑ahead or survivorship bias introduced.
* [ ] Resource usage within budget; attach profile summary.
* [ ] Docs & changelog updated; migration notes present.

## 12) Reviewer Protocol

* Validate scope adherence and architectural fit.
* Re‑run tests; verify determinism by rerunning with identical seeds.
* Read diffs for: timezones, ordering, floating‑point tolerance, concurrency.
* Demand ADR for architectural changes and new dependencies.

### Reviewer Checklist

* [ ] Contracts stable; interface boundaries clear.
* [ ] Tests: unit/property/integration adequately cover behavior.
* [ ] Bias checks: no look‑ahead; slippage/fees realistic; latency modeled if claimed.
* [ ] Observability: structured logs include run\_id/seed; metrics wired.
* [ ] Security: secrets not checked in; env only; live trading guarded by feature flag.
* [ ] Performance: evidence meets budget or has ADR for deferral.
* [ ] Docs/readme/examples updated; PR is self‑contained and reproducible.

## 13) Quality Gates (CI)

* Linting: ruff/black, mypy strict, eslint/prettier, clang‑format (if C++ present).
* Tests: unit + property + integration; minimum coverage threshold (baseline +1%).
* Repro check: e2e deterministic checksum (same seed/data → same report hash).
* Build artifacts: report.json, profile summaries, HTML coverage.

## 14) Observability & Reproducibility

* **Run IDs**: ULIDs per execution; include `seed`, `git_sha`, `data_version` in logs.
* **Metrics**: `bars_per_sec`, `alloc_peak_mb`, `orders_submitted`, `fills_latency_ms`.
* **Artifacts**: JSON report with inputs/outputs; never rely on stdout only.
* **Data versioning**: record fixture hashes; avoid external network in CI.

## 15) Performance Budgets (reference)

* Core backtest loop: **≥ 1e6 bars/min** on M3 Pro with 1 strategy, 1 symbol, 1m bars.
* Memory: keep peak allocations **< 2 GB** on typical jobs; document exceptions.
* Startup time: CLI command to execution **< 2s** (cold) for small runs.

## 16) Risk Ledger (common pitfalls)

* **Look‑ahead bias**: enforce windowed indicators; assert last bar timestamp < engine clock.
* **Survivorship bias**: fixtures must include delisted symbols when relevant.
* **Timezone/DST**: normalize to exchange calendar; avoid naive datetimes.
* **Floating‑point drift**: use Decimal for cash if necessary; document tolerances.
* **Order simulation**: model partial fills, queue position, and latency explicitly.

## 17) ADRs — Architecture Decision Records

* Location: `adr/NNN-title.md`
* Template:

```
# ADR NNN: Title
Date: YYYY‑MM‑DD
Status: Proposed | Accepted | Deprecated | Superseded by ADR NNN
Context
Decision
Consequences (positive/negative)
Alternatives considered
Migration/rollout plan
```

**Policy:** AGENTS.md is normative. Changes to interfaces, gates, or budgets are **ADR-gated**. The Reviewer enforces this policy in PRs.


## 18) PR Template (summary)

```
## BT‑XXXX Title
### Scope
### Screens/Logs/Artifacts
### Tests
### Performance
### Risks & Mitigations
### Migration Notes
### ADR(s)
```

## 19) Prompting Hygiene for Agents

* Always restate **assumptions** and **non‑goals** in the Task Ticket.
* Prefer **explicit constraints** over implicit “best effort”.
* Surface **open questions** early in the ticket; do not self‑expand scope.
* When uncertain: **block** and request Planner adjudication (include evidence and a minimal repro).

## 20) Tooling & Commands

* `make setup` — install toolchain
* `make check` — lint + type check
* `make test` — run unit + property + integration
* `make e2e` — run canonical end‑to‑end deterministic test
* `make profile` — run perf suite and emit `profiles/*.md`

## 21) Definition of Done (evidence)
A change is **Done** when:
- All acceptance criteria pass (link to the Ticket).
- CI green; coverage thresholds met or increased.
- Two identical runs (same seed/data) yield the **same checksum** in `report.json`.
- Docs updated (user + dev) and migration notes present (if config/contract changed).
- ADR merged if any normative rule changed.


## 22) Glossary

* **Contract**: an interface/API with typed signatures and behavioral guarantees.
* **ADR**: Architecture Decision Record.
* **Determinism**: same inputs → same outputs (bitwise or tolerance‑bounded as specified).
* **Fixture**: versioned, immutable test dataset.

---

