# AGENTS.md — Multi-Agent Coding Protocol for Backtester_v2

This document defines how our coding agents collaborate to plan, research, implement, and review features for **Backtester_v2**. Keep it simple, ship thin slices, and protect determinism.

> **Version:** 1.1.0
> **Last updated:** 2025-09-22
> **DRI:** Nicolas Ibach
> **Change control:** Any change that affects contracts, CI gates, budgets, or repo structure **must** go through an ADR. Minor wording fixes may be edited directly by the DRI.

---

## 1) Purpose & Scope

- Orchestrate **Planner**, **Researcher**, **Implementer**, and **Reviewer** to ship small, safe increments.
- Applies to all code in this project (mono-repo supported). Default language: **Python 3.11+**.

---

## 2) Repo Principles & Global Invariants

1) **Determinism by default** — same code+config+data+seed ⇒ same outputs; log the seed and git SHA.
2) **Safety first** — fail-closed; no live orders unless explicitly enabled; global halt/flatten exists.
3) **Small slices** — target ≤ 400 LOC per PR; if bigger, justify.
4) **Interfaces before code** — lock contracts first; breaking changes require ADR + migration notes.
5) **Observability** — structured logs, metrics, run IDs; snapshot/replay.
6) **Test pyramid** — property + unit > integration > e2e; every PR raises the test bar.
7) **Time & ordering** — UTC internally; strictly increasing market data timestamps; no look-ahead.
8) **I/O policy** — no network in backtests unless `--allow-net` is set.

---

## 3) Roles at a Glance

### Planner (Copilot)
- Breaks the idea into a minimal, testable **slice**.
- Produces a **Task Ticket** with contracts, acceptance criteria, failing tests, ADR stub if needed.

### Researcher (Copilot)
- Builds a precise map of **where to look** before anyone edits code.
- Finds the **relevant files/symbols/line ranges**, dependencies, and repro evidence; updates the ticket.

### Implementer (Codex or Copilot)
- Codes to the ticket and locked contracts; updates tests/docs; keeps determinism and style.

### Reviewer (Copilot)
- Enforces scope, quality bars, and ADR policy; validates determinism and CI gates.

---

## 4) End-to-End Workflow (states & gates)

1) **Idea → Planner**
   Planner drafts a **slice & ticket** (links to contract stubs, failing tests, ADR stub, slice plan).
   Ticket state: `draft`.

2) **Planner → Researcher**
   Assign ticket to Researcher with a **Research Brief** (search seeds, excludes, success criteria).
   Ticket state: `needs-research`.

3) **Researcher → Implementer**
   Researcher adds **Research Findings** (below) and sets ticket to `ready-for-impl`.
   **Gate:** findings complete (repro, file map with line ranges, suspected causes, artifacts).

4) **Implementer → Reviewer**
   Implementer delivers code + tests; notes any **Research deltas** (drift from file map).
   Ticket state: `in-review`.

5) **Reviewer → Done**
   Reviewer runs the checklist; if ADR needed, block until merged; then approve.
   Ticket state: `done`.

**Guardrails:** Researcher **never** edits code; Implementer **never** bypasses locked contracts.

---

## 5) Task Ticket (handoff artifact)

**File:** `tickets/BT-XXXX.json`

```json
{
  "id": "BT-0000",
  "title": "Concise, actionable title",
  "context": {
    "problem_statement": "What & why",
    "background": "Relevant ADRs/constraints",
    "assumptions": ["explicit assumptions"],
    "non_goals": ["what we will NOT do"]
  },
  "scope": {
    "includes": ["modules/endpoints/flags"],
    "excludes": ["out-of-scope"]
  },
  "contracts": [
    {"name": "MarketDataProvider", "module": "backtester.ports.data", "signature": "get_history(...)->Iterable[Bar]"}
  ],
  "acceptance_criteria": ["Concrete, testable outcomes"],
  "test_plan": {
    "unit": ["..."],
    "property_based": ["..."],
    "integration": ["..."],
    "fixtures": ["..."]
  },
  "observability": {
    "logs": ["run_id", "seed", "git_sha"],
    "metrics": ["bars_lag", "order_rtt"]
  },
  "research_findings": {
    "repro": {"commands": ["make test -k 'name'"], "dataset": "fixture/path", "seed": 1234},
    "files": [
      {"path": "backtester/core/runner.py", "symbol": "LiveRunner.run", "lines": "45-88", "why": "bar scheduling", "last_sha": "ULID-or-SHA"},
      {"path": "backtester/adapters/paper/router.py", "symbol": "PaperOrderRouter.submit", "lines": "102-165", "why": "idempotency", "last_sha": "ULID-or-SHA"}
    ],
    "dependencies": ["backtester/ports/order_router.py → adapters/paper/router.py"],
    "suspected_causes": ["Null check on client_order_id missing"],
    "artifacts": ["artifacts/logs/BT-0000-grep.txt", "artifacts/stacktrace.txt"]
  },
  "risks": [{"risk": "look-ahead", "mitigation": "windowed indicators"}],
  "migration_notes": "schema/config changes if any",
  "links": {
    "slice_plan": "docs/slices.md#slice-…",
    "adrs": ["adr/007-utc-everywhere.md"],
    "contracts": ["backtester/ports/..."],
    "failing_tests": ["tests/e2e/test_parity.py::test_mvp_parity"]
  }
}
