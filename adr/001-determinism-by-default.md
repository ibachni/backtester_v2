# ADR 001: Determinism By Default

Status: Accepted  
Date: 2025-09-23  
Deciders: DRI (Nicolas Ibach) + Core Contributors  
Context Version: 1.0

## Context
Backtests must be reproducible for scientific comparison and debugging. Non-determinism (implicit randomness, time.localtime, unordered iteration over sets/dicts pre-Python 3.7 assumptions, reliance on wall-clock network calls) leads to irreproducible performance metrics and invalid strategy research.

## Decision
1. Every backtest run records (a) git SHA, (b) global random seed(s), (c) config hash, (d) data snapshot identifier.  
2. Any stochastic component (slippage model, synthetic data generator) must accept an explicit seed.  
3. Hidden randomness (e.g., `random.random()` without prior `seed` provisioning) is prohibited; prefer injecting a `RandomProvider` or `numpy.Generator` instance.  
4. Parallelism (if later introduced) must partition seeds deterministically (e.g. base_seed + index offset).  
5. Time sources: use a monotonic event clock or UTC timestamps from data; never `datetime.now()` in core logic.

## Consequences
+ Enables exact reproduction of performance curves and order/fill sequences.  
+ Facilitates regression detection when refactoring.  
− Slight increase in ceremony: must thread seed/context objects.  
− Some third-party libs may need wrapping if they hide RNG state.

## Alternatives Considered
*Allow ambient randomness*: Rejected; debugging divergent runs too costly.  
*Snapshot full in-memory state periodically*: Deferred; heavier and not needed if deterministic.

## Implementation Notes
- Introduce a `RunContext` dataclass early containing `run_id`, `git_sha`, `seed`, `config_digest`.  
- Log once at engine start; propagate to metrics / reports.

## Related
Principle 1 in `AGENTS.md`. Will influence later ADR on run artifact layout.

## Clarifications (2025-09-23)
The separate "Deterministic, Single-Threaded Backtests" decision (see forthcoming ADR 003 in expanded set) is complementary — this ADR governs seed + state reproducibility, while ADR 003 constrains concurrency to preserve determinism guarantees. Modular monolith choice (ADR 001 in expanded numbering) reinforces a single-process assumption reducing cross-service clock drift.
