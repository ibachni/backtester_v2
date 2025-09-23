# ADR 003: Small Slices (≤ 400 LOC per PR)

Status: Accepted  
Date: 2025-09-23  
Context Version: 1.0

## Context
Large, multi-purpose pull requests slow review cycles, obscure defects, and reduce the ability to bisect regressions. A lightweight backtesting engine benefits from iterative layering of capabilities.

## Decision
1. Target ≤ 400 net new lines per PR excluding generated fixtures.  
2. If exceeding, PR description must justify scope & note why splitting harms coherence.  
3. Each slice should introduce: (a) interface/contract, (b) minimal implementation, (c) tests, (d) docs.  
4. Feature flags or stubs acceptable to defer non-critical pathways instead of bloating a single PR.  
5. Changelog / release notes aggregate from merged slices.

## Consequences
+ Faster, focused reviews; easier revert.  
+ Encourages explicit interfaces early.  
− Possible overhead in sequencing dependent changes.

## Alternatives Considered
*Large milestone branches*: Rejected due to integration risk.

## Implementation Notes
- Add a CI guard script later to warn on large diffs.  
- Provide template in `tickets/` for slice articulation.

## Related
Principle 3 in `AGENTS.md`.

## Clarifications (2025-09-23)
Small slices reinforce the Modular Monolith decision: boundaries (ports/adapters) are evolved iteratively—each slice may introduce or refine a port. Any slice that *renames or breaks* a published interface must include an ADR update or migration note per Interfaces ADR.
