# ADR 011: Deterministic Single-Threaded Backtests

Status: Accepted  
Date: 2025-09-23  
Context Version: 1.0

## Context
Concurrency introduces scheduler variance and hidden race conditions that degrade reproducibility. Most early strategies and bar granularities (≥1m) do not require parallel execution. Simplicity and determinism outweigh throughput needs initially.

## Decision
Execute each backtest run in a single OS thread with deterministic iteration over events. Parallel strategy evaluation (if needed) occurs via multiple processes, each maintaining its own deterministic run context.

## Consequences
+ Bitwise determinism (when combined with seed policy).  
+ Simplified debugging and profiling.  
− Throughput limited to single core; aggregated batch runs require process orchestration.

## Alternatives Considered
*Multi-threaded event processing*: Higher complexity for uncertain gain.  
*Async IO loop*: Deferred until live streaming demands it.

## Revisit Criteria
- Need >25k bars/sec processing on commodity laptop.  
- Introduction of sub-minute tick or order book data volumes.

## Implementation Notes
- Keep RNG + clock objects run-local, not global singletons.  
- Provide a future `--parallel N` harness spawning processes assigning disjoint seeds.

## Related
Determinism ADR (001), Event-Driven Core Loop (010).
