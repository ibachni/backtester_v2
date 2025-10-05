# ADR 008: I/O Policy – No Implicit Network in Backtests

Status: Accepted
Date: 2025-09-23
Context Version: 1.0

## Context
Network calls during deterministic backtests introduce latency variance, rate-limit failure modes, and accidental data drift. Offline reproducibility requires immutable local data inputs.

## Decision
1. Backtests forbid network I/O unless explicitly enabled via CLI flag `--allow-net` or config `allow_network: true`.
2. Data acquisition (downloading historical data) occurs in a preprocessing step, not inside the engine loop.
3. Adapters performing network I/O must check a passed `io_policy` object and raise if disallowed.
4. All external data used in a run is referenced by content hash (or file path) in run manifest.
5. CI enforces zero external network calls in default test suite.

## Consequences
+ Stable, reproducible performance metrics.
+ Simplifies test isolation.
− Requires upfront data preparation step.

## Alternatives Considered
*Allow best-effort network with caching*: Non-deterministic on cache miss.

## Implementation Notes
- Provide `IOPolicy(allow_network: bool)` passed through dependency graph.
- Potential later extension: allowlist specific hosts.

## Related
Principle 8 in `AGENTS.md`.
