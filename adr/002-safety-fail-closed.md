# ADR 002: Safety / Fail-Closed Execution

Status: Accepted
Date: 2025-09-23
Context Version: 1.0

## Context
Running strategies against simulated or paper environments must never accidentally place real live orders unless explicitly intended. Misconfiguration (e.g., pointing to a live broker API) can cause significant financial risk.

## Decision
1. Default mode is offline backtest; any network or broker adapter must require an explicit `--allow-net` or config flag `allow_live=true`.
2. A global kill/flatten mechanism exists in the engine to cancel open orders and flatten positions when triggered.
3. Live/paper adapters must implement an `is_live()` boolean + `safety_check(config)` method.
4. If safety flag not set, attempting to instantiate a live adapter raises a hard error (not warning).
5. Logging includes an unmistakable banner when in any live-capable mode.

## Consequences
+ Reduces accidental live exposure risk.
+ Clear separation between research and execution contexts.
− Friction when legitimately running live tests.

## Alternatives Considered
- None

## Implementation Notes
-
