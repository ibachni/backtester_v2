# ADR 002: Safety / Fail-Closed Execution

Status: Accepted  
Date: 2025-09-23  
Context Version: 1.0

## Context
Running strategies against simulated or paper environments must never accidentally place real live orders unless explicitly intended. Misconfiguration (e.g., pointing to a live broker API) can cause financial or compliance risk.

## Decision
1. Default mode is offline backtest; any network or broker adapter must require an explicit `--allow-net` or config flag `allow_live=true`.  
2. A global kill/flatten mechanism exists in the engine to cancel open orders and flatten positions when triggered.  
3. Live/paper adapters reside under `backtester.adapters.*` and must implement an `is_live()` boolean + `safety_check(config)` method.  
4. If safety flag not set, attempting to instantiate a live adapter raises a hard error (not warning).  
5. Logging includes an unmistakable banner when in any live-capable mode.

## Consequences
+ Reduces accidental live exposure risk.  
+ Clear separation between research and execution contexts.  
− Slight friction when legitimately running live tests.

## Alternatives Considered
*Silent paper/live auto-detection*: Rejected; too opaque.  
*Environment variable only*: Rejected; config + explicit flag safer.

## Implementation Notes
- Provide `SafetyMode` enum: BACKTEST, PAPER, LIVE.  
- Engine startup asserts mode transitions are explicit.  
- Unit test: instantiating live adapter without flag raises.

## Related
Principle 2 in `AGENTS.md`.

## Clarifications (2025-09-23)
Risk rails (pre-trade checks + global kill switch) are formalized in a separate ADR (Risk Rails). This safety ADR focuses on *environment & mode gating* (live/paper/backtest) while Risk Rails governs *order-level risk thresholds*. Execution model without partial fills (future ADR) simplifies safety enforcement because fewer intermediate order states require guarding.
