Backtester_v2 — Known Issues and Fix Plan
=========================================

Purpose: single-source, skimmable tracker for bugs/regressions. Each entry is optimized for both human and LLM triage: clear title, severity, repro, suspected cause, exact file:line refs, suggested fix, and acceptance criteria.

Legend: [P]riority (P0 critical, P1 high, P2 normal), [A]rea tags.

---

1) P0 — CLI config plumbing miswired (overrides treated as file path)
-------------------------------------------------------------------
- Summary: `bt backtest --noop` ignores `--config` and crashes/misbehaves when `--set` is used because `main()` passes CLI overrides into the file-config parameter.
- Evidence/Repro:
  - Run: `bt backtest --noop --out runs/x --set foo=1`
  - In `backtester/cli/bt.py:200-203`, `run_noop(clock, manifest_store, out_dir, args.seed, args.config_overrides)` passes a list where a `Path | None` is expected. Inside `run_noop`, `Path(file_config).read_text()` will fail or `--config` is ignored.
- Suspected Cause: Parameter order mismatch; `file_config` and `config_overrides` swapped at call site.
- File refs:
  - backtester/cli/bt.py:192-205, 118-170
- Suggested Fix:
  - Call `run_noop(clock, manifest_store, out_dir, args.seed, file_config=args.config, config_overrides=args.config_overrides)`.
  - Add unit test for `--config` and `--set` round-trip.
- Acceptance:
  - `bt backtest --noop --out runs/x --config configs/sample.json` writes manifest with redacted config and correct hash.
  - `--set key=value` merges into resolved config without exceptions.
- Tags: [CLI] [Config] [Determinism]

2) P0 — Realtime clock sleeps for days (ms used as seconds)
----------------------------------------------------------
- Summary: `Clock.sleep_until_next_boundary()` calls `asyncio.sleep(target + extra_delay_ms)` (absolute ms value interpreted as seconds). `sleep_for()` is unimplemented.
- Evidence/Repro:
  - backtester/core/clock.py:126-143 — uses absolute epoch ms as seconds.
  - Any realtime loop would block for an extremely long time.
- Suspected Cause: Units mismatch and missing relative delta computation.
- File refs:
  - backtester/core/clock.py:113-115, 126-143
- Suggested Fix:
  - Implement `sleep_for()` and compute `delta_ms = max(0, target - self.now())`; sleep with `delta_ms / 1000.0 + extra_delay_ms/1000.0`.
- Acceptance:
  - Unit test: boundary alignment returns instantly when already past boundary; sleeps roughly expected duration when before boundary.
- Tags: [Clock] [Realtime] [Performance]

3) P1 — Topic schema mismatches break validation and contracts
-------------------------------------------------------------
- Summary: Topics are registered with schemas that don’t match actual payloads, preventing schema validation and undermining contracts.
- Instances:
  - `T_ORDERS_ACK` registered as `OrderIntent`, but publishes `OrderAck` in validation.
    - backtester/core/backtest_engine.py:181-186
    - backtester/strategy/order_validation.py:94-159 (publishes `OrderAck`)
  - `T_ACCOUNT_SNAPSHOT` registered as `LotClosedEvent`, but publishes `PortfolioSnapshot`.
    - backtester/core/backtest_engine.py:211-216
    - backtester/core/account.py:493-495
  - `T_TRADE_EVENT` registered as `LotClosedEvent` while `account.apply_fill()` also publishes `TradeEvent` on the same topic.
    - backtester/core/backtest_engine.py:205-210
    - backtester/core/account.py:225, 269, 332
- Suspected Cause: Evolving topic design without updating registrations.
- Suggested Fix:
  - Register topics with one payload type each and enable `validate_schema=True`.
  - Split `account.trade` into `account.lot_closed` (LotClosedEvent) and `account.trade` (TradeEvent) or choose one canonical event and stick to it.
- Acceptance:
  - Contract tests pass with `validate_schema=True` for all topics.
  - No runtime `BusError` due to schema mismatches.
- Tags: [Bus] [Contracts] [Validation]

4) P1 — Strategy daily loss limit never triggers
-----------------------------------------------
- Summary: `SMAExtendedStrategy` computes `risk_block` from `_daily_pnl`, but `_daily_pnl` is never updated on fills, so the guardrail is ineffective.
- Evidence/Refs:
  - backtester/strategy/sma_extended.py:76, 116, 217-228 (no updates on fill).
  - `on_fill()` mutates position state but does not adjust `_daily_pnl`.
- Suspected Cause: Incomplete implementation; intended to consume PnL from account/trade events.
- Suggested Fix:
  - Update `_daily_pnl` in `on_fill()` using realized PnL contribution (or subscribe to `TradeEvent` and aggregate per day).
  - Add unit test that forces losses past the threshold and asserts no further buy intents until day rolls.
- Acceptance:
  - Given controlled price path, after crossing daily loss limit, `on_candle()` emits zero new open-intents until next day.
- Tags: [Strategy] [Risk] [Correctness]

5) P1 — Global RNG side-effect at import time (latency model)
-------------------------------------------------------------
- Summary: `LatencyModel` seeds Python’s global RNG at class definition time, harming determinism across the process and tests.
- Evidence/Refs:
  - backtester/sim/sim.py:59-79 — `random.seed(seed)` runs in class body; affects all randomness globally on import.
- Suspected Cause: Attempt to bind default seed per model instance.
- Suggested Fix:
  - Remove global `random.seed(seed)`. Either store a `random.Random(seed)` instance per model, or accept an injected RNG.
- Acceptance:
  - Creating a `LatencyModel` no longer changes `random.random()` output in unrelated code.
- Tags: [Sim] [Determinism] [Testing]

6) P2 — `sleep_for()` is a stub in `Clock`
------------------------------------------
- Summary: Abstract helper is unimplemented; subclasses should provide a deterministic default.
- Evidence/Refs:
  - backtester/core/clock.py:113-115
- Suggested Fix:
  - Provide base implementation: `await self.sleep_until(self.now() + max(0, int(delta_ms)))`.
- Acceptance:
  - Unit test verifying `sleep_for(0)` returns promptly and delegates properly.
- Tags: [Clock] [API]

7) P2 — AuditWriter redundant double flush
------------------------------------------
- Summary: `AuditWriter.emit()` calls `self._fh.flush()` twice; unnecessary overhead.
- Evidence/Refs:
  - backtester/core/audit.py:86-107 (duplicate flush).
- Suggested Fix:
  - Remove duplicated flush.
- Acceptance:
  - Lint clean; no change in file output semantics.
- Tags: [Observability]

8) P2 — Error/message quality and minor typos
---------------------------------------------
- Summary: Several messages/typos reduce clarity and polish (e.g., `Enginge`, `sanitzed`, log level `DEBUGGING`).
- Evidence/Refs:
  - backtester/core/clock.py:4-7 (“Enginge”),
  - backtester/strategy/order_validation.py:26 (“sanitzed”),
  - backtester/core/account.py:300-305 (`DEBUGGING` level),
  - backtester/core/clock.py:65 (“Invalid timeframe: ” missing the value).
- Suggested Fix:
  - Normalize wording/naming; include offending value in exceptions.
- Acceptance:
  - Grep-based checks for common typos pass; messages include relevant context.
- Tags: [DX] [Polish]

9) P2 — Mixed payloads on a single topic complicate consumers
-------------------------------------------------------------
- Summary: `account.trade` carries both `LotClosedEvent` and `TradeEvent`, forcing consumers to branch by type and undermining schema validation.
- Evidence/Refs:
  - backtester/core/account.py:225, 269 (LotClosedEvent), 332 (TradeEvent)
- Suggested Fix:
  - Split into `account.lot_closed` and `account.trade`, or standardize on one event and publish the other to a separate topic.
- Acceptance:
  - Single payload type per topic; contract test enforces homogeneity.
- Tags: [Bus] [Contracts]

10) P2 — Commented “BUG: First OrderIntent is immediately sanitized” without issue link
-------------------------------------------------------------------------------------
- Summary: Code comment flags a bug but lacks reproduction or tracking ticket, risking bit-rot.
- Evidence/Refs:
  - backtester/core/backtest_engine.py:18-23 (TODO/BUG comment).
- Suggested Fix:
  - Convert into a ticket under `tickets/` with repro and attach logs; reference the ticket ID in the comment.
- Acceptance:
  - Ticket exists with failing test or grep artifacts; comment links to it.
- Tags: [Process]

Notes
-----
- Several “stubs” (e.g., order validation rules) are intentional placeholders; they are not classified as bugs but should be guarded by feature flags or explicit “accept_all” config to avoid misleading behavior in demos.
