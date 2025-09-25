THIS FILE IS NOT VALID. only AGENTS.md is valid.

---
applyTo: '**'
---
## Backtester V2 – AI Contributor Guide

This repository is currently a scaffold (no domain code yet). Use this guide to add the first real components in a consistent, extensible way. Keep instructions concise, pragmatic, and tied to this codebase’s emerging shape.

### Intended Architecture (to establish now)
1. Core engine: Portfolio + Positions + Order + Fill models, event queue, clock.
2. Data layer: Pluggable market data providers (historical CSV, API, synthetic generator) behind a common interface.
3. Strategy layer: User strategies implement a small interface (e.g. on_bar(data), on_fill(fill)).
4. Execution simulator: Converts strategy intents (orders) into fills with latency + slippage models.
5. Metrics & reporting: Incremental performance tracker (PNL, drawdown, exposure) + final report exporter (Markdown/HTML/CSV).

Prefer narrow, composable modules over a monolith. Each layer should have an interface/protocol first, then concrete implementations.

### Directory Layout (proposed as you implement)
backtester_v2/
	src/
		core/ (engine loop, events, domain models)
		data/ (data source abstractions + adapters)
		strategy/ (base strategy class + examples)
		exec/ (order routing & fill simulation)
		metrics/ (performance aggregation, risk stats)
		reports/ (renderers / exporters)
	tests/ (mirror src/ structure)

Create this structure incrementally; don’t add empty folders—add when first file lands.

### Coding Conventions
- Language: Assume Python 3.12 unless a different runtime is introduced; add `pyproject.toml` when first code is added.
- Use type hints everywhere; expose minimal public surface in `__all__` for each package.
- Prefer dataclasses (frozen where possible) for immutable domain entities: Order, Fill, Bar, PositionSnapshot.
- Event-driven core: An Engine processes a priority queue of Events (TIME, MARKET_DATA, ORDER, FILL, METRIC_FLUSH). Each event is a lightweight dataclass.
- Dependency inversion: Strategies receive abstract interfaces (e.g. MarketDataFeed, Broker) rather than concrete classes.
- Configuration: Accept a single EngineConfig dataclass; avoid scattering kwargs.

### Incremental Implementation Order (recommended for first PRs)
1. Define domain models (core/datamodel.py) + event types.
2. Implement Engine skeleton with event dispatch & simple loop.
3. Add a CSV historical data feed (data/csv_feed.py) yielding Bar events.
4. Implement simple strategy example (strategy/buy_and_hold.py).
5. Add in-memory execution simulator (exec/simulator.py) supporting market orders.
6. Add metrics accumulator (metrics/performance.py) tracking equity curve.
7. Provide a CLI entry point (e.g. `python -m backtester_v2.run demo_config.yaml`).

### Testing Patterns
- Use pytest; one test module per component: `tests/core/test_engine.py`, etc.
- Property-style checks for conservation (cash + market value consistency after fills).
- Deterministic seeds for any randomness (e.g. slippage).

### Performance & Extensibility Notes
- Keep the event loop allocation-light: reuse objects where safe; avoid pandas inside the inner loop (parse externally, feed primitive types / small dataclasses).
- Data providers must stream (generators / iterators) rather than preload large datasets.

### Documentation & Examples
- Every new public class: module docstring + short usage snippet.
- Provide an `examples/` script once Engine + first strategy are stable.

### What NOT to do
- Don’t bake strategy-specific logic into the engine.
- Don’t couple data ingestion to execution—keep feeds pure.
- Don’t introduce heavy frameworks prematurely; standard library + lightweight libs only (e.g. `pydantic` optional for config validation later).

### When Unsure
Add a small ADR (Architecture Decision Record) in `docs/adr/NNN-short-title.md` summarizing the decision (context, options, decision, consequences). Keep under 200 words.

### Planner Chat Mode Integration
`/.github/chatmodes/planner.chatmode.md` exists: when the user selects Planner mode, output ONLY a plan (no code edits). Normal mode may implement the plan.

### Ready for Next Steps
If adding real code, first create `pyproject.toml` with dependencies (pytest). Then proceed with Step 1 above.

---
Keep this guide updated as real modules are added; document only what actually exists (remove speculative sections once implemented or adjust to match reality).
