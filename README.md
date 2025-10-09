Backtester v2 (Slice 0000)
=========================

Deterministic trading backtester skeleton. This initial slice provides:

- Port interface Protocols (Clock, EventBus, MarketDataProvider, OrderRouter, RiskEngine, SnapshotStore, RunManifestStore, PortfolioStore, ConfigProvider, SecretsProvider, Telemetry)
- CLI entrypoint `bt` with subcommands: backtest, shadow, paper, live (only `backtest --noop` implemented)
- Run manifest writer + JSONL telemetry stub.

Quick Start (development)
------------------------

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
bt backtest --noop --out runs/example --seed 42
cat runs/example/run_manifest.json
```

CLI Usage
---------

After installation (`pip install -e .`), the `bt` console script is available. You can also invoke the module directly with `python -m backtester.cli.bt`.

```bash
# Display available commands
bt --help

# Run the noop pipeline (no market data), writing artifacts under runs/my_run
bt backtest --noop --out runs/my_run --seed 123

# Inspect telemetry emitted during the run
cat runs/my_run/events.log.jsonl

# Other modes are stubs today, but share the same flags
bt shadow --noop --out runs/shadow_probe --seed 99
```

Manifest Fields:

```json
{
	"id": "<uuid>",
	"timestamp": "<UTC ISO>",
	"version": "0.0.1",
	"seed": 42,
	"schema_version": 1,
	"mode": "noop"
}
```

Next Slices:
------------
See `docs/slices/` for planned incremental functionality (strategy loop, risk rails, observability, adapters).

Contributing / CI parity
-----------------------
- Lint: `ruff check . && ruff format --check .`
- Types: `mypy .`
- Tests: `pytest -q`
