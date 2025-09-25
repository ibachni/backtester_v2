import importlib

import pytest

PORT_MODULES = [
    "backtester.ports.clock",
    "backtester.ports.event_bus",
    "backtester.ports.market_data",
    "backtester.ports.order_router",
    "backtester.ports.risk_engine",
    "backtester.ports.snapshot_store",
    "backtester.ports.run_manifest_store",
    "backtester.ports.portfolio_store",
    "backtester.ports.config_provider",
    "backtester.ports.secrets_provider",
    "backtester.ports.telemetry",
]


@pytest.mark.parametrize("module_name", PORT_MODULES)
def test_all_ports_import(module_name):
    # Will fail until files exist
    assert importlib.import_module(module_name)
