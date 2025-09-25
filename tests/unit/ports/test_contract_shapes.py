import importlib
import inspect

import pytest

# Mapping of module -> (ProtocolName, required_methods: {name: arity})
PORT_PROTOCOLS = {
    "backtester.ports.clock": ("Clock", {"now": 0}),
    "backtester.ports.event_bus": ("EventBus", {"publish": 1, "subscribe": 2}),
    "backtester.ports.market_data": ("MarketDataProvider", {"stream_bars": 1}),
    "backtester.ports.order_router": ("OrderRouter", {"submit": 1, "cancel": 1}),
    "backtester.ports.risk_engine": ("RiskEngine", {"pre_trade_check": 2}),
    "backtester.ports.snapshot_store": ("SnapshotStore", {"write": 1, "latest": 0}),
    "backtester.ports.run_manifest_store": ("RunManifestStore", {"init_run": 1}),
    "backtester.ports.portfolio_store": ("PortfolioStore", {"load": 0, "save": 1}),
    "backtester.ports.config_provider": ("ConfigProvider", {"get": 1}),
    "backtester.ports.secrets_provider": ("SecretsProvider", {"get": 1}),
    "backtester.ports.telemetry": ("Telemetry", {"log": -1}),  # variable kwargs
}


@pytest.mark.parametrize("module_name,meta", PORT_PROTOCOLS.items())
def test_required_port_signatures(module_name, meta):
    proto_name, methods = meta
    module = importlib.import_module(module_name)
    proto = getattr(module, proto_name)
    assert inspect.isclass(proto), f"{proto_name} not a class"
    # For each required method ensure existence and arg count (basic heuristic; -1 means skip)
    for method_name, arity in methods.items():
        fn = getattr(proto, method_name, None)
        assert fn is not None, f"Missing method {method_name} on {proto_name}"
        if arity >= 0:
            sig = inspect.signature(fn)
            # remove self / cls
            params = [p for p in sig.parameters.values() if p.kind == p.POSITIONAL_OR_KEYWORD][1:]
            assert (
                len(params) == arity
            ), f"{proto_name}.{method_name} expected {arity} args got {len(params)}"
