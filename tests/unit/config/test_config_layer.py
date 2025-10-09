from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Tuple

import pytest

from backtester.core.config_loader import ConfigLoader
from backtester.core.models import Config


@dataclass
class StubTelemetry:
    events: List[Tuple[str, Dict[str, Any]]] = field(default_factory=list)

    def log(self, event: str, **fields: Any) -> None:
        self.events.append((event, fields))


def _flatten_keys(tree: Mapping[str, Any], prefix: Tuple[str, ...] = ()) -> List[str]:
    flattened: List[str] = []
    for key, value in tree.items():
        path = prefix + (str(key),)
        if isinstance(value, Mapping):
            flattened.extend(_flatten_keys(value, path))
        else:
            flattened.append(".".join(path))
    return flattened


@pytest.mark.xfail(reason="Not implemented yet")
def test_config_precedence_simple_key() -> None:
    telemetry = StubTelemetry()
    loader = ConfigLoader(telemetry)

    defaults = Config().model_dump()
    defaults["risk"]["max_position"] = 1
    file_cfg = {"risk": {"max_position": 2}}
    env_cfg = {"risk": {"max_position": 3}}
    cli_overrides = {"risk": {"max_position": 4}}

    resolved = loader.resolve(
        defaults=defaults,
        file_cfg=file_cfg,
        env_cfg=env_cfg,
        cli_overrides=cli_overrides,
    )

    assert resolved.internal_config["risk"]["max_position"] == 4
    # Ensure leaf counting reflects flattened keys
    assert resolved.config_keys_total == len(_flatten_keys(resolved.internal_config))


@pytest.mark.xfail(reason="Not implemented yet")
def test_invalid_required_key_raises() -> None:
    telemetry = StubTelemetry()
    loader = ConfigLoader(telemetry)
    defaults = Config().model_dump()
    defaults.pop("symbols")

    with pytest.raises(KeyError) as excinfo:
        loader.resolve(defaults=defaults, file_cfg=None, env_cfg=None, cli_overrides=None)

    assert "symbols" in str(excinfo.value)


@pytest.mark.xfail(reason="Not implemented yet")
def test_secrets_redacted_in_manifest() -> None:
    telemetry = StubTelemetry()
    loader = ConfigLoader(telemetry)

    defaults = Config().model_dump()
    cli_overrides = {"secrets": {"api_key": "super-secret", "api_secret": "shh"}}

    resolved = loader.resolve(
        defaults=defaults,
        file_cfg=None,
        env_cfg=None,
        cli_overrides=cli_overrides,
    )

    assert resolved.redacted_config["secrets"]["api_key"] == "***REDACTED***"
    assert resolved.redacted_config["secrets"]["api_secret"] == "***REDACTED***"
    # Internal config retains clear-text values for downstream dependencies
    assert resolved.internal_config["secrets"]["api_key"] == "super-secret"
    assert resolved.internal_config["secrets"]["api_secret"] == "shh"


@pytest.mark.xfail(reason="Not implemented yet")
def test_config_hash_determinism() -> None:
    telemetry = StubTelemetry()
    loader = ConfigLoader(telemetry)
    defaults = Config().model_dump()

    resolved_a = loader.resolve(
        defaults=defaults,
        file_cfg={"risk": {"max_position": 7}},
        env_cfg={"secrets": {"api_key": "abc123"}},
        cli_overrides={"symbols": ["BTCUSDT", "ETHUSDT"]},
    )

    resolved_b = loader.resolve(
        defaults=defaults,
        file_cfg={"secrets": {"api_key": "abc123"}},
        env_cfg={"risk": {"max_position": 7}},
        cli_overrides={"symbols": ["BTCUSDT", "ETHUSDT"]},
    )

    assert resolved_a.internal_config == resolved_b.internal_config
    assert resolved_a.config_hash == resolved_b.config_hash
