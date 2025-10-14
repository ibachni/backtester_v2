from typing import Any, Mapping

from backtester.core.models import Config
from backtester.core.other import insert_path

# --- test_bt.py -------------------------------------------------

parser_argument: list[str] = [
    "backtest",
    "--out",
    "some_dir",
    "--seed",
    "123",
    "--config",
    "new_path",
    "--set",
    "KEY==VALUE",
]


def _parse_cli_overrides(pairs: list[str]) -> Mapping[str, Any]:
    overrides: dict[str, Any] = {}

    for item in pairs:
        key, sep, value = item.partition("=")
        if sep == "":
            raise ValueError(f"--set requires KEY=VALUE format (got {item!r})")

        insert_path(overrides, key, value)  # use helper to expand dotted keys
    return overrides


# --- test_config_layer.py ---------------------------------------

# test_deep_merge()
# test_validate()
DefaultConfig = Config().model_dump()
FileConfig: dict[str, Any] = {
    "symbols": ["BTCUSDT"],
    "risk": {"max_position": 4, "allowed_symbols": ["BTCUSDT"]},
    "secrets": {"api_key": "api_key_string", "api_secret": None},
}
CLIConfig: dict[str, Any] = {}
env_cfg = {"risk": {"max_position": 3}}
