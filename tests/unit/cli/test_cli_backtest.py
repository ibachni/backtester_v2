import json
import subprocess
from pathlib import Path

from backtester.cli.bt import ENV_PREFIX, _collect_env_config

# GIVEN no run directory exists
# WHEN invoking `bt backtest --noop --out <dir>`
# THEN run_manifest.json should be created with required keys

REQUIRED_KEYS = {"id", "timestamp", "version", "seed"}


def test_cli_backtest_noop_creates_manifest(tmp_path: Path):
    out_dir = tmp_path / "noop_run"
    cmd = ["bt", "backtest", "--noop", "--out", str(out_dir)]
    # This will currently fail because `bt` entrypoint & code do not exist yet.
    proc = subprocess.run(cmd, capture_output=True, text=True)
    # Expect non-zero for now (will assert 0 after implementation) so force failure to mark red
    # state.
    assert (
        proc.returncode == 0
    ), f"Expected exit 0 after implementation, got {proc.returncode}. stderr={proc.stderr}"
    manifest = json.loads((out_dir / "run_manifest.json").read_text())
    assert REQUIRED_KEYS.issubset(manifest.keys())


def test_collect_env_config_builds_nested_mapping():
    environ = {
        f"{ENV_PREFIX}RISK__MAX_POSITION": "7",
        f"{ENV_PREFIX}SECRETS__API_KEY": "super-secret",
        "IGNORED": "value",
    }

    env_cfg = _collect_env_config(environ, prefix=ENV_PREFIX)
    assert env_cfg == {
        "risk": {"max_position": "7"},
        "secrets": {"api_key": "super-secret"},
    }


def test_collect_env_config_returns_none_for_missing_prefix():
    assert _collect_env_config({"OTHER": "value"}, prefix=ENV_PREFIX) is None
