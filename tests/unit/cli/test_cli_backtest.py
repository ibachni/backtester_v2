import json
import os
import subprocess
from pathlib import Path

# GIVEN no run directory exists
# WHEN invoking `bt backtest --noop --out <dir>`
# THEN run_manifest.json should be created with required keys

REQUIRED_KEYS = {"id", "timestamp", "version", "seed"}


def test_cli_backtest_noop_creates_manifest(tmp_path: Path):
    out_dir = tmp_path / "noop_run"
    cmd = ["bt", "backtest", "--noop", "--out", str(out_dir)]
    # This will currently fail because `bt` entrypoint & code do not exist yet.
    proc = subprocess.run(cmd, capture_output=True, text=True)
    # Expect non-zero for now (will assert 0 after implementation) so force failure to mark red state.
    assert proc.returncode == 0, f"Expected exit 0 after implementation, got {proc.returncode}. stderr={proc.stderr}"
    manifest = json.loads((out_dir / "run_manifest.json").read_text())
    assert REQUIRED_KEYS.issubset(manifest.keys())
