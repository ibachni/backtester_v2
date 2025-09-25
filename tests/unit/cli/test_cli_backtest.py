import json
import subprocess
from pathlib import Path

# GIVEN no run directory exists
# WHEN invoking `bt backtest --noop --out <dir>`
# THEN run_manifest.json should be created with required keys

REQUIRED_KEYS = {"id", "timestamp", "version", "seed", "schema_version", "mode"}


def test_cli_backtest_noop_creates_manifest(tmp_path: Path):
    out_dir = tmp_path / "noop_run"
    cmd = ["bt", "backtest", "--noop", "--out", str(out_dir)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert (
        proc.returncode == 0
    ), f"Expected exit 0 after implementation, got {proc.returncode}. stderr={proc.stderr}"
    manifest = json.loads((out_dir / "run_manifest.json").read_text())
    assert REQUIRED_KEYS.issubset(manifest.keys())


def test_noop_twice_same_seed_identical_manifest_except_timestamp(tmp_path: Path):
    out1 = tmp_path / "run1"
    out2 = tmp_path / "run2"
    seed = 123
    cmd1 = ["bt", "backtest", "--noop", "--out", str(out1), "--seed", str(seed)]
    cmd2 = ["bt", "backtest", "--noop", "--out", str(out2), "--seed", str(seed)]
    p1 = subprocess.run(cmd1, capture_output=True, text=True)
    p2 = subprocess.run(cmd2, capture_output=True, text=True)
    assert p1.returncode == 0 and p2.returncode == 0
    m1 = json.loads((out1 / "run_manifest.json").read_text())
    m2 = json.loads((out2 / "run_manifest.json").read_text())
    # Compare excluding timestamp and id which are run-specific
    for k in ["timestamp", "id"]:
        m1.pop(k, None)
        m2.pop(k, None)
    assert m1 == m2, f"Manifests differ for same seed: \n{m1}\nvs\n{m2}"
