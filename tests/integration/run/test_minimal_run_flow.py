import json
import subprocess
from pathlib import Path

REQUIRED_KEYS = {"id", "timestamp", "version", "seed"}


def test_noop_run_produces_artifacts(tmp_path: Path):
    out_dir = tmp_path / "artifact_run"
    cmd = ["bt", "backtest", "--noop", "--out", str(out_dir), "--seed", "123"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode == 0, f"Expected exit 0 after implementation, got {proc.returncode}. stderr={proc.stderr}"
    manifest_path = out_dir / "run_manifest.json"
    assert manifest_path.exists(), "run_manifest.json missing"
    manifest = json.loads(manifest_path.read_text())
    assert REQUIRED_KEYS.issubset(manifest.keys())
