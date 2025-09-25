"""bt CLI entrypoint.

Subcommands: backtest, shadow, paper, live (only backtest --noop implemented).

Writes a run manifest JSON and emits structured log lines (JSONL) to the run directory.

Determinism: For --noop mode we only depend on provided --seed and current UTC timestamp.
Observability: Every log record contains common fields: run_id, git_sha, seed, component, ts_utc.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from backtester.ports.clock import Clock
from backtester.ports.run_manifest_store import RunManifestStore
from backtester.ports.telemetry import Telemetry

MANIFEST_SCHEMA_VERSION = 1
APP_VERSION = "0.0.1"


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class FilesystemRunManifestStore:
    def __init__(self, out_dir: Path):
        self.out_dir = out_dir

    def init_run(self, manifest: dict[str, Any]) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        (self.out_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2))


class JsonlTelemetry:
    """Very small JSONL telemetry that injects common fields to each record."""

    def __init__(self, out_dir: Path, base_fields: dict[str, Any] | None = None):
        self.out_dir = out_dir
        self._log_path = self.out_dir / "events.log.jsonl"
        self._base_fields: dict[str, Any] = base_fields or {}
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def log(self, event: str, **fields: Any) -> None:
        record: dict[str, Any] = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "event": event,
            **self._base_fields,
            **fields,
        }
        with self._log_path.open("a") as f:
            f.write(json.dumps(record) + "\n")


def _get_git_sha() -> str:
    """Return the current git SHA (short), or 'unknown' if not available."""
    try:
        res = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        sha = res.stdout.strip()
        return sha or "unknown"
    except Exception:
        return "unknown"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="bt")
    sub = p.add_subparsers(dest="command", required=True)

    def add_common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--out", type=Path, required=True, help="Output run directory")
        sp.add_argument("--seed", type=int, default=42, help="Determinism seed")

    # backtest
    bt = sub.add_parser("backtest", help="Run a backtest pipeline")
    add_common(bt)
    bt.add_argument("--noop", action="store_true", help="Run noop pipeline (no data)")

    # shadow/paper/live placeholders
    for name in ("shadow", "paper", "live"):
        sp = sub.add_parser(name, help=f"{name} mode (not implemented)")
        add_common(sp)
        sp.add_argument("--noop", action="store_true")
    return p


def run_noop(
    clock: Clock,
    manifest_store: RunManifestStore,
    telemetry: Telemetry,
    out: Path,
    seed: int,
    run_id: str,
) -> int:
    ts = clock.now().isoformat()
    manifest: Dict[str, Any] = {
        "id": run_id,
        "timestamp": ts,
        "version": APP_VERSION,
        "seed": seed,
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "mode": "noop",
    }
    manifest_store.init_run(manifest)
    telemetry.log("cli_invocation", command="backtest", mode="noop")
    telemetry.log("run_manifest_written", path=str(out / "run_manifest.json"))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    clock = SystemClock()
    out_dir: Path = args.out
    # Common observability context
    run_id = str(uuid.uuid4())
    git_sha = _get_git_sha()
    seed: int = getattr(args, "seed", 42)
    telemetry = JsonlTelemetry(
        out_dir,
        base_fields={
            "run_id": run_id,
            "git_sha": git_sha,
            "seed": seed,
            "component": "cli.bt",
        },
    )
    manifest_store = FilesystemRunManifestStore(out_dir)

    if args.command == "backtest" and getattr(args, "noop", False):
        return run_noop(clock, manifest_store, telemetry, out_dir, seed, run_id)
    else:
        telemetry.log("unimplemented_mode", command=args.command)
        print(f"Mode '{args.command}' not implemented yet", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
