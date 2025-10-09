"""bt CLI entrypoint.

Subcommands: backtest, shadow, paper, live (only backtest --noop implemented).

Writes a run manifest JSON and emits structured log lines (JSONL) to the run directory.

Determinism: For --noop mode we only depend on provided --seed and current UTC timestamp.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict

from backtester.adapters.telemetry.jsonl import JsonlTelemetry
from backtester.core.config_loader import Config, ConfigLoader
from backtester.ports.clock import Clock
from backtester.ports.run_manifest_store import RunManifestStore
from backtester.ports.telemetry import Telemetry

MANIFEST_SCHEMA_VERSION = 1
APP_VERSION = "0.0.1"


class SystemClock:
    """Clock adapter that returns the current UTC time."""

    def now(self) -> datetime:
        """Return the current UTC timestamp."""
        return datetime.now(timezone.utc)


class FilesystemRunManifestStore:
    """Persist run manifests to the filesystem."""

    def __init__(self, out_dir: Path):
        """Create a manifest store rooted at `out_dir`."""
        self.out_dir = out_dir

    def init_run(self, manifest: dict[str, Any]) -> None:
        """Write the provided manifest to `run_manifest.json`."""
        self.out_dir.mkdir(parents=True, exist_ok=True)
        (self.out_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2))


def build_parser() -> argparse.ArgumentParser:
    """
    Return the top-level CLI argument parser.
    """
    p = argparse.ArgumentParser(prog="bt")
    sub = p.add_subparsers(dest="command", required=True)

    def add_common(sp: argparse.ArgumentParser) -> None:
        """Add arguments shared across all subcommands."""
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
    out: Path,
    seed: int,
    *,
    # allows for the injection of different telemetry implementations
    telemetry_factory: Callable[[str, Path, int, str], Telemetry] | None = None,
) -> int:
    """Execute the noop run flow and emit manifest + telemetry artifacts."""
    # Tag every run with unique identifiers and provenance.
    run_id = str(uuid.uuid4())
    git_sha = _resolve_git_sha()
    ts = clock.now().isoformat()

    # Initializes the telemetry
    telemetry = (
        telemetry_factory(run_id, out, seed, git_sha)
        if telemetry_factory
        else JsonlTelemetry(
            run_id=run_id,
            git_sha=git_sha,
            seed=seed,
            sink_path=out / "events.log.jsonl",
            # component="bt.cli",
        )
    )

    # TODO: Make canonical form!

    # Assemble manifest dict with metadata, etc.

    # Instantiate ConfigLoader and call package_results with required arguments
    if type(telemetry) is JsonlTelemetry:
        config_loader = ConfigLoader(telemetry)
        config = Config().model_dump()

        # Provide dummy/default values for file_cfg, env_cfg, cli_overrides as needed
        # TODO Inject file_cfg, env_cfg, CLI
        file_cfg = {}
        env_cfg = {}
        cli_overrides = {}
        return_config = config_loader.resolve(
            defaults=config, file_cfg=file_cfg, env_cfg=env_cfg, cli_overrides=cli_overrides
        )

        manifest: Dict[str, Any] = {
            "id": run_id,
            "timestamp": ts,
            "version": APP_VERSION,
            "seed": seed,
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "mode": "noop",
            "redacted_config": return_config.redacted_config,
            "config_hash": return_config.config_hash,
            "config_keys_total": return_config.config_keys_total,
        }
        #
        manifest_store.init_run(manifest)

        telemetry.log("cli_invocation", command="backtest", mode="noop")
        telemetry.log("run_manifest_written", path=str(out / "run_manifest.json"))
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint wrapper compatible with setuptools scripts."""
    parser = build_parser()
    args = parser.parse_args(argv)
    clock = SystemClock()
    out_dir: Path = args.out
    manifest_store = FilesystemRunManifestStore(out_dir)

    # running the "noop" pipeline
    if args.command == "backtest" and getattr(args, "noop", False):
        return run_noop(clock, manifest_store, out_dir, args.seed)

    # Logs that requested mode is not implemented and exists with error
    else:
        telemetry = JsonlTelemetry(
            run_id="bootstrap",
            git_sha=_resolve_git_sha(),
            seed=args.seed,
            sink_path=out_dir / "events.log.jsonl",
            # component="bt.cli",
        )
        telemetry.log("unimplemented_mode", command=args.command)
        print(f"Mode '{args.command}' not implemented yet", file=sys.stderr)
        return 2


def _resolve_git_sha() -> str:
    """
    Best-effort (i.e., tries to, but does not guarantee)
    resolution of the current git commit hash.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
