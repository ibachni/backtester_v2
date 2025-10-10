"""bt CLI entrypoint.

Subcommands: backtest, shadow, paper, live (only backtest --noop implemented).

Writes a run manifest JSON and emits structured log lines (JSONL) to the run directory.

Determinism: For --noop mode we only depend on provided --seed and current UTC timestamp.
"""

# TODO Remove ENV overrides

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional

from backtester.adapters.jsonl import JsonlTelemetry
from backtester.core.config_loader import ConfigLoader
from backtester.core.models import Config
from backtester.core.other import insert_path
from backtester.ports.clock import Clock
from backtester.ports.run_manifest_store import RunManifestStore
from backtester.ports.telemetry import Telemetry

MANIFEST_SCHEMA_VERSION = 1
APP_VERSION = "0.0.1"
ENV_PREFIX = "BT_"


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
        sp.add_argument("--config", type=Path, required=False, help="Path to config overrides")
        sp.add_argument(
            "--set",
            dest="config_overrides",
            action="append",  # builds a Python list (config_overrides) containing each key=value
            default=[],
            metavar="KEY=VALUE",
            help="Override a config entry (may be repeated)",
        )

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


def _parse_cli_overrides(pairs: list[str]) -> Mapping[str, Any]:
    overrides: dict[str, Any] = {}

    for item in pairs:
        key, sep, value = item.partition("=")
        if sep == "":
            raise ValueError(f"--set requires KEY=VALUE format (got {item!r})")

        insert_path(overrides, key, value)  # use helper to expand dotted keys
    return overrides


def run_noop(
    clock: Clock,
    manifest_store: RunManifestStore,
    out: Path,
    seed: int,
    file_config: Optional[Path] = None,
    config_overrides: Optional[list[str]] = None,
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
    # 1. Default (valid for all strategies)
    # TODO Eventuall store defaults in a file (YAML, TOML)
    config_loader = ConfigLoader(telemetry)
    defaults = Config().model_dump()
    # 2. file config (valid for specific strategies)
    # TODO TEST (does that work?)
    if file_config:
        raw_text = Path(file_config).read_text()
        file_cfg: Mapping[str, Any] = json.loads(raw_text)
        if not isinstance(file_cfg, Mapping):
            telemetry.log(
                event="File_cfg non confirming",
                layer="Parse file cfg in run_noop",
                error=[
                    {
                        "code": "placeholder",
                        "path": file_config,
                        "message": "file_cfg is not a mapping",
                    }
                ],
            )
            raise TypeError("--config must deserialize to an object/dictionary")
    else:
        file_cfg = {}

    # 3. CLI Overrides
    if config_overrides:
        cli_overrides: Mapping[str, Any] = _parse_cli_overrides(config_overrides)
    else:
        cli_overrides = {}

    resolved = config_loader.resolve(
        defaults=defaults,
        file_cfg=file_cfg,
        cli_overrides=cli_overrides,
    )

    manifest: Dict[str, Any] = {
        "id": run_id,
        "timestamp": ts,
        "version": APP_VERSION,
        "seed": seed,
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "mode": "noop",
        "redacted_config": resolved.redacted_config,
        "config_hash": resolved.config_hash,
        "config_keys_total": resolved.config_keys_total,
    }
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
        return run_noop(clock, manifest_store, out_dir, args.seed, args.config_overrides)

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
