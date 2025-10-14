from pathlib import Path

from backtester.cli.bt import build_parser

REQUIRED_KEYS = {"id", "timestamp", "version", "seed"}


def test_build_parser():
    p = build_parser()
    assert p.prog == "bt"
    test = p.parse_args(
        [
            "backtest",
            "--out",
            "some_dir",
            "--seed",
            "123",
            "--config",
            "new_path",
            "--set",
            "KEY=VALUE",
        ]
    )
    assert test.command == "backtest"
    assert test.out == Path("some_dir")
    assert test.seed == 123
    assert test.config == Path("new_path")
    assert test.config_overrides == ["KEY=VALUE"]
    assert test.noop is False


def test_parse_cli_overrides():
    pass


# Testing FileSystsemRunManifestStore
def file_exists_in_dir(dir_path, filename):
    return (Path(dir_path) / filename).exists()


# def test_cli_backtest_noop_creates_manifest(tmp_path: Path):
#     out_dir = tmp_path / "noop_run"
#     cmd = ["bt", "backtest", "--noop", "--out", str(out_dir)]
#     # This will currently fail because `bt` entrypoint & code do not exist yet.
#     proc = subprocess.run(cmd, capture_output=True, text=True)
#     # Expect non-zero for now (will assert 0 after implementation) so force failure to mark red
#     # state.
#     assert (
#         proc.returncode == 0
#     ), f"Expected exit 0 after implementation, got {proc.returncode}. stderr={proc.stderr}"
#     manifest = json.loads((out_dir / "run_manifest.json").read_text())
#     assert REQUIRED_KEYS.issubset(manifest.keys())


# --- test _parse_cli_overrides -------------------------------------------------------------------
