import os
from pathlib import Path
from typing import Iterable

import pytest

from backtester.audit.audit import AuditWriter
from backtester.config.configs import AuditConfig, BusConfig, DownloaderConfig, RunContext

run_context_test_no_net = RunContext(run_id="TEST", seed=42, git_sha="TEST", allow_net=False)
run_context_test_net = RunContext(run_id="TEST", seed=42, git_sha="TEST", allow_net=True)
downloader_config = DownloaderConfig(
    base_dir=Path(os.path.join("tests/test_output/download_test")),
    log_dir=Path(os.path.join("tests/test_output/download_test")),
    download_run_id="TEST",
)
audit_config = AuditConfig(log_dir=Path("test_output/bus_test"))
bus_config = BusConfig()
bus_config_validate_schema = BusConfig(validate_schema=True)


def audit_writer_for(log_dir: Path | str, *, allow_net: bool = False) -> AuditWriter:
    """
    Lazy factory to avoid side effects at import time.
    Prefer using the pytest fixtures below in tests.
    """
    ctx = RunContext(run_id="TEST", seed=42, git_sha="TEST", allow_net=allow_net)
    cfg = AuditConfig(log_dir=Path(log_dir))
    return AuditWriter(ctx, cfg)


@pytest.fixture
def audit_writer_tmp(tmp_path: Path) -> Iterable[AuditWriter]:
    """
    Provides an AuditWriter that writes into a per-test temp directory.
    """
    writer = audit_writer_for(tmp_path)
    try:
        yield writer
    finally:
        writer.stop()
