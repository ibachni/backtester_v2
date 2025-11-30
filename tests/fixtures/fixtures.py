import os
from pathlib import Path

from backtester.audit.audit import AuditWriter
from backtester.config.configs import AuditConfig, BusConfig, DownloaderConfig, RunContext

run_context_test_no_net = RunContext(run_id="TEST", seed=42, git_sha="TEST", allow_net=False)
run_context_test_net = RunContext(run_id="TEST", seed=42, git_sha="TEST", allow_net=True)
downloader_config = DownloaderConfig(
    base_dir=Path(os.path.join("test_output/download_test")),
    log_dir=Path(os.path.join("test_output/download_test")),
    download_run_id="TEST",
)
audit_config = AuditConfig(log_dir=Path("test_output/bus_test"))
bus_config = BusConfig()
bus_config_validate_schema = BusConfig(validate_schema=True)
canonical_audit_writer = AuditWriter(run_context_test_no_net, audit_config)
# canonical_bus = Bus(bus_config, canonical_audit_writer)
# canonical_bus_schema_val = Bus(bus_config_validate_schema, canonical_audit_writer)
