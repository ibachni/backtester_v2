import os
from pathlib import Path

from backtester.config.configs import DownloaderConfig, RunContext

run_context_test_no_net = RunContext(run_id="TEST", seed=42, git_sha="TEST", allow_net=False)
run_context_test_net = RunContext(run_id="TEST", seed=42, git_sha="TEST", allow_net=True)
downloader_config = DownloaderConfig(
    base_dir=Path(os.path.join("test_output/download_test")),
    log_dir=Path(os.path.join("test_output/download_test")),
    download_run_id="TEST",
)
