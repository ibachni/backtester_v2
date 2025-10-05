"""RunManifestStore Port Interface.

References: ADR-001 (Determinism), ADR-005 (Observability foundation).

Contract: Initialize run manifest (write-once per run).
"""

from __future__ import annotations

from typing import Any, Protocol


class RunManifestStore(Protocol):
    def init_run(self, manifest: dict[str, Any]) -> None: ...

    """
    Creare and save the manifest for a backtest or live run.
    This manifest is a dictioniary containing run ID, timestamp, version,
    seed, and schema version (captures exact exact configuration and context for every run)

    Function is called once at the start of each run to record these details in a file
    or storage sustem, making the run reproducible and traceable.

    See runs/example/run_manifest.json.
    """
