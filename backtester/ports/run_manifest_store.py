"""RunManifestStore Port Interface.

References: ADR-001 (Determinism), ADR-005 (Observability foundation).

Contract: Initialize run manifest (write-once per run).
"""

from __future__ import annotations

from typing import Any, Protocol


class RunManifestStore(Protocol):
    def init_run(self, manifest: dict[str, Any]) -> None: ...
