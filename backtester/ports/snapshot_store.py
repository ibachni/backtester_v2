"""SnapshotStore Port Interface.

References: ADR-014 (Storage), ADR-001.

Contract: Persist and retrieve latest snapshot.
"""

from __future__ import annotations

from typing import Any, Protocol


class SnapshotStore(Protocol):
    def write(self, snapshot: Any) -> None: ...
    def latest(self) -> Any | None: ...
