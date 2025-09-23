"""Telemetry Port Interface.

References: ADR-005 (Observability foundation), ADR-020 (Minimal metrics & alerts).

Contract: Log structured events (later metrics/traces). For this slice only a log(event, **fields) stub.
"""
from __future__ import annotations
from typing import Protocol, Any

class Telemetry(Protocol):
    def log(self, event: str, **fields: Any) -> None: ...
