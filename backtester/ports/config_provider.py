"""ConfigProvider Port Interface.

References: ADR-019 (Config & Secrets mgmt).

Contract: Retrieve configuration values by key.
"""
from __future__ import annotations
from typing import Protocol, Any

class ConfigProvider(Protocol):
    def get(self, key: str) -> Any: ...
