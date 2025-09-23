"""SecretsProvider Port Interface.

References: ADR-019 (Config & Secrets).

Contract: Retrieve secret material by logical name; no persistence here.
"""
from __future__ import annotations
from typing import Protocol

class SecretsProvider(Protocol):
    def get(self, secret_name: str) -> str: ...
