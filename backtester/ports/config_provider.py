"""ConfigProvider Port Interface.

References: ADR-019 (Config & Secrets mgmt).

Contract: Retrieve configuration values by key.
"""

from __future__ import annotations

from typing import Any, Protocol


class ConfigProvider(Protocol):
    def get(self, key: str) -> Any: ...

    """
    Fetch a configuration value using a key (a string). It returns the value
    associated with that key from the system's configuration source
    (e.g., file, environment variable. or settings object).

    This allows the application to access settings and options in a
    consisten way.
    """
