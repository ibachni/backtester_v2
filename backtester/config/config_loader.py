"""
Purpose:
    - Loads a config file
    - Validate the config file

Steps:
1. Input & parsing
    - Read from .toml
    - Multiple sources (base file, profile file, CLI etc)
    - Env interpolation
2. Normalization
    - Canonical types
    - Defaults: Fill missing optional fields with explicit defaults
    - Dedup (e.g., Symbols)
3. Validation
    - Schema validation
    - cross-field rules
        - move to instance creation or individual modules
    - referential checks
        - file/dir existence,
        - readable data files etc.
    - Unknown keys forbidden
4. Composition & overrides:
    - Layered merge
5. Security & hygiene
    - secrets handling
    - no code execution
6. Outputs:
    - Frozen, typed object: immutable Pydantic model or frozen dataclass
    - effective config dump: write endresult to JSON for reproducibility
    - hashing: deterministic hash for effective config
7. Telemetry hook
    - emit load/ validate timings
8. Version gate; refuse incompatible versions
"""

import tomllib
from pathlib import Path

from pydantic import BaseModel


class LoadedConfig(BaseModel):
    pass


class ConfigLoader:
    """
    Config-loader; loading toml file.
    """

    def __init__(self, base_dir: str) -> None:
        self._base_dir = base_dir

    def load(self, file_name: str) -> dict:
        path = Path(self._base_dir + "/" + file_name)

        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with path.open("rb") as f:
            return tomllib.load(f)

    def validate(self, cfg: LoadedConfig) -> bool:
        return True

    def dump_json(self, cfg: LoadedConfig) -> None:
        return None

    def hash(self, cfg: LoadedConfig) -> str:
        return "1"
