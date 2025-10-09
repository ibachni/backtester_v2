"""JSON Lines Telemetry adapter.

Provides a deterministic, context-aware implementation of the Telemetry port by
writing structured JSON objects (one per line) to disk.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping


class JsonlTelemetry:
    _REDACTION_TOKEN = "***REDACTED***"
    _DEFAULT_SECRET_KEYS = frozenset(
        {
            "api_key",
            "api_secret",
            "secret",
            "password",
            "token",
            "auth_token",
        }
    )

    def __init__(
        self,
        run_id: str,
        git_sha: str,
        seed: int,
        sink_path: Path,
        secret_keys: Iterable[str] = _DEFAULT_SECRET_KEYS,
    ) -> None:
        self._run_id = str(run_id)
        self._git_sha = str(git_sha)
        self._seed = seed
        self._sink_path = sink_path if isinstance(sink_path, Path) else Path(sink_path)
        self._secret_keys = secret_keys

    def log(self, event: str, **fields) -> None:
        extras = dict(fields)

        # Tagging telemetry events with their subsystem (origin)
        # component = extras.pop("component", self._component)

        sanitized_fields, redacted = self._sanitize_fields(extras)

        record: dict[str, Any] = {
            "event": event,
            "ts_utc": "clock",  # Requires CLOCK implementation!
            "run_id": self._run_id,
            "git_sha": self._git_sha,
            "seed": self._seed,
            **sanitized_fields,
        }

        # if component is not None:
        #    record["component"] = component
        if redacted:
            record["redacted_fields"] = sorted(redacted)

        self._write_record(record)

    def _sanitize_fields(self, fields: Mapping[str, Any]) -> tuple[dict[str, Any], set[str]]:
        sanitized: dict[str, Any] = {}
        redacted: set[str] = set()
        for key, value in fields.items():
            if key in self._secret_keys:
                sanitized[key] = self._REDACTION_TOKEN
                redacted.add(key)
            else:
                sanitized[key] = value

        return sanitized, redacted

    def _write_record(self, record: Mapping[str, Any]) -> None:
        payload = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        with self._sink_path.open("a", encoding="utf-8") as handle:
            handle.write(payload + "\n")
