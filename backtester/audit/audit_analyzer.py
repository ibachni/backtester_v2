from __future__ import annotations

import json
from pathlib import Path
from typing import Counter, Iterator

from backtester.types.aliases import AuditRecord


class AuditLogAnalyzer:
    """
    Load NDJSON audit logs and compute simple aggregates.

    Example
    -------
    >>> analyzer = AuditLogAnalyzer(\"runs/audit/1762096175969.ndjson\")
    >>> analyzer.level_counts()
    {'INFO': 3055, 'WARN': 794, 'DEBUG': 2}
    >>> for rec in analyzer.filter_records(component=\"strategy\", event=\"STRATEGY_INTENTS\",
    symbol=\"BTCUSDT\"):
    ...     print(rec[\"sim_time\"], rec[\"payload\"][\"count\"])

    # TODO Goal: Attach parent_ids, track order to final status
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    def iter_records(self) -> Iterator[AuditRecord]:
        """Yield every JSON record in the audit file as a dict."""
        with self._path.open() as fh:
            for idx, raw in enumerate(fh, start=1):
                line = raw.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"Invalid JSON at line {idx} in {self._path}") from exc

    def head(self, n: int = 5) -> list[AuditRecord]:
        """Return the first ``n`` decoded records from the log (default 5)."""
        out: list[AuditRecord] = []
        for record in self.iter_records():
            out.append(record)
            if len(out) >= n:
                break
        return out

    def filter_records(
        self,
        *,
        component: str | None = None,
        level: str | None = None,
        event: str | None = None,
        symbol: str | None = None,
        order_id: str | None = None,
    ) -> Iterator[AuditRecord]:
        """
        Stream records matching the supplied filters.

        Parameters
        ----------
        component, level, event, symbol:
            Optional equality filters applied to the respective record fields.
        """
        for record in self.iter_records():
            if component and record.get("component") != component:
                continue
            if level and record.get("level") != level:
                continue
            if event and record.get("event") != event:
                continue
            if symbol and record.get("symbol") != symbol:
                continue
            if order_id and record.get("order_id") != order_id:
                continue
            yield record

    def level_counts(self) -> dict[str, int]:
        """Return counts grouped by record ``level``."""
        return self._count_by("level")

    def component_counts(self) -> dict[str, int]:
        """Return counts grouped by ``component``."""
        return self._count_by("component")

    def event_counts(self) -> dict[str, int]:
        """Return counts grouped by ``event`` name."""
        return self._count_by("event")

    def warning_topics(self, event_filter: str | None = None) -> dict[str, int]:
        """
        Summarize WARN-level records by bus ``topic``.

        Parameters
        ----------
        event_filter:
            If set, only consider warnings for matching ``event`` values.
        """
        counts: Counter[str] = Counter()
        for record in self.iter_records():
            if record.get("level") != "WARN":
                continue
            if event_filter and record.get("event") != event_filter:
                continue
            payload = record.get("payload") or {}
            topic = payload.get("topic")
            if topic:
                counts[str(topic)] += 1
        return dict(counts)

    def sim_time_bounds(self) -> tuple[int | None, int | None]:
        """Return the first and last non-null ``sim_time`` values encountered."""
        first: int | None = None
        last: int | None = None
        for record in self.iter_records():
            stamp = record.get("sim_time")
            if stamp is None:
                continue
            if first is None:
                first = stamp
            last = stamp
        return first, last

    def _count_by(self, key: str) -> dict[str, int]:
        """Internal helper to aggregate counts for any scalar record field."""
        counts: Counter[str] = Counter()
        for record in self.iter_records():
            value = record.get(key)
            if value is None:
                continue
            counts[str(value)] += 1
        return dict(counts)
