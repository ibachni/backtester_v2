from __future__ import annotations

import json
import time
from collections import Counter
from datetime import datetime, timezone
from functools import singledispatchmethod
from pathlib import Path
from typing import Any, Iterator, Optional, Union

from backtester.config.configs import AuditConfig, RunContext
from backtester.types.types import (
    AuditRecord,
    Candle,
    ControlEvent,
    Fill,
    LimitOrderIntent,
    MarketOrderIntent,
    OrderAck,
    OrderIntent,
    StopLimitOrderIntent,
    StopMarketOrderIntent,
    ValidatedLimitOrderIntent,
    ValidatedMarketOrderIntent,
    ValidatedOrderIntent,
    ValidatedStopLimitOrderIntent,
    ValidatedStopMarketOrderIntent,
)


class AuditWriter:
    """
    Simple NDJSON audit logger. Write dict-like events to a file, one JSON per line.

    Core functionality:
    - Append-only JSONL
    - Consistent schema:

    component
        What it is: the subsystem emitting the line.
        Why: lets you filter noise (e.g., keep exec_sim INFO, strategy DEBUG).

    level
        What it is: importance/severity for filtering.
        Suggested ladder (monotone):
            DEBUG:  very chatty, step-by-step traces
            INFO: normal lifecycle events (BAR, ACK, FILL, CANCEL)
            WARN: unusual but non-fatal (dropped log lines, truncated payloads, skipped notional
            check)
            ERROR: something failed (writer I/O error, irrecoverable validation bug)

    parent_id:
        - Causal linkage, ties event to the immediately preceding event in its causal chain
        - Candle -> None
        - OrderIntent -> Candle
        - ACK / CANCEL -> OrderIntent
        - FILL -> ORDERIntent
        - For derived logs, e.g., account snapshot

    # TODO: Addition of _should_log, which compares the level by component with the provided level
    # Goal: Determine whether it is worth logging.

    """

    def __init__(
        self, run_ctx: RunContext, cfg: AuditConfig, path: str, run_id: Optional[str] = None
    ) -> None:
        self._run_ctx = run_ctx
        self._cfg = cfg
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self._path.open("a")
        self._run_id = run_id or str(int(time.time()))
        self._seq = 0

    def emit(
        self,
        *,
        component: str,
        event: str,
        simple: bool = False,
        level: str = "INFO",
        sim_time: Optional[int] = None,
        payload: Any = None,
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        parent_id: Optional[str] = None,
    ) -> None:
        self._seq += 1
        rec_1 = {
            "seq": self._seq,
            "component": component,
            "level": level,
            "event": event,
            "payload": payload or {},
        }
        rec_2 = {
            "symbol": symbol,
            "order_id": order_id,
            "parent_id": parent_id,
        }

        rec_3 = {
            "sim_time": sim_time,
            "schema_ver": 1,
            "run_id": self._run_id,
            "ts_wall": datetime.now(timezone.utc).isoformat(),
        }
        if simple:
            rec = rec_1 | rec_3
        else:
            rec = rec_1 | rec_2 | rec_3
        line = json.dumps(rec, default=str)
        self._fh.write(line + "\n")
        self._fh.flush()
        self._fh.flush()

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass

    @property
    def run_id(self) -> str:
        return self._run_id

    # --- OrderIntent to payload ---

    def market_intent_to_payload(
        self, intent: Union[MarketOrderIntent, ValidatedMarketOrderIntent]
    ) -> dict[str, Any]:
        return {
            "reduce_only": intent.reduce_only,
            "validated": getattr(intent, "validated", None),
        } | self.abc_intent_to_payload(intent)

    def limit_intent_to_payload(
        self, intent: Union[LimitOrderIntent, ValidatedLimitOrderIntent]
    ) -> dict[str, Any]:
        return {
            "price": intent.price,
            "reduce_only": intent.reduce_only,
            "post_only": intent.post_only,
            "validated": getattr(intent, "validated", None),
        } | self.abc_intent_to_payload(intent)

    def stop_market_intent_to_payload(
        self, intent: Union[StopMarketOrderIntent, ValidatedStopMarketOrderIntent]
    ) -> dict[str, Any]:
        return {
            "stop_price": intent.stop_price,
            "reduce_only": intent.reduce_only,
            "post_only": intent.post_only,
            "validated": getattr(intent, "validated", None),
        } | self.abc_intent_to_payload(intent)

    def stop_limit_intent_to_payload(
        self, intent: Union[StopLimitOrderIntent, ValidatedStopLimitOrderIntent]
    ) -> dict[str, Any]:
        return {
            "price": intent.price,
            "stop_price": intent.stop_price,
            "reduce_only": intent.reduce_only,
            "post_only": intent.post_only,
            "validated": getattr(intent, "validated", None),
        } | self.abc_intent_to_payload(intent)

    def abc_intent_to_payload(
        self, intent: Union[OrderIntent, ValidatedOrderIntent]
    ) -> dict[str, Any]:
        return {
            "symbol": intent.symbol,
            "market": str(intent.market),
            "side": str(intent.side),
            "id": intent.id,
            "ts_utc": str(intent.ts_utc),
            "qty": str(intent.qty),
            "strategy_id": intent.strategy_id,
            "tif": str(intent.tif),
            "tags": intent.tags,
        }

    @singledispatchmethod
    def intent_to_payload(self, intent: OrderIntent) -> dict[str, Any]:
        """Fallback for unknown OrderIntent subtypes."""
        return self.abc_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: ValidatedOrderIntent) -> dict[str, Any]:
        return self.abc_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: MarketOrderIntent) -> dict[str, Any]:
        return self.market_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: ValidatedMarketOrderIntent) -> dict[str, Any]:
        return self.market_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: LimitOrderIntent) -> dict[str, Any]:
        return self.limit_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: ValidatedLimitOrderIntent) -> dict[str, Any]:
        return self.limit_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: StopMarketOrderIntent) -> dict[str, Any]:
        return self.stop_market_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: ValidatedStopMarketOrderIntent) -> dict[str, Any]:
        return self.stop_market_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: StopLimitOrderIntent) -> dict[str, Any]:
        return self.stop_limit_intent_to_payload(intent)

    @intent_to_payload.register
    def _(self, intent: ValidatedStopLimitOrderIntent) -> dict[str, Any]:
        return self.stop_limit_intent_to_payload(intent)

    # --- Other type to payload conversion ---

    def ack_to_payload(self, ack: OrderAck) -> dict[str, Any]:
        raw_tags = getattr(ack, "tags", None)
        if isinstance(raw_tags, dict):
            tags: dict[str, Any] = dict(raw_tags)
        else:
            tags = {} if raw_tags is None else {"_raw": repr(raw_tags)}

        return {
            "intent_id": ack.intent_id,
            "strategy_id": getattr(ack, "strategy_id", None),
            "component": getattr(ack, "component", None),
            "symbol": getattr(ack, "symbol", None),
            "side": str(getattr(ack, "side", None)),
            "market": str(getattr(ack, "market", None)),
            "status": str(getattr(ack, "status", None)),
            "reason_code": getattr(ack, "reason_code", None),
            "reason": getattr(ack, "reason", None),
            "exchange_order_id": getattr(ack, "exchange_order_id", None),
            "router_order_id": getattr(ack, "router_order_id", None),
            "ts_utc": getattr(ack, "ts_utc", None),
            "venue_ts": getattr(ack, "venue_ts", None),
            "venue": getattr(ack, "venue", None),
            "seq": getattr(ack, "seq", 0),
            "tags": tags,
        }

    def candle_to_payload(self, candle: Candle) -> dict[str, Any]:
        return {
            "symbol": candle.symbol,
            "timeframe": candle.timeframe,
            "start_ms": candle.start_ms,
            "end_ms": candle.end_ms,
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
            "volume": float(candle.volume),
            "trades": candle.trades,
            "is_final": candle.is_final,
        }

    def fill_to_payload(self, fill: Fill) -> dict[str, Any]:
        return {
            "fill_id": fill.fill_id,
            "order_id": fill.order_id,
            "symbol": fill.symbol,
            "market": str(fill.market),
            "qty": str(fill.qty),
            "price": str(fill.price),
            "side": str(fill.side.value),
            "ts": fill.ts,
            "venue": fill.venue,
            "liquidity_flag": str(fill.liquidity_flag),
            "fees_explicit": str(fill.fees_explicit),
            "rebates": str(fill.rebates),
            "slippage_components": fill.slippage_components,
            "tags": list(fill.tags),
        }

    def control_to_payload(self, control: ControlEvent) -> dict[str, Any]:
        return {
            "type": control.type,
            "source": control.source,
            "ts_utc": control.ts_utc,
            "run_id": control.run_id,
            "details": dict(control.details) if control.details is not None else {},
        }


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
