import hashlib
import json
from decimal import Decimal
from enum import Enum
from typing import Any, Iterable, Mapping, Tuple

from backtester.types.types import (
    LimitOrderIntent,
    MarketOrderIntent,
    OrderIntent,
    StopLimitOrderIntent,
    StopMarketOrderIntent,
)

# --- OrderIntent Hashing ---


def _norm_decimal(d: Decimal) -> Mapping[str, Any]:
    # normalize to canonical sign/digits/exponent representation
    nd = d.normalize()
    t = nd.as_tuple()
    # represent digits as a string to avoid list/tuple ambiguity in JSON
    digits = "".join(map(str, t.digits)) if t.digits else "0"
    return {"_dec": True, "sign": t.sign, "digits": digits, "exp": t.exponent}


def _norm_val(v: Any) -> Any:
    """
    Normalize values to a hash-stable representation
    """
    if v is None:
        return None
    if isinstance(v, Decimal):
        return _norm_decimal(v)
    if isinstance(v, Enum):
        return v.value
    if isinstance(v, (int, float, str, bool)):
        return v
    return str(v)


def order_fingerprint_tuple(intent: object, fields: Iterable[str]) -> list[Tuple[str, Any]]:
    """
    Build a canonical ordered list of (field_name, normalized_value).
    Caller should supply the canonical field names to include.
    """
    fp: list[Tuple[str, Any]] = []
    for name in fields:
        val = getattr(intent, name, None)
        fp.append((name, _norm_val(val)))
    return fp


def order_fingerprint_hash(intent: object, fields: Iterable[str]) -> str:
    """
    Return a deterministic hex SHA256 over the canonical fingerprint.
    Example fields:
    ("symbol","market","side","qty","price","stop_price","tif","reduce_only","strategy_id")
    """
    fp = order_fingerprint_tuple(intent, fields)
    # stable JSON encoding; separators to avoid whitespace variance
    blob = json.dumps(fp, ensure_ascii=False, separators=(",", ":"), sort_keys=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def relevant_fields(intent: OrderIntent) -> tuple[str, ...]:
    if isinstance(intent, MarketOrderIntent):
        return (
            "symbol",
            "market",
            "side",
            "qty",
            "strategy_id",
            "tif",
            "reduce_only",
        )
    elif isinstance(intent, LimitOrderIntent):
        return (
            "symbol",
            "market",
            "side",
            "qty",
            "strategy_id",
            "tif",
            "price",
            "reduce_only",
            "post_only",
        )
    elif isinstance(intent, StopMarketOrderIntent):
        return (
            "symbol",
            "market",
            "side",
            "qty",
            "strategy_id",
            "tif",
            "stop_price",
            "reduce_only",
            "post_only",
        )
    elif isinstance(intent, StopLimitOrderIntent):
        return (
            "symbol",
            "market",
            "side",
            "qty",
            "strategy_id",
            "tif",
            "price",
            "stop_price,reduce_only",
            "post_only",
        )

    raise TypeError(f"Unsupported intent type for fingerprinting: {type(intent).__name__}")
