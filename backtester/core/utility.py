import datetime as dt
import hashlib
import json
from datetime import date
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Optional, Tuple, Union

import polars as pl
from pydantic import ValidationError

from backtester.types.types import (
    LimitOrderIntent,
    MarketOrderIntent,
    OrderIntent,
    StopLimitOrderIntent,
    StopMarketOrderIntent,
)


def deep_merge(a: Mapping[str, Any], b: Mapping[str, Any]) -> Mapping[str, Any]:
    out = dict(a)
    for k, v in b.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def validation_error_parser(error: ValidationError) -> list[dict[str, str]]:
    parsed_error = [
        {
            "component": "config.schema.defaults",
            "path": ".".join(map(str, err["loc"])),
            "message": err["msg"],
            "error_type": err["type"],
        }
        for err in error.errors()
    ]
    return parsed_error


def month_range(start: dt.date, end: dt.date) -> Iterator[tuple[int, int]]:
    """
    Inclusive month iteration: (YYYY,MM) pairs covering [start,end].
    """
    y, m = start.year, start.month
    while (y < end.year) or (y == end.year and m <= end.month):
        yield y, m
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1


def target_path(
    root: Path,
    exchange: str,
    market: str,
    symbol: str,
    data_type: str,
    interval: str,
    d: Optional[date] = None,
) -> Path:
    # root = Path("/Users/nicolas/data/crypto_data/parquet")
    except_year = (
        root
        / f"exchange={exchange}"
        / f"market={market}"
        / f"symbol={symbol}"
        / f"type={data_type}"
        / f"timeframe={interval}"
    )
    if d is not None:
        return except_year / f"year={d.year:04d}"
    else:
        return except_year


# --- General parquet reader ---


def open_parquet_file(path: Path, file_name: str) -> pl.DataFrame:
    fp = path / file_name
    if not fp.is_file():
        raise FileNotFoundError(f"parquet file not found: {fp}")
    return pl.read_parquet(path / file_name)


# --- Decimals ---
def dec(x: Union[str, int, float, Decimal]) -> Decimal:
    """
    Safe conversion to Decimal:
    - Prefer passing strings (e.g., "150.23") for exact values.
    - Floats are stringified first to avoid binary float artifacts.
    """
    if isinstance(x, Decimal):
        return x
    if isinstance(x, (int)):
        return Decimal(x)
    if isinstance(x, float):
        return Decimal(str(x))
    # assume string
    return Decimal(x)


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
