import datetime
import datetime as dt
import uuid
from dataclasses import asdict, is_dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional, Union

import polars as pl
from binance import Client
from pydantic import ValidationError

from backtester.types.types import SymbolSpec

# --- Sanitization Helper ---


def make_serializable(obj: Any) -> Any:
    """
    Recursively converts non-JSON-safe objects (Decimal, datetime, UUID, Dataclass)
    into standard Python primitives (str, dict, list).
    """
    # Fast path for primitives
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj

    if isinstance(obj, dict):
        return {k: make_serializable(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple, set)):
        return [make_serializable(x) for x in obj]

    # Complex types
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if is_dataclass(obj):
        return asdict(obj)  # type: ignore
    # Fallback for custom objects
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    return str(obj)


# --- Other ---


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


# --- Symbol Spec ---


def get_symbol_spec(symbols: list[str]) -> dict[str, SymbolSpec]:
    """
    Retrieve symbol-specific information from Binance and populate _exchange_info.

    Args:
        symbols: List of symbols to retrieve information for (e.g., ["BTCUSDT", "ETHUSDT"])

    Fetches exchange info from Binance REST API and creates SymbolSpec objects
    for each symbol with trading rules and filters.
    """
    client = Client()
    exchange_info = client.get_exchange_info()

    symbol_specs = {}

    for symbol_info in exchange_info.get("symbols", []):
        symbol = symbol_info.get("symbol")

        # Only process symbols in the requested list
        if symbol not in symbols:
            continue

        # Extract filter information
        filters = {f["filterType"]: f for f in symbol_info.get("filters", [])}

        # Extract LOT_SIZE filter
        lot_size_filter = filters.get("LOT_SIZE", {})
        # The smallest increment of quantity one can buy / sell -> round down
        lot_size = dec(lot_size_filter.get("stepSize", 0.0))
        # the smallest amount one can touch: reject if qty < min qty
        min_qty = dec(lot_size_filter.get("minQty", 0.0))
        max_qty = dec(lot_size_filter.get("maxQty", float("inf")))

        # Extract PRICE_FILTER filter (controls price of order)
        price_filter = filters.get("PRICE_FILTER", {})
        # tick_size: the smallest increment a price can move
        tick_size = dec(price_filter.get("tickSize", 0.0))
        # the hard limits of the order book
        price_band_low = dec(price_filter.get("minPrice", 0.0))
        price_band_high = dec(price_filter.get("maxPrice", float("inf")))

        # Extract MIN_NOTIONAL or NOTIONAL filter
        # controls the total value (Price x quantity of the order)
        min_notional_filter = filters.get("MIN_NOTIONAL") or filters.get("NOTIONAL", {})
        # Min value of order in the quote asset (reject if price * qty < minNotional)
        min_notional = dec(min_notional_filter.get("minNotional", 0.0))

        # Create SymbolSpec
        spec = SymbolSpec(
            symbol=symbol,
            base_asset=symbol_info.get("baseAsset", ""),
            quote_asset=symbol_info.get("quoteAsset", ""),
            tick_size=tick_size,
            lot_size=lot_size,
            min_notional=min_notional,
            min_qty=min_qty,
            max_qty=max_qty,
            price_band_low=price_band_low,
            price_band_high=price_band_high,
            trading=symbol_info.get("status") == "TRADING",
        )

        symbol_specs[symbol] = spec
    return symbol_specs
