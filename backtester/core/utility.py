import datetime as dt
from datetime import date
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional

import polars as pl
from pydantic import ValidationError


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
