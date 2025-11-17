from __future__ import annotations

import datetime as dt
import io
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, Literal, Optional

import polars as pl

from backtester.types.types import UnixMillis

# --- backtester.data.downloader ----


@dataclass(frozen=True)
class RawBytes:
    """
    Step-1 → Step-2 handoff:
    - Either `bytes` is set (small payloads), or `path` points to a cached ZIP.
    - Includes enough metadata for logging, manifests, and routing.
    """

    exchange: str
    market: str  # logical label ("spot","options","futures",…)
    symbol: str
    interval: Optional[str]  # None for EOHSummary
    granularity: Literal["daily", "monthly"]
    period: str  # "YYYY-MM" or "YYYY-MM-DD"
    url: str
    http_status: int
    sha256: Optional[str]
    size: int
    path: Optional[Path]
    bytes: Optional[bytes]  # in-memory payload if not persisted
    run_id: str

    @property
    def ok(self) -> bool:
        return self.http_status == 200 and (self.bytes is not None or self.path is not None)

    def open(self) -> BinaryIO:
        if self.bytes is not None:
            return io.BytesIO(self.bytes)
        if self.path is not None:
            return open(self.path, "rb")
        raise RuntimeError("RawBytes has no content")


@dataclass(frozen=True)
class SchemaSpec:
    name: str
    schema: Literal["kline", "option"]
    required_columns: tuple[str, ...]
    partition_column: str
    start_field: str
    end_field: str
    dedup_keys: tuple[str, ...]
    sort_columns: tuple[str, ...]


@dataclass(frozen=True)
class SanitizedBatch:
    raw: RawBytes
    schema: Literal["kline", "option"]
    dataframe: pl.DataFrame
    rows: int
    partitions: int
    start: dt.datetime | dt.date | int
    end: dt.datetime | dt.date | int
    issues: tuple[str, ...] = ()


@dataclass(frozen=True)
class ParsingReport:
    attempts: int = 0
    ok: int = 0
    skipped: int = 0
    errors: int = 0


@dataclass(frozen=True)
class WriteReport:
    attempts: int = 0
    ok: int = 0
    skipped: int = 0
    errors: int = 0
    rows: int = 0
    partitions: int = 0


# --- backtester.data.source ----


@dataclass(slots=True)
class FinalMessage:
    topic: str
    message: str
    timestamp: dt.datetime
    metadata: SourceMeta


class DataType:
    """Abstract DataType"""

    pass


class OptionType(DataType):
    def __init__(self) -> None:
        super().__init__()
        raise NotImplementedError


class CandleType(DataType):
    """
    Abstract CandleType: CandleSources must have these columns
    """

    start_col: str
    end_col: str
    o_col: str
    h_col: str
    l_col: str
    c_col: str
    v_col: str
    n_trades_col: str
    is_final_col: str


class BinanceCandleType(CandleType):
    """
    Column mapping and validation knobs for sources.
    Column names adjusted to match raw files.
    Contract: All bars have is_final=True (enforced by downloader).
    """

    start_col: str = "start_ms"
    end_col: str = "end_ms"
    o_col: str = "open"
    h_col: str = "high"
    l_col: str = "low"
    c_col: str = "close"
    v_col: str = "volume"
    n_trades_col: str = "trades"
    is_final_col: str = "is_final"


@dataclass(slots=True)
class SourceMeta:
    """
    Metadata collector
    """

    symbol: str  # Symbol
    timeframe: str  # Timeframe
    format: str  # csv, parquet etc.
    paths: list[Path] = field(default_factory=list)  # list of underlying file paths read by this
    # source

    # Obs
    rows_read: int = 0  # total input rows parsed from files (timeframe_data dependent)
    rows_emitted: int = 0  # candles successfully yielded (timeframe dependent)
    rows_dropped: int = 0
    rows_failed: int = 0  # rows rejected due to errors
    first_ts: Optional[UnixMillis] = None  # unix ms of the first candle's timestamp seen / emitted
    last_ts: Optional[UnixMillis] = None
