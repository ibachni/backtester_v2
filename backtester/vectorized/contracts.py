from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from backtester.types.data import BinanceCandleType, CandleType


def _to_epoch_ms(ts: dt.datetime) -> int:
    # Treat naive timestamps as UTC to maintain determinism.
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    else:
        ts = ts.astimezone(dt.timezone.utc)
    return int(ts.timestamp() * 1000)


@dataclass(frozen=True, slots=True)
class DateRange:
    start: dt.datetime
    end: dt.datetime

    def __post_init__(self) -> None:
        if self.end <= self.start:
            raise ValueError("DateRange.end must be after DateRange.start")

    def to_epoch_ms(self) -> tuple[int, int]:
        return (_to_epoch_ms(self.start), _to_epoch_ms(self.end))


@dataclass(frozen=True, slots=True)
class DataSpec:
    base_dir: Path
    exchange: str
    market: str
    symbols: tuple[str, ...]
    timeframe: str  # target timeframe for backtesting
    date_range: DateRange
    timeframe_data: str | None = None  # actual timeframe of parquet files (defaults to timeframe)
    data_type: str = "candle"
    columns: tuple[str, ...] | None = None
    final_only: bool = True
    candle_type: CandleType = field(default_factory=BinanceCandleType)
    sort_keys: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "base_dir", Path(self.base_dir))

        if not self.symbols:
            raise ValueError("DataSpec.symbols must include at least one symbol")
        if not self.timeframe:
            raise ValueError("DataSpec.timeframe cannot be empty")
        if not self.exchange or not self.market:
            raise ValueError("DataSpec.exchange and DataSpec.market are required")

        # Default timeframe_data to timeframe if not specified
        if self.timeframe_data is None:
            object.__setattr__(self, "timeframe_data", self.timeframe)

        symbols = tuple(self.symbols)
        invalid_symbols = [s for s in symbols if not isinstance(s, str) or not s.strip()]
        if invalid_symbols:
            raise ValueError("DataSpec.symbols must be non-empty strings")
        object.__setattr__(self, "symbols", symbols)

        if self.columns is not None:
            columns = tuple(self.columns)
            object.__setattr__(self, "columns", columns)

    def with_symbols(self, symbols: Iterable[str]) -> DataSpec:
        return DataSpec(
            base_dir=self.base_dir,
            exchange=self.exchange,
            market=self.market,
            symbols=tuple(symbols),
            timeframe=self.timeframe,
            date_range=self.date_range,
            timeframe_data=self.timeframe_data,
            data_type=self.data_type,
            columns=self.columns,
            final_only=self.final_only,
            candle_type=self.candle_type,
            sort_keys=self.sort_keys,
        )
