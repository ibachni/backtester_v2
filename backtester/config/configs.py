from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel

from backtester.core.clock import parse_timeframe
from backtester.core.utility import target_path
from backtester.errors.errors import SourceError
from backtester.types.data import BinanceCandleType, CandleType

"""
Here, we collect all the different configs
"""


@dataclass(frozen=True)
class RunContext:
    run_id: str
    seed: int
    git_sha: str
    # start_ts: UnixMillis
    allow_net: bool = False


# --- Data Section ---


class DownloaderConfig(BaseModel):
    base_dir: Path  # Where to download
    log_dir: Path  # Where to log
    download_run_id: str
    exchange: Literal["binance"] = "binance"
    data_type: Literal["candle"] = "candle"
    binance_base: str = "https://data.binance.vision"
    timeout: float = 30.0
    retries: int = 3
    chunk_size: int = 1 << 20


@dataclass(slots=True)
class DataSubscriptionConfig:
    # Sub data
    symbol: str
    start_dt: dt.datetime
    end_dt: dt.datetime
    timeframe: str
    timeframe_data: str
    data_type: CandleType = BinanceCandleType()
    batch_size: int = 8192

    # path
    base_path: str = "/Users/nicolas/Data/crypto_data/parquet/"
    exchange: str = "binance"
    market: str = "spot"
    data_kind: str = "candle"

    # derived (not passed by caller)
    start_ms: int = field(init=False)
    end_ms: int = field(init=False)
    tf_ms: int = field(init=False)  # target TF (e.g., 5m)
    tf_ms_data: int = field(init=False)  # input TF (e.g., 1m)
    target_path: Path = field(init=False)
    sha_256: str = field(init=False)

    def __post_init__(self) -> None:
        # Normalize and validate timezone: require UTC-aware
        self.start_dt = self._ensure_utc(self.start_dt)
        self.end_dt = self._ensure_utc(self.end_dt)

        self.start_ms = int(self.start_dt.timestamp() * 1000)
        self.end_ms = int(self.end_dt.timestamp() * 1000)
        # Target TF for frames (what BarFeed/heap schedules on)
        self.tf_ms = parse_timeframe(self.timeframe)
        # Input TF for parsing (what the Parquet files contain)
        self.tf_ms_data = parse_timeframe(self.timeframe_data)

        if (self.tf_ms / self.tf_ms_data) % 1 != 0:
            raise SourceError("Target TF must be integer multiple of data TF")

        self.target_path = target_path(
            Path(self.base_path),
            self.exchange,
            self.market,
            self.symbol,
            self.data_kind,
            self.timeframe_data,
        )
        self.sha_256 = self._compute_sha256()
        # print(self.start_dt.tzinfo, self.end_dt.tzinfo)

    def _stable_key(self) -> dict[str, Any]:
        return {
            "v": 1,  # key version for forward-compat
            "symbol": self.symbol.lower(),
            "timeframe": self.timeframe.lower(),
            "data_kind": self.data_kind.lower(),
        }

    def _compute_sha256(self) -> str:
        payload = json.dumps(
            self._stable_key(), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _ensure_utc(d: dt.datetime) -> dt.datetime:
        if d.tzinfo is None:
            raise SourceError("start_dt/end_dt must be timezone-aware; pass tzinfo=dt.timezone.utc")
        return d.astimezone(dt.timezone.utc)


# --- Core Section ---


@dataclass(frozen=True)
class AccountConfig:
    pass


@dataclass(frozen=True)
class AuditConfig:
    pass


# --- Backtest aggregation ---


@dataclass(frozen=True)
class BacktestConfig:
    run_name: str
    # strategy_params: Mapping[str, BacktestParams]  # Strategy: StrategyParams
    # audit_cfg = AuditConfig
    downloader_cfg: DownloaderConfig
    # source_cfg = list[DataSubscriptionConfig]


@dataclass(frozen=True)
class BacktestParams:
    strategy: str
    symbol: list[str]
    start_dt: dt.datetime
    end_dt: dt.datetime
    timeframe: str
    other_params: Any
    source: DataSubscriptionConfig
