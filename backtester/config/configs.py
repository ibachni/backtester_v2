from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass, field, fields, is_dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Literal, Mapping, Optional, Union

from pydantic import BaseModel

from backtester.core.clock import parse_timeframe
from backtester.errors.errors import SourceError
from backtester.sim.sim_models import FeeModel, LatencyModel, SlippageModel, SpreadPovSlippage
from backtester.types.data import BinanceCandleType, CandleType
from backtester.utils.utility import target_path

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
    base_path: Path = Path("/Users/nicolas/Data/crypto_data/parquet/")
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


AlignPolicy = Literal["outer", "inner"]


@dataclass(slots=True)
class FeederConfig:
    align: AlignPolicy = "outer"


# --- Core Section ---


@dataclass(frozen=True)
class AccountConfig:
    starting_cash: Decimal = Decimal("10000")
    venue: str = "Binance"
    base_ccy: str = "USDT"


@dataclass(frozen=True)
class AuditConfig:
    log_dir: Path
    log_level: int = 10
    debug: bool = True
    capture_market_data: bool = False


@dataclass
class SubscriptionConfig:
    topics: set[str]
    # None means "use topic defaults or bus default"
    buffer_size: Optional[int] = None


@dataclass
class BusConfig:
    # seconds to wait between flush state checks (how often flush()
    # re-checks progress if no events arrive)
    flush_check_interval: float = 0.01
    # default mailbox size when a subscription doesn't specify one and topics don't either
    default_buffer_size: int = 1024
    validate_schema: bool = False


# --- Risk & Order Val---


@dataclass
class ValidationConfig:
    pass


@dataclass
class RiskConfig:
    pass


# --- Strategy ----


@dataclass(frozen=True)
class StrategyInfo:
    name: str  # stable identifier
    version: str = "0.1.0"
    description: str = ""


@dataclass
class StrategyConfig:
    info: StrategyInfo
    strategy_params: Mapping[str, Any]
    start_dt: dt.datetime
    end_dt: dt.datetime
    data_cfg: dict[str, DataSubscriptionConfig] = field(default_factory=dict)
    data_base_path: Path = Path("/Users/nicolas/Data/crypto_data/parquet/")

    def __post_init__(self) -> None:
        if not bool(self.data_cfg):
            self.build_data_sub_config()

    def build_data_sub_config(self) -> None:
        symbols = self.strategy_params.get("symbols")
        timeframe = self.strategy_params.get("timeframe")
        if symbols is None or timeframe is None:
            raise ValueError(
                f"strategy_params is missing symbols {symbols} or timeframe {timeframe}"
            )
        for symbol in symbols:
            self.data_cfg[symbol] = DataSubscriptionConfig(
                symbol=symbol,
                start_dt=self.start_dt,
                end_dt=self.end_dt,
                timeframe=timeframe,
                timeframe_data="1m",
                base_path=self.data_base_path,
            )


# --- Sim ---


@dataclass
class SimConfig:
    slip_model: Union[SlippageModel, SpreadPovSlippage]
    fee_model: FeeModel
    latency_model: Optional[LatencyModel] = None
    conservative_gaps: bool = True
    participation_bps: int = 500


# --- Performance ---


@dataclass
class PerformanceConfig:
    trading_interval: str
    metrics_interval: Optional[str] = None
    base_ccy: str = "USDC"
    returns_kind: str = "log"  # "log" | "simple"
    risk_free: float = 0.0

    def __post_init__(self) -> None:
        if self.metrics_interval is None:
            self.metrics_interval = self.trading_interval


# --- Backtest aggregation ---


@dataclass(frozen=True)
class BacktestConfig:
    run_name: str
    bus_cfg: BusConfig
    audit_cfg: AuditConfig
    feed_cfg: FeederConfig
    strategy_cfg: StrategyConfig
    orderval_cfg: ValidationConfig
    sim_cfg: SimConfig
    account_cfg: AccountConfig
    performance_cfg: PerformanceConfig

    def stable_hash(self) -> str:
        """
        Deterministic fingerprint of the input (caller-provided) fields.
        Derived fields (init=False) are ignored to keep the hash stable across refactors.
        """
        payload = {"v": 1, **self._normalize_value(self)}
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if is_dataclass(value):
            return {
                f.name: BacktestConfig._normalize_value(getattr(value, f.name))
                for f in fields(value)
                if f.init
            }
        if isinstance(value, Mapping):
            return {
                str(k): BacktestConfig._normalize_value(v)
                for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))
            }
        if isinstance(value, (list, tuple)):
            return [BacktestConfig._normalize_value(v) for v in value]
        if isinstance(value, set):
            return [BacktestConfig._normalize_value(v) for v in sorted(value, key=lambda x: str(x))]
        if isinstance(value, dt.datetime):
            normalized = value.astimezone(dt.timezone.utc) if value.tzinfo else value
            return normalized.isoformat()
        if isinstance(value, dt.date):
            return value.isoformat()
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, CandleType):
            return value.__class__.__name__
        return str(value)
