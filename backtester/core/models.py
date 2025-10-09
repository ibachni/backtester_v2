from dataclasses import dataclass
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field


class RiskConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    max_position: int = Field(default=4, ge=0, description="max simultaneous positions")
    allowed_symbols: list[str] = Field(
        default=["BTCUSDT"], description="Double check list of symbols"
    )


class SecretsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    api_key: str = Field(default="api_key_string", description="api placeholder")
    api_secret: str | None = Field(default=None, description="api secret placeholder")


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid")
    symbols: list[str] = Field(default=["BTCUSDT"], description="List of symbols")
    risk: RiskConfig = Field(default_factory=RiskConfig, description="Risk parameters")
    secrets: SecretsConfig = Field(default_factory=SecretsConfig, description="Secrets config")

    # 2. Runtime control
    # runtime.mode
    # runtime.seed
    # runtime.strict
    # runtime.allow_net
    # runtime.from_date
    # runtime.to_date

    # 3. Data Sourcing
    # data.source
    # data.path
    # data.scheme_version
    # data.timeframe
    # data.cache_dir
    # data.gap_mode
    # data.fill_forward

    # 4. Strategy module
    # strategy.class_name
    # strategy_params
    # strategy.state_store

    # 5. Costs & slippage (toggle fees and slippage deterministically)
    # costs.enabled
    # costs.fee_bps
    # costs.slippage_model
    # costs.spread_bps
    # costs.impact_coeff

    # 6. risk rails

    # 7. Paper/live adapters
    # paper.poll_interval_ms
    # paper.backoff_ms
    # paper_max_lag_warn_s
    # live.adapter
    # live.api_base_url
    # live.recv_window>ms
    # live.tiny_order_size
    # live.max_retries
    # live.backoff_policy
    # live.flatten_on_error

    # 8. Observability & persistence
    # telemetry.log_level
    # telemetry.metrics_flush_interval
    # telemtry.metrics_enabled
    # telemetry.snapshot_interval
    # telemetry.alerts
    # storage.run_dir
    # storage.snapshot_dir
    # storage.wal_path

    # 9. Secrets & credentials
    # ecrets.api_key
    # secrets.api_secret
    # secrets.passphrase
    # secrets.hmac_key
    # secrets.store


@dataclass(frozen=True)
class ReturnConfig:
    internal_config: Mapping[str, Any]
    redacted_config: Mapping[str, Any]
    redacted_count: int
    config_hash: str
    config_keys_total: int
