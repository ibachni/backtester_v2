"""
Purpose:
    - Loads a config file
    - Validate the config file

ToDo:
    - Make strategy loading strategy-agnostic
    - Defaults: Fill missing optional fields with explicit defaults
    - deterministic hash
    - raise on unknown keys
    - secrets handling!
    - write rendresult to JSON
"""

import datetime as dt
import tomllib
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from backtester.config.configs import (
    AccountConfig,
    AuditConfig,
    BacktestConfig,
    BusConfig,
    FeederConfig,
    PerformanceConfig,
    SimConfig,
    StrategyConfig,
    StrategyInfo,
    ValidationConfig,
)
from backtester.sim.sim_models import FixedBps, SlippageModel, SpreadPovSlippage


class LoadedConfig(BaseModel):
    pass


class ConfigLoader:
    """
    Config-loader; loading toml file.
    """

    def __init__(self, base_dir: str = ".") -> None:
        self._base_dir = base_dir

    def load(self, file_name: str) -> dict[str, Any]:
        path = Path(file_name)
        if not path.is_absolute():
            path = Path(self._base_dir) / file_name

        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with path.open("rb") as f:
            return tomllib.load(f)

    def load_backtest_config(self, backtest_path: str, strategy_path: str) -> BacktestConfig:
        bt_data = self.load(backtest_path)
        strat_data = self.load(strategy_path)

        # 1. Parse Strategy Config
        info_data = strat_data.get("info", {})
        info = StrategyInfo(
            name=info_data.get("name", "unknown"),
            version=info_data.get("version", "0.0.0"),
            description=info_data.get("description", ""),
        )
        strategy_params = strat_data.get("params", {})
        # Convert qty to decimal
        if "qty" in strategy_params and isinstance(strategy_params["qty"], str):
            strategy_params["qty"] = Decimal(strategy_params["qty"])

        # 2. Parse Backtest Config sections
        general_data = bt_data.get("general", {})
        output_dir_raw = general_data.get("output_dir", "runs")
        output_dir = Path(output_dir_raw)
        if not output_dir.is_absolute():
            output_dir = Path(self._base_dir) / output_dir

        data_path_raw = general_data.get("data_path", "")
        if data_path_raw == "":
            raise ValueError("DataPath missing")
        data_path = Path(data_path_raw)
        if not data_path.is_absolute():
            data_path = Path(self._base_dir) / data_path

        bus_data = bt_data.get("bus", {})
        bus_cfg = BusConfig(
            flush_check_interval=bus_data.get("flush_check_interval", 0.01),
            default_buffer_size=bus_data.get("default_buffer_size", 1024),
            validate_schema=bus_data.get("validate_schema", False),
        )

        audit_data = bt_data.get("audit", {})
        audit_cfg = AuditConfig(
            log_dir=Path(output_dir),
            capture_market_data=audit_data.get("capture_market_data", False),
        )

        feed_data = bt_data.get("feeder", {})
        feed_cfg = FeederConfig(
            align=feed_data.get("align", "outer"),
        )

        # ValidationConfig is currently empty
        orderval_cfg = ValidationConfig()

        sim_data = bt_data.get("sim", {})
        # Instantiate models
        slip_bps = sim_data.get("slippage_bps", 0)
        fee_bps = sim_data.get("fee_bps", 0)
        slippage_params = sim_data.get("slippage_params", {}) or {}
        spread_bps = sim_data.get("spread_bps", slippage_params.get("spread_bps", 0))
        pov_k = slippage_params.get("k", 0)
        min_volume_guard = slippage_params.get("min_volume_guard", 1.0)

        slip_model = self._build_slip_model(
            slip_model_name=sim_data.get("slippage_model", "SlippageModel"),
            slip_bps=slip_bps,
            spread_bps=spread_bps,
            pov_k=pov_k,
            min_volume_guard=min_volume_guard,
        )

        fee_model_name = sim_data.get("fee_model", "FixedBps")
        if fee_model_name == "FixedBps":
            fee_model = FixedBps(bps=fee_bps)
        else:
            fee_model = FixedBps(bps=0)

        sim_cfg = SimConfig(
            slip_model=slip_model,
            fee_model=fee_model,
            conservative_gaps=sim_data.get("conservative_gaps", True),
            participation_bps=sim_data.get("participation_bps", 500),
        )

        acct_data = bt_data.get("account", {})
        account_cfg = AccountConfig(
            starting_cash=Decimal(str(acct_data.get("starting_cash", "10000"))),
            base_ccy=acct_data.get("base_ccy", "USDT"),
        )

        # Performance
        # perf_data = bt_data.get("performance", {})
        trading_interval = strategy_params.get("timeframe")
        performance_cfg = PerformanceConfig(
            trading_interval=trading_interval,
            base_ccy=acct_data.get("base_ccy", "USDT"),
        )

        run_info = bt_data.get("run_info", {})
        run_name = run_info.get("run_name", "TEST")

        period_data = bt_data.get("period", {})
        start_dt = dt.datetime.fromisoformat(period_data["start_dt"])
        start_dt = start_dt.replace(tzinfo=dt.timezone.utc)
        end_dt = dt.datetime.fromisoformat(period_data["end_dt"])
        end_dt = end_dt.replace(tzinfo=dt.timezone.utc)

        # requires start_dt, and end_dt
        strategy_cfg = StrategyConfig(
            info=info,
            strategy_params=strategy_params,
            start_dt=start_dt,
            end_dt=end_dt,
            data_base_path=data_path,
        )

        return BacktestConfig(
            run_name=run_name,
            bus_cfg=bus_cfg,
            audit_cfg=audit_cfg,
            feed_cfg=feed_cfg,
            strategy_cfg=strategy_cfg,
            orderval_cfg=orderval_cfg,
            sim_cfg=sim_cfg,
            account_cfg=account_cfg,
            performance_cfg=performance_cfg,
        )

    @staticmethod
    def _build_slip_model(
        *,
        slip_model_name: str,
        slip_bps: int,
        spread_bps: int,
        pov_k: float,
        min_volume_guard: float,
    ) -> SlippageModel | SpreadPovSlippage:
        if slip_model_name in ("SpreadPovSlippage", "SpreadPov", "SpreadPOV"):
            return SpreadPovSlippage(
                spread_bps=spread_bps, k=pov_k, min_volume_guard=min_volume_guard
            )
        if slip_model_name == "SlippageModel":
            return SlippageModel(bps=slip_bps)
        return SlippageModel(bps=0)

    def validate(self, cfg: LoadedConfig) -> bool:
        return True

    def dump_json(self, cfg: LoadedConfig) -> None:
        return None

    def hash(self, cfg: LoadedConfig) -> str:
        return "1"
