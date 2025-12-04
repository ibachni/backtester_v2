"""
Minimal CLI entrypoint to run a backtest and generate analysis artifacts.

Usage: bt --strategy sma_extended --symbols BTCUSDT ETHUSDT --start-date 2019-01-01

Options (mirrors the original docstring intent):
  --strategy TEXT       Strategy name (expects configs/<strategy>.toml) [REQUIRED]
  --symbols TEXT ...    One or more symbols, e.g., BTCUSDT ETHUSDT [REQUIRED]
  --start-date TEXT     ISO start date (e.g., 2023-01-01); defaults to config
  --end-date TEXT       ISO end date; defaults to config
  --cash FLOAT          Initial portfolio capital (default from config)
  --params JSON         Override strategy params as JSON object
  --fees INT            FixedBPS fees (default from config)
  --slippage INT        FixedBPS slippage (default from config)
  --output FILE         Output root directory (default: configs/general.output_dir)
  --debug               Enable verbose audit logging
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from backtester.config.configs import BacktestConfig, RunContext

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="bt", description="Run a backtest (MVP)")
    parser.add_argument(
        "--strategy",
        required=True,
        help="Strategy name (expects configs/<name>.toml)",
    )
    parser.add_argument(
        "--symbols",
        "--symbol",
        dest="symbols",
        nargs="+",
        help='Space-separated symbols, e.g., BTCUSDT ETHUSDT (mirrors "--symbol(s)")',
    )
    parser.add_argument(
        "--start-date",
        help="ISO start date (YYYY-MM-DD or full ISO). Defaults to config if omitted.",
    )
    parser.add_argument(
        "--end-date",
        help="ISO end date. Defaults to config if omitted.",
    )
    parser.add_argument(
        "--cash",
        type=float,
        help="Initial portfolio capital. Defaults to config value.",
    )
    parser.add_argument(
        "--params",
        help="JSON object to override strategy params if present in the config.",
    )
    parser.add_argument(
        "--fees",
        type=int,
        help="FixedBPS fees; overrides config if provided.",
    )
    parser.add_argument(
        "--slippage",
        type=int,
        help="FixedBPS slippage; overrides config if provided.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Root output directory. Defaults to configs/general.output_dir.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose audit logging.",
    )
    return parser.parse_args(argv)


def _parse_iso_date(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _resolve_git_sha(repo_root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return result.stdout.strip()


def _merge_strategy_params(
    base_params: Mapping[str, Any],
    symbols: list[str] | None,
    params_override: str | None,
) -> Mapping[str, Any]:
    updated = dict(base_params)

    if symbols:
        updated["symbols"] = symbols

    if params_override:
        try:
            overrides = json.loads(params_override)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON for --params: {exc}") from exc
        if not isinstance(overrides, dict):
            raise ValueError("--params must be a JSON object")
        for key, value in overrides.items():
            if key not in updated:
                print(f"[!] Skipping unknown param override: {key}")
                continue
            updated[key] = value

    return updated


def _apply_overrides(base_cfg: BacktestConfig, args: argparse.Namespace) -> BacktestConfig:
    """
    If overrides are applied via CLI input, it should override the values provided in the tomls.
    """
    from backtester.config.configs import PerformanceConfig, StrategyConfig
    from backtester.sim.sim_models import FixedBps, SlippageModel

    strategy_params = _merge_strategy_params(
        base_cfg.strategy_cfg.strategy_params, args.symbols, args.params
    )

    start_dt = (
        _parse_iso_date(args.start_date)
        if args.start_date
        else base_cfg.strategy_cfg.start_dt.astimezone(timezone.utc)
    )
    end_dt = (
        _parse_iso_date(args.end_date)
        if args.end_date
        else base_cfg.strategy_cfg.end_dt.astimezone(timezone.utc)
    )

    strategy_cfg = StrategyConfig(
        info=base_cfg.strategy_cfg.info,
        strategy_params=strategy_params,
        start_dt=start_dt,
        end_dt=end_dt,
    )

    account_cfg = replace(
        base_cfg.account_cfg,
        starting_cash=Decimal(str(args.cash))
        if args.cash is not None
        else base_cfg.account_cfg.starting_cash,
    )

    slip_bps = args.slippage if args.slippage is not None else base_cfg.sim_cfg.slip_model.bps

    fee_model = base_cfg.sim_cfg.fee_model
    if not isinstance(fee_model, FixedBps):
        raise ValueError("Fee overrides require a FixedBps fee model in the base config.")
    fee_bps = args.fees if args.fees is not None else fee_model.bps

    sim_cfg = replace(
        base_cfg.sim_cfg,
        slip_model=SlippageModel(bps=slip_bps),
        fee_model=FixedBps(bps=fee_bps),
    )

    out_dir = Path(args.output) if args.output else base_cfg.audit_cfg.log_dir
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir

    audit_cfg = replace(
        base_cfg.audit_cfg,
        log_dir=out_dir,
        debug=args.debug or base_cfg.audit_cfg.debug,
    )

    perf_cfg = PerformanceConfig(
        trading_interval=strategy_params.get(
            "timeframe", base_cfg.performance_cfg.trading_interval
        ),
        metrics_interval=base_cfg.performance_cfg.metrics_interval,
        base_ccy=base_cfg.performance_cfg.base_ccy,
        returns_kind=base_cfg.performance_cfg.returns_kind,
        risk_free=base_cfg.performance_cfg.risk_free,
    )

    return replace(
        base_cfg,
        audit_cfg=audit_cfg,
        strategy_cfg=strategy_cfg,
        sim_cfg=sim_cfg,
        account_cfg=account_cfg,
        performance_cfg=perf_cfg,
    )


async def _run_backtest(backtest_cfg: BacktestConfig, run_ctx: RunContext) -> None:
    from backtester.core.backtest_engine import BacktestEngine
    from backtester.core.clock import SimClock

    start_ms = int(backtest_cfg.strategy_cfg.start_dt.timestamp() * 1000)
    clock = SimClock(start_ms=start_ms)
    engine = BacktestEngine(run_ctx=run_ctx, backtest_cfg=backtest_cfg, clock=clock)
    await engine.run_async()


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    from backtester.config.config_loader import ConfigLoader
    from backtester.config.configs import RunContext

    repo_root = Path(__file__).resolve().parent.parent
    loader = ConfigLoader(base_dir=str(repo_root))

    backtest_cfg_path = Path("configs/backtest.toml")
    strategy_cfg_path = Path(f"configs/{args.strategy}.toml")

    try:
        base_cfg = loader.load_backtest_config(str(backtest_cfg_path), str(strategy_cfg_path))
    except FileNotFoundError as exc:
        print(f"[!] {exc}")
        return 1

    try:
        backtest_cfg = _apply_overrides(base_cfg, args)
    except ValueError as exc:
        print(f"[!] {exc}")
        return 1

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_id = f"{backtest_cfg.run_name}_{timestamp}"
    run_ctx = RunContext(
        run_id=run_id,
        seed=42,
        git_sha=_resolve_git_sha(repo_root),
        allow_net=False,
    )

    try:
        asyncio.run(_run_backtest(backtest_cfg, run_ctx))
    except KeyboardInterrupt:
        print("Interrupted by user.")
        return 130
    except Exception as exc:
        print(f"[!] Backtest failed: {exc}")
        return 1

    try:
        from backtester.analysis import BacktestAnalyzer
    except ImportError as exc:
        print(f"[!] Analysis skipped (dependency missing): {exc}")
        return 0

    result_dir = Path(backtest_cfg.audit_cfg.log_dir) / f"{backtest_cfg.run_name}_{timestamp}"

    try:
        analyzer = BacktestAnalyzer(str(result_dir))
    except FileNotFoundError as exc:
        print(f"[!] Analysis skipped: {exc}")
        return 0

    analyzer.print_stats()
    analyzer.plot_analysis()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
