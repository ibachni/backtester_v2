import json
from pathlib import Path
from typing import Any, Dict, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas import Series

plt.style.use("ggplot")  # Use a nice visual style


class BacktestAnalyzer:
    def __init__(self, log_dir: str) -> None:
        self.log_dir = Path(log_dir)
        self.account_file = self.log_dir / "account.csv"
        self.report_file = self.log_dir / "report.json"
        self.ledger_file = self.log_dir / "ledger.jsonl"

        # Data containers
        self.df_account: Optional[pd.DataFrame] = None
        self.df_trades: Optional[Series[float]] = None
        self.report: Dict[str, Any] = {}
        self.meta: dict[str, Any] = {}

        self.load_data()

    def load_data(self) -> None:
        """Parses the raw log files into Pandas DataFrames."""
        # 1. Load Account Data (Equity Curve)
        if self.account_file.exists():
            try:
                self.df_account = pd.read_csv(self.account_file)
            except pd.errors.EmptyDataError:
                print(f"[-] Warning: {self.account_file} is empty.")
                self.df_account = None
                return

            # Ensure numeric columns are actually numeric
            for col in ("equity", "rpnl"):
                if col in self.df_account.columns:
                    self.df_account[col] = pd.to_numeric(
                        self.df_account[col], errors="coerce"
                    ).fillna(0)
            # Sort by time before setting the index
            self.df_account = self.df_account.sort_values("ts")
            self.df_account["datetime"] = pd.to_datetime(self.df_account["ts"], unit="ms")
            self.df_account.set_index("datetime", inplace=True)

            # Calculate Drawdown series immediately
            roll_max = self.df_account["equity"].cummax()
            self.df_account["drawdown"] = self.df_account["equity"] / roll_max - 1.0
        else:
            raise FileNotFoundError(f"Could not find {self.account_file}")

        # 2. Infer Trades from Realized PnL (RPnL) changes
        # This is more robust than parsing raw ledger fills for simple stats
        if self.df_account is not None and "rpnl" in self.df_account.columns:
            rpnl_diff = self.df_account["rpnl"].diff().fillna(0)
            # Filter for non-zero PnL events (closed trades)
            self.df_trades = rpnl_diff[rpnl_diff != 0]

        # 3. Load Metadata from Report
        if self.report_file and self.report_file.exists():
            self.report = self._parse_report(self.report_file)
            self.meta = self.report.get("meta", {})

    def _parse_report(self, path: Path) -> Dict[str, Any]:
        """
        Parse the report file.
        """
        # First try JSON/JSONL (one dict per line)
        try:
            with path.open() as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        if isinstance(data, dict):
                            return data
                    except json.JSONDecodeError:
                        continue
        except Exception:
            pass
        return {}

    def calculate_stats(self) -> dict[str, Any]:
        """Calculates standard financial metrics."""
        if self.df_account is not None:
            df = self.df_account
            equity = df["equity"]

            # Time-Series Metrics
            total_days = (df.index[-1] - df.index[0]).days
            if total_days == 0:
                total_days = 1

            start_equity = equity.iloc[0]
            end_equity = equity.iloc[-1]
            net_profit = end_equity - start_equity
            cagr = (end_equity / start_equity) ** (365 / total_days) - 1 if start_equity > 0 else 0

            # Daily Returns for Volatility/Sharpe
            daily_equity = equity.resample("D").last().dropna()
            # Avoid FutureWarning: explicitly set fill_method to None
            daily_rets = daily_equity.pct_change(fill_method=None).dropna()

            std_dev = daily_rets.std()
            if pd.isna(std_dev) or std_dev == 0:
                ann_vol = 0.0
                sharpe = 0.0
            else:
                ann_vol = std_dev * np.sqrt(365)
                # Assuming 0% risk-free rate for simplicity in crypto/backtests
                sharpe = (daily_rets.mean() / std_dev) * np.sqrt(365)

            max_dd = df["drawdown"].min()
        else:
            return {}

        # Trade Statistics (derived from PnL events)
        trades = self.df_trades if self.df_trades is not None else pd.Series(dtype="float64")

        # Ensure numeric dtype so operators and the type checker behave predictably
        try:
            trades = trades.astype("float64")
        except Exception:
            trades = pd.to_numeric(trades, errors="coerce").fillna(0.0).astype("float64")

        total_trades = len(trades)
        wins = trades[trades > 0]
        losses = trades[trades < 0]

        win_rate = len(wins) / total_trades if total_trades > 0 else 0
        avg_win = float(wins.mean()) if len(wins) > 0 else 0.0
        avg_loss = float(losses.mean()) if len(losses) > 0 else 0.0

        # Compute sums as floats to satisfy static type checkers and avoid object dtypes
        wins_sum = float(wins.sum()) if not wins.empty else 0.0
        losses_sum = float(losses.sum()) if not losses.empty else 0.0

        # Profit factor: wins / abs(losses). If no losses, set to +inf
        profit_factor = (wins_sum / abs(losses_sum)) if losses_sum != 0.0 else float("inf")
        expectancy = (win_rate * avg_win) + ((1 - win_rate) * avg_loss)

        def safe_round(val: float, decimals: int = 2) -> float:
            if pd.isna(val) or np.isinf(val):
                return 0.0
            return round(val, decimals)

        stats = {
            "Net Profit ($)": safe_round(net_profit, 2),
            "CAGR (%)": safe_round(cagr * 100, 2),
            "Sharpe Ratio": safe_round(sharpe, 2),
            "Ann. Volatility (%)": safe_round(ann_vol * 100, 2),
            "Max Drawdown (%)": safe_round(max_dd * 100, 2),
            "Total Trades": total_trades,
            "Win Rate (%)": safe_round(win_rate * 100, 2),
            "Profit Factor": (
                round(profit_factor, 2) if not np.isinf(profit_factor) else float("inf")
            ),
            "Avg Win ($)": safe_round(avg_win, 2),
            "Avg Loss ($)": safe_round(avg_loss, 2),
            "Expectancy ($)": safe_round(expectancy, 2),
        }
        return stats

    def print_stats(self) -> None:
        stats = self.calculate_stats()
        print("=" * 40)
        print(f"{'BACKTEST PERFORMANCE REPORT':^40}")
        print("=" * 40)
        for k, v in stats.items():
            print(f"{k:<25} {v}")
        print("=" * 40)

    def plot_analysis(self) -> None:
        """Generates and saves the standard analysis plots."""
        if self.df_account is None or self.df_account.empty:
            print("[-] No account data to plot.")
            return

        df = self.df_account

        fig = plt.figure(figsize=(14, 12))
        gs = fig.add_gridspec(3, 2)

        # 1. Equity Curve
        ax1 = fig.add_subplot(gs[0, :])
        ax1.plot(df.index, df["equity"], color="#2980b9", linewidth=1.5)
        ax1.set_title("Account Equity", fontweight="bold")
        ax1.set_ylabel("Equity ($)")
        ax1.grid(True, alpha=0.3)

        # 2. Drawdown Area
        ax2 = fig.add_subplot(gs[1, :], sharex=ax1)
        ax2.fill_between(df.index, df["drawdown"] * 100, 0, color="#c0392b", alpha=0.4)
        ax2.set_title("Drawdown (%)", fontweight="bold")
        ax2.set_ylabel("Drawdown %")
        ax2.grid(True, alpha=0.3)

        # 3. Daily Returns Distribution
        daily_rets = df["equity"].resample("D").last().pct_change(fill_method=None).dropna() * 100
        ax3 = fig.add_subplot(gs[2, 0])
        if not daily_rets.empty:
            ax3.hist(daily_rets, bins=50, color="#8e44ad", alpha=0.7, edgecolor="black")
        ax3.set_title("Daily Returns Distribution", fontweight="bold")
        ax3.set_xlabel("Return (%)")
        ax3.set_ylabel("Frequency")

        # 4. Cumulative Realized PnL
        ax4 = fig.add_subplot(gs[2, 1])
        if "rpnl" in df.columns:
            ax4.plot(df.index, df["rpnl"], color="#27ae60", linewidth=1.5)
        ax4.set_title("Cumulative Realized PnL", fontweight="bold")
        ax4.set_ylabel("PnL ($)")
        ax4.grid(True, alpha=0.3)

        plt.tight_layout()
        save_path = self.log_dir / "analysis_dashboard.png"
        plt.savefig(save_path, dpi=100)
        print(f"\n[+] Analysis plots saved to: {save_path}")
        plt.close()
