from datetime import datetime, timezone
from typing import Optional

from tqdm import tqdm

from backtester.types.types import Candle, PortfolioSnapshot


class ConsoleMonitor:
    def __init__(self, sim_start_ms: int, timeframe_ms: int) -> None:
        self.sim_start_ms = sim_start_ms
        self.timeframe_ms = timeframe_ms
        self.pbar = None
        self.last_equity: float = 0.0
        self.peak_equity: float = 0.0
        self.last_dd: float = 0.0
        self.current_time: Optional[datetime] = None
        self.monthly_equity: float = 0.0

        # State to prevent flickering/excessive I/O
        self._initialized = False

    def on_start(self, total_ticks: int, end_of_period: bool = False) -> None:
        if end_of_period:
            print("\n" + "=" * 90)
            print(
                f"{'DATE':<12} | {'NAV':<12} | {'MONTHLY %':<10} | {'MAX DD':<10} |{'EVENTS/s':<8}"
            )
            print("-" * 90)

        # Initialize tqdm manually
        self.pbar = tqdm(total=total_ticks, unit="tick", bar_format="{l_bar}{bar} | {postfix}")
        self._initialized = True

    def on_market_event(self, event: Candle, end_of_period: bool = False) -> None:
        """
        Updates the progress bar. If new month, call on_end_of_period
        """
        if not self._initialized:
            return

        event_dt = datetime.fromtimestamp(event.end_ms / 1000, tz=timezone.utc)

        if self.current_time is None:
            self.current_time = event_dt

        # If new month
        if end_of_period:
            if (self.current_time.year, self.current_time.month) < (event_dt.year, event_dt.month):
                self.on_end_of_period(event_dt)

        self.current_time = event_dt

    def on_portfolio_update(self, event: PortfolioSnapshot) -> None:
        """Updates internal state so the next Heartbeat is accurate"""
        self.last_equity = float(event.equity)
        if self.monthly_equity == 0.0:
            self.monthly_equity = self.last_equity
        self.peak_equity = max(self.peak_equity, self.last_equity)

        if self.peak_equity > 0:
            self.last_dd = (self.last_equity - self.peak_equity) / self.peak_equity
        else:
            self.last_dd = 0.0

        # Update the heartbeat text (bottom line)
        if self.pbar is not None:
            if self.pbar.n % 100 == 0 and self.current_time:
                self.pbar.set_postfix_str(
                    f"{self.current_time} | "
                    f"NAV: ${self.last_equity/1000:.1f}k | "
                    f"DD: {self.last_dd:.1%}"
                )
            self.pbar.update(1)

    def on_end_of_period(self, event_dt: datetime) -> None:
        """
        Triggered by an 'EndOfDay' or 'EndOfMonth' event.
        Uses tqdm.write to print the snapshot line safely.
        """
        if not self._initialized:
            return

        monthly_return = 0.0
        if self.monthly_equity != 0.0:
            monthly_return = (self.last_equity - self.monthly_equity) / self.monthly_equity

        # The following here messes up the progress bar, so only either on of the should be used.

        # Format colors/strings
        row = (
            f"{event_dt.date()}   | "
            f"${self.last_equity:,.0f}     | "
            f"{monthly_return:>7.2%}    | "
            f"{self.last_dd:>7.2%}    | "
            f"OK"
        )
        if self.pbar is not None:
            self.pbar.write(row)

        self.monthly_equity = self.last_equity

    def on_finish(self) -> None:
        if self.pbar:
            self.pbar.set_postfix_str(
                f"{self.current_time} | "
                f"NAV: ${self.last_equity/1000:.1f}k | "
                f"DD: {self.last_dd:.1%}"
            )
            self.pbar.update(1)
            self.pbar.close()
        print("\n")
        print("-" * 90)
        print(">>> BACKTEST COMPLETE")
        print("-" * 90 + "\n")
