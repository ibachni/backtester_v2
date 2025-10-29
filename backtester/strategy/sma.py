from __future__ import annotations

from collections import deque
from collections.abc import Mapping as CMapping
from dataclasses import dataclass, fields
from typing import Any, Deque, Dict, Iterator, List, Mapping, Optional

from backtester.adapters.types import Candle, Fill, OrderIntent, OrderType, Side, Timeframe
from backtester.strategy.base import Strategy, StrategyInfo


@dataclass(frozen=True)
class SMACrossParams(CMapping[str, Any]):
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")
    timeframe: Timeframe | None = None
    fast: int = 5
    slow: int = 20
    qty: float = 0.01
    allow_reentry: bool = False  # if False, one round-trip per symbol

    def __iter__(self) -> Iterator[str]:
        return (f.name for f in fields(self))

    def __len__(self) -> int:
        return len(fields(self))

    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError as e:
            raise KeyError(key) from e


class SMACrossStrategy(Strategy):
    """
    Deterministic, long-only SMA(fast/slow) crossover:
      - Enter long when fast_sma > slow_sma and not in position
      - Exit (sell) when fast_sma < slow_sma and in position
    Warmup bars: max(fast, slow)
    State:
      - _price_win[s]: rolling close prices (len <= slow)
      - _pos[s]: whether we consider ourselves long
      - _done_round_trip[s]: block re-entry if allow_reentry=False
      - _pending[s]: None | "buy" | "sell" to avoid duplicate intents while waiting for fills
      - _last_signal_ts[s]: last candle end_ms we signaled on (observability)
    """

    def __init__(self, params: Mapping[str, Any], *, info: Optional[StrategyInfo] = None) -> None:
        # ...existing code...
        super().__init__(params, info=info or StrategyInfo(name="sma_cross"))

        if isinstance(params, SMACrossParams):
            p = params
        else:
            p = SMACrossParams(
                symbols=tuple(self.params.get("symbols", ())),
                timeframe=self.params.get("timeframe"),
                fast=int(self.params.get("fast", 5)),
                slow=int(self.params.get("slow", 20)),
                qty=float(self.params.get("qty", 0.001)),
                allow_reentry=bool(self.params.get("allow_reentry", False)),
            )
        if p.fast <= 0 or p.slow <= 0 or p.fast >= p.slow:
            raise ValueError("Require 0 < fast < slow for SMA crossover")

        self._p = p
        # Per symbol rolling window of recent prices (deque), sized to at most slow
        self._price_win: Dict[str, Deque[float]] = {}
        # Per symbol boolean indicating if the strategy considers itself currently long.
        self._pos: Dict[str, bool] = {}
        # Per-symbol boolean to block re-entry after a completed buy-sell cycle when allow_reentry
        # is False
        self._done_round_trip: Dict[str, bool] = {}
        # Track pending action to prevent duplicate intents before a fill arrives
        self._pending: Dict[str, Optional[str]] = {}
        # UTC end_ms of last signal per symbol (observability/idempotency guard)
        self._last_signal_ts: Dict[str, Optional[int]] = {}
        # ...existing code...

    # --- strategy metadata ---

    def symbols(self) -> tuple[str, ...]:
        return self._p.symbols

    def timeframe(self) -> tuple[Timeframe, ...]:
        return (self._p.timeframe,) if self._p.timeframe is not None else ()

    def warmup_bars(self) -> int:
        return max(self._p.fast, self._p.slow)

    # --- lifecycle ---

    def on_start(
        self,
    ) -> list[OrderIntent]:
        # Initialize per-symbol state
        for s in self.symbols() or ():
            self._price_win[s] = deque(maxlen=self._p.slow)
            self._pos[s] = False
            self._done_round_trip[s] = False
            self._pending[s] = None
            self._last_signal_ts[s] = None
        # self.log_event(ctx, "info", "strategy_started", symbols=",".join(self.symbols() or ()))
        return []

    def on_candle(self, candle: Candle) -> List[OrderIntent]:
        intents: List[OrderIntent] = []

        # Process
        s = candle.symbol
        if self.symbols() and s not in self.symbols():
            return intents  # ignore symbols we didn't declare

        # Lazy init for unforeseen symbols (defensive)
        if s not in self._price_win:
            self._price_win[s] = deque(maxlen=self._p.slow)
            self._pos[s] = False
            self._done_round_trip[s] = False
            self._pending[s] = None
            self._last_signal_ts[s] = None

        # Update window
        self._price_win[s].append(float(candle.close))
        if len(self._price_win[s]) < self.warmup_bars():
            return intents  # not warmed up

        # Compute SMAs (window length is at most slow)
        prices = list(self._price_win[s])
        slow_sma = sum(prices) / len(prices)
        fast_len = min(self._p.fast, len(prices))
        fast_sma = sum(prices[-fast_len:]) / fast_len

        in_pos = self._pos[s]
        # can_reenter = self._p.allow_reentry or not self._done_round_trip[s]
        # pending = self._pending[s]

        # Entry: fast > slow, not in position, not blocked, no pending order
        if (
            (not in_pos)
            and
            # can_reenter and
            # pending is None and
            fast_sma > slow_sma
        ):
            intent = self._mk_market_intent(
                symbol=s, side="buy", qty=self._p.qty, reason="entry_fast_gt_slow"
            )
            intents.append(intent)
            self._pending[s] = "buy"
            self._last_signal_ts[s] = candle.end_ms
            # self.log_event(
            #     ctx,
            #     "info",
            #     "signal_entry",
            #     symbol=s,
            #     end_ms=candle.end_ms,
            #     fast_sma=round(fast_sma, 8),
            #     slow_sma=round(slow_sma, 8),
            #     qty=self._p.qty,
            # )
            print("buy", self.snapshot_state())
            return intents

        # Exit: fast < slow, in position, no pending order
        if (
            # in_pos and
            # pending is None and
            fast_sma < slow_sma
        ):
            intent = self._mk_market_intent(
                symbol=s, side="sell", qty=self._p.qty, reason="exit_fast_lt_slow"
            )
            intents.append(intent)
            self._pending[s] = "sell"
            self._last_signal_ts[s] = candle.end_ms
            # self.log_event(
            #     ctx,
            #     "info",
            #     "signal_exit",
            #     symbol=s,
            #     end_ms=candle.end_ms,
            #     fast_sma=round(fast_sma, 8),
            #     slow_sma=round(slow_sma, 8),
            #     qty=self._p.qty,
            # )
            print("sell", self.snapshot_state())
            return intents

        print("neither", self.snapshot_state())
        return intents

    def on_fill(self, fill: Fill) -> Optional[List[OrderIntent]]:
        # Minimal state update based on fill side
        s = getattr(fill, "symbol", None)
        sd = str(getattr(fill, "side", "")).lower()
        if not s:
            return None

        if sd == "buy":
            self._pos[s] = True
        elif sd == "sell":
            self._pos[s] = False
            if not self._p.allow_reentry:
                self._done_round_trip[s] = True

        # Clear pending after a fill is observed
        self._pending[s] = None
        # self.log_event(
        #     ctx, "info", "fill_applied", symbol=s, side=sd, qty=getattr(fill, "qty", "?")
        # )
        return []

    def on_end(self) -> None:
        open_syms = [s for s, p in self._pos.items() if p]
        print(f"On_end: positions open: {open_syms}")
        # self.log_event(
        #     ctx,
        #     "info",
        #     "strategy_ended",
        #     open_positions=",".join(open_syms) if open_syms else "",
        # )

    # --- observability ---

    def snapshot_state(self) -> dict[str, Any]:
        snap = super().snapshot_state()
        snap.update(
            {
                "fast": self._p.fast,
                "slow": self._p.slow,
                "qty": self._p.qty,
                "allow_reentry": self._p.allow_reentry,
                "pos": dict(self._pos),
                "done_round_trip": dict(self._done_round_trip),
                "pending": dict(self._pending),
                "last_signal_ts": dict(self._last_signal_ts),
            }
        )
        return snap

    # --- helpers ---

    def _mk_market_intent(self, *, symbol: str, side: str, qty: float, reason: str) -> OrderIntent:
        """
        Use ctx.order_factory if available; otherwise try OrderIntent constructor; finally fall back
        to a simple dict (useful for dry integration tests before wiring the execution path).
        """
        tag = f"sma_cross:{reason}"
        # # Order Factory not yet available!
        # of = getattr(ctx, "order_factory", None)
        # tag = f"sma_cross:{reason}"
        # try:
        #     if of is not None and hasattr(of, "market"):
        #         return of.market(symbol=symbol, side=side, qty=qty, tag=tag)
        # except Exception:
        #     pass

        # Fallbacks (be liberal in what we output — engine/risk layer enforces constraints)
        side_enum = Side.BUY if side == "buy" else Side.SELL
        return OrderIntent(
            symbol=symbol, side=side_enum, type=OrderType.MARKET, qty=qty, tags={"tag": tag}
        )
