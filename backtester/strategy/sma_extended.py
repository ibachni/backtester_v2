from __future__ import annotations

from collections import deque
from dataclasses import dataclass, fields
from decimal import Decimal
from typing import Any, Deque, Dict, Iterator, List, Mapping, Optional

from backtester.core.clock import Clock
from backtester.strategy.base import Strategy
from backtester.types.types import (
    Candle,
    Fill,
    Market,
    MarketOrderIntent,
    OrderIntent,
    Side,
    StrategyInfo,
)


@dataclass(frozen=True)
class SMAExtendedParams:
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")
    timeframe: str | None = None
    fast: int = 10
    slow: int = 40
    qty: Decimal = Decimal("0.01")
    allow_reentry: bool = True
    max_positions: int = 3
    cooldown_bars: int = 5
    tp_pct: float = 0.02
    sl_pct: float = 0.01
    trail_pct: float = 0.005
    daily_loss_limit: float = 200.0

    def __iter__(self) -> Iterator[str]:
        return (f.name for f in fields(self))

    def __len__(self) -> int:
        return len(fields(self))

    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError as exc:
            raise KeyError(key) from exc


class SMAExtendedStrategy(Strategy):
    def __init__(
        self,
        clock: Clock,
        params: Mapping[str, Any],
        *,
        info: Optional[StrategyInfo] = None,
    ) -> None:
        super().__init__(params, info=info or StrategyInfo(name="sma_extended"))
        self._clock = clock

        self._p = SMAExtendedParams(
            symbols=tuple(self.params.get("symbols", ("BTCUSDT", "ETHUSDT"))),
            timeframe=self.params.get("timeframe"),
            fast=int(self.params.get("fast", 10)),
            slow=int(self.params.get("slow", 40)),
            qty=Decimal(str(self.params.get("qty", "0.01"))),
            allow_reentry=bool(self.params.get("allow_reentry", True)),
            max_positions=int(self.params.get("max_positions", 3)),
            cooldown_bars=int(self.params.get("cooldown_bars", 5)),
            tp_pct=float(self.params.get("tp_pct", 0.02)),
            sl_pct=float(self.params.get("sl_pct", 0.01)),
            trail_pct=float(self.params.get("trail_pct", 0.005)),
            daily_loss_limit=float(self.params.get("daily_loss_limit", 200.0)),
        )
        if not (0 < self._p.fast < self._p.slow):
            raise ValueError("Require 0 < fast < slow")

        self._price_win: Dict[str, Deque[float]] = {}  # deque of recent closes for SMA calc
        self._pos: Dict[str, bool] = {}  # bool
        self._pending: Dict[
            str, Optional[str]
        ] = {}  # pending signal (to prevent duplicate submits)
        self._cooldown: Dict[str, int] = {}  # bars remaining before re-entry
        self._entry_px: Dict[str, float] = {}  # entry_price
        self._trail: Dict[str, float] = {}  # trailing-stop-level
        self._last_signal_ts: Dict[str, Optional[int]] = {}
        self._daily_pnl: float = 0.0  # dailing PnL gating
        self._current_day: Optional[int] = None  # dailing PnL gating
        self._seq = 0  # Local sequence counter used to construct readable intent IDs

    # --- metadata hooks ---

    def symbols(self) -> tuple[str, ...]:
        return self._p.symbols

    def timeframe(self) -> tuple[str, ...]:
        return (self._p.timeframe,) if self._p.timeframe else ()

    def warmup_bars(self) -> int:
        return max(self._p.fast, self._p.slow)

    # --- lifecycle ---

    def on_start(self) -> list[OrderIntent]:
        for sym in self.symbols():
            self._init_symbol(sym)
        return []

    def on_candle(self, candle: Candle) -> List[OrderIntent]:
        sym = candle.symbol
        self._init_symbol(sym)
        self._roll_day(candle)
        self._update_price(sym, float(candle.close))
        intents: List[OrderIntent] = []

        if not self._is_warm(sym):
            return intents

        fast, slow = self._sma(sym)
        in_pos = self._pos[sym]
        pending = self._pending[sym]
        cooldown_left = self._cooldown[sym]
        slots_used = sum(self._pos.values())
        can_reenter = (
            self._p.allow_reentry and cooldown_left == 0 or not in_pos and cooldown_left == 0
        )
        risk_block = self._daily_pnl <= -self._p.daily_loss_limit

        reason = None
        side = None

        if not in_pos and pending is None and can_reenter and not risk_block:
            if slots_used < self._p.max_positions and fast > slow:
                side, reason = "buy", "fast_gt_slow"
        elif in_pos and pending is None:
            price = float(candle.close)
            entry = self._entry_px.get(sym, price)
            gain = (price - entry) / entry
            trail_hit = self._trail_hit(sym, price)
            if fast < slow:
                side, reason = "sell", "fast_lt_slow"
            elif gain >= self._p.tp_pct:
                side, reason = "sell", "take_profit"
            elif gain <= -self._p.sl_pct:
                side, reason = "sell", "stop_loss"
            elif trail_hit:
                side, reason = "sell", "trailing_stop"

        if side:
            intent = self._mk_market_intent(sym, side, str(reason))
            intents.append(intent)
            self._pending[sym] = side

        if cooldown_left > 0:
            self._cooldown[sym] -= 1

        return intents

    def on_fill(self, fill: Fill) -> list[OrderIntent]:
        sym = fill.symbol
        self._init_symbol(sym)
        self._pending[sym] = None
        if fill.side == Side.BUY:
            self._pos[sym] = True
            self._entry_px[sym] = float(fill.price)
            self._trail[sym] = float(fill.price) * (1 - self._p.trail_pct)
        else:
            self._pos[sym] = False
            self._cooldown[sym] = self._p.cooldown_bars
            self._entry_px.pop(sym, None)
            self._trail.pop(sym, None)
        return []

    # --- helpers ---

    def _mk_market_intent(self, symbol: str, side: str, reason: str) -> OrderIntent:
        self._seq += 1
        side_enum = Side.BUY if side == "buy" else Side.SELL
        return MarketOrderIntent(
            symbol=symbol,
            market=Market.SPOT,
            side=side_enum,
            id=f"{self.info.name}-{symbol}-{self._seq}",
            ts_utc=self._clock.now(),
            strategy_id=self.info.name,
            qty=self._p.qty,
            tags={"reason": reason},
        )

    def _init_symbol(self, sym: str) -> None:
        if sym in self._price_win:
            return
        self._price_win[sym] = deque(maxlen=self._p.slow)
        self._pos[sym] = False
        self._pending[sym] = None
        self._cooldown[sym] = 0
        self._entry_px[sym] = 0.0
        self._trail[sym] = 0.0
        self._last_signal_ts[sym] = None

    def _update_price(self, sym: str, price: float) -> None:
        self._price_win[sym].append(price)
        if self._pos[sym]:
            trail = self._trail[sym]
            if price > trail:
                self._trail[sym] = price * (1 - self._p.trail_pct)

    def _is_warm(self, sym: str) -> bool:
        return len(self._price_win[sym]) >= self.warmup_bars()

    def _sma(self, sym: str) -> tuple[float, float]:
        prices = list(self._price_win[sym])
        slow = sum(prices) / len(prices)
        fast_len = min(self._p.fast, len(prices))
        fast = sum(prices[-fast_len:]) / fast_len
        return fast, slow

    def _trail_hit(self, sym: str, price: float) -> bool:
        return self._pos[sym] and self._trail[sym] > 0 and price <= self._trail[sym]

    def _roll_day(self, candle: Candle) -> None:
        day = candle.end_ms // 86_400_000
        if self._current_day is None:
            self._current_day = day
            return
        if day != self._current_day:
            self._current_day = day
            self._daily_pnl = 0.0

    def snapshot_state(self) -> dict[str, Any]:
        snap = super().snapshot_state()
        snap.update(
            {
                "pos": dict(self._pos),
                "pending": dict(self._pending),
                "cooldown": dict(self._cooldown),
                "entry_px": dict(self._entry_px),
                "trail": dict(self._trail),
                "daily_pnl": self._daily_pnl,
            }
        )
        return snap
