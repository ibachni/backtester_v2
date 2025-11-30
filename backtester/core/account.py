"""
Responsibilities:
- Cash, positions, equity, rPnL, uPnL
- Constraints: Deterministic, Rebuildable (to be added / reconstruct state from
initial snapshot) and venue-aware (to be added, Binance MVP -> respects tick/lot/min-notional
via SymbolSpec

Scope:
- No leverage no funding.
- Average-Cost position method
- Fees accrued in quote (USDC) and reduce (equity or cash??)

Optional:
- A ledger, a trail what the account applied!
- _validate_fill (when fills come from exchange!)
- _check_invariants (check fees paid, ts, positions consistency, )
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Callable, Mapping, Optional, Tuple

from backtester.audit.audit import AuditWriter
from backtester.config.configs import AccountConfig
from backtester.core.bus import Bus
from backtester.core.clock import Clock
from backtester.types.aliases import Symbol, UnixMillis
from backtester.types.topics import T_ACCOUNT_SNAPSHOT, T_LOG, T_LOT_EVENT, T_TRADE_EVENT
from backtester.types.types import (
    ZERO,
    Fill,
    LogEvent,
    LotClosedEvent,
    PortfolioSnapshot,
    Position,
    PositionView,
    SymbolSpec,
    TradeEvent,
)
from backtester.utils.utility import dec

# --- Internal position state (mutable). External views use core.types.Position (frozen). ---


class Account:
    """
    Responsibilities:
    - Maintain portfolio state: cash, positions, equity
    - Apply fills/transactions: update quantities, cash, fees, P&L
    - Mark-to-markt: value positions from prices at eahc bar/tick
    - Track P&L: realized vs. unrealized, equity surve, drawdowns
    - Expose read-only snashots for metric/reporting

    # Fees: Computation in Sim/Broker, apply/account them in Portfolio

    MVP:
    - Long only (no borrowing margin cost)


    # Working:
    - If fill increases exposure on a side, append a new lot at (qty, price)
    - When a fill decreases exposure, we match against the head of deque: close longs when
    sell arrives and cover shorts when a buy arrives
    - If a lot hits zero, it is popped

    Later Additions
    - Multi currency portfolio; Venue aware precision
    - Funding, Borrowing, Margin
    - Ledger/Journaling: Immutable journal of entries; rebuild state from journal (determinism)
    - Risk layer: position limits, max notional, sector exposure, VAR, pre-trade veto hooks
    - Futures, options, crypto, bonds
    - Taxes
    - OrderTypes: Stop/limit, IOC, FOK, iceberg, multiple venues &latency
    - Cahs management: interest on cash, sweeps, margin interest, cash drag
    - Risk& constraints layer: position/sector/VAR limits, pre-trade veto
    - Portfolio Construction Layer: Target weights, rebalancing schedules, drift bands
    - Multi-strategy book: sleeves/sub books, capital allocation, PnL attribution vs strategy
    - Scenario and stress testing; factor/sector exposures; attribution (allocation vs selection)
    - Persistence and replay; checkpoints, journaling, deterministic re-runs
    - live trading bridgel reconciliation with broker statements

    """

    def __init__(
        self,
        cfg: AccountConfig,
        bus: Bus,
        audit_writer: AuditWriter,
        clock: Clock,
        starting_cash: Decimal = ZERO,
    ) -> None:
        self._cfg = cfg
        self._bus = bus
        self._audit = audit_writer
        self._clock = clock
        self._cash: Decimal = self._cfg.starting_cash
        self._venue: str = self._cfg.venue
        self._base_ccy: str = self._cfg.base_ccy

        # Single Source of Truth (updated by apply_fill)
        self._positions: dict[Symbol, Position] = {}
        self._rpnl = ZERO
        self._fees_paid = ZERO

        # Additional: mark to market price map
        self._marks: dict[Symbol, Decimal] = {}

        self._last_ts: UnixMillis = 0
        self._last_snapshot: Optional[PortfolioSnapshot] = None
        self._get_specs: Optional[Callable[[str], Optional[SymbolSpec]]] = None

        # Idempotency: skip duplicate fills (by client_id if present, else deterministic key)
        self._seen_fills: set[str] = set()

        # Publish snapshot if updated=True at the end of a cycle
        self._updated: bool = False

        # stats
        self._peak_equity: float = 0
        self._max_drawdown_pct: float = 0
        self._total_fees: float = 0
        self._total_turnover: float = 0
        self._dust_cleanups: int = 0
        self._open_positions: int = 0

    # --- Property methods ---

    @property
    def cash(self) -> Decimal:
        return self._cash

    @property
    def rpnl(self) -> Decimal:
        return self._rpnl

    @property
    def fees_paid(self) -> Decimal:
        return self._fees_paid

    @property
    def marks(self) -> Mapping[Symbol, Decimal]:
        return dict(self._marks)

    @property
    def last_ts(self) -> UnixMillis:
        return self._last_ts

    def equity(self) -> Decimal:
        if self._last_snapshot:
            return self._last_snapshot.equity
        else:
            return self._cash

    def position_qty(self, symbol: str) -> Optional[Decimal]:
        if symbol not in self._positions.keys():
            return None
        return self._positions[symbol].qty

    def positions_view(self) -> Mapping[Symbol, Tuple[Decimal, Decimal]]:
        """Read-only mapping: symbol -> (qty, avg_price)."""
        return {s: (p.qty, p.avg_cost) for s, p in self._positions.items()}

    def portfolio_view(self) -> Optional[PortfolioSnapshot]:
        return self._last_snapshot

    async def publish_latest_snapshot(self) -> None:
        await self._bus.publish(T_ACCOUNT_SNAPSHOT, self._clock.now(), self._last_snapshot)

    # --- Core API ---

    async def apply_fill(self, fill: Fill) -> None:
        """
        Apply a single fill
        """
        key = self._fill_key(fill)
        if key in self._seen_fills:
            return

        if fill.price <= ZERO:
            raise ValueError("Fill.price must be > 0 for accounting.")

        sym: Symbol = fill.symbol
        q_fill: Decimal = fill.qty
        p_fill: Decimal = fill.price
        fees: Decimal = fill.fees_explicit

        fill_notional = abs(q_fill * p_fill)

        pos = self._positions.get(sym)
        if pos is None:
            pos = Position(symbol=sym)
            self._positions[sym] = pos

        eps = self._pos_epsilon(sym)

        # 1) cash update
        self._cash -= (q_fill * p_fill) + fees
        self._fees_paid += fees
        self._total_turnover += float(fill_notional)
        self._total_fees += float(fees)

        # 2) Lot matching
        # 2.1) Buy Case
        if q_fill > ZERO:
            # If head lots is short (negative qty) offset them first (FIFO)
            # (q_fill, pos._lots)
            while q_fill > ZERO and pos._lots and pos._lots[0][0] < ZERO:
                lot_qty, lot_px, lot_ts, lot_fee = pos._lots[0]
                match_qty = min(q_fill, -lot_qty)
                # Closing a short: realized = (short_px - buy_px) * matched
                realized_delta = (lot_px - p_fill) * match_qty
                # When lot is closed, account wide pnl is realized
                pos.realized_pnl += realized_delta
                self._rpnl += realized_delta

                lot_qty += match_qty
                q_fill -= match_qty
                # If short los it full covered, pop it
                if lot_qty == ZERO:
                    pos._lots.popleft()
                    # LotClosedEvent used for WinRate and per-trade PnL
                    lot_closed = LotClosedEvent(
                        symbol=pos.symbol,
                        qty=match_qty,
                        entry_ts=lot_ts,
                        entry_price=lot_px,
                        exit_ts=fill.ts,
                        exit_price=p_fill,
                        realized_pnl=realized_delta,
                        fee_entry=lot_fee,
                        fee_exit=dec(fees * (match_qty / fill.qty)),
                    )
                    await self._bus.publish(T_LOT_EVENT, fill.ts, lot_closed)
                else:
                    # If not zero, update the lot with new values
                    pos._lots[0] = (lot_qty, lot_px, lot_ts, lot_fee)

            if q_fill > ZERO:
                pos._lots.append((q_fill, p_fill, fill.ts, fees))
                q_fill = ZERO

        # 2.2) Sell Case: q_fill <0
        else:
            sell_abs = -q_fill
            # If existing lots contain logs (pos qty) offset them first (FIFO)
            while sell_abs > ZERO and pos._lots and pos._lots[0][0] > ZERO:
                lot_qty, lot_px, lot_ts, lot_fee = pos._lots[0]
                match_qty = min(sell_abs, lot_qty)
                # closing a long: realized = (p_fill - lot_px) * match_qty
                realized_delta = (p_fill - lot_px) * match_qty
                pos.realized_pnl += realized_delta
                self._rpnl += realized_delta
                lot_qty -= match_qty
                sell_abs -= match_qty
                if lot_qty == ZERO:
                    pos._lots.popleft()
                    lot_closed = LotClosedEvent(
                        symbol=pos.symbol,
                        qty=match_qty,
                        entry_ts=lot_ts,
                        entry_price=lot_px,
                        exit_ts=fill.ts,
                        exit_price=p_fill,
                        realized_pnl=realized_delta,
                        fee_entry=lot_fee,
                        fee_exit=dec(fees * (match_qty / fill.qty)),
                    )
                    await self._bus.publish(T_LOT_EVENT, fill.ts, lot_closed)
                else:
                    pos._lots[0] = (lot_qty, lot_px, lot_ts, lot_fee)

            # Any remaining sell increases / creates short lots
            if sell_abs > ZERO:
                pos._lots.append((q_fill, p_fill, fill.ts, fees))
                sell_abs = ZERO
            q_fill = ZERO

        # 3) Recompute qty/avg_cost
        pos._recalc_from_lots()

        mark_px = self._marks.get(sym)
        if mark_px is not None:
            pos._apply_mark(mark_px)
        elif pos.last_price <= ZERO:
            pos._apply_mark(p_fill)

        # 4) Clean up dust
        pos = self._positions.get(sym)
        if pos and pos.qty != ZERO and abs(pos.qty) <= eps:
            pos.qty = ZERO
            pos.avg_cost = ZERO
            pos.market_value = ZERO
            pos.unrealized_pnl = ZERO
            pos.clear_lots()
            self._dust_cleanups += 1
            await self._emit_log(
                level="DEBUG",
                msg="ACCOUNT_DUST_CLEANED",
                payload={"position": pos},
                sim_time=self._clock.now(),
            )

        self._update_performance_metrics()

        # 5) Update snapshot
        self._last_snapshot = await self.snapshot(ts=fill.ts, store=True, publish=False)
        self._updated = True

        # 6) Emit TradeEvent (for Stats)
        # Used for total turnover and feed paid on open positions
        trade_event = TradeEvent(
            ts=self._clock.now(),
            symbol=fill.symbol,
            side=fill.side,
            qty=float(fill.qty),
            price=float(fill.price),
            fee=float(fees),
            liquidity_flag=fill.liquidity_flag,
        )
        await self._bus.publish(T_TRADE_EVENT, ts_utc=self._clock.now(), payload=trade_event)

        # 7) Mark as applied
        self._seen_fills.add(key)

    def set_mark(
        self, symbol: Symbol, price: Decimal, ts: UnixMillis, update_metrics: bool = True
    ) -> None:
        """
        Set the latest mark for a symbol (on price update!)
        """
        if ts < self._last_ts:
            raise ValueError(
                "MonotonicClock Error: given ts smaller than last_ts: {ts}<{self._last_ts}"
            )
        p_dec = dec(price)
        if p_dec <= ZERO:
            raise ValueError("Mark price must be >0.")
        self._marks[symbol] = p_dec
        self._last_ts = ts
        pos = self._positions.get(symbol)
        if pos is not None:
            pos._apply_mark(p_dec)
        if update_metrics:
            self._update_performance_metrics()

    def mark_all(self, marks: Mapping[Symbol, Decimal], ts: UnixMillis) -> None:
        if ts < self._last_ts:
            raise ValueError(
                "MonotonicClock Error: given ts smaller than last_ts: {ts}<{self._last_ts}"
            )
        self._last_ts = ts
        for symbol, price in marks.items():
            p_dec = dec(price)
            if p_dec <= ZERO:
                raise ValueError("Mark price must be >0.")
            self._marks[symbol] = p_dec
            pos = self._positions.get(symbol)
            if pos is not None:
                pos._apply_mark(p_dec)
        self._update_performance_metrics()

    def deposit(self, amount: Decimal) -> None:
        if amount <= ZERO:
            raise ValueError("Deposit amount must be positive.")
        self._cash += amount

    def withdraw(self, amount: Decimal) -> None:
        if amount <= ZERO:
            raise ValueError("Deposit amount must be positive.")
        self._cash -= amount

    async def close_all(self, ts: Optional[UnixMillis] = None) -> PortfolioSnapshot:
        """
        Flatten all positions at current marks. Return a portfolio Snapshot.
        """
        if ts:
            self._last_ts = ts
        for sym, pos in self._positions.items():
            eps = self._pos_epsilon(sym)
            if abs(pos.qty) <= eps:
                continue

            # require a current mark
            p_close = self._marks.get(sym)
            if p_close is None or p_close <= ZERO:
                raise ValueError(f"Missing/invalid mark for {sym}")

            q = pos.qty  # signed
            # Realized PnL from full flatten at one price equals (p - avg_cost) * qty
            realized_total = (p_close - pos.avg_cost) * q

            # Cash impact: sell longs / buy shorts at p_close
            self._cash += q * p_close

            # Update realized PnL at both levels
            pos.realized_pnl += realized_total
            self._rpnl += realized_total

            # Zero out the position and clear lots
            pos.last_price = p_close
            pos.qty = ZERO
            pos.avg_cost = ZERO
            pos.market_value = ZERO
            pos.unrealized_pnl = ZERO
            pos.clear_lots()

        return await self.snapshot(self._last_ts)

    # --- Stats ---

    def get_stats(self) -> dict[str, Any]:
        return {
            "final_equity": float(self._calculate_total_equity()),
            "total_rpnl": float(self._rpnl),
            "peak_equity": self._peak_equity,
            "max_drawdown_pct": self._max_drawdown_pct,
            "total_fees": float(self._fees_paid),
            "total_turnover": self._total_turnover,
            "dust_cleanups": self._dust_cleanups,
            "open_positions": self._open_positions,
        }

    # --- Snapshots ---

    async def snapshot(
        self,
        ts: Optional[UnixMillis] = None,
        store: bool = True,
        publish: bool = False,
    ) -> PortfolioSnapshot:
        """
        Parameter:
        - Store: bool - Whether to safe last snapshot in-memory in self._last_snapshot
        """
        positions: list[PositionView] = []
        upnl_total: Decimal = ZERO
        gross_exposure: Decimal = ZERO
        net_exposure: Decimal = ZERO

        for symbol, pos in self._positions.items():
            # skip dust-sized positions
            if abs(pos.qty) <= self._pos_epsilon(symbol):
                continue
                # raise ValueError("Dust Error in snapshot") / emit log

            # Require a valid mark
            if pos.last_price <= ZERO:
                raise ValueError(f"Mark for symbol {pos.symbol} missing")

            # Take a copy for the snapshot
            positions.append(
                PositionView(
                    symbol=symbol,
                    qty=pos.qty,
                    avg_cost=pos.avg_cost,
                    last_price=pos.last_price,
                    market_value=pos.market_value,
                    unrealized_pnl=pos.unrealized_pnl,
                    realized_pnl=pos.realized_pnl,
                )
            )

            # Aggregate metrics
            upnl_total += pos.unrealized_pnl
            gross_exposure += abs(pos.market_value)
            net_exposure += pos.market_value

        # Equity = cash + net market value
        equity_total = self._cash + net_exposure
        snap_ts = int(ts) if ts is not None else int(self._last_ts)

        snapshot: PortfolioSnapshot = PortfolioSnapshot(
            ts=snap_ts,
            base_ccy=self._base_ccy,
            cash=self._cash,
            equity=equity_total,
            upnl=upnl_total,
            rpnl=self._rpnl,
            gross_exposure=gross_exposure,
            net_exposure=net_exposure,
            fees_paid=self._fees_paid,
            positions=tuple(positions),
        )
        # Only emit if change in cash (buy / sell changes)
        if store:
            self._last_snapshot = snapshot
        if publish:
            await self._bus.publish(
                topic=T_ACCOUNT_SNAPSHOT, ts_utc=self._clock.now(), payload=snapshot
            )

        return snapshot

    async def snapshot_on_bar(
        self, mark: tuple[str, Decimal], ts: UnixMillis, store: bool = True
    ) -> None:
        """
        Ad-hoc method;
        input:
            - new price information mark[symbol: str, price: Decimal] with ts
            - store: bool: Whether snapshot is to be stored in-memory
        Output: None
        """
        self.set_mark(ts=ts, symbol=mark[0], price=mark[1])
        await self.snapshot(ts=ts, store=store, publish=True)

    # --- Helpers ---

    def reset(self, starting_cash: Optional[Decimal] = None, clear_marks: bool = True) -> None:
        if starting_cash is not None:
            self._cash = starting_cash
        self._rpnl = ZERO
        self._fees_paid = ZERO
        self._last_snapshot = None
        self._positions.clear()
        if clear_marks:
            self._marks.clear()
        self._last_ts = 0
        self._seen_fills.clear()
        self._fees_paid = ZERO
        self._peak_equity = 0
        self._max_drawdown_pct = 0
        self._total_fees = 0
        self._total_turnover = 0
        self._dust_cleanups = 0
        self._open_positions = 0
        self._updated = False

    def _pos_epsilon(self, symbol: Symbol) -> Decimal:
        spec = self._get_specs
        lot = getattr(spec, "lot_size", None)
        if lot is None or lot <= 0:
            return Decimal("1e-12")
        lot_dec = dec(lot)
        return max(Decimal("1e-12"), lot_dec * Decimal("0.5"))

    def _fill_key(self, fill: Fill) -> str:
        fid = getattr(fill, "fill_id", None)
        if fid:
            return str(fid)
        ts = getattr(fill, "ts", 0)
        return "|".join(
            map(str, (fill.symbol, fill.side, fill.qty, fill.price, fill.fees_explicit, ts))
        )

    # --- Logging ---

    async def _emit_log(
        self,
        *,
        level: str,
        msg: str,
        payload: Optional[dict] = None,
        sim_time: Optional[int] = None,
    ) -> None:
        """
        Publish Account log events via bus per ADR 022.
        """
        ts = sim_time if sim_time is not None else int(self._clock.now())
        log_event = LogEvent(
            level=level,
            component="account",
            msg=msg,
            payload=payload or {},
            sim_time=ts,
        )
        await self._bus.publish(topic=T_LOG, ts_utc=ts, payload=log_event)

    # --- Metrics ---

    def _calculate_total_equity(self) -> Decimal:
        # Sum Market Value of all non-dust positions
        # This is O(N) but avoids object allocation overhead of Snapshot
        pos_equity = sum(p.market_value for p in self._positions.values())
        return self._cash + pos_equity

    def _update_performance_metrics(self) -> None:
        """
        Updates peak equity and max drawdown based on current internal state.
        Should be called after fills or price updates.
        """
        current_equity = float(self._calculate_total_equity())

        # Initialize peak if first run
        if self._peak_equity == 0:
            self._peak_equity = current_equity
        else:
            self._peak_equity = max(self._peak_equity, current_equity)

        # Update Drawdown
        if self._peak_equity > 0:
            dd = (self._peak_equity - current_equity) / self._peak_equity
            self._max_drawdown_pct = max(self._max_drawdown_pct, dd)

        # Open positions count (simple integer check)
        self._open_positions = len(
            [p for p in self._positions.values() if abs(p.qty) > self._pos_epsilon(p.symbol)]
        )
