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

# TODO turn to a "realistic clock"
# time passes and Portfolio update stamped with real time instead of
# instant "bar" time

from __future__ import annotations

from decimal import Decimal
from typing import Mapping, Optional, Tuple

from backtester.config.configs import AccountConfig
from backtester.core.audit import AuditWriter
from backtester.core.bus import Bus
from backtester.core.clock import Clock
from backtester.core.utility import dec
from backtester.types.types import (
    ZERO,
    Fill,
    LotClosedEvent,
    PortfolioSnapshot,
    Position,
    PositionView,
    Symbol,
    SymbolSpec,
    TradeEvent,
    UnixMillis,
)

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
        specs: Optional[Mapping[Symbol, SymbolSpec]] = None,
    ) -> None:
        self._cfg = cfg
        self._bus = bus
        self._audit = audit_writer
        self._clock = clock
        self._venue: str = "Binance"  # currently unused
        self._base_ccy: str = "USDC"
        self._cash: Decimal = starting_cash
        self._rpnl = ZERO
        self._fees_paid = ZERO

        # Single Source of Truth (updated by apply_fill)
        self._positions: dict[Symbol, Position] = {}

        # Additional: mark to market price map
        self._marks: dict[Symbol, Decimal] = {}

        self._last_ts: UnixMillis = 0
        self._last_snapshot: Optional[PortfolioSnapshot] = None
        self._specs: dict[Symbol, SymbolSpec] = dict(specs or {})
        # Idempotency: skip duplicate fills (by client_id if present, else deterministic key)
        self._seen_fills: set[str] = set()

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

    def positions_view(self) -> Mapping[Symbol, Tuple[Decimal, Decimal]]:
        """Read-only mapping: symbol -> (qty, avg_price)."""
        return {s: (p.qty, p.avg_cost) for s, p in self._positions.items()}

    def portfolio_view(self) -> Optional[PortfolioSnapshot]:
        return self._last_snapshot

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

        pos = self._positions.get(sym)
        # print("POS LOTS START:", pos._lots if pos else "POS NONE")
        if pos is None:
            pos = Position(symbol=sym)
            self._positions[sym] = pos

        # TODO Add _pos_epsilon to creating order Intents
        # TODO Hook symbol spec to binance

        eps = self._pos_epsilon(sym)

        # 1) cash update
        self._cash -= (q_fill * p_fill) + fees
        self._fees_paid += fees

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
                    lot_closed = LotClosedEvent(
                        symbol=pos.symbol,
                        qty=match_qty,
                        entry_ts=lot_ts,
                        entry_price=lot_px,
                        exit_ts=fill.ts,
                        exit_price=p_fill,
                        realized_pnl=realized_delta,
                        fee_entry=lot_fee,
                        fee_exit=fees * (match_qty / fill.qty),
                    )
                    await self._bus.publish("account.trade", fill.ts, lot_closed)
                    self._audit.emit(
                        component="account",
                        event="ACCOUNT_TRADE",
                        level="INFO",
                        payload=lot_closed,
                        sim_time=self._clock.now(),
                        simple=True,
                    )
                    print(f"TradeEvent Published {lot_closed}")
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
                        fee_exit=fees * (match_qty / fill.qty),
                    )
                    await self._bus.publish("account.trade", fill.ts, lot_closed)
                    self._audit.emit(
                        component="account",
                        event="ACCOUNT_TRADE",
                        level="INFO",
                        payload=lot_closed,
                        sim_time=self._clock.now(),
                        simple=True,
                    )
                    print(f"TradeEvent Published {lot_closed}")
                else:
                    pos._lots[0] = (lot_qty, lot_px, lot_ts, lot_fee)

            # Any remaining sell increases / creates short lots
            if sell_abs > ZERO:
                pos._lots.append((q_fill, p_fill, fill.ts, fees))
                sell_abs = ZERO
            q_fill = ZERO

        # 3) Recompute qty/avg_cost
        pos._recalc_from_lots()

        # 4) Clean up dust
        pos = self._positions.get(sym)
        if pos and abs(pos.qty) <= eps:
            pos.qty = ZERO
            pos.avg_cost = ZERO
            pos.market_value = ZERO
            pos.unrealized_pnl = ZERO
            pos.clear_lots()
            self._audit.emit(
                component="account",
                event="ACCOUNT DUST CLEANED",
                level="DEBUGGING",
                payload=pos,
                sim_time=self._clock.now(),
            )
        # 5) apply mark:
        self._positions[sym]._apply_mark(price=fill.price)

        # 6) Update snapshot (implicitly emit portfolio snapshot event)
        self._last_snapshot = await self.snapshot(
            ts=fill.ts, order_id=fill.order_id, store=True, publish=True
        )

        # 7) Emit TradeEvent (for Stats)
        trade_event = TradeEvent(
            ts=self._clock.now(),
            symbol=fill.symbol,
            side=fill.side,
            qty=float(fill.qty),
            price=float(fill.price),
            fee=float(fees),
            liquidity_flag=fill.liquidity_flag,
        )
        self._audit.emit(
            component="account",
            event="ACCOUNT_TRADE",
            level="INFO",
            payload=trade_event,
            sim_time=self._clock.now(),
            simple=True,
        )
        await self._bus.publish("account.trade", ts_utc=self._clock.now(), payload=trade_event)

        # 6. Mark as applied
        self._seen_fills.add(key)

    def set_mark(self, symbol: Symbol, price: Decimal, ts: UnixMillis) -> None:
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

    # --- Snapshots ---

    async def snapshot(
        self,
        ts: Optional[UnixMillis] = None,
        store: bool = True,
        publish: bool = True,
        order_id: Optional[str] = None,
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
        if self._last_snapshot is not None:
            if snapshot.cash != self._last_snapshot.cash:
                self._audit.emit(
                    component="account",
                    event="ACCOUNT_SNAPSHOT",
                    order_id=order_id,
                    level="INFO",
                    sim_time=self._clock.now(),
                    payload=snapshot,
                )

        if store:
            self._last_snapshot = snapshot
        if publish:
            await self._bus.publish(
                topic="account.snapshot", ts_utc=self._clock.now(), payload=snapshot
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

    def _pos_epsilon(self, symbol: Symbol) -> Decimal:
        spec = self._specs.get(symbol)
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
