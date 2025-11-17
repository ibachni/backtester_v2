import itertools
import random
from dataclasses import dataclass, field, replace
from decimal import ROUND_DOWN, Decimal
from typing import Any, Optional, Union

from backtester.core.audit import AuditWriter
from backtester.core.bus import Bus
from backtester.core.clock import Clock
from backtester.core.fees import FeeModel, FixedBps
from backtester.types.types import (
    ZERO,
    AckStatus,
    Candle,
    ExecSimError,
    Fill,
    Liquidity,
    OrderAck,
    OrderState,
    OrderStatus,
    Side,
    TimeInForce,
    ValidatedLimitOrderIntent,
    ValidatedMarketOrderIntent,
    ValidatedOrderIntent,
    ValidatedStopLimitOrderIntent,
    ValidatedStopMarketOrderIntent,
)

"""
Design choices:
    - Liquidity and fill modeling: based on data
        - Bar-based ()
            - Market: fill at open/close/mid/next bar open pm slippage
            - Limit: fill if limit crosses bar range; price = limit or better
            - Stops: trigger if bar touches stop; convert to market / limit per type
            - Volume participation: cap fills to a friction of bar volume
        - quote-based (l1)
        - order-book (l2)
"""


PREC = Decimal("0.00000001")

# --------------------------------------------------------------------------------------
# Models
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class SlippageModel:
    """
    Static slippage applied to reference price (bar close in MVP).
    bps: basis points (1 bps = 0.01%). BUY pays up, SELL receives down.
    Returns the adjusted price.
    """

    bps: float = 0.0

    def apply(self, side: Side, ref_price: Decimal) -> Decimal:
        if self.bps == 0.0:
            return ref_price
        m = Decimal(self.bps) / Decimal(10_000.0)
        if side == Side.BUY:
            return (ref_price) * (Decimal(1.0) + m)
        else:
            return Decimal(ref_price) * (Decimal(1.0) - m)


class FillModel:
    pass


class ImpactModel:
    pass


@dataclass(frozen=True)
class LatencyModel:
    """
    Currently not in use for.
    """

    seed: int = 1
    random.seed(seed)
    random_bariers: tuple[int, int] = field(default=(10, 200))  # Latency in ms

    def random_latency(self) -> int:
        return random.randrange(self.random_bariers[0], self.random_bariers[1], 5)


class RoutingModel:
    pass


# --- Information Provision (not implemented yet) ---


class ExchangeInfoProvider:
    """Abstract accessor for symbol metadata & filters."""

    # ---- Spot ----
    def get_spot_symbol(self, symbol: str) -> Optional[dict[str, Any]]:
        raise NotImplementedError

    def get_options_contract(self, symbol: str) -> Optional[dict[str, Any]]:
        raise NotImplementedError


class AccountProvider:
    """Abstract accessor for balances, margins, and outstanding client IDs."""

    def has_unique_client_id(self, client_order_id: Optional[str]) -> bool:
        return True if client_order_id else True

    def get_spot_balances(self) -> dict[str, str]:
        # e.g. {"USDT": Decimal("1000"), "ETH": Decimal("2")}
        return {}

    def options_can_short(self) -> bool:
        # Gate for writing options (jurisdiction/mode)
        return False


# --- Main Orchestrator ---


class ExecutionSimulator:
    """
    (FULL) Responsibilities:
        - Accept Orders (validate, timestamp, route)
        - Simulate market interaction -> queueing, matching, partial fills, rejections, cancels,
        amendments
        - Produce fills -> price, size, time, fees, slippage, venue
        - Emit events -> Order status transitions, trade prints, risk/capacity alerts

    Questions:
        - how to manage if bars are pushed as far as possible?
        - Answer: await after buy and sell the response of the exchange first, before pushing the
        next bar

    Orders submitted at bar t arrive at venue on bar t+arrival_lag_bars

    """

    # --- Init ---

    def __init__(
        self,
        bus: Bus,
        clock: Clock,
        audit: AuditWriter,
        slip_model: SlippageModel,
        latency_model: Optional[LatencyModel] = None,
        fee_model: Optional[FeeModel] = None,
        conservative_gaps: bool = True,
        participation_bps: int = 500,  # in basis points
    ) -> None:
        self._bus = bus
        self._clock = clock
        self._slip_model = slip_model
        self._latency_model = latency_model if latency_model else LatencyModel()
        self._fee_model = fee_model if fee_model else FixedBps(10)
        self._audit = audit

        # ex: ExchangeInfoProvider
        # acct: AccountProvider

        self.conservative_gaps = conservative_gaps
        self.participation_bps = participation_bps

        # Status of orders
        self._orders: dict[str, OrderState] = {}
        self._working: dict[str, list[OrderState]] = {}
        self._hash_index: dict[str, str] = {}  # fingerprint -> order_id
        self._seq = itertools.count()  # Decider on venue_arrival_index ties
        self._next_fill_id = itertools.count()

    # --- API: market data driven ---

    async def on_order(self, intent: ValidatedOrderIntent) -> None:
        """
        Triggered when an ValidatedOrderIntent is received.
        Manages duplicates, creates OrderState and logs order in-memory, then publishes OrderAck.

        arrival_lag_bars (latency in bars): how many bars after submission the order is considered
        to arrive at the venue.
        """
        # 1. De-dup managing: Reject for same ids, but different hashes:
        if intent.id in self._orders and intent.hash not in self._hash_index:
            # Log intent and order ack
            order_ack = self._intent_to_order_ack(
                intent=intent, ack_status=AckStatus.DUPLICATE, reason="same id, different hash"
            )
            rejected_intent = self._audit.intent_to_payload(intent)
            self._audit.emit(
                component="exec_sim",
                event="ORDER_DUPLICATE",
                level="ERROR",
                sim_time=int(self._clock.now()),
                payload=(order_ack, rejected_intent),
                symbol=intent.symbol,
                order_id=intent.id,
                parent_id=intent.id,
            )
        # Same ids, same hashes: republish

        # 2. Create Order State
        # 2.1. Create OrderState
        # stop orders need a trigger event later; other orders are considered already triggered
        triggered = not isinstance(
            intent, (ValidatedStopMarketOrderIntent, ValidatedStopLimitOrderIntent)
        )

        st = OrderState(
            order=intent,
            remaining=Decimal(intent.qty),
            hash=intent.hash,
            status=OrderStatus.ACK,
            venue_arrival_ts=intent.ts_utc + self._latency_model.random_latency(),
            triggered=triggered,
            created_seq=next(self._seq),
        )

        # 2.2. Update Values
        self._orders[intent.id] = st
        self._working.setdefault(intent.symbol, []).append(st)
        self._hash_index[intent.hash] = intent.id

        # 3. Publish order acknowledged
        order_ack = self._intent_to_order_ack(intent=intent, ack_status=AckStatus.ACCEPTED)
        await self._bus.publish(topic="orders.ack", ts_utc=self._clock.now(), payload=order_ack)

        # 4. Log the OrderAck as well (we skip logging intent, since accepted)
        payload = self._audit.ack_to_payload(order_ack)
        # Add additional fields
        payload["arrival_index"] = st.venue_arrival_ts
        payload["remaining"] = str(st.remaining)
        self._audit.emit(
            component="exec_sim",
            event="ORDER_ACCEPTED",
            sim_time=int(self._clock.now()),
            payload=payload,
            symbol=intent.symbol,
            order_id=intent.id,
            parent_id=intent.id,
        )

    async def on_candle(self, symbol: str, candle: Candle) -> list[Fill]:
        """
        # TODO: Use symbol-specific quantization (lot_size)
        # TODO need to consult bar_index; eligible list must filter by arrival as well.
        # TODO Parent_id stuff
        """
        fills: list[Fill] = []
        book = self._working.get(symbol, [])

        # 1. Calculate Participation Budget
        # 1.1. normalize to decimal
        vol = candle.volume if isinstance(candle.volume, Decimal) else Decimal(str(candle.volume))
        # 1.2. Compute budget and round to 8 decimal places
        bar_budget = (vol * (Decimal(self.participation_bps) / Decimal("10000"))).quantize(
            Decimal("0.00000001")
        )
        # 1.3. init remaining budget
        budget_left = bar_budget

        # 2. Collect eligible orders
        eligible = self._select_processable_orders(book, candle)

        # 3. Move to Working; FOK check, move to canceled if not fillable
        eligible = await self._fok_check(candle, budget_left, eligible)

        # 4. Allocate fills
        for st in eligible:
            if budget_left <= ZERO:
                break
            o = st.order

            # 4.1. Determine price (after slippage)
            price = self._determine_price(st, o, candle)

            # 4.2. If price is none,
            if price is None:
                # If IOC: Cancel
                if o.tif == TimeInForce.IOC:
                    st.status = OrderStatus.CANCELED
                    st.remaining = Decimal("0")
                    await self._bus.publish("orders.canceled", self._clock.now(), payload=st.order)
                    if self._audit is not None:
                        payload = self._audit.intent_to_payload(st.order)
                        payload["reason"] = "ioc_not_marketable"
                        self._audit.emit(
                            component="exec_sim",
                            event="ORDER_CANCEL_IOC",
                            level="INFO",
                            sim_time=int(self._clock.now()),
                            payload=payload,
                            symbol=st.order.symbol,
                            order_id=st.order.id,
                            parent_id=st.order.id,
                        )
                # Else, just continue
                continue

            # 4.3. Determine liquidity flag (taker or maker)
            liq_flag = self._fill_liquidity(order=o, exec_price=price, candle=candle)

            # 4.4. Determine quantity
            max_fill = min(st.remaining, budget_left)
            # FOK uses remaining (we already pre-checked fillability in _fok_check)
            fill_qty = st.remaining if o.tif == TimeInForce.FOK else max_fill
            # Quant down to avoid creating a fill that exceeds budget
            fill_qty = fill_qty.quantize(Decimal("0.00000001"))
            if fill_qty <= ZERO:
                continue

            budget_left = budget_left.quantize(PREC, rounding=ROUND_DOWN)
            if fill_qty > budget_left:
                fill_qty = budget_left
                if fill_qty <= ZERO:
                    continue

            # 4.5. produce fill
            fill = Fill(
                fill_id=str(next(self._next_fill_id)),
                order_id=o.id,
                symbol=o.symbol,
                market=o.market,
                qty=fill_qty if st.order.side == Side.BUY else -fill_qty,
                price=price,
                side=st.order.side,
                ts=self._clock.now(),
                venue="Binance",
                liquidity_flag=liq_flag,
                fees_explicit=Decimal("0"),
                rebates=Decimal("0"),
                slippage_components="{}",
                tags=[],
            )

            # 4.6. attach costs
            fees = Decimal(self._fee_model.fee(fill))
            fill = replace(fill, fees_explicit=fees)

            # 4.7. Update Order State
            st.remaining = (st.remaining - fill_qty).quantize(PREC)
            if ZERO <= st.remaining < PREC:
                st.remaining = ZERO
            budget_left = budget_left - fill_qty
            new_status = (
                OrderStatus.FILLED if st.remaining <= ZERO else OrderStatus.PARTIALLY_FILLED
            )
            if new_status != st.status:
                st.status = new_status

            # 4.8. Remove hash index for terminal orders
            if st.hash and st.status in (OrderStatus.FILLED, OrderStatus.CANCELED):
                self._hash_index.pop(st.hash, None)
            fills.append(fill)

            # 4.9. Publish and log
            payload = self._audit.fill_to_payload(fill)
            payload["remaining"] = str(st.remaining)
            payload["budget_left"] = str(budget_left)
            self._audit.emit(
                component="exec_sim",
                event="ORDER_FILL",
                sim_time=int(fill.ts),
                payload=payload,
                symbol=fill.symbol,
                order_id=fill.order_id,
                parent_id=fill.order_id,
            )
            await self._bus.publish(topic="orders.fills", ts_utc=self._clock.now(), payload=fill)

            # 4.10. IOC: cancel leftovers immediately
            if o.tif == TimeInForce.IOC and st.remaining > 0:
                st.status = OrderStatus.CANCELED
                st.remaining = ZERO
                await self._bus.publish(
                    topic="orders.canceled", ts_utc=self._clock.now(), payload=o
                )
                payload = self._audit.intent_to_payload(o)
                payload["reason"] = "ioc_post_fill_cancel"
                self._audit.emit(
                    component="exec_sim",
                    event="ORDER_CANCEL_IOC",
                    level="INFO",
                    sim_time=int(self._clock.now()),
                    payload=payload,
                    symbol=o.symbol,
                    order_id=o.id,
                    parent_id=o.id,
                )

            # --- Temporary debug: print current order state after processing this st ---
            try:
                dbg = {
                    "now_ms": int(self._clock.now()),
                    "order_id": getattr(st.order, "id", None),
                    "symbol": getattr(st.order, "symbol", None),
                    "side": str(getattr(st.order, "side", None)),
                    "tif": str(getattr(st.order, "tif", None)),
                    "status": getattr(st, "status", None)
                    if hasattr(st, "status")
                    else str(getattr(st, "status", None)),
                    "remaining": str(getattr(st, "remaining", None)),
                    "venue_arrival_ts": getattr(st, "venue_arrival_ts", None),
                    "created_seq": getattr(st, "created_seq", None),
                    "hash": getattr(st, "hash", None),
                    "price": str(price) if "price" in locals() and price is not None else None,
                    "fill_qty": str(fill_qty) if "fill_qty" in locals() else None,
                    "liq_flag": str(liq_flag) if "liq_flag" in locals() else None,
                    "budget_left": str(budget_left) if "budget_left" in locals() else None,
                }
            except Exception as _dbg_exc:
                # fall back to a simple safe print if something unexpected happens
                print("EXEC_SIM_DBG: failed building debug payload", repr(_dbg_exc))
            else:
                pass
                # print("EXEC_SIM_DBG:", dbg)

        self._working[symbol] = [
            st
            for st in book
            if st.status in (OrderStatus.ACK, OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED)
        ]

        if self._audit is not None and budget_left <= 0 and eligible:
            self._audit.emit(
                component="exec_sim",
                event="PARTICIPATION_BUDGET_EXHAUSTED",
                level="DEBUG",
                simple=True,
                sim_time=int(self._clock.now()),
                payload={
                    "symbol": symbol,
                    "bar_end": candle.end_ms if hasattr(candle, "end_ms") else None,
                    "eligibles": len(eligible),
                },
            )
        return fills

    # --- Helpers ---

    def can_full(self, st: OrderState, budget_left: Decimal, candle: Candle) -> bool:
        o = st.order
        if isinstance(o, ValidatedMarketOrderIntent):
            return st.remaining <= budget_left
        # Limit
        if isinstance(o, ValidatedLimitOrderIntent):
            return self._limit_touched(o, candle) and st.remaining <= budget_left
        # Stop-Market
        if isinstance(o, ValidatedStopMarketOrderIntent):
            return self._stop_trigger(st, candle) and st.remaining <= budget_left
        # Stop-Limit
        if isinstance(o, ValidatedStopLimitOrderIntent):
            return (
                self._stop_trigger(st, candle)
                and self._limit_touched(o, candle)
                and st.remaining <= budget_left
            )
        # Unknown intent type
        return False

    @staticmethod
    def _limit_touched(order: ValidatedOrderIntent, candle: Candle) -> bool:
        """Determine whether the price touched the candle range"""
        if isinstance(order, (ValidatedLimitOrderIntent, ValidatedStopLimitOrderIntent)):
            if order.price is None:
                return False
            return (
                (candle.low <= order.price)
                if order.side == Side.BUY
                else (candle.high >= order.price)
            )
        return False

    def _stop_trigger(self, st: OrderState, candle: Candle) -> bool:
        """Determine, whether the stop feature of the order has been triggered"""
        if isinstance(st.order, (ValidatedStopMarketOrderIntent, ValidatedStopLimitOrderIntent)):
            if st.triggered:
                return True
            o = st.order
            if o.stop_price is None:
                return False
            if o.side == Side.BUY and candle.high >= o.stop_price:
                st.triggered = True
            elif o.side == Side.SELL and candle.low <= o.stop_price:
                st.triggered = True
            return st.triggered
        return False

    def _stop_market_exec_price(
        self, order: ValidatedStopMarketOrderIntent, candle: Candle
    ) -> Decimal:
        """Enforces: Fill at stop price, unless an opening gap makes that impossible"""
        if order.stop_price is None:
            raise ExecSimError(
                f"Order {order.stop_price} has no stop price in stop_market_exec_price()"
            )
        if self.conservative_gaps:
            if order.side == Side.BUY and candle.open > order.stop_price:
                return Decimal(candle.open)
            if order.side == Side.SELL and candle.open < order.stop_price:
                return Decimal(candle.open)
        return Decimal(order.stop_price)

    def _fill_liquidity(
        self, order: ValidatedOrderIntent, exec_price: Decimal, candle: Candle
    ) -> Liquidity:
        """
        Return the correct lquidity flag (taker vs. maker)
        """
        if isinstance(order, (ValidatedMarketOrderIntent, ValidatedStopMarketOrderIntent)):
            return Liquidity.TAKER

        if isinstance(order, (ValidatedLimitOrderIntent, ValidatedStopLimitOrderIntent)):
            limit_px = Decimal(order.price) if order.price is not None else exec_price

            # Decide if the limit was marketable at the arrival bar.
            if order.side == Side.BUY:
                crossed = limit_px >= Decimal(candle.open)
            else:
                crossed = limit_px <= Decimal(candle.open)
            return Liquidity.TAKER if crossed else Liquidity.MAKER
        return Liquidity.UNKNOWN

    def _intent_to_order_ack(
        self, intent: ValidatedOrderIntent, ack_status: AckStatus, reason: Optional[str] = None
    ) -> OrderAck:
        return OrderAck(
            intent_id=intent.id,
            strategy_id=intent.strategy_id,
            component="sim",
            symbol=intent.symbol,
            market=intent.market,
            side=intent.side,
            status=ack_status,
            ts_utc=self._clock.now(),
            reason=reason,
        )

    def _select_processable_orders(
        self, book: list[OrderState], candle: Candle
    ) -> list[OrderState]:
        eligible = [
            st
            for st in book
            if st.status
            in (
                OrderStatus.ACK,
                OrderStatus.WORKING,
                OrderStatus.PARTIALLY_FILLED,
            )
            and st.remaining > 0
            and st.venue_arrival_ts <= candle.end_ms
        ]
        eligible.sort(key=lambda s: (s.venue_arrival_ts, s.created_seq))
        return eligible

    # FOK (Fill or Kill) pre-check
    async def _fok_check(
        self, candle: Candle, budget_left: Decimal, eligible: list[OrderState]
    ) -> list[OrderState]:
        new_eligible: list[OrderState] = []
        for st in eligible:
            # 1. Change Status to working
            if st.status == OrderStatus.ACK:
                st.status = OrderStatus.WORKING
            # FOK check: if not fillable, cancel
            if st.order.tif == TimeInForce.FOK and not self.can_full(
                st=st, budget_left=budget_left, candle=candle
            ):
                st.status = OrderStatus.CANCELED
                st.remaining = Decimal("0")
                await self._bus.publish("orders.canceled", self._clock.now(), payload=st.order)
                payload = self._audit.intent_to_payload(st.order)
                payload["reason"] = "fok_not_fillable"
                self._audit.emit(
                    component="exec_sim",
                    event="ORDER_CANCEL_FOK",
                    level="WARN",
                    sim_time=int(self._clock.now()),
                    payload=payload,
                    symbol=st.order.symbol,
                    order_id=st.order.id,
                    parent_id=st.order.id,
                )
                continue
            new_eligible.append(st)

        return new_eligible

    def _determine_price(
        self, st: OrderState, o: ValidatedOrderIntent, candle: Candle
    ) -> Optional[Decimal]:
        """
        Price can be none, if
            - LimitOrder not touched
            - StopMarket not triggered
            - StopLimit not triggered or not touched
        """
        price: Optional[Decimal] = None
        if isinstance(o, ValidatedMarketOrderIntent):
            base = self._d(candle.open)
            price = self._slip_model.apply(side=o.side, ref_price=base)
        elif isinstance(o, ValidatedLimitOrderIntent) and self._limit_touched(o, candle):
            price = o.price
        elif isinstance(o, ValidatedStopMarketOrderIntent) and self._stop_trigger(st, candle):
            initial = self._stop_market_exec_price(o, candle)
            price = self._slip_model.apply(side=o.side, ref_price=initial)
        elif (
            isinstance(o, ValidatedStopLimitOrderIntent)
            and self._stop_trigger(st, candle)
            and self._limit_touched(o, candle)
        ):
            limit_px = o.price
            crossed = (
                limit_px >= self._d(candle.open)
                if o.side == Side.BUY
                else limit_px <= self._d(candle.open)
            )
            if crossed:  # behaves like a taker once stop is triggered
                base = self._d(candle.open)
                price = self._slip_model.apply(side=o.side, ref_price=base)
            else:
                price = limit_px  # genuine maker fill
        return price

    # --- Utils ---

    def _d(self, x: Union[str, int, float]) -> Decimal:
        return x if isinstance(x, Decimal) else Decimal(str(x))
