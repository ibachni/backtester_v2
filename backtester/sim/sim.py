import itertools
from dataclasses import replace
from decimal import Decimal
from typing import Any, Optional, Union

from backtester.audit.audit import AuditWriter
from backtester.config.configs import SimConfig
from backtester.core.bus import Bus
from backtester.core.clock import Clock
from backtester.core.ctx import Context
from backtester.types.topics import (
    T_FILLS,
    T_LOG,
    T_ORDERS_ACK,
    T_ORDERS_CANCELED,
    T_ORDERS_REJECTED,
)
from backtester.types.types import (
    ZERO,
    Candle,
    ExecSimError,
    Fill,
    Liquidity,
    LogEvent,
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
from backtester.utils.utility import dec

from .sim_models import LatencyModel

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
        self, ctx: Context, cfg: SimConfig, bus: Bus, clock: Clock, audit: AuditWriter
    ) -> None:
        self._ctx = ctx
        self._cfg = cfg
        self._bus = bus
        self._clock = clock
        self._slip_model = cfg.slip_model
        self._fee_model = cfg.fee_model
        self._latency_model = cfg.latency_model if cfg.latency_model else LatencyModel()
        self._audit = audit

        self.conservative_gaps = cfg.conservative_gaps
        self.participation_bps = cfg.participation_bps

        # Status of orders
        self._orders: dict[str, OrderState] = {}
        self._working: dict[str, list[OrderState]] = {}
        self._hash_index: dict[str, str] = {}  # fingerprint -> order_id
        self._seq = itertools.count()  # Decider on venue_arrival_index ties
        self._next_fill_id = itertools.count()

        # Stats
        self._orders_received: int = 0
        self._fills_generated: int = 0
        self._volume_traded: float = 0
        self._budget_exhaustions: int = 0

    # --- API: market data driven ---

    async def on_order(self, intent: ValidatedOrderIntent) -> None:
        """
        Triggered when an ValidatedOrderIntent is received.
        Manages duplicates, creates OrderState and logs order in-memory, then publishes OrderAck.

        arrival_lag_bars (latency in bars): how many bars after submission the order is considered
        to arrive at the venue.
        """
        self._orders_received += 1
        # 1. De-dup managing: Reject for same ids, but different hashes:
        if intent.id in self._orders and intent.hash not in self._hash_index:
            # Log intent and order ack
            order_ack = self._intent_to_order_ack(
                intent=intent, order_status=OrderStatus.REJECTED, reason="same id, different hash"
            )
            await self._bus.publish(T_ORDERS_REJECTED, self._clock.now(), order_ack)
            return

        # Same ids, same hashes: republish order_ack
        if intent.id in self._orders and intent.hash in self._hash_index:
            order_ack = self._orders[intent.id].order_ack
            order_ack.status = OrderStatus.ACK
            await self._bus.publish(T_ORDERS_ACK, self._clock.now(), order_ack)

        # 2. Create Order State
        # stop orders need a trigger event later; other orders are considered already triggered
        triggered = not isinstance(
            intent, (ValidatedStopMarketOrderIntent, ValidatedStopLimitOrderIntent)
        )
        order_ack = self._intent_to_order_ack(intent=intent, order_status=OrderStatus.ACK)

        st = OrderState(
            order=intent,
            remaining=Decimal(intent.qty),
            hash=intent.hash,
            status=OrderStatus.ACK,
            order_ack=order_ack,
            venue_arrival_ts=intent.ts_utc + self._latency_model.random_latency(),
            triggered=triggered,
            created_seq=next(self._seq),
        )
        # 2.2. Update Values
        self._orders[intent.id] = st
        self._working.setdefault(intent.symbol, []).append(st)
        self._hash_index[intent.hash] = intent.id

        # 3. Publish order acknowledged
        await self._bus.publish(topic=T_ORDERS_ACK, ts_utc=self._clock.now(), payload=order_ack)

    async def on_candle(self, symbol: str, candle: Candle) -> list[Fill]:
        fills: list[Fill] = []
        book = self._working.get(symbol, [])

        # 1. Calculate Participation Budget
        # 1.1. normalize to decimal
        vol = candle.volume
        # 1.2. Compute budget and round to 8 decimal places
        bar_budget = vol * self.participation_bps / 10000

        # 1.3. init remaining budget
        budget_left = bar_budget

        # 2. Collect eligible orders
        eligible = self._select_processable_orders(book, candle)

        # 3. Move to Working; FOK check, move to canceled if not fillable
        eligible = await self._fok_check(candle, budget_left, eligible)

        # 4. Allocate fills
        for i, st in enumerate(eligible):
            if budget_left <= 0.0:
                # Break when budget exhausted; however, IOC must be canceled
                for remaining_st in eligible[i:]:
                    if remaining_st.order.tif in (TimeInForce.IOC, TimeInForce.FOK):
                        await self._cancel_unfilled_ioc(remaining_st)
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
                    st.order_ack.status = OrderStatus.CANCELED
                    st.order_ack.reason = "Cancel: IOC Price is None"
                    await self._bus.publish(
                        T_ORDERS_CANCELED,
                        self._clock.now(),
                        payload=st.order_ack,
                    )
                continue
            # 4.3. Determine liquidity flag (taker or maker)
            liq_flag = self._fill_liquidity(order=o, exec_price=price, candle=candle)

            # 4.4. Determine quantity
            max_fill = min(st.remaining, dec(budget_left))
            # FOK uses remaining (we already pre-checked fillability in _fok_check)
            fill_qty = st.remaining if o.tif == TimeInForce.FOK else max_fill
            # Quant down to avoid creating a fill that exceeds budget
            fill_qty = fill_qty.quantize(Decimal("0.00000001"))
            if fill_qty <= ZERO:
                continue
            budget_left = round(budget_left, 8)
            if fill_qty > budget_left:
                fill_qty = dec(budget_left).quantize(PREC)
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
            budget_left = budget_left - float(fill_qty)
            new_status = (
                OrderStatus.FILLED if st.remaining <= ZERO else OrderStatus.PARTIALLY_FILLED
            )
            if new_status != st.status:
                st.status = new_status

            # 4.8. Remove hash index for terminal orders
            if st.hash and st.status in (OrderStatus.FILLED, OrderStatus.CANCELED):
                self._hash_index.pop(st.hash, None)
            fills.append(fill)
            self._fills_generated += 1
            self._volume_traded += float(fill.qty * fill.price)

            # 4.9. Publish
            await self._bus.publish(topic=T_FILLS, ts_utc=self._clock.now(), payload=fill)

            # 4.10. IOC: cancel leftovers immediately
            if o.tif == TimeInForce.IOC and st.remaining > 0:
                st.status = OrderStatus.CANCELED
                st.remaining = ZERO
                st.order_ack.status = OrderStatus.CANCELED
                st.order_ack.reason = "IOC Cancel: Leftover canceled"
                await self._bus.publish(
                    topic=T_ORDERS_CANCELED, ts_utc=self._clock.now(), payload=st.order_ack
                )

            # # --- Temporary debug: print current order state after processing this st ---
            # try:
            #     dbg = {
            #         "now_ms": int(self._clock.now()),
            #         "order_id": getattr(st.order, "id", None),
            #         "symbol": getattr(st.order, "symbol", None),
            #         "side": str(getattr(st.order, "side", None)),
            #         "tif": str(getattr(st.order, "tif", None)),
            #         "status": getattr(st, "status", None)
            #         if hasattr(st, "status")
            #         else str(getattr(st, "status", None)),
            #         "remaining": str(getattr(st, "remaining", None)),
            #         "venue_arrival_ts": getattr(st, "venue_arrival_ts", None),
            #         "created_seq": getattr(st, "created_seq", None),
            #         "hash": getattr(st, "hash", None),
            #         "price": str(price) if "price" in locals() and price is not None else None,
            #         "fill_qty": str(fill_qty) if "fill_qty" in locals() else None,
            #         "liq_flag": str(liq_flag) if "liq_flag" in locals() else None,
            #         "budget_left": str(budget_left) if "budget_left" in locals() else None,
            #     }
            # except Exception as _dbg_exc:
            #     # fall back to a simple safe print if something unexpected happens
            #     print("EXEC_SIM_DBG: failed building debug payload", repr(_dbg_exc))
            # else:
            #     pass
            #     # print("EXEC_SIM_DBG:", dbg)

        self._working[symbol] = [
            st
            for st in book
            if st.status in (OrderStatus.ACK, OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED)
        ]

        if self._audit is not None and budget_left <= 0 and eligible:
            self._budget_exhaustions += 1
            await self._emit_log(
                level="DEBUG",
                msg="PARTICIPATION_BUDGET_EXHAUSTED",
                sim_time=int(self._clock.now()),
                payload={
                    "symbol": symbol,
                    "bar_end": candle.end_ms if hasattr(candle, "end_ms") else None,
                    "eligibles": len(eligible),
                },
            )
        return fills

    def get_stats(self) -> dict[str, Any]:
        return {
            "orders_received": self._orders_received,
            "fills_generated": self._fills_generated,
            "volume_traded": self._volume_traded,
            "budget_exhaustions": self._budget_exhaustions,
        }

    # --- Helpers ---

    def can_full(self, st: OrderState, budget_left: float, candle: Candle) -> bool:
        o = st.order
        if isinstance(o, ValidatedMarketOrderIntent):
            return float(st.remaining) <= budget_left
        # Limit
        if isinstance(o, ValidatedLimitOrderIntent):
            return self._limit_touched(o, candle) and float(st.remaining) <= budget_left
        # Stop-Market
        if isinstance(o, ValidatedStopMarketOrderIntent):
            return self._stop_trigger(st, candle) and float(st.remaining) <= budget_left
        # Stop-Limit
        if isinstance(o, ValidatedStopLimitOrderIntent):
            return (
                self._stop_trigger(st, candle)
                and self._limit_touched(o, candle)
                and float(st.remaining) <= budget_left
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
            if order.side == Side.BUY and candle.open > float(order.stop_price):
                return Decimal(candle.open)
            if order.side == Side.SELL and candle.open < float(order.stop_price):
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
        self, intent: ValidatedOrderIntent, order_status: OrderStatus, reason: Optional[str] = None
    ) -> OrderAck:
        return OrderAck(
            intent_id=intent.id,
            strategy_id=intent.strategy_id,
            component="sim",
            symbol=intent.symbol,
            market=intent.market,
            side=intent.side,
            status=order_status,
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

    # --- Cancel unfilled IOC ---

    # FOK (Fill or Kill) pre-check
    async def _fok_check(
        self, candle: Candle, budget_left: float, eligible: list[OrderState]
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
                st.order_ack.status = OrderStatus.CANCELED
                st.order_ack.reason = "FOK check: not fillable"
                st.remaining = Decimal("0")
                await self._bus.publish(T_ORDERS_CANCELED, self._clock.now(), payload=st.order_ack)
                continue
            new_eligible.append(st)

        return new_eligible

    async def _cancel_unfilled_ioc(self, st: OrderState) -> None:
        st.status = OrderStatus.CANCELED
        st.remaining = ZERO
        st.order_ack.status = OrderStatus.CANCELED
        st.order_ack.reason = "Cancel: unfilled IOC"
        await self._bus.publish(T_ORDERS_CANCELED, ts_utc=self._clock.now(), payload=st.order_ack)

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

    async def _emit_log(
        self,
        *,
        level: str,
        msg: str,
        payload: Optional[dict[str, Any]] = None,
        sim_time: Optional[int] = None,
    ) -> None:
        """Publish LogEvent for exec_sim via bus."""
        ts = sim_time if sim_time is not None else int(self._clock.now())
        log_event = LogEvent(
            level=level,
            component="exec_sim",
            msg=msg,
            payload=payload or {},
            sim_time=ts,
        )
        await self._bus.publish(topic=T_LOG, ts_utc=ts, payload=log_event)
