from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Optional, Tuple

from backtester.config.configs import ValidationConfig
from backtester.core.bus import Bus
from backtester.core.clock import Clock
from backtester.types.topics import T_LOG, T_ORDERS_ACK, T_ORDERS_REJECTED, T_ORDERS_SANITIZED
from backtester.types.types import (
    ZERO,
    LimitOrderIntent,
    LogEvent,
    Market,
    MarketOrderIntent,
    OrderAck,
    OrderIntent,
    OrderStatus,
    PortfolioSnapshot,
    StopLimitOrderIntent,
    StopMarketOrderIntent,
    SymbolSpec,
    ValidatedLimitOrderIntent,
    ValidatedMarketOrderIntent,
    ValidatedOrderIntent,
    ValidatedStopLimitOrderIntent,
    ValidatedStopMarketOrderIntent,
)
from backtester.utils.order_utility import order_fingerprint_hash, relevant_fields


@dataclass
class ValidationResult:
    ok: bool
    errors: list[str] = field(default_factory=list)  # fatal problems (order rejected)
    warnings: list[str] = field(default_factory=list)  # non-fatal notes (might need to log/alert)

    def add(self, cond: bool, msg: str) -> None:
        # if the condition is false, the message is appended to errors
        # if true, nothing is added
        if not cond:
            self.errors.append(msg)

    def merge(self, other: "ValidationResult") -> None:
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)


class OrderValidation:
    """
    listens to order.intent. Publishes order.sanitzed.
    """

    def __init__(self, cfg: ValidationConfig, bus: Bus, clock: Clock) -> None:
        self._bus = bus
        self._clock = clock
        self._cfg = cfg

        # Obs
        self._order_val_received_total: int = 0
        self._order_val_accepted_total: int = 0
        self._order_val_rejected_total: int = 0
        self._rejection_reasons: dict[str, int] = {}

        self._get_specs: Optional[Callable[[str], Optional[SymbolSpec]]] = None

        # Shadow State
        self._cash_balance: float = 0.0
        self._positions: dict[str, float] = {}

        # Pending Exposure (Accepted but not Fills/Canceled yet)
        # We must deduct this from Buying Power immediately
        self._pending_cost: float = 0.0

    async def validate_order(self, intent: OrderIntent) -> None:
        """
        The order in itself is valid, by type requirements and checks at creation.
        To be added:
        - Symbol exists/tradable
        - Quantization/steps
            - lot_size, price on tick
            - min_qty, max_qty, min_notional, price bands
        - Account/risk
            - sufficient balance/buying power;
            - reduce_only will not increase exposure
        - ts monotonic (no back-in-time)
        - numeric sanity: finite decimals
        - TIF compatibility
        - Optional Warn for LimitOrder if far from band
        - Optional Warn for StopMarket (min distance)
        """

        # Ensure vr is always defined for static analyzers; specific validators may overwrite
        self._order_val_received_total += 1
        vr = ValidationResult(ok=False)

        # Here: do specialized validation
        if intent.market == Market.SPOT:
            vr = await self._validate_spot_order(intent)
        elif intent.market == Market.OPTION:
            raise NotImplementedError
        elif intent.market == Market.FUTURE:
            raise NotImplementedError

        # Create and publish the validated or canceled Order
        if vr.ok:
            await self._order_accepted(intent=intent, vr=vr)
        else:
            await self._order_rejected(intent=intent, vr=vr)

    # --- Validation (SPOT) ---

    async def _validate_spot_order(
        self,
        intent: OrderIntent,
        # close_px: float
    ) -> ValidationResult:
        """
        Stub - Information from exchange to be added!
        """
        vr = ValidationResult(ok=False)

        # 1. Validate Information with the exchange:
        # 1.3. if order type not allowed

        # 2. Validate Information with the Account system
        # 2.1. Account: Client order id uniqueness
        if self._get_specs is None:
            raise ValueError("Function _get_spec not available in class OrderValidator")

        specs = self._get_specs(intent.symbol)
        if specs is None:
            # Do not fail the backtest, but log an error
            log_event = LogEvent(
                level="ERROR",
                component="OrderValidation",
                msg=f"SymbolSpec for {intent.symbol} is None",
                payload={},
                sim_time=self._clock.now(),
            )
            await self._bus.publish(topic=T_LOG, ts_utc=self._clock.now(), payload=log_event)
            vr.add(False, f"SymbolSpec {intent.symbol} not found")
            vr.ok = False
            return vr

        # Info is not none
        if not specs.trading:
            vr.add(False, "Trading disabled for symbol")

        # price filtering:
        if isinstance(intent, (LimitOrderIntent, StopLimitOrderIntent)):
            # Enforce tick_size at order creation
            if intent.price % specs.tick_size != 0:
                vr.add(False, "Qty not a multiple of tick_size")

            # minPrice, maxPrice:
            if intent.price < specs.price_band_low:
                vr.add(False, "Price below price band")
            if intent.price > specs.price_band_high:
                vr.add(False, "Price above price band")

            if intent.qty * intent.price <= specs.min_notional:
                vr.add(False, "Notional value too small")

        if isinstance(intent, (StopLimitOrderIntent, StopMarketOrderIntent)):
            # minPrice, maxPrice:
            if intent.stop_price < specs.price_band_low:
                vr.add(False, "Stop price below price band")
            if intent.stop_price > specs.price_band_high:
                vr.add(False, "Stop price above price band")

        # Lot filtering!
        # enforce at order creation!
        if intent.qty % specs.lot_size != 0:
            vr.add(False, "Qty not a multiple of lot_size")

        # min/ max qty
        if intent.qty < specs.min_qty:
            vr.add(False, "Price below min qty")
        if intent.qty > specs.max_qty:
            vr.add(False, "Price above max qty")

        vr.ok = len(vr.errors) == 0
        return vr

    def _base_intent_data(self, intent: OrderIntent, vr: ValidationResult) -> dict:
        """
        Build shared intent fields, adding validation results into tags.
        Slots/frozen friendly (no __dict__).
        """
        tags = dict(getattr(intent, "tags", {}))
        tags["val_results"] = {
            "ok": vr.ok,
            "errors": list(vr.errors),
            "warnings": list(vr.warnings),
        }
        return {
            "symbol": intent.symbol,
            "market": intent.market,
            "side": intent.side,
            "id": intent.id,
            "ts_utc": intent.ts_utc,
            "qty": intent.qty,
            "strategy_id": intent.strategy_id,
            "tif": intent.tif,
            "tags": tags,
        }

    def _intent_to_validated(
        self, intent: OrderIntent, vr: ValidationResult
    ) -> ValidatedOrderIntent:
        """
        Convert a raw intent to its validated counterpart by reusing the same fields.
        Adds 'val_results' to tags. Returns a new frozen instance.
        """
        data = self._base_intent_data(intent, vr)
        if isinstance(intent, MarketOrderIntent):
            fields = relevant_fields(intent)
            data["hash"] = order_fingerprint_hash(intent, fields)
            data["reduce_only"] = intent.reduce_only
            return ValidatedMarketOrderIntent(**data)
        if isinstance(intent, LimitOrderIntent):
            fields = relevant_fields(intent)
            data["hash"] = order_fingerprint_hash(intent, fields)
            data["price"] = intent.price
            data["reduce_only"] = intent.reduce_only
            data["post_only"] = intent.post_only
            return ValidatedLimitOrderIntent(**data)
        if isinstance(intent, StopMarketOrderIntent):
            fields = relevant_fields(intent)
            data["hash"] = order_fingerprint_hash(intent, fields)
            data["stop_price"] = intent.stop_price
            data["reduce_only"] = intent.reduce_only
            data["post_only"] = intent.post_only
            return ValidatedStopMarketOrderIntent(**data)
        if isinstance(intent, StopLimitOrderIntent):
            fields = relevant_fields(intent)
            data["hash"] = order_fingerprint_hash(intent, fields)
            data["price"] = intent.price
            data["stop_price"] = intent.stop_price
            data["reduce_only"] = intent.reduce_only
            data["post_only"] = intent.post_only
            return ValidatedStopLimitOrderIntent(**data)

        raise TypeError(f"Unsupported intent type: {type(intent).__name__}")

    # --- Validation (Option) ---

    def _validate_option_order(self) -> None:
        raise NotImplementedError

    # --- Validation (Future) ---

    def _validate_future_order(self) -> None:
        raise NotImplementedError

    # --- Publish results (OrderIntent, OrderAck) ---

    async def _order_accepted(
        self,
        intent: OrderIntent,
        vr: ValidationResult,
    ) -> None:
        """
        Create OrderAck object (AckStatus.ACCEPTED), publish OderAck, publish validated_Order
        """
        order_ack = OrderAck(
            intent_id=intent.id,
            strategy_id=intent.strategy_id,
            component="OrderValidation",
            symbol=intent.symbol,
            market=intent.market,
            side=intent.side,
            status=OrderStatus.VALIDATED,
            warnings=vr.warnings,
            ts_utc=self._clock.now(),
        )
        validated_order = self._intent_to_validated(intent=intent, vr=vr)
        self._order_val_accepted_total += 1

        # Publish OrdersAck and ValidatedOrder
        await self._bus.publish(
            topic=T_ORDERS_SANITIZED, ts_utc=self._clock.now(), payload=validated_order
        )
        await self._bus.publish(topic=T_ORDERS_ACK, ts_utc=self._clock.now(), payload=order_ack)

    async def _order_rejected(
        self,
        intent: OrderIntent,
        vr: ValidationResult,
    ) -> None:
        """
        Create an OrderAck from the intent and vr, and publish to the bus under order.ack.
        Should be captured by AuditRunner
        """
        order_ack = OrderAck(
            intent_id=intent.id,
            strategy_id=intent.strategy_id,
            component="OrderValidation",
            symbol=intent.symbol,
            market=intent.market,
            side=intent.side,
            status=OrderStatus.REJECTED,
            reason="OrderValidationFailed",
            errors=vr.errors,
            warnings=vr.warnings,
            ts_utc=self._clock.now(),
        )
        await self._bus.publish(
            topic=T_ORDERS_REJECTED, ts_utc=self._clock.now(), payload=order_ack
        )
        self._order_val_rejected_total += 1
        for error in vr.errors:
            if error in self._rejection_reasons.keys():
                self._rejection_reasons[error] += 1
            else:
                self._rejection_reasons[error] = 1

    # --- RIK MANAGEMENT ---
    """
    - AccountSnaoshot: sync with official ledger: overwrite self._cash balance and positions
    with official data
    - T_ORDERS_INTENT: Validate new requests
    - T_ORDERS_ACK: Rejected / canceled: release pending cost of an order dies
    - T_FILLS: release conding post
    """

    async def on_account_snapshot(self, snap: PortfolioSnapshot) -> None:
        """Received from Bus Level 5"""
        self._cash_balance = float(snap.cash)

    def _validate_risk(self, intent: OrderIntent) -> Tuple[bool, str]:
        est_price: Decimal = ZERO
        if isinstance(intent, (MarketOrderIntent, StopMarketOrderIntent)):
            # We might need the last known price from a T_CANDLE subscription
            # or apply a safety buffer (e.g. +5%)
            pass
            # est_price = self._last_known_price.get(intent.symbol, 0) * 1.05

        elif isinstance(intent, (LimitOrderIntent, StopLimitOrderIntent)):
            est_price = intent.price

        est_cost = est_price * intent.qty

        # Check Balance
        available = self._cash_balance - self._pending_cost

        if est_cost > available:
            return False, "Balance too low"

        return (True, "")

    # --- Expose metrics ---

    def get_stats(self) -> dict[str, Any]:
        return {
            "validation_received": self._order_val_received_total,
            "validation_accepted": self._order_val_accepted_total,
            "validation_rejected": self._order_val_rejected_total,
            "validation_rejection_rate": self._order_val_rejected_total
            / self._order_val_received_total,
            "validation_rejection_reason": self._rejection_reasons,
        }
