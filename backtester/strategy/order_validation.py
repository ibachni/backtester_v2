from dataclasses import asdict, dataclass, field

from backtester.core.audit import AuditWriter
from backtester.core.bus import Bus
from backtester.core.clock import Clock
from backtester.core.utility import order_fingerprint_hash, relevant_fields
from backtester.types.types import (
    AckStatus,
    LimitOrderIntent,
    Market,
    MarketOrderIntent,
    OrderAck,
    OrderIntent,
    StopLimitOrderIntent,
    StopMarketOrderIntent,
    ValidatedLimitOrderIntent,
    ValidatedMarketOrderIntent,
    ValidatedOrderIntent,
    ValidatedStopLimitOrderIntent,
    ValidatedStopMarketOrderIntent,
)

# TODO Optimize OrderRejection


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

    def __init__(self, bus: Bus, clock: Clock, audit: AuditWriter) -> None:
        self._bus = bus
        self._clock = clock
        self._audit = audit

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
        vr = ValidationResult(ok=False)

        # Here: do specialized validation
        if intent.market == Market.SPOT:
            vr = self._validate_spot_order()
        elif intent.market == Market.OPTION:
            raise NotImplementedError
        elif intent.market == Market.FUTURE:
            raise NotImplementedError

        # Create and publish the validated or canceled Order
        if vr.ok:
            await self._order_accepted(intent=intent, vr=vr)
        else:
            await self._order_rejected(intent=intent, vr=vr)

    # --- Validation ---

    def _validate_spot_order(
        self,
        # intent: OrderIntent,
        # close_px: float
    ) -> ValidationResult:
        """
        Stub - Information from exchange to be added!
        """
        vr = ValidationResult(ok=False)

        # 1. Validate Information with the exchange:
        # 1.1. if not symbol exists, return vr
        # 1.2. If symbol is not tradeable, add error to vr
        # 1.3. if order type not allowed
        # 1.4. Get filters from the exchange -> Apply price, lot and notional filters
        # -> each own function

        # 2. Validate Information with the Account system
        # 2.1. Account: Client order id uniqueness
        vr.ok = len(vr.errors) == 0
        return vr

    def _validate_option_order(self) -> None:
        raise NotImplementedError

    def _intent_to_validated(
        self, intent: OrderIntent, vr: ValidationResult
    ) -> ValidatedOrderIntent:
        """
        Convert a raw intent to its validated counterpart by reusing the same fields.
        Adds 'val_results' to tags. Returns a new frozen instance.
        """
        data = asdict(intent)  # deep-copies nested structures (tags included)
        tags = dict(data.get("tags", {}))
        tags["val_results"] = {
            "ok": vr.ok,
            "errors": list(vr.errors),
            "warnings": list(vr.warnings),
        }
        data["tags"] = tags

        if isinstance(intent, MarketOrderIntent):
            fields = relevant_fields(intent)
            data["hash"] = order_fingerprint_hash(intent, fields)
            return ValidatedMarketOrderIntent(**data)
        if isinstance(intent, LimitOrderIntent):
            fields = relevant_fields(intent)
            data["hash"] = order_fingerprint_hash(intent, fields)
            return ValidatedLimitOrderIntent(**data)
        if isinstance(intent, StopMarketOrderIntent):
            fields = relevant_fields(intent)
            data["hash"] = order_fingerprint_hash(intent, fields)
            return ValidatedStopMarketOrderIntent(**data)
        if isinstance(intent, StopLimitOrderIntent):
            fields = relevant_fields(intent)
            data["hash"] = order_fingerprint_hash(intent, fields)
            return ValidatedStopLimitOrderIntent(**data)

        raise TypeError(f"Unsupported intent type: {type(intent).__name__}")

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
            status=AckStatus.ACCEPTED,
            ts_utc=self._clock.now(),
        )
        validated_order = self._intent_to_validated(intent=intent, vr=vr)

        # Log ValidationResult (only OrderAck)
        self._log_validation_result(order_ack=order_ack, intent=intent, vr=vr, log_intent=False)

        # Publish OrdersAck and ValidatedOrder
        await self._bus.publish(topic="orders.ack", ts_utc=self._clock.now(), payload=order_ack)
        await self._bus.publish(
            topic="orders.sanitized", ts_utc=self._clock.now(), payload=validated_order
        )

    async def _order_rejected(
        self,
        intent: OrderIntent,
        vr: ValidationResult,
    ) -> None:
        order_ack = OrderAck(
            intent_id=intent.id,
            strategy_id=intent.strategy_id,
            component="OrderValidation",
            symbol=intent.symbol,
            market=intent.market,
            side=intent.side,
            status=AckStatus.REJECTED,
            reason="OrderValidationFailed",
            ts_utc=self._clock.now(),
        )
        await self._bus.publish(topic="orders.ack", ts_utc=self._clock.now(), payload=order_ack)
        intent.tags["val_results"] = vr
        await self._bus.publish(topic="orders.canceled", ts_utc=self._clock.now(), payload=intent)
        self._log_validation_result(order_ack=order_ack, intent=intent, vr=vr, log_intent=True)

    def _log_validation_result(
        self, order_ack: OrderAck, intent: OrderIntent, vr: ValidationResult, log_intent: bool
    ) -> None:
        # log the OrderAck
        payload_ack = self._audit.ack_to_payload(order_ack)
        payload_ack["val_result"] = {
            "ok": vr.ok,
            "errors": list(vr.errors),
            "warnings": list(vr.warnings),
        }
        self._audit.emit(
            component="order_validation",
            event="ORDER_INTENT_VALIDATION_ACK",
            level="INFO" if vr.ok else "WARN",
            sim_time=self._clock.now(),
            payload=payload_ack,
            symbol=intent.symbol,
            order_id=intent.id,
            parent_id=intent.id,
        )

        # Log the intent
        if log_intent:
            payload_intent = self._audit.intent_to_payload(intent)
            payload_intent["val_result"] = {
                "ok": vr.ok,
                "errors": list(vr.errors),
                "warnings": list(vr.warnings),
            }
            self._audit.emit(
                component="order_validation",
                event="ORDER_INTENT_VALIDATION_INTENT",
                level="INFO" if vr.ok else "WARN",
                sim_time=self._clock.now(),
                payload=payload_intent,
                symbol=intent.symbol,
                order_id=intent.id,
            )
