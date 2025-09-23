"""EventBus Port Interface.

References: ADR-010 (Event Driven Core Loop).

Contract: Publish/subscribe mechanism for domain events (bars, orders, fills, risk decisions).
"""
from __future__ import annotations
from typing import Protocol, Callable, Type, Any

class EventBus(Protocol):
    def publish(self, event: Any) -> None:
        """Publish an event instance to all subscribers."""
        ...

    def subscribe(self, type: Type[Any], handler: Callable[[Any], None]) -> None:
        """Register handler for all events of given type."""
        ...
