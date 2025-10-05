"""OrderRouter Port Interface.

References: ADR-016 (Idempotent client order IDs), ADR-015 (Execution model).

Contract: Submit & cancel orders; idempotent per client_order_id.
"""

from __future__ import annotations

from typing import Protocol


class Order:  # minimal placeholder
    def __init__(self, client_order_id: str):
        self.client_order_id = client_order_id


class OrderAck:  # placeholder
    def __init__(self, client_order_id: str, accepted: bool):
        self.client_order_id = client_order_id
        self.accepted = accepted


class CancelAck:  # placeholder
    def __init__(self, client_order_id: str, cancelled: bool):
        self.client_order_id = client_order_id
        self.cancelled = cancelled


class OrderRouter(Protocol):
    def submit(self, order: Order) -> OrderAck: ...
    def cancel(self, client_order_id: str) -> CancelAck: ...
