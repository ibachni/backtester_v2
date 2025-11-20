"""
Centralized topic constants for the event bus.

This module exists to break circular dependencies between modules
that need to reference topics (e.g., audit.py, backtest_engine_new.py).
"""

# Market data topics
T_CANDLES = "mkt.candles"

# Order lifecycle topics
T_ORDERS_INTENT = "orders.intent"
T_ORDERS_SANITIZED = "orders.sanitized"
T_ORDERS_ACK = "orders.ack"
T_ORDERS_CANCELED = "orders.canceled"
T_FILLS = "orders.fills"

# Account topics
T_METRICS = "account.metrics"  # e.g., NAV, DD, turnover
T_TRADE_EVENT = "account.trade"
T_ACCOUNT_SNAPSHOT = "account.snapshot"

# Control topics
T_CONTROL = "control"
T_LOG = "log.event"
