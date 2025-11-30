# Bus Topology & Module I/O

This document serves as a registry of inputs (subscriptions) and outputs (publications) for each module in the system. It defines the data flow contract between components.

## Format
- **Inputs**: Topics the module subscribes to.
- **Outputs**: Topics the module publishes to.

---

## Strategy (SMA Extended)
**Component:** `SMAExtendedStrategy` (`backtester.strategy.sma_extended`)
**Runner:** `StrategyRunner` (`backtester.core.backtest_engine_newest`)

### Inputs
| Topic | Payload | Handler | Notes |
| :--- | :--- | :--- | :--- |
| `T_CANDLES` | `Candle` | `on_candle` | Main driver. Evaluates indicators and generates signals. |
| `T_FILLS` | `Fill` | `on_fill` | Updates position state, entry prices, and PnL. |
| `T_ORDERS_CANCELED` | `OrderAck` | `on_reject` | Clears pending state for the symbol. |
| `T_ORDERS_REJECTED` | `OrderAck` | `on_reject` | Clears pending state for the symbol. |
| `T_CONTROL` | `ControlEvent` | `Runner.run` | Handles `STOP` signals to terminate the loop. |

### Outputs
| Topic | Payload | Trigger | Notes |
| :--- | :--- | :--- | :--- |
| `T_ORDERS_INTENT` | `OrderIntent` | `on_candle` | Emits new order requests (Market/Limit/Stop) based on signal logic. |

---

## Order Validation
**Component:** `OrderValidation` (`backtester.strategy.order_validation`)
**Runner:** `ValidationRunner` (`backtester.core.backtest_engine_newest`)

### Inputs
| Topic | Payload | Handler | Notes |
| :--- | :--- | :--- | :--- |
| `T_ORDERS_INTENT` | `OrderIntent` | `validate_order` | Validates structure, limits, and risk. |
| `T_ACCOUNT_SNAPSHOT` | `PortfolioSnapshot` | `on_account_snapshot` | Updates internal cash balance for risk checks. |

### Outputs
| Topic | Payload | Trigger | Notes |
| :--- | :--- | :--- | :--- |
| `T_ORDERS_SANITIZED` | `ValidatedOrderIntent` | `_order_accepted` | Emitted when an order passes all checks. |
| `T_ORDERS_ACK` | `OrderAck` | `_order_accepted` | Emitted with status `VALIDATED` to acknowledge receipt. |
| `T_ORDERS_REJECTED` | `OrderAck` | `_order_rejected` | Emitted with status `REJECTED` if validation fails. |
| `T_LOG` | `LogEvent` | `_validate_spot_order` | Emits error logs if symbol specs are missing. |

---

## Execution Simulator
**Component:** `ExecutionSimulator` (`backtester.sim.sim`)
**Runner:** `SimRunner` (`backtester.core.backtest_engine_newest`)

### Inputs
| Topic | Payload | Handler | Notes |
| :--- | :--- | :--- | :--- |
| `T_ORDERS_SANITIZED` | `ValidatedOrderIntent` | `on_order` | Accepts validated orders, handles de-duplication, and queues them. |
| `T_CANDLES` | `Candle` | `on_candle` | Matches working orders against market data to generate fills. |

### Outputs
| Topic | Payload | Trigger | Notes |
| :--- | :--- | :--- | :--- |
| `T_ORDERS_ACK` | `OrderAck` | `on_order` | Acknowledges receipt of a new order (Status: `ACK`). |
| `T_ORDERS_REJECTED` | `OrderAck` | `on_order` | Rejects duplicate orders (same ID, different hash). |
| `T_FILLS` | `Fill` | `on_candle` | Emits fill events when orders match. |
| `T_ORDERS_CANCELED` | `OrderAck` | `on_candle` | Emits cancels for FOK failures, IOC leftovers, or budget exhaustion. |
| `T_LOG` | `LogEvent` | `on_candle` | Logs debug info (e.g., participation budget exhaustion). |

---

## Account
**Component:** `Account` (`backtester.core.account`)
**Runner:** `AccountRunner` (`backtester.core.backtest_engine_newest`)

### Inputs
| Topic | Payload | Handler | Notes |
| :--- | :--- | :--- | :--- |
| `T_CANDLES` | `Candle` | `set_mark` | Updates mark-to-market prices for positions. |
| `T_FILLS` | `Fill` | `apply_fill` | Updates cash, positions, and PnL based on executions. |

### Outputs
| Topic | Payload | Trigger | Notes |
| :--- | :--- | :--- | :--- |
| `T_ACCOUNT_SNAPSHOT` | `PortfolioSnapshot` | `publish_latest_snapshot` | Emits portfolio state (equity, cash, positions). |
| `T_TRADE_EVENT` | `TradeEvent` / `LotClosedEvent` | `apply_fill` | Emits trade details and realized PnL events. |
| `T_LOG` | `LogEvent` | `_emit_log` | Logs account-specific events (e.g., dust cleanup). |
