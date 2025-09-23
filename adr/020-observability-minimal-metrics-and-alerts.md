# ADR 020: Observability – Minimal Metrics & Alerts Only

Status: Accepted  
Date: 2025-09-23  
Context Version: 1.0

## Context
A full observability stack (Prometheus/Grafana) is excessive for local single-user development. Need a minimal set of high-signal metrics and alerts to diagnose performance, data gaps, and risk events.

## Decision
Emit structured JSONL logs plus a limited metric set: `order_rtt`, `bars_lag`, `error_rate`, `pnl_intraday`. Provide three alert conditions: (1) bar gap detected, (2) order error rate above threshold, (3) daily loss halt triggered. Defer persistent time-series database until unattended operation becomes common.

## Consequences
+ Low overhead; easy to parse with simple scripts.  
+ Encourages focusing on signal metrics.  
− No historical dashboards out of the box.

## Alternatives Considered
*Full Prom stack now*: Overkill.  
*Logs only*: Harder to threshold & alert.

## Revisit Criteria
- Need for multi-run analytics or trend dashboards.  
- Long-running unattended live processes.

## Implementation Notes
- Metrics aggregator flush interval configurable.  
- Alerts emitted as special log events with `alert_type` field.

## Related
Observability Foundation (005), Determinism (001), Event Loop (010).
