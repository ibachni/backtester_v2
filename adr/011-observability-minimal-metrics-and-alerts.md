# ADR 020: Observability – Minimal Metrics & Alerts Only

Status: Accepted
Date: 2025-09-23
Context Version: 1.0

## Context
A full observability stack (Prometheus/Grafana) is excessive for local single-user development. Need a minimal set of high-signal metrics and alerts to diagnose performance, data gaps, and risk events.

## Decision
Emit structured JSONL logs plus a limited metric set. Cross-check metrics (such as events dropped, fills send and received).

## Consequences
+ Low overhead; easy to parse with simple scripts.
+ Encourages focusing on signal metrics.
− No historical dashboards out of the box.

## Alternatives Considered
*Logs only*: Harder to threshold & alert.

## Revisit Criteria
- Need for multi-run analytics or trend dashboards.
- Long-running unattended live processes.
