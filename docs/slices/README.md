# Slices Index

This directory tracks incremental, minimal-value slices. Each slice is a focused change set (≤400 LOC target) aligned with ADRs.

| ID | Title | Status | Depends | Ticket | Brief |
|----|-------|--------|---------|--------|-------|
| SLICE-0000 | Bootstrap & Contracts | planned | - | BT-0000 | Repo scaffold, ports, CLI stub, CI green |
| SLICE-0001 | Config & Secrets Layer | planned | 0000 | BT-0001 | Typed config, layered overrides, secrets redaction |
| SLICE-0002 | Backtest Core (Single Symbol) | planned | 0000,0001 | BT-0002 | Deterministic 1m loop, Parquet bars, accounting |
| SLICE-0003 | Strategy & Execution (Slippage + Minimal Risk) | planned | 0002 | BT-0003 | Strategy API, cost model, minimal risk rules |
| SLICE-0004 | Paper Trading Parity | planned | 0003 | BT-0004 | Live REST polling + paper broker parity |
| SLICE-0005 | Shadow Mode Runtime | planned | 0004 | BT-0005 | Null router, soak testing, latency metrics |
| SLICE-0006 | Risk Rails & Safety Expansion | planned | 0003,0005 | BT-0006 | Pre-trade rules + halt/flatten controls |
| SLICE-0007 | Binance REST Live Adapter (Tiny Orders) | planned | 0004,0006 | BT-0007 | Auth, order placement, reconciliation |
| SLICE-0008 | Observability & Recovery | planned | 0000-0007 | BT-0008 | Metrics, snapshots, replay, alerts |
| SLICE-0009 | Contract Test Harness | planned | 0000-0008 | BT-0009 | Port adapter contract test suite |

Legend: planned | in-progress | merged | archived

Update this table when slice status changes. Automated generation can replace manual edits later.
