# ADR 012: REST Polling for Initial Live Feed

Status: Accepted
Date: 2025-09-23
Context Version: 1.0

## Context
WebSocket streaming provides lower latency but requires connection lifecycle management. Early live-mode needs prioritize simplicity and parity with historical data ingestion for testing.

## Decision
Implement live market data ingestion via periodic REST polling first. Keep the `DataFeed` interface transport-agnostic so a WebSocket adapter can later plug in without strategy changes.

## Consequences
+ Simpler error handling & retry logic.
+ Easier to simulate in tests (HTTP fixture responses).
− Potential for missed intra-interval microstructure.

## Alternatives Considered
*Immediate WebSocket support*: Higher initial complexity.

## Revisit Criteria
- Transition to sub-minute bars or tick data.

## Implementation Notes
- Poll interval configurable; enforce min interval guard.
- Maintain last received timestamp; detect gaps & emit bar-gap alert metric.
