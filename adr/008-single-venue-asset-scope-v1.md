# ADR 013: Single Venue & Asset Class Scope (V1)

Status: Accepted
Date: 2025-09-23
Context Version: 1.0

## Context
Supporting multiple venues and asset classes (e.g., crypto spot + futures) introduces complexity in calendars, fees, precision, and execution semantics. Early focus is on stability and parity between simulation and a single paper/live venue.

## Decision
Limit initial implementation to one venue and one asset class (spot). Multi-venue routing deferred.

## Consequences
+ Faster delivery of reliable core loop & metrics.
+ Reduced surface area for bugs (fees, precision differences).
− No immediate support for venue selection / best-ex routing.

## Alternatives Considered
*Abstract multi-venue now*: Over-engineering risk.

## Revisit Criteria
- Strategy requires venue diversification or latency arbitrage.
- Need to compare execution quality across venues.
