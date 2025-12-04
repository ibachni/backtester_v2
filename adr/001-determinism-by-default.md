# ADR 001: Determinism By Default

Status: Accepted
Date: 2025-09-23
Deciders: Nicolas Ibach
Context Version: 1.0

## Context
Backtests must be reproducible for scientific comparison and debugging. Non-determinism (implicit randomness, time.localtime, reliance on wall-clock network calls) leads to irreproducible performance metrics and invalid strategy research.

## Decision
1. Every backtest run records (a) git SHA, (b) global random seed(s), (c) config hash
2. Any stochastic component (slippage model, synthetic data generator) must accept an explicit seed.
3. Hidden randomness (e.g., `random.random()` without prior `seed` provisioning) is prohibited
4. Time sources: use a monotonic event clock; never `datetime.now()` in core logic.

## Consequences
+ Enables exact reproduction of order/fill sequences.
− Additional variables to watch and libs to import

## Alternatives Considered
- None

## Implementation Notes
- tbd.
