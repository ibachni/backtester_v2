# ADR 017: Slippage Model – Spread + Participation of Volume

Status: Accepted  
Date: 2025-09-23  
Context Version: 1.0

## Context
Ignoring slippage overstates performance while full order book simulation is high effort. Fixed basis point models fail to adapt to volume conditions. Participation of volume (POV) plus spread offers a middle ground.

## Decision
Model execution price as: `fill_price = mid_price + 0.5 * spread + impact`, where `impact = k * (order_qty / bar_volume)` and `k` is a tunable deterministic constant. If bar volume = 0, treat impact as `k * (order_qty / min_volume_guard)`.

## Consequences
+ More realistic than fixed bps; stays deterministic.  
+ Scales impact with aggressiveness relative to liquidity.  
− Not suitable for HFT / microstructure modeling; ignores queue dynamics.

## Alternatives Considered
*No slippage*: Unrealistic PnL.  
*Fixed bps*: Non-adaptive.  
*Full OB simulation*: Deferred until sub-minute data available.

## Revisit Criteria
- Availability of level 2 / top-of-book snapshots.  
- Need to model partial fills or time-sliced execution.

## Implementation Notes
- Include `slippage_params` in config with `k`, `min_volume_guard`.  
- Unit test invariants: impact increases with order size; zero spread => price >= mid.

## Related
Execution Model (015), Determinism (001).
