def test_params_validation_fast_lt_slow():
    """
    Constructor should raise ValueError unless 0 < fast < slow; valid params
    should construct successfully.
    """
    pass


def test_params_dataclass_iter_len_getitem():
    """
    `SMAExtendedParams` should support iteration over field names, `len(params)`
    equals number of fields, and `params["field"]` lookup.
    """
    pass


def test_symbols_timeframe_and_warmup_bars():
    """
    `symbols()` echoes configured symbols, `timeframe()` returns tuple when set,
    and `warmup_bars()` equals max(fast, slow).
    """
    pass


def test_on_start_initializes_internal_state():
    """
    `on_start()` should initialize per-symbol windows, position flags, pending
    map, cooldown counters, entry price, trail, and last signal ts. It returns
    an empty list.
    """
    pass


def test_is_warm_and_sma_computation():
    """
    `_is_warm` should only be True after `warmup_bars()` prices. `_sma` should
    compute fast and slow averages from the window.
    """
    pass


def test_buy_signal_fast_cross_over_slow():
    """
    When not in position, no pending, cooldown=0, under `max_positions`, and
    fast > slow, `on_candle` should emit a BUY MarketOrderIntent with reason
    'fast_gt_slow' and set pending.
    """
    pass


def test_sell_signal_fast_cross_below_slow():
    """
    When in position and fast < slow, `on_candle` should emit a SELL intent with
    reason 'fast_lt_slow'.
    """
    pass


def test_sell_take_profit_stop_loss_and_trailing():
    """
    In position: emit SELL when gain >= tp_pct (take_profit) or gain <= -sl_pct
    (stop_loss), or when price <= trailing level (trailing_stop).
    """
    pass


def test_pending_prevents_duplicate_submits():
    """
    If a pending signal exists for the symbol, a subsequent qualifying candle in
    the same state should not emit a duplicate intent.
    """
    pass


def test_cooldown_blocks_and_decrements_then_allows_reentry():
    """
    After a SELL fill, cooldown is set; subsequent candles should decrement it
    and block reentry until it reaches zero (respecting `allow_reentry`).
    """
    pass


def test_max_positions_limit_enforced():
    """
    When number of open positions equals `max_positions`, further BUY signals
    should be suppressed.
    """
    pass


def test_daily_loss_limit_blocks_new_entries():
    """
    If `daily_pnl` <= -`daily_loss_limit`, new BUY entries should be blocked
    until day rollover resets the limit.
    """
    pass


def test_on_fill_updates_position_entry_and_trail():
    """
    BUY fill sets position True, records entry price and trail; SELL fill sets
    position False, applies cooldown, and clears entry/trail; pending cleared.
    """
    pass


def test_update_price_raises_trail_when_price_increases():
    """
    While in position, `_update_price` should raise the trailing stop level when
    price makes a new high.
    """
    pass


def test_trail_hit_detection():
    """
    `_trail_hit` returns True only when in position, trail > 0, and price is at
    or below the trail level.
    """
    pass


def test_roll_day_resets_daily_pnl_on_new_day():
    """
    `_roll_day` should detect day boundary via floor(end_ms/86400000) and reset
    `daily_pnl` when the day changes.
    """
    pass


def test_mk_market_intent_fields_and_ids():
    """
    `_mk_market_intent` should set market=SPOT, side BUY/SELL, include tags with
    reason, `strategy_id` equals strategy name, and incrementing human-readable
    id; `ts_utc` should take value from `clock.now()`.
    """
    pass


def test_snapshot_state_includes_internal_maps():
    """
    `snapshot_state()` extends base snapshot with pos, pending, cooldown,
    entry_px, trail, and daily_pnl keys.
    """
    pass
