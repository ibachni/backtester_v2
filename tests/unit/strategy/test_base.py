def test_strategy_init_params_readonly_and_hash():
    """
    Verify params are exposed as an immutable mapping and params_hash is a
    stable, order-independent digest.
    """
    pass


def test_strategy_info_default_name_lowercase():
    """
    By default, `Strategy.info.name` should be the class name in lowercase.
    """
    pass


def test_strategy_default_hooks_and_needs():
    """
    Defaults: subscriptions(), symbols(), timeframe() return empty tuples and
    warmup_bars() returns 0.
    """
    pass


def test_on_start_returns_empty_list_by_default():
    """
    `on_start()` should return an empty list of OrderIntent by default.
    """
    pass


def test_on_candle_is_abstract_and_must_be_implemented():
    """
    Instantiating a concrete Strategy without implementing `on_candle` should
    raise a TypeError due to abstractmethod.
    """
    pass


def test_on_fill_optional_and_may_return_none():
    """
    Default `on_fill` returns None, indicating no follow-up intents are
    required.
    """
    pass


def test_on_timer_returns_empty_list_and_is_idempotent():
    """
    `on_timer(ts)` should return an empty list by default and be idempotent for
    the same timestamp.
    """
    pass


def test_on_end_performs_no_io_and_returns_none():
    """
    `on_end()` should perform no I/O and return None by default.
    """
    pass


def test_snapshot_state_contains_params_hash_minimal():
    """
    `snapshot_state()` should return a small JSON-safe dict including
    `params_hash`.
    """
    pass


def test_log_event_routes_levels_and_includes_fields():
    """
    `log_event` should route to ctx.log.debug/info/warning/error based on level
    and include strategy name, version, run_id, params_hash, and mode fields.
    """
    pass


def test_dec_converts_to_decimal_and_preserves_decimal():
    """
    `dec()` should return a Decimal; if input is already Decimal, return as-is;
    otherwise convert via Decimal(str(x)).
    """
    pass


def test_to_step_handles_non_power_of_10_steps():
    """
    `_to_step` should quantize values to arbitrary steps (e.g., 0.005) using
    provided rounding mode.
    """
    pass


def test_quantize_price_applies_tick_size_or_passthrough():
    """
    `quantize_price` should snap to `tick_size` when spec provided; otherwise
    return the original price.
    """
    pass


def test_quantize_qty_applies_lot_size_and_sign_rounding():
    """
    `quantize_qty` should snap magnitude to `lot_size` with floor semantics and
    preserve sign; passthrough when no spec.
    """
    pass


def test_meets_min_notional_threshold():
    """
    `meets_min_notional` should return True when |qty|*price >= min_notional;
    True if no spec or no min_notional is set.
    """
    pass
