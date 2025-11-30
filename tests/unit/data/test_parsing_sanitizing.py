def test_init_sets_registry_and_limits():
    """
    Ensure constructor sets `_parser_registry` with spot/futures/option parsers
    and `_max_zip_members` to a sane limit.
    """
    pass


def test_parse_archives_emits_summary_and_counts():
    """
    Provide a mix of successful and skipped blobs; assert returned
    (batches, report) where counts (attempts/ok/skipped/errors) and audit
    summary emission are correct.
    """
    pass


def test_parse_archives_handles_empty_archive_and_errors():
    """
    When a blob has no CSV members or raises `EmptyArchiveError`, it should be
    skipped/recorded without crashing; errors should increment error count.
    """
    pass


def test_parse_one_chooses_correct_parser_and_computes_bounds():
    """
    For a futures blob, ensure futures parser is used, partitions/rows filled,
    and start/end bounds coerced to expected scalar types.
    """
    pass


def test_parse_one_raises_on_unknown_parser_or_empty_df():
    """
    Unknown market family should raise or be reported as error; an empty
    parsed DataFrame should be treated as error/skip with audit event.
    """
    pass


def test_market_family_mapping_spot_futures_options():
    """
    Validate `_market_family` maps labels to families:
    option/options → 'option'; futures/um/cm variants → 'futures';
    otherwise 'spot'.
    """
    pass


def test_collect_csv_members_reads_csv_only_and_limits():
    """
    From a ZIP with CSV and non-CSV files, only CSV members are returned in
    order; enforce `_max_zip_members` cap if too many entries.
    """
    pass


def test_emit_writes_expected_payload_to_audit():
    """
    Patch audit writer and call `_emit`; verify component='parsing', event,
    level, symbol, market, interval, granularity, period, url, and run_id are
    present in payload.
    """
    pass


def test_coerce_scalar_bound_accepts_int_date_datetime():
    """
    Passing int ms returns int; date/datetime returned as-is; rejects None.
    """
    pass


def test_coerce_scalar_bound_from_series_and_numpy():
    """
    When given a scalar wrapped in `pl.Series` or a NumPy scalar, extract and
    coerce to the correct Python type.
    """
    pass


def test_coerce_scalar_bound_rejects_nonconvertible():
    """
    Non-numeric strings or unsupported objects should raise a clear error and
    emit an audit event if applicable.
    """
    pass
