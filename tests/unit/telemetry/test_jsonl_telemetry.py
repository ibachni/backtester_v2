import json

from backtester.adapters.jsonl import JsonlTelemetry


def _read_single_record(path):
    content = path.read_text(encoding="utf-8").strip()
    assert content, "expected telemetry sink to contain at least one record"
    return json.loads(content)


def test_JsonLTelemtry_initialization(tmp_path):
    sink = tmp_path / "events.log.jsonl"
    init = JsonlTelemetry(
        run_id="run_2",
        git_sha="abc123def456",
        seed=12,
        sink_path=sink,
        secret_keys="api_key",
    )

    init.log("config_resolved", keys_total=3)
    record = _read_single_record(sink)
    assert record["event"] == "config_resolved"
    assert record["run_id"] == "run_2"
    assert record["git_sha"] == "abc123def456"
    assert record["seed"] == 12
    assert record["keys_total"] == 3


def test_santize_fields():
    pass


# def test_component_override_and_secret_redaction(tmp_path):
#     sink = tmp_path / "events.log.jsonl"
#     telemetry = JsonlTelemetry(
#         run_id="run-123",
#         git_sha="abc123def456",
#         seed=42,
#         sink_path=sink,
#         component="default-component",
#         clock=lambda: datetime(2025, 1, 2, tzinfo=timezone.utc),
#     )

#     telemetry.log(
#         "config_hash_computed",
#         component="overridden",
#         api_key="super-secret",
#         config_hash="deadbeef",
#     )

#     record = _read_single_record(sink)
#     assert record["component"] == "overridden"
#     assert record["api_key"] == "***REDACTED***"
#     assert record["config_hash"] == "deadbeef"
#     assert record["redacted_fields"] == ["api_key"]


# def test_component_omitted_when_not_configured(tmp_path):
#     sink = tmp_path / "events.log.jsonl"
#     telemetry = JsonlTelemetry(
#         run_id="run-xyz",
#         git_sha="unknown",
#         seed=7,
#         sink_path=sink,
#         component=None,
#         clock=lambda: datetime(2025, 1, 2, tzinfo=timezone.utc),
#     )

#     telemetry.log("secrets_redacted", redacted_count=1)

#     record = _read_single_record(sink)
#     assert record["event"] == "secrets_redacted"
#     assert "component" not in record


# def test_log_rejects_blank_event(tmp_path):
#     sink = tmp_path / "events.log.jsonl"
#     telemetry = JsonlTelemetry(
#         run_id="run-xyz",
#         git_sha="unknown",
#         seed=7,
#         sink_path=sink,
#         clock=lambda: datetime(2025, 1, 2, tzinfo=timezone.utc),
#     )

#     with pytest.raises(ValueError):
#         telemetry.log("")
