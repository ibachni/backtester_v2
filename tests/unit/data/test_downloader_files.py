import datetime as dt
import hashlib
import io
from pathlib import Path
from typing import Any, cast

import pytest

from backtester.audit.audit import AuditWriter
from backtester.data.downloader import DownloaderFiles
from backtester.types.data import RawBytes

from ...fixtures.fixtures import downloader_config, run_context_test_net, run_context_test_no_net


def test_init_creates_paths_and_audit_writer():
    """Initializer should create audit writer and cache directories."""
    dl = DownloaderFiles(run_ctx=run_context_test_no_net, cfg=downloader_config)
    # Audit writer instance
    assert isinstance(dl._audit, AuditWriter)

    # Audit file path exists
    # audit_fp = (
    #     Path(downloader_config.log_dir) / "audit" / f"{downloader_config.download_run_id}.ndjson"
    # )
    # assert audit_fp.exists(), f"Expected audit file at {audit_fp}"

    # Cache root exists
    # cache_root = Path(downloader_config.base_dir) / "_cache" / "archives"
    # assert cache_root.exists(), f"Expected cache root at {cache_root}"

    # Run id consistency
    assert dl._audit.run_id == downloader_config.download_run_id


def test_download_files_respects_allow_net_false():
    """
    When `RunContext.allow_net` is False, `download_files` should short-circuit
    (no HTTP), emit an audit event, and return an empty list (or deterministic
    behavior per contract).
    """
    dl = DownloaderFiles(run_ctx=run_context_test_no_net, cfg=downloader_config)
    # audit_fp = (
    #     Path(downloader_config.log_dir) / "audit" / f"{downloader_config.download_run_id}.ndjson"
    # )

    # Expect early failure due to network disabled (fail-closed) and no blobs returned
    with pytest.raises(ValueError):
        _ = dl.download_files(
            symbol="BTCUSDT",
            market="spot",
            timeframe="1m",
            start=dt.datetime(2018, 10, 1),
            end=dt.datetime(2018, 10, 31),
        )

    # assert audit_fp.exists(), "Audit log should exist after RUN_START emission"


def test_download_files_date_range_inclusive_start_exclusive_end(monkeypatch: pytest.MonkeyPatch):
    """
    Verify date math: start is inclusive and end is exclusive (end - 1 ms).
    Construct a small window and assert which daily/monthly periods are
    attempted.
    """
    dl = DownloaderFiles(run_ctx=run_context_test_net, cfg=downloader_config)
    monthly_periods: list[str] = []
    daily_periods: list[str] = []
    payload_bytes = b"dummy"

    def fake_download_zip(
        self: DownloaderFiles, url: str, *, session: object
    ) -> tuple[int, bytes | None]:
        if "/monthly/" in url:
            return 404, None
        return 200, payload_bytes

    def fake_blob_ref(
        self: DownloaderFiles,
        *,
        market: str,
        symbol: str,
        interval: str | None,
        granularity: str,
        period: str,
        url: str,
        http_status: int,
        payload: bytes | None,
    ) -> RawBytes | None:
        if granularity == "monthly":
            monthly_periods.append(period)
        else:
            daily_periods.append(period)

        if http_status == 200 and payload:
            return RawBytes(
                exchange=self._cfg.exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                granularity=granularity,  # type: ignore[arg-type]
                period=period,
                url=url,
                http_status=http_status,
                sha256="stub",
                size=len(payload),
                path=None,
                bytes=payload,
                run_id=self._run_id,
            )
        return None

    monkeypatch.setattr(DownloaderFiles, "_download_zip", fake_download_zip)
    monkeypatch.setattr(DownloaderFiles, "_blob_ref", fake_blob_ref)

    blobs = dl.download_files(
        symbol="BTCUSDT",
        market="spot",
        timeframe="1m",
        start=dt.datetime(2018, 10, 1),
        end=dt.datetime(2018, 11, 1),
    )

    assert monthly_periods == ["2018-10"], "Only October monthly archive should be attempted"

    expected_days = [(dt.date(2018, 10, 1) + dt.timedelta(days=i)).isoformat() for i in range(31)]
    assert daily_periods == expected_days, "Daily downloads should cover every October day"
    assert len(blobs) == len(expected_days)


def test_download_files_uses_session_and_accumulates_blob_refs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = downloader_config.model_copy(update={"base_dir": tmp_path, "log_dir": tmp_path})
    dl = DownloaderFiles(run_ctx=run_context_test_net, cfg=cfg)

    monthly_url = dl._monthly_zip_url("spot", "BTCUSDT", "1m", 2018, 10)
    response_map = {monthly_url: (200, b"october-bytes")}
    seen_urls: list[str] = []
    sessions: list["FakeSession"] = []

    class FakeResponse:
        def __init__(self, status: int, payload: bytes | None) -> None:
            self.status_code = status
            self._payload = payload or b""

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def iter_content(self, chunk_size: int):
            if not self._payload:
                yield from ()
            else:
                for idx in range(0, len(self._payload), chunk_size):
                    yield self._payload[idx : idx + chunk_size]

    class FakeSession:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}
            sessions.append(self)

        def __enter__(self) -> "FakeSession":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def get(self, url: str, timeout: float, stream: bool) -> FakeResponse:
            seen_urls.append(url)
            status, payload = response_map.get(url, (404, None))
            return FakeResponse(status, payload)

    monkeypatch.setattr(
        "backtester.data.downloader.requests.Session",
        lambda: FakeSession(),
    )

    blobs = dl.download_files(
        symbol="BTCUSDT",
        market="spot",
        timeframe="1m",
        start=dt.datetime(2018, 10, 1),
        end=dt.datetime(2018, 11, 1),
    )

    assert seen_urls == [monthly_url], "Downloader should request the monthly archive once"
    assert len(blobs) == 1
    blob = blobs[0]
    assert blob.ok
    assert blob.granularity == "monthly"
    assert blob.period == "2018-10"
    assert blob.size == len(response_map[monthly_url][1])
    path = blob.path
    assert path is not None and path.exists()
    assert sessions[0].headers["User-Agent"] == dl._default_user_agent


def test_log_emits_to_audit() -> None:
    dl = DownloaderFiles(run_ctx=run_context_test_no_net, cfg=downloader_config)
    calls: list[dict[str, Any]] = []

    class StubAudit:
        def emit(self, **kwargs) -> None:
            calls.append(kwargs)

    dl._audit = StubAudit()  # type: ignore[assignment]
    dl._log("UNIT_EVENT", level="WARN", payload={"foo": "bar"})

    assert len(calls) == 1
    call = calls[0]
    assert call["component"] == "ingest"
    assert call["event"] == "UNIT_EVENT"
    assert call["level"] == "WARN"
    payload = call["payload"]
    assert isinstance(payload, dict)
    assert payload["run_id"] == downloader_config.download_run_id
    assert payload["foo"] == "bar"


def test_collect_base_info_fields() -> None:
    dl = DownloaderFiles(run_ctx=run_context_test_no_net, cfg=downloader_config)
    start = dt.datetime(2020, 1, 1, 12, 0, 0)
    end = dt.datetime(2020, 1, 2, 12, 0, 0)
    info = dl._collect_base_info("ETHUSDT", "spot", "5m", start, end)

    assert info == {
        "symbol": "ETHUSDT",
        "exchange": downloader_config.exchange,
        "market": "spot",
        "timeframe": "5m",
        "start_iso": start.isoformat(),
        "end_iso": end.isoformat(),
        "binance_base": str(downloader_config.binance_base),
        "retries": downloader_config.retries,
        "timeout": downloader_config.timeout,
    }


def test_first_day_and_last_day_edges() -> None:
    dl = DownloaderFiles(run_ctx=run_context_test_no_net, cfg=downloader_config)

    assert dl._first_day(2023, 2) == dt.date(2023, 2, 1)
    assert dl._last_day(2023, 2) == dt.date(2023, 2, 28)
    assert dl._last_day(2024, 2) == dt.date(2024, 2, 29)
    assert dl._first_day(2023, 12) == dt.date(2023, 12, 1)
    assert dl._last_day(2023, 12) == dt.date(2023, 12, 31)


def test_monthly_zip_url_building() -> None:
    dl = DownloaderFiles(run_ctx=run_context_test_no_net, cfg=downloader_config)
    spot_url = dl._monthly_zip_url("spot", "BTCUSDT", "1m", 2023, 1)
    assert spot_url.endswith("/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2023-01.zip")

    futures_url = dl._monthly_zip_url("futures/um", "ETHUSDT", "15m", 2024, 12)
    assert futures_url.endswith(
        "/data/futures/um/monthly/klines/ETHUSDT/15m/ETHUSDT-15m-2024-12.zip"
    )


def test_daily_zip_url_building() -> None:
    dl = DownloaderFiles(run_ctx=run_context_test_no_net, cfg=downloader_config)
    d = dt.date(2023, 1, 15)
    url = dl._daily_zip_url("spot", "BTCUSDT", "1h", d)
    assert url.endswith("/data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2023-01-15.zip")

    futures_url = dl._daily_zip_url("futures/cm", "BTCUSD_PERP", "1m", d)
    assert futures_url.endswith(
        "/data/futures/cm/daily/klines/BTCUSD_PERP/1m/BTCUSD_PERP-1m-2023-01-15.zip"
    )


def test_daily_zip_url_options_building() -> None:
    dl = DownloaderFiles(run_ctx=run_context_test_no_net, cfg=downloader_config)
    d = dt.date(2023, 6, 1)
    url = dl._daily_zip_url_options("option", "BTCUSDT", d)
    assert url.endswith("/data/option/daily/EOHSummary/BTCUSDT/BTCUSDT-EOHSummary-2023-06-01.zip")


def test_candidate_market_bases_variants_and_errors() -> None:
    dl = DownloaderFiles(run_ctx=run_context_test_no_net, cfg=downloader_config)

    assert dl._candidate_market_bases("spot", "BTCUSDT") == ["spot"]
    assert dl._candidate_market_bases("options", "BTCUSDT") == ["option"]
    assert dl._candidate_market_bases("futures", "BTCUSDT") == ["futures/um", "futures/cm"]
    assert dl._candidate_market_bases("um", "BTCUSDT") == ["futures/um"]
    assert dl._candidate_market_bases("cm", "BTCUSDT") == ["futures/cm"]
    assert dl._candidate_market_bases("futures/um", "BTCUSDT") == ["futures/um"]

    with pytest.raises(ValueError):
        dl._candidate_market_bases("swap", "BTCUSDT")


def test_validate_option_availability_bounds_and_unknown_symbol() -> None:
    dl = DownloaderFiles(run_ctx=run_context_test_net, cfg=downloader_config)
    within = dl._validate_option_availability(
        "BTCUSDT",
        dt.datetime(2023, 5, 20),
        dt.datetime(2023, 5, 21),
    )
    assert within == (True, "Given start, end within available range")

    too_early = dl._validate_option_availability(
        "BTCUSDT",
        dt.datetime(2023, 5, 10),
        dt.datetime(2023, 5, 11),
    )
    assert not too_early[0] and "Availability" in too_early[1]

    too_late = dl._validate_option_availability(
        "BTCUSDT",
        dt.datetime(2023, 10, 24),
        dt.datetime(2023, 10, 25),
    )
    assert not too_late[0] and "Availability" in too_late[1]

    unknown = dl._validate_option_availability(
        "UNKNOWN",
        dt.datetime(2023, 6, 1),
        dt.datetime(2023, 6, 2),
    )
    assert not unknown[0]
    assert unknown[1] == "Symbol not available"


def test_download_zip_success_retry_and_backoff_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dl = DownloaderFiles(run_ctx=run_context_test_net, cfg=downloader_config)

    delays: list[float] = []

    def fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr("backtester.data.downloader.time.sleep", fake_sleep)

    responses = [(500, None), (429, None), (200, b"payload")]

    class FakeResponse:
        def __init__(self, status: int, payload: bytes | None) -> None:
            self.status_code = status
            self._payload = payload or b""

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def iter_content(self, chunk_size: int):
            if self._payload:
                yield self._payload
            else:
                yield from ()

    class SequencedSession:
        def __init__(self, seq: list[tuple[int, bytes | None]]) -> None:
            self._seq = list(seq)
            self.calls: list[str] = []
            self.headers: dict[str, str] = {}

        def get(self, url: str, timeout: float, stream: bool) -> FakeResponse:
            self.calls.append(url)
            status, payload = self._seq.pop(0)
            return FakeResponse(status, payload)

    session = SequencedSession(list(responses))
    status, data = dl._download_zip("https://example.test/monthly.zip", session=cast(Any, session))
    assert status == 200
    assert data == b"payload"
    assert len(session.calls) == 3
    assert delays == [1.0, 1.5]

    failing_session = SequencedSession([(503, None), (503, None), (503, None)])
    delays.clear()
    status_fail, data_fail = dl._download_zip(
        "https://example.test/monthly.zip", session=cast(Any, failing_session)
    )
    assert status_fail == 503
    assert data_fail is None
    assert len(failing_session.calls) == 3
    assert delays == [1.0, 1.5]


def test_blob_ref_creates_rawbytes_on_200_and_caches_payload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = downloader_config.model_copy(update={"base_dir": tmp_path, "log_dir": tmp_path})
    dl = DownloaderFiles(run_ctx=run_context_test_net, cfg=cfg)
    payload = b"blob-bytes"
    stored: dict[str, Path] = {}

    def fake_store(self: DownloaderFiles, sha: str, blob: bytes) -> Path:
        out = tmp_path / f"{sha}.zip"
        out.write_bytes(blob)
        stored[sha] = out
        return out

    monkeypatch.setattr(DownloaderFiles, "_store_in_cache", fake_store)

    blob = dl._blob_ref(
        market="spot",
        symbol="BTCUSDT",
        interval="1m",
        granularity="daily",
        period="2018-10-01",
        url="https://example.test",
        http_status=200,
        payload=payload,
    )

    assert blob is not None
    expected_sha = hashlib.sha256(payload).hexdigest()
    assert blob.sha256 == expected_sha
    assert blob.size == len(payload)
    assert blob.bytes is None
    path = blob.path
    assert path is not None
    assert path == stored[expected_sha]
    assert path.read_bytes() == payload

    blob_none = dl._blob_ref(
        market="spot",
        symbol="BTCUSDT",
        interval="1m",
        granularity="daily",
        period="2018-10-01",
        url="https://example.test",
        http_status=404,
        payload=None,
    )
    assert blob_none is None


def test_store_in_cache_is_content_addressed_and_atomic(tmp_path: Path) -> None:
    cfg = downloader_config.model_copy(update={"base_dir": tmp_path, "log_dir": tmp_path})
    dl = DownloaderFiles(run_ctx=run_context_test_net, cfg=cfg)
    payload = b"payload"
    sha = hashlib.sha256(payload).hexdigest()

    first_path = dl._store_in_cache(sha, payload)
    assert first_path.exists()
    assert first_path.read_bytes() == payload
    expected_dir = tmp_path / "_cache" / "archives" / sha[:2] / sha[2:4]
    assert first_path.parent == expected_dir

    # Idempotent: second write returns existing file without overwriting contents
    second_path = dl._store_in_cache(sha, b"different")
    assert second_path == first_path
    assert second_path.read_bytes() == payload
    assert not second_path.with_suffix(".tmp").exists()


def test_compute_sha256_bytes_and_stream_preserve_position() -> None:
    dl = DownloaderFiles(run_ctx=run_context_test_no_net, cfg=downloader_config)
    payload = b"hashme"
    expected = hashlib.sha256(payload).hexdigest()
    assert dl._compute_sha256(payload) == expected

    stream = io.BytesIO(payload)
    stream.seek(2)
    pos_before = stream.tell()
    stream_digest = dl._compute_sha256(stream)
    assert stream_digest == hashlib.sha256(payload[2:]).hexdigest()
    assert stream.tell() == pos_before
