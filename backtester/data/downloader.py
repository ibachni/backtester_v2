from __future__ import annotations

import datetime as dt
import hashlib
import io
import os
import time
import zipfile
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any, BinaryIO, Literal, Optional

import polars as pl
import requests

from backtester.config.configs import AuditConfig, DownloaderConfig, RunContext
from backtester.core.audit import AuditWriter
from backtester.core.utility import month_range, target_path
from backtester.errors.errors import EmptyArchiveError
from backtester.types.data import ParsingReport, RawBytes, SanitizedBatch, SchemaSpec, WriteReport

# ------------------------- Orchestration ------------------------------


class HistoricalDataService:
    def __init__(self, run_ctx: RunContext, cfg: DownloaderConfig) -> None:
        self._ctx = run_ctx
        self._cfg = cfg

        # Individual Modules
        self._data_dl: Optional[DownloaderFiles] = None
        self._data_ps: Optional[ParsingSanitizing] = None
        self._data_writer: Optional[WritingParquet] = None

    def boot(self) -> None:
        self._data_dl = DownloaderFiles(run_ctx=self._ctx, cfg=self._cfg)
        self._data_ps = ParsingSanitizing(audit=self._data_dl._audit)
        self._data_writer = WritingParquet(cfg=self._cfg, audit=self._data_dl._audit)

    def download(
        self,
        symbol: str,
        market: str,
        timeframe: str,
        start: dt.datetime,
        end: dt.datetime,
    ) -> tuple[ParsingReport, WriteReport]:
        if self._data_dl is None or self._data_ps is None or self._data_writer is None:
            raise RuntimeError("Downloader not initialized. Run boot() first")
        blobs = self._data_dl.download_files(symbol, market, timeframe, start, end)
        batches, parse_report = self._data_ps.parse_archives(blobs)
        write_report = self._data_writer.write_batches(batches, strict=False)

        return (parse_report, write_report)


# ------------------------- Downloading --------------------------------


class DownloaderFiles:
    def __init__(self, run_ctx: RunContext, cfg: DownloaderConfig) -> None:
        self._run_ctx = run_ctx
        self._cfg = cfg
        self._base_dir = cfg.base_dir
        self._log_dir = cfg.log_dir
        self._run_id = cfg.download_run_id  # id of the download run

        # Logging (Individual Logger)
        Path(self._log_dir).mkdir(parents=True, exist_ok=True)
        audit_cfg = AuditConfig(log_dir=Path(self._log_dir) / "audit")
        self._audit = AuditWriter(run_ctx=self._run_ctx, cfg=audit_cfg)

        # Request
        self._default_user_agent: str = (
            "backtester_v2-ingest/1.0 (+https://github.com/Backtester-v2)"
        )

        # Temporary saving of downloaded files
        self._cache_root = Path(self._base_dir) / "_cache" / "archives"
        self._cache_root.mkdir(parents=True, exist_ok=True)

    def download_files(
        self, symbol: str, market: str, timeframe: str, start: dt.datetime, end: dt.datetime
    ) -> list[RawBytes]:
        # 1. Initialize Audit-Wrapper
        base_info = self._collect_base_info(symbol, market, timeframe, start, end)
        self._log("RUN_START", payload=base_info)

        if not self._run_ctx.allow_net:
            self._log("RUN_STOP_ALLOW_NET_FALSE", payload=base_info, level="WARN")
            raise ValueError("DownloaderFile: allow_net=False")

        totals = {"attempts": 0, "ok": 0, "errors": 0}

        # 2. BlogRefList
        blob_refs: list[RawBytes] = []

        # 2. Start inclusive, end exclusive
        start_date = start.date()
        end_date = (end - dt.timedelta(milliseconds=1)).date()

        # 3. Download
        with requests.Session() as session:
            session.headers["User-Agent"] = self._default_user_agent

            # 3.1. Try monthly ZIPs
            for y, m in month_range(start_date, end_date):
                m_first, m_last = self._first_day(y, m), self._last_day(y, m)
                full_month = (m_first >= start_date) and (m_last <= end_date)
                monthly_status: Optional[int] = None
                monthly_bytes: Optional[bytes] = None

                month_covered = False
                if full_month:
                    # Try all all candidate bases
                    for market_base in self._candidate_market_bases(market, symbol):
                        if market_base == "option":
                            # Options are published only as daily.
                            continue
                        totals["attempts"] += 1
                        url_m = self._monthly_zip_url(market_base, symbol, timeframe, y, m)
                        monthly_status, monthly_bytes = self._download_zip(url_m, session=session)
                        blob = self._blob_ref(
                            market=market,
                            symbol=symbol,
                            interval=timeframe,
                            granularity="monthly",
                            period=f"{y:04d}-{m:02d}",
                            url=url_m,
                            http_status=monthly_status,
                            payload=monthly_bytes,
                        )
                        if blob is not None:
                            totals["ok"] += 1
                            blob_refs.append(blob)
                        else:
                            totals["errors"] += 1

                        if monthly_status == 200 and monthly_bytes:
                            month_covered = True
                            break

                if month_covered:
                    continue  # month satisfied via monthly ZIP, skip daily loop

                day = max(m_first, start_date)
                stop = min(m_last, end_date)
                first_daily_day = day  # track first attempted day for skip logic
                while day <= stop:
                    # Try all candidate bases for this market
                    # Stop at the first that succeeds (200 with bytes)
                    status, z = None, None
                    archive_succeeded = False

                    for market_base in self._candidate_market_bases(market, symbol):
                        if market_base == "option":
                            # Validate availability:
                            ok, msg = self._validate_option_availability(symbol, start, end)
                            if not ok:
                                raise ValueError(msg)
                            url_d = self._daily_zip_url_options(market_base, symbol, day)
                        else:
                            url_d = self._daily_zip_url(market_base, symbol, timeframe, day)
                        totals["attempts"] += 1
                        status, z = self._download_zip(url_d, session=session)

                        blob = self._blob_ref(
                            market=market,
                            symbol=symbol,
                            interval=None if market_base == "option" else timeframe,
                            granularity="daily",
                            period=day.strftime("%Y-%m-%d"),
                            url=url_d,
                            http_status=status,
                            payload=z,
                        )
                        if blob is not None:
                            totals["ok"] += 1
                            blob_refs.append(blob)
                            archive_succeeded = True
                            break  # stop trying other bases for this day
                        else:
                            totals["errors"] += 1

                    if not archive_succeeded and day == first_daily_day and status == 404:
                        self._log("DAILY_SKIP_MONTH", payload={**base_info, "day": day.isoformat()})
                        break

                    day += dt.timedelta(days=1)
            self._log("RUN_SUMMARY", payload={**base_info, **totals})
            self._log("RUN_END", payload={**base_info, **totals})
        return blob_refs

    # --- Helpers (AUDIT) ----

    def _log(
        self,
        event: str,
        simple: bool = True,
        level: str = "INFO",
        payload: dict[str, Any] | None = None,
    ) -> None:
        if self._audit is None:
            raise RuntimeError("Downloader has no attribute 'Audit'")
        else:
            self._audit.emit(
                component="ingest",
                event=event,
                level=level,
                simple=simple,
                payload={"run_id": self._run_id, **({} if payload is None else payload)},
            )

    def _collect_base_info(
        self, symbol: str, market: str, timeframe: str, start: dt.datetime, end: dt.datetime
    ) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "exchange": self._cfg.exchange,
            "market": market,
            "timeframe": timeframe,
            "start_iso": start.isoformat(),
            "end_iso": end.isoformat(),
            "binance_base": str(self._cfg.binance_base),
            "retries": self._cfg.retries,
            "timeout": self._cfg.timeout,
        }

    # --- Helpers Date Resolution ---

    def _first_day(self, y: int, m: int) -> dt.date:
        return dt.date(y, m, 1)

    def _last_day(self, y: int, m: int) -> dt.date:
        if m == 12:
            return dt.date(y, m, 31)
        # The day 28 exists in every month. 4 days later, it's always next month
        next_month = dt.date(y, m, 28) + dt.timedelta(days=4)
        # subtracting the number of the current day brings us back one month
        return next_month - dt.timedelta(days=next_month.day)

    # --- Helpers Build URL ---

    def _monthly_zip_url(
        self, market: str, symbol: str, interval: str, year: int, month: int
    ) -> str:
        """
        Converts inputs into a monthly ZIP URL for the chosen market base.
        Examples:
        - /data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2023-01.zip
        - /data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2023-01.zip
        """
        m = f"{month:02d}"
        base_root = str(self._cfg.binance_base).rstrip("/")
        base = f"{base_root}/data/{market}/monthly/klines"
        return f"{base}/{symbol}/{interval}/{symbol}-{interval}-{year}-{m}.zip"

    def _daily_zip_url(self, market: str, symbol: str, interval: str, d: dt.date) -> str:
        """
        Converts inputs into a daily ZIP URL for the chosen market base.
        Examples:
        - /data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2023-01-15.zip
        - /data/futures/cm/daily/klines/BTCUSD_PERP/1m/BTCUSD_PERP-1m-2023-01-15.zip
        """
        base_root = str(self._cfg.binance_base).rstrip("/")
        base = f"{base_root}/data/{market}/daily/klines"
        return f"{base}/{symbol}/{interval}/{symbol}-{interval}-{d:%Y-%m-%d}.zip"

    def _daily_zip_url_options(self, market_base: str, symbol: str, d: dt.date) -> str:
        base_root = str(self._cfg.binance_base).rstrip("/")
        base = f"{base_root}/data/{market_base}/daily/EOHSummary"
        return f"{base}/{symbol}/{symbol}-EOHSummary-{d:%Y-%m-%d}.zip"

    # --- Resolve market variable ---

    def _candidate_market_bases(self, market: str, symbol: str) -> list[str]:
        """
        Return list of market base path segments to try for this logical market.

        Inputs:
        - market: one of "spot", "option", "options", "futures",
                  or explicit aliases: "um", "futures-um", "cm", "futures-cm".
        - symbol: currently unused, but reserved for future heuristics.

        Output examples:
        - spot     → ["spot"]
        - option(s)→ ["option"]
        - futures  → ["futures/um", "futures/cm"]  (UM first)
        - um       → ["futures/um"]
        - cm       → ["futures/cm"]
        """
        m = market.strip().lower()
        if m in {"spot"}:
            return ["spot"]
        if m in {"option", "options"}:
            return ["option"]
        if m in {"um", "futures-um", "futures_um"}:
            return ["futures/um"]
        if m in {"cm", "futures-cm", "futures_cm"}:
            return ["futures/cm"]
        if m == "futures":
            # Try USD-M first, then COIN-M
            return ["futures/um", "futures/cm"]
        # Fallback: pass-through (assume caller gave a valid base like "futures/um")
        if "/" in m:
            return [m]
        raise ValueError(
            f"Unsupported market '{market}'. Use 'spot', 'option(s)', 'futures', 'um', or 'cm'."
        )

    def _validate_option_availability(
        self, symbol: str, start: dt.datetime, end: dt.datetime
    ) -> tuple[bool, str]:
        availability: dict[str, dict[str, str]] = {
            "BNBUSDT": {"start": "2023-05-18", "end": "2023-10-23"},
            "BTCUSDT": {"start": "2023-05-18", "end": "2023-10-23"},
            "DOGEUSDT": {"start": "2023-08-30", "end": "2023-10-20"},
            "ETHUSDT": {"start": "2023-05-18", "end": "2023-10-23"},
            "XRPUSDT": {"start": "2023-05-19", "end": "2023-10-20"},
        }
        start_end = availability.get(symbol)
        if start_end is None:
            return (False, "Symbol not available")
        else:
            avail_start = dt.datetime.strptime(availability[symbol]["start"], "%Y-%m-%d").date()
            avail_end = dt.datetime.strptime(availability[symbol]["end"], "%Y-%m-%d").date()
            if start.date() < avail_start:
                return (False, f"Start too early; Availability from {avail_start}")
            elif start.date() > avail_end:
                return (False, f"End too late; Availability from {avail_end}")
        return (True, "Given start, end within available range")

    # --- Download bytes ---

    def _download_zip(
        self,
        url: str,
        *,
        session: requests.Session,
    ) -> tuple[int, Optional[bytes]]:
        """
        Download a ZIP file over HTTP(S) with streaming and deterministic retries.

        Args:
            url: Fully-qualified URL to the ZIP resource.
            session: Shared requests.Session to reuse TCP connections.
            timeout: Per-request timeout (seconds).
            retries: Max retry attempts for retryable statuses (5xx, 429).
            chunk_size: Bytes per iter_content chunk.

        Returns:
            bytes of the ZIP file on success, otherwise None (not found or network error).
        """
        attempts = max(1, self._cfg.retries)
        delay = 1.0
        last_status = 0

        for attempt in range(attempts):
            retryable = False
            try:
                with session.get(url, timeout=self._cfg.timeout, stream=True) as response:
                    last_status = response.status_code

                    if last_status == 404:
                        print(f"[downloader] GET 404 {url}")
                        return last_status, None

                    retryable = last_status == 429 or 500 <= last_status < 600
                    if not retryable:
                        buf = io.BytesIO()
                        for chunk in response.iter_content(chunk_size=self._cfg.chunk_size):
                            if chunk:
                                buf.write(chunk)
                        print(f"[downloader] GET {last_status} {url} bytes={buf.tell()}")
                        return last_status, buf.getvalue()
            except requests.RequestException as ex:
                last_status = 0
                retryable = True
                print(f"[downloader] EXC {type(ex).__name__} {url}")

            if not retryable:
                print(f"[downloader] STOP {last_status} {url}")
                return last_status, None

            if attempt == attempts - 1:
                break

            time.sleep(delay)
            delay = min(2.0, delay * 1.5)

        print(f"[downloader] FAIL {last_status} {url}")
        return last_status, None

    def _blob_ref(
        self,
        *,
        market: str,
        symbol: str,
        interval: Optional[str],
        granularity: Literal["daily", "monthly"],
        period: str,
        url: str,
        http_status: int,
        payload: Optional[bytes],
    ) -> Optional[RawBytes]:
        if http_status == 200 and payload:
            sha = self._compute_sha256(payload)
            size = len(payload)
            path = self._store_in_cache(sha, payload)
            blob_bytes: Optional[bytes] = None

            return RawBytes(
                exchange=self._cfg.exchange,
                market=market,
                symbol=symbol,
                interval=interval,
                granularity=granularity,
                period=period,
                url=url,
                http_status=http_status,
                sha256=sha,
                size=size,
                path=path,
                bytes=blob_bytes,
                run_id=self._run_id,
            )
        return None

    def _store_in_cache(self, sha256_hex: str, payload: bytes) -> Path:
        """
        Content-addressed cache: <base_dir>/_cache/archives/{sha[:2]}/{sha[2:4]}/{sha}.zip
        Atomic write to avoid partial files.
        """
        shard = Path(sha256_hex[:2]) / sha256_hex[2:4]
        out_dir = self._cache_root / shard
        out_dir.mkdir(parents=True, exist_ok=True)
        out_fp = out_dir / f"{sha256_hex}.zip"
        if out_fp.exists():
            return out_fp
        tmp_fp = out_fp.with_suffix(".tmp")
        with open(tmp_fp, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        tmp_fp.replace(out_fp)
        return out_fp

    def _compute_sha256(self, blob: bytes | bytearray | memoryview | BinaryIO) -> str:
        h = hashlib.sha256()

        if isinstance(blob, (bytes, bytearray, memoryview)):
            if isinstance(blob, memoryview):
                h.update(blob.tobytes())
            else:
                h.update(blob)
            return h.hexdigest()
        # Treat remaining case as BinaryIO
        stream = blob
        pos = stream.tell() if hasattr(stream, "tell") else None
        for chunck in iter(lambda: stream.read(8192), b""):
            h.update(chunck)
        if pos is not None and hasattr(stream, "seek"):
            stream.seek(pos)
        return h.hexdigest()


# ------------------------- Parsing&Sanitizing -------------------------


class BaseArchiveParser:
    def __init__(self, spec: SchemaSpec, *, nonneg_epsilon: float) -> None:
        self.spec = spec
        self._nonneg_epsilon = nonneg_epsilon

    def parse(
        self, raw: RawBytes, members: Sequence[tuple[str, bytes]]
    ) -> tuple[pl.DataFrame, list[str]]:
        raise NotImplementedError


class CandleParser(BaseArchiveParser):
    def parse(
        self, raw: RawBytes, members: Sequence[tuple[str, bytes]]
    ) -> tuple[pl.DataFrame, list[str]]:
        if raw.interval is None:
            raise ValueError("Candle archives require an interval")

        frames: list[pl.DataFrame] = []
        for _, payload in members:
            frames.append(self._parse_csv(payload, raw.symbol, raw.interval))

        if not frames:
            raise EmptyArchiveError

        df = pl.concat(frames, how="vertical_relaxed")
        df = self._normalize_epoch(df)
        df = self._zeroize_tiny_negatives(df, ["open", "high", "low", "close", "volume"])
        df = self._validate_ranges(df)
        issues: list[str] = []
        df, dedup_issue = self._dedup_and_sort(df)
        if dedup_issue:
            issues.append(dedup_issue)
        df = self._attach_day(df)
        return df, issues

    def _parse_csv(self, payload: bytes, symbol: str, interval: str) -> pl.DataFrame:
        df = pl.read_csv(
            io.BytesIO(payload),
            has_header=False,
            infer_schema_length=1000,
            new_columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "qav",
                "trades",
                "tbb",
                "tbq",
                "ignore",
            ],
        )
        df = self._filter_numeric_rows(df)
        return df.select(
            [
                pl.lit(symbol).alias("symbol"),
                pl.lit(interval).alias("timeframe"),
                pl.col("open_time").cast(pl.Int64).alias("start_ms"),
                pl.col("close_time").cast(pl.Int64).alias("end_ms"),
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume").cast(pl.Float64),
                pl.col("trades").cast(pl.Int64),
                pl.lit(True).alias("is_final"),
            ]
        )

    def _filter_numeric_rows(self, df: pl.DataFrame) -> pl.DataFrame:
        numeric_cols = {
            "open_time": pl.Int64,
            "close_time": pl.Int64,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
            "trades": pl.Int64,
        }
        checks = [
            pl.col(col).cast(dtype, strict=False).is_not_null()
            for col, dtype in numeric_cols.items()
            if col in df.columns
        ]
        if not checks:
            return df
        return df.filter(pl.all_horizontal(*checks))

    def _normalize_epoch(self, df: pl.DataFrame) -> pl.DataFrame:
        emax = df.select(pl.col("end_ms").max().alias("emax")).item()
        if emax > 10_000_000_000_000:
            return df.with_columns(
                (pl.col("start_ms") // 1000).cast(pl.Int64).alias("start_ms"),
                (pl.col("end_ms") // 1000).cast(pl.Int64).alias("end_ms"),
            )
        if emax < 10_000_000_000:
            return df.with_columns(
                (pl.col("start_ms") * 1000).cast(pl.Int64).alias("start_ms"),
                (pl.col("end_ms") * 1000).cast(pl.Int64).alias("end_ms"),
            )
        return df

    def _zeroize_tiny_negatives(self, df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
        exprs: list[pl.Expr] = []
        for c in cols:
            if c not in df.columns:
                continue
            exprs.append(
                pl.when((pl.col(c) < 0) & (pl.col(c) > -self._nonneg_epsilon))
                .then(pl.lit(0.0))
                .otherwise(pl.col(c))
                .alias(c)
            )
        return df.with_columns(exprs) if exprs else df

    def _validate_ranges(self, df: pl.DataFrame) -> pl.DataFrame:
        checks = df.select(
            pl.any_horizontal(
                pl.col("open") < 0,
                pl.col("high") < 0,
                pl.col("low") < 0,
                pl.col("close") < 0,
                pl.col("volume") < 0,
            )
            .any()
            .alias("has_negative"),
            (
                (pl.col("low") > pl.min_horizontal("open", "close", "high"))
                | (pl.col("high") < pl.max_horizontal("open", "close", "low"))
            )
            .any()
            .alias("hl_invalid"),
            (pl.col("start_ms") > pl.col("end_ms")).any().alias("start_gt_end"),
            ((pl.col("end_ms") < 1230768000000) | (pl.col("end_ms") >= 4102444800000))
            .any()
            .alias("timestamp_invalid"),
        )

        if checks["has_negative"][0]:
            raise ValueError("Negative OHLCV detected.")
        if checks["hl_invalid"][0]:
            raise ValueError("H/L outside O/C range.")
        if checks["start_gt_end"][0]:
            raise ValueError("start_ms exceeds end_ms.")
        if checks["timestamp_invalid"][0]:
            mins = df.select(pl.col("end_ms").min().alias("min_end_ms")).to_dict(as_series=False)
            maxs = df.select(pl.col("end_ms").max().alias("max_end_ms")).to_dict(as_series=False)
            raise ValueError(f"end_ms out of range; min/max={mins}|{maxs}")
        return df

    def _dedup_and_sort(self, df: pl.DataFrame) -> tuple[pl.DataFrame, str | None]:
        before = df.height
        df = df.unique(subset=list(self.spec.dedup_keys), keep="last")
        after = df.height
        issue = None
        if after < before:
            issue = f"deduped_{before - after}_rows"
        # Ensure deterministic order (symbol, tf, end_ms)
        df = df.sort(list(self.spec.sort_columns))
        return df, issue

    def _attach_day(self, df: pl.DataFrame) -> pl.DataFrame:
        if "day" in df.columns:
            return df.with_columns(pl.col("day").cast(pl.Date))
        return df.with_columns(
            pl.from_epoch(pl.col("end_ms"), time_unit="ms").dt.date().alias("day")
        )


class OptionParser(BaseArchiveParser):
    def parse(
        self, raw: RawBytes, members: Sequence[tuple[str, bytes]]
    ) -> tuple[pl.DataFrame, list[str]]:
        frames: list[pl.DataFrame] = []
        for _, payload in members:
            frames.append(self._parse_csv(payload))
        if not frames:
            raise EmptyArchiveError
        df = pl.concat(frames, how="vertical_relaxed")
        df = self._prepare_numeric(df)
        df = df.with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False))
        df = self._zeroize_tiny_negatives(
            df,
            [
                "open",
                "high",
                "low",
                "close",
                "volume_contracts",
                "volume_usdt",
                "best_bid_price",
                "best_ask_price",
                "best_bid_qty",
                "best_ask_qty",
                "best_buy_iv",
                "best_sell_iv",
                "mark_price",
                "mark_iv",
                "gamma",
                "vega",
                "theta",
                "openinterest_contracts",
                "openinterest_usdt",
            ],
        )
        df = self._validate_ranges(df)
        df = df.with_columns(pl.col("date").alias("day"))
        issues: list[str] = []
        df, dedup_issue = self._dedup_and_sort(df)
        if dedup_issue:
            issues.append(dedup_issue)
        self._validate_hours(df)
        return df, issues

    def _parse_csv(self, payload: bytes) -> pl.DataFrame:
        return pl.read_csv(
            io.BytesIO(payload),
            has_header=True,
            infer_schema_length=1000,
            new_columns=[
                "date",
                "hour",
                "symbol",
                "underlying",
                "type",
                "strike",
                "open",
                "high",
                "low",
                "close",
                "volume_contracts",
                "volume_usdt",
                "best_bid_price",
                "best_ask_price",
                "best_bid_qty",
                "best_ask_qty",
                "best_buy_iv",
                "best_sell_iv",
                "mark_price",
                "mark_iv",
                "delta",
                "gamma",
                "vega",
                "theta",
                "openinterest_contracts",
                "openinterest_usdt",
            ],
        )

    def _prepare_numeric(self, df: pl.DataFrame) -> pl.DataFrame:
        cleanse_exprs: list[pl.Expr] = []
        cast_exprs: list[pl.Expr] = []
        numeric_cols: dict[str, Any] = {
            "hour": pl.Int32,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume_contracts": pl.Float64,
            "volume_usdt": pl.Float64,
            "best_bid_price": pl.Float64,
            "best_ask_price": pl.Float64,
            "best_bid_qty": pl.Float64,
            "best_ask_qty": pl.Float64,
            "best_buy_iv": pl.Float64,
            "best_sell_iv": pl.Float64,
            "mark_price": pl.Float64,
            "mark_iv": pl.Float64,
            "delta": pl.Float64,
            "gamma": pl.Float64,
            "vega": pl.Float64,
            "theta": pl.Float64,
            "openinterest_contracts": pl.Float64,
            "openinterest_usdt": pl.Float64,
        }
        for col, dtype in numeric_cols.items():
            if col not in df.columns:
                continue
            col_utf8 = pl.col(col).cast(pl.Utf8, strict=False)
            is_blank = col_utf8.str.strip_chars().fill_null("") == ""
            cleanse_exprs.append(
                pl.when(is_blank).then(pl.lit(None)).otherwise(pl.col(col)).alias(col)
            )
            cast_exprs.append(pl.col(col).cast(dtype, strict=False).alias(col))
        if cleanse_exprs:
            df = df.with_columns(cleanse_exprs)
        if cast_exprs:
            df = df.with_columns(cast_exprs)
        return df

    def _zeroize_tiny_negatives(self, df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
        exprs: list[pl.Expr] = []
        for c in cols:
            if c not in df.columns:
                continue
            exprs.append(
                pl.when((pl.col(c) < 0) & (pl.col(c) > -self._nonneg_epsilon))
                .then(pl.lit(0.0))
                .otherwise(pl.col(c))
                .alias(c)
            )
        return df.with_columns(exprs) if exprs else df

    def _validate_ranges(self, df: pl.DataFrame) -> pl.DataFrame:
        checks = df.select(
            pl.any_horizontal(
                pl.col("open") < 0,
                pl.col("high") < 0,
                pl.col("low") < 0,
                pl.col("close") < 0,
                pl.col("volume_contracts") < 0,
                pl.col("volume_usdt") < 0,
                pl.col("best_bid_price") < 0,
                pl.col("best_ask_price") < 0,
                pl.col("best_bid_qty") < 0,
                pl.col("best_ask_qty") < 0,
                pl.col("best_buy_iv") < 0,
                pl.col("best_sell_iv") < 0,
                pl.col("mark_price") < 0,
                pl.col("mark_iv") < 0,
                pl.col("gamma") < 0,
                pl.col("vega") < 0,
                pl.col("openinterest_contracts") < 0,
                pl.col("openinterest_usdt") < 0,
            )
            .any()
            .alias("has_negative"),
            (
                (pl.col("low") > pl.min_horizontal("open", "close", "high"))
                | (pl.col("high") < pl.max_horizontal("open", "close", "low"))
            )
            .any()
            .alias("hl_invalid"),
        )
        if checks["has_negative"][0]:
            raise ValueError("Negative EOHSummary values detected.")
        if checks["hl_invalid"][0]:
            raise ValueError("Option high/low outside open/close range.")
        return df

    def _dedup_and_sort(self, df: pl.DataFrame) -> tuple[pl.DataFrame, str | None]:
        before = df.height
        df = df.unique(subset=list(self.spec.dedup_keys), keep="last")
        after = df.height
        issue = None
        if after < before:
            issue = f"deduped_{before - after}_rows"
        df = df.sort(list(self.spec.sort_columns))
        return df, issue

    def _validate_hours(self, df: pl.DataFrame) -> None:
        invalid = df.filter((pl.col("hour") < 0) | (pl.col("hour") > 23))
        if not invalid.is_empty():
            raise ValueError("Option hour outside [0, 23].")


class ParsingSanitizing:
    def __init__(self, audit: AuditWriter) -> None:
        self._audit = audit
        self._nonneg_epsilon = 1e-7
        candle_spec = SchemaSpec(
            name="binance-kline",
            schema="kline",
            required_columns=(
                "symbol",
                "timeframe",
                "start_ms",
                "end_ms",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "trades",
                "is_final",
            ),
            partition_column="day",
            start_field="start_ms",
            end_field="end_ms",
            dedup_keys=("symbol", "timeframe", "end_ms"),
            sort_columns=("symbol", "timeframe", "end_ms"),
        )
        option_spec = SchemaSpec(
            name="binance-eohsummary",
            schema="option",
            required_columns=(
                "symbol",
                "date",
                "hour",
                "open",
                "high",
                "low",
                "close",
            ),
            partition_column="day",
            start_field="date",
            end_field="date",
            dedup_keys=("symbol", "date", "hour"),
            sort_columns=("symbol", "date", "hour"),
        )
        self._parser_registry: dict[str, BaseArchiveParser] = {
            "spot": CandleParser(candle_spec, nonneg_epsilon=self._nonneg_epsilon),
            "futures": CandleParser(candle_spec, nonneg_epsilon=self._nonneg_epsilon),
            "option": OptionParser(option_spec, nonneg_epsilon=self._nonneg_epsilon),
        }
        self._max_zip_members = 64

    def parse_archives(
        self, blobs: Sequence[RawBytes]
    ) -> tuple[list[SanitizedBatch], ParsingReport]:
        batches: list[SanitizedBatch] = []
        attempts = ok = skipped = errors = 0

        for raw in blobs:
            attempts += 1
            if not raw.ok:
                skipped += 1
                self._emit(
                    "PARSE_SKIP_HTTP",
                    raw,
                    http_status=raw.http_status,
                    sha256=raw.sha256,
                    bytes=raw.size,
                )
                continue

            self._emit("PARSE_BEGIN", raw, sha256=raw.sha256, bytes=raw.size)
            try:
                batch = self._parse_one(raw)
            except EmptyArchiveError:
                skipped += 1
                self._emit("PARSE_EMPTY", raw)
            except Exception as exc:  # pragma: no cover - surfaced to caller
                errors += 1
                self._emit(
                    "PARSE_ERROR",
                    raw,
                    level="ERROR",
                    error=f"{type(exc).__name__}: {exc}",
                )
            else:
                ok += 1
                batches.append(batch)
                self._emit(
                    "PARSE_OK",
                    raw,
                    rows=batch.rows,
                    partitions=batch.partitions,
                    start=batch.start,
                    end=batch.end,
                )

        return batches, ParsingReport(attempts=attempts, ok=ok, skipped=skipped, errors=errors)

    def _parse_one(self, raw: RawBytes) -> SanitizedBatch:
        parser_key = self._market_family(raw.market)
        parser = self._parser_registry.get(parser_key)
        if parser is None:
            raise ValueError(f"No parser registered for market '{raw.market}'")

        members = self._collect_csv_members(raw)
        df, issues = parser.parse(raw, members)

        if df.is_empty():
            raise EmptyArchiveError

        spec = parser.spec
        partitions = int(df[spec.partition_column].n_unique())
        rows = int(df.height)
        start_raw = df[spec.start_field].min()
        end_raw = df[spec.end_field].max()
        start = self._coerce_scalar_bound(start_raw, spec.start_field)
        end = self._coerce_scalar_bound(end_raw, spec.end_field)

        return SanitizedBatch(
            raw=raw,
            schema=spec.schema,
            dataframe=df,
            rows=rows,
            partitions=partitions,
            start=start,
            end=end,
            issues=tuple(issues),
        )

    def _market_family(self, label: str) -> str:
        m = label.strip().lower()
        if "option" in m:
            return "option"
        if (
            "future" in m
            or m.startswith("futures")
            or m in {"um", "cm", "futures/um", "futures/cm"}
        ):
            return "futures"
        return "spot"

    def _collect_csv_members(self, raw: RawBytes) -> list[tuple[str, bytes]]:
        members: list[tuple[str, bytes]] = []
        with raw.open() as blob:
            with zipfile.ZipFile(blob) as archive:
                csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
                if not csv_names:
                    raise EmptyArchiveError
                if len(csv_names) > self._max_zip_members:
                    msg = f"Archive has {len(csv_names)}"
                    msg_2 = f"CSV files, exceeds limit {self._max_zip_members}."
                    raise ValueError(msg + msg_2)
                for name in csv_names:
                    with archive.open(name) as member:
                        payload = member.read()
                    members.append((name, payload))
                    self._emit(
                        "PARSE_MEMBER",
                        raw,
                        name=name,
                        bytes=len(payload),
                        members=len(members),
                    )
        return members

    def _emit(self, event: str, raw: RawBytes, *, level: str = "INFO", **payload: Any) -> None:
        if self._audit is None:
            return
        self._audit.emit(
            component="parsing",
            event=event,
            level=level,
            simple=True,
            payload={
                "run_id": raw.run_id,
                "symbol": raw.symbol,
                "market": raw.market,
                "interval": raw.interval,
                "granularity": raw.granularity,
                "period": raw.period,
                "url": raw.url,
                **payload,
            },
            symbol=raw.symbol,
        )

    def _coerce_scalar_bound(self, value: Any, field: str) -> dt.datetime | dt.date | int:
        """
        Normalize aggregation results (min/max) into the SanitizedBatch types:
        - For kline epoch fields: return int (ms)
        - For date fields: return dt.date or dt.datetime as-is
        - Reject None and unsupported types early.
        """
        # Polars sometimes returns Series wrappers
        if isinstance(value, pl.Series):
            value = value.item()

        if value is None:
            raise ValueError(f"{field} aggregation returned None")

        if isinstance(value, (dt.datetime, dt.date, int)):
            return value

        if isinstance(value, (float, Decimal)):
            # convert numeric scalar to int milliseconds
            return int(value)

        # numpy scalar types
        try:
            import numpy as _np  # local import to avoid hard dependency in type stubs

            if isinstance(value, _np.generic):
                return int(value)  # cast numpy scalar -> int
        except Exception:
            pass

        # Fallback: try int() conversion (may raise)
        try:
            return int(value)
        except Exception:
            raise TypeError(f"Unsupported scalar type for {field}: {type(value).__name__}")


# ------------------------- WritingParquet -----------------------------


class WritingParquet:
    def __init__(
        self,
        cfg: DownloaderConfig,
        audit: AuditWriter | None = None,
    ) -> None:
        self._cfg = cfg
        self._base_dir = Path(cfg.base_dir)
        self._run_id = cfg.download_run_id
        self._audit = audit
        self._option_file_tag = "EOHSummary"
        self._candle_unique_keys = ["symbol", "timeframe", "end_ms"]
        self._candle_sort_columns = ["symbol", "timeframe", "end_ms"]
        self._option_unique_keys = ["symbol", "date", "hour"]
        self._option_sort_columns = ["symbol", "date", "hour"]

    def write_batches(
        self, batches: Sequence[SanitizedBatch], *, strict: bool = False
    ) -> WriteReport:
        attempts = ok = skipped = errors = rows = partitions = 0
        manifest_rows: list[dict[str, Any]] = []

        for batch in batches:
            attempts += 1
            df = batch.dataframe
            if df.is_empty() or batch.rows == 0:
                skipped += 1
                self._emit("WRITE_SKIP_EMPTY", batch.raw, rows=batch.rows)
                continue

            try:
                manifest_row = self._write_batch(batch)
            except Exception as exc:
                errors += 1
                self._emit(
                    "WRITE_ERROR",
                    batch.raw,
                    level="ERROR",
                    error=f"{type(exc).__name__}: {exc}",
                )
                if strict:
                    raise
            else:
                ok += 1
                rows += manifest_row["rows"]
                partitions += manifest_row["partitions_written"]
                manifest_rows.append(manifest_row)

        if manifest_rows:
            self._upsert_manifest(manifest_rows)

        self._emit(
            "WRITE_SUMMARY",
            None,
            attempts=attempts,
            ok=ok,
            skipped=skipped,
            errors=errors,
            rows=rows,
            partitions=partitions,
        )
        return WriteReport(
            attempts=attempts,
            ok=ok,
            skipped=skipped,
            errors=errors,
            rows=rows,
            partitions=partitions,
        )

    def _write_batch(self, batch: SanitizedBatch) -> dict[str, Any]:
        raw = batch.raw
        df = self._ensure_partition_day(batch.dataframe)
        partitions_written = int(df["day"].n_unique())
        rows = int(df.height)
        # For option, no interval tag required
        interval_tag = self._interval_tag(batch)
        month_anchor = self._month_anchor(raw)

        self._emit(
            "WRITE_BEGIN",
            raw,
            schema=batch.schema,
            partitions=partitions_written,
            rows=rows,
            start=batch.start,
            end=batch.end,
        )
        for issue in batch.issues:
            self._emit("WRITE_ISSUE", raw, level="WARN", issue=issue)

        out_fp = self._output_file_path(raw, interval_tag, month_anchor)
        out_fp.parent.mkdir(parents=True, exist_ok=True)

        if batch.schema == "kline":
            self._write_candle_frame(out_fp, df)
        elif batch.schema == "option":
            self._write_option_frame(out_fp, df)
        else:
            raise ValueError(f"Unsupported schema '{batch.schema}'")

        self._emit(
            "WRITE_DONE",
            raw,
            rows=rows,
            partitions_written=partitions_written,
            path=str(out_fp),
        )

        return self._manifest_row(batch, partitions_written, rows)

    def _write_candle_frame(self, out_fp: Path, df: pl.DataFrame) -> None:
        needed = {
            "symbol",
            "timeframe",
            "start_ms",
            "end_ms",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trades",
            "is_final",
        }
        missing = needed - set(df.columns)
        if missing:
            raise ValueError(f"Candle batch missing columns: {sorted(missing)}")

        part = df.drop("day") if "day" in df.columns else df
        if out_fp.exists():
            existing = pl.read_parquet(str(out_fp))
            merged = pl.concat([existing, part], how="vertical_relaxed")
            payload = self._dedup_and_sort_candles(merged)
        else:
            payload = part

        self._atomic_write_parquet(payload, out_fp)

    def _write_option_frame(self, out_fp: Path, df: pl.DataFrame) -> None:
        needed = {"symbol", "date", "hour", "open", "high", "low", "close"}
        missing = needed - set(df.columns)
        if missing:
            raise ValueError(f"Option batch missing columns: {sorted(missing)}")

        norm = self._normalize_option_frame(df)
        part = norm.drop("day") if "day" in norm.columns else norm

        if out_fp.exists():
            existing = pl.read_parquet(str(out_fp))
            merged = pl.concat([existing, part], how="vertical_relaxed")
            payload = self._dedup_and_sort_options(merged)
        else:
            payload = part

        self._atomic_write_parquet(payload, out_fp)

    def _normalize_option_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        out = df
        if out.schema.get("date") != pl.Date:
            out = out.with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False))
        if "hour" not in out.columns:
            raise ValueError("Option batches require an 'hour' column")
        out = out.with_columns(pl.col("hour").cast(pl.Int32, strict=False))
        return out

    def _output_file_path_option(self, raw: RawBytes, anchor: dt.date) -> Path:
        out_dir = (
            self._base_dir
            / f"exchange={raw.exchange}"
            / f"market={raw.market}"
            / f"symbol={raw.symbol}"
            / "timeframe=EOHSummary"
            / f"year={anchor:04d}"
        )

        return Path(out_dir) / f"{raw.symbol}--{anchor.year}-{anchor.month}.parquet"

    def _output_file_path(self, raw: RawBytes, interval_tag: str, anchor: dt.date) -> Path:
        out_dir = target_path(
            root=self._base_dir,
            exchange=raw.exchange,
            market=raw.market,
            symbol=raw.symbol,
            data_type=self._cfg.data_type,
            interval=interval_tag,
            d=anchor,
        )
        return (
            Path(out_dir) / f"{raw.symbol}-{interval_tag}-{anchor.year}-{anchor.month:02d}.parquet"
        )

    def _interval_tag(self, batch: SanitizedBatch) -> str:
        if batch.schema == "option":
            return self._option_file_tag
        return batch.raw.interval or "unknown"

    def _month_anchor(self, raw: RawBytes) -> dt.date:
        if raw.granularity == "monthly":
            stamp = dt.datetime.strptime(raw.period, "%Y-%m")
        else:
            stamp = dt.datetime.strptime(raw.period, "%Y-%m-%d")
        return dt.date(stamp.year, stamp.month, 1)

    def _manifest_row(
        self, batch: SanitizedBatch, partitions_written: int, rows: int
    ) -> dict[str, Any]:
        raw = batch.raw
        sha = self._require_sha(raw)
        interval = self._manifest_interval(batch)
        now = dt.datetime.now(dt.timezone.utc)
        return {
            "exchange": raw.exchange,
            "market": raw.market,
            "symbol": raw.symbol,
            "interval": interval,
            "granularity": raw.granularity,
            "period": raw.period,
            "url": raw.url,
            "sha256": sha,
            "bytes": raw.size,
            "rows": rows,
            "start": self._coerce_scalar(batch.start),
            "end": self._coerce_scalar(batch.end),
            "unique_days": partitions_written,
            "partitions_written": partitions_written,
            "write_ts_utc": now,
            "run_id": self._run_id,
        }

    def _manifest_interval(self, batch: SanitizedBatch) -> str:
        if batch.schema == "option":
            return self._option_file_tag
        return batch.raw.interval or "unknown"

    def _dedup_and_sort_candles(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.unique(subset=self._candle_unique_keys, keep="last").sort(
            self._candle_sort_columns
        )

    def _dedup_and_sort_options(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.unique(subset=self._option_unique_keys, keep="last").sort(
            self._option_sort_columns
        )

    def _ensure_partition_day(self, df: pl.DataFrame) -> pl.DataFrame:
        if "day" in df.columns:
            return df.with_columns(pl.col("day").cast(pl.Date))
        if "end_ms" in df.columns:
            return df.with_columns(
                pl.from_epoch(pl.col("end_ms"), time_unit="ms").dt.date().alias("day")
            )
        if "date" in df.columns:
            if df.schema.get("date") == pl.Date:
                return df.with_columns(pl.col("date").alias("day"))
            return df.with_columns(
                pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("day")
            )
        if "open_time" in df.columns:
            return df.with_columns(
                pl.from_epoch(pl.col("open_time"), time_unit="ms").dt.date().alias("day")
            )
        raise ValueError("Cannot derive 'day' column for partitioning.")

    def _atomic_write_parquet(self, df: pl.DataFrame, out_fp: Path) -> None:
        tmp_fp = out_fp.with_suffix(out_fp.suffix + ".tmp")
        try:
            df.write_parquet(str(tmp_fp), compression="zstd")

            # Ensure temp file is flushed to disk before rename
            with open(tmp_fp, "rb+") as f:
                f.flush()
                os.fsync(f.fileno())

            # Atomic replace
            tmp_fp.replace(out_fp)

            # Ensure directory entry is durable (best-effort)
            try:
                dir_fd = os.open(out_fp.parent, os.O_DIRECTORY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except Exception:
                pass
        except Exception:
            # Best-effort cleanup of leftover temp file
            try:
                if tmp_fp.exists():
                    tmp_fp.unlink()
            except Exception:
                pass
            raise

    def _upsert_manifest(self, rows: Sequence[dict[str, Any]]) -> None:
        """
        Document download:
        Fields: exchange, market, symbol, interval, granularity,
                period, url, sha256, bytes, rows, start, end, unique_days,
                partitions_written, write_ts_utc, run_id

        Incoming rows are uniqued by key_cols = ["exchange","market","symbol",
            "interval","granularity","period","sha256"].

        If manifest already exists, it's read; some legacy column renames are normalized
        (start_ms → start, end_ms → end) and start/end cast to Int64 as needed.

        Note: start/end may be ints, dates, or datetimes depending on schema and
        are stored as-is (with some coercion).
        """
        if not rows:
            return
        meta_dir = self._base_dir / "_meta"
        meta_dir.mkdir(parents=True, exist_ok=True)
        manifest_fp = meta_dir / "manifest.parquet"

        # Prepare incoming data
        incoming = pl.DataFrame(rows)
        key_cols = ["exchange", "market", "symbol", "interval", "granularity", "period", "sha256"]

        # Deduplicate incoming data, keeping last
        incoming = incoming.unique(subset=key_cols, keep="last")

        if manifest_fp.exists():
            existing = pl.read_parquet(str(manifest_fp))

            # Normalize legacy column names
            rename_map = {}
            if "start_ms" in existing.columns and "start" not in existing.columns:
                rename_map["start_ms"] = "start"
            if "end_ms" in existing.columns and "end" not in existing.columns:
                rename_map["end_ms"] = "end"
            if rename_map:
                existing = existing.rename(rename_map)

            # Ensure start/end are compatible types
            if "start" in existing.columns:
                existing = existing.with_columns(
                    pl.col("start").cast(pl.Int64, strict=False).alias("start")
                )
            if "end" in existing.columns:
                existing = existing.with_columns(
                    pl.col("end").cast(pl.Int64, strict=False).alias("end")
                )

            # FIX: Keep rows from existing that are NOT being updated
            survivors = existing.join(incoming, on=key_cols, how="anti")

            # Merge: survivors (unchanged) + incoming (new/updated)
            merged = pl.concat([survivors, incoming], how="vertical_relaxed")
        else:
            merged = incoming

        # Sort for deterministic output
        merged = merged.sort(["exchange", "market", "symbol", "interval", "granularity", "period"])
        merged.write_parquet(str(manifest_fp), compression="zstd")

    def _coerce_scalar(self, value: Any) -> Any:
        if isinstance(value, pl.Series):
            return value.item()
        return value

    def _require_sha(self, raw: RawBytes) -> str:
        if raw.sha256:
            return raw.sha256
        if raw.bytes is not None:
            return hashlib.sha256(raw.bytes).hexdigest()
        if raw.path is not None and raw.path.exists():
            h = hashlib.sha256()
            with raw.path.open("rb") as src:
                for chunk in iter(lambda: src.read(8192), b""):
                    if not chunk:
                        break
                    h.update(chunk)
            return h.hexdigest()
        raise ValueError("RawBytes missing sha256 and payload for manifest.")

    def _emit(
        self,
        event: str,
        raw: RawBytes | None,
        *,
        level: str = "INFO",
        **payload: Any,
    ) -> None:
        if self._audit is None:
            return
        base_payload: dict[str, Any] = {"run_id": self._run_id, **payload}
        symbol = None
        if raw is not None:
            symbol = raw.symbol
            base_payload.update(
                {
                    "symbol": raw.symbol,
                    "market": raw.market,
                    "interval": raw.interval,
                    "granularity": raw.granularity,
                    "period": raw.period,
                    "url": raw.url,
                }
            )
        self._audit.emit(
            component="writer",
            event=event,
            level=level,
            simple=True,
            payload=base_payload,
            symbol=symbol,
        )
