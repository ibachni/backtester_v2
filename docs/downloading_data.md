The following script demonstrates how to download Spot, Futures, and Options data from Binance, parse it, and save it to a local Parquet dataset.

```python
from pathlib import Path
import datetime as dt
from backtester.config.configs import DownloaderConfig, RunContext
from backtester.data.downloader import DownloaderFiles, ParsingSanitizing, WritingParquet

# Configure your data directory
BASE_DIR = Path("/Users/name/data/crypto_data/parquet")
LOG_DIR = Path("runs")

def run_e2e(symbol: str, market: str, interval: str | None, start: dt.datetime, end: dt.datetime):
    # 1) Config and services
    run_ctx = RunContext(run_id=f"DL-{market}-{symbol}", seed=42, git_sha="HEAD", allow_net=True)
    cfg = DownloaderConfig(base_dir=BASE_DIR, log_dir=LOG_DIR, download_run_id=run_ctx.run_id)
    dl = DownloaderFiles(run_ctx=run_ctx, cfg=cfg)

    # 2) Download → RawBytes[]
    # For options, interval is ignored by the dataset; pass any string (e.g., "1h") or None.
    tf = interval or "1m"
    blobs = dl.download_files(symbol, market, tf, start, end)
    print(f"Downloaded: {len(blobs)} blobs")

    # 3) Parse & sanitize → [SanitizedBatch]
    ps = ParsingSanitizing(audit=dl._audit)
    batches, parse_report = ps.parse_archives(blobs)
    print("Parse report:", parse_report)

    # 4) Write Parquet (+ manifest)
    writer = WritingParquet(cfg=cfg, audit=dl._audit)
    write_report = writer.write_batches(batches, strict=False)
    print("Write report:", write_report)

# ---- Usage Examples ----

# 1. Spot (ETHUSDC)
run_e2e(
    symbol="ETHUSDC",
    market="spot",
    interval="1m",
    start=dt.datetime(2024, 9, 1, tzinfo=dt.timezone.utc),
    end=dt.datetime(2024, 10, 1, tzinfo=dt.timezone.utc),
)

# 2. Futures (BTCUSDT)
run_e2e(
    symbol="BTCUSDT",
    market="futures",
    interval="1m",
    start=dt.datetime(2024, 9, 1, tzinfo=dt.timezone.utc),
    end=dt.datetime(2024, 10, 1, tzinfo=dt.timezone.utc),
)

# 3. Options (ETHUSDT)
run_e2e(
    symbol="ETHUSDT",
    market="option",
    interval=None,
    start=dt.datetime(2023, 8, 1, tzinfo=dt.timezone.utc),
    end=dt.datetime(2023, 8, 5, tzinfo=dt.timezone.utc),
)
```

Data will be saved to `BASE_DIR` in a partitioned Parquet format (e.g., `year=YYYY/symbol-tf-YYYY-MM.parquet`).
