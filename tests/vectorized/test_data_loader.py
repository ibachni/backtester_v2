import datetime as dt
from pathlib import Path

import polars as pl
import pytest

from backtester.utils.utility import target_path
from backtester.vectorized.contracts import DataSpec, DateRange
from backtester.vectorized.data_loader import LoadStats, ParquetLoader


def _ms(ts: dt.datetime) -> int:
    return int(ts.timestamp() * 1000)


def _write_parquet(path: Path, df: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def test_date_range_epoch_ms_and_validation() -> None:
    naive_start = dt.datetime(2024, 1, 1, 0, 0, 0)
    naive_end = dt.datetime(2024, 1, 2, 0, 0, 0)
    dr = DateRange(start=naive_start, end=naive_end)
    utc_start = naive_start.replace(tzinfo=dt.timezone.utc)
    utc_end = naive_end.replace(tzinfo=dt.timezone.utc)
    assert dr.to_epoch_ms() == (_ms(utc_start), _ms(utc_end))

    with pytest.raises(ValueError):
        DateRange(start=naive_end, end=naive_start)


def test_data_spec_normalization_and_with_symbols(tmp_path: Path) -> None:
    dr = DateRange(
        start=dt.datetime(2024, 1, 1, 0, 0, tzinfo=dt.timezone.utc),
        end=dt.datetime(2024, 1, 2, 0, 0, tzinfo=dt.timezone.utc),
    )
    spec = DataSpec(
        base_dir=tmp_path,
        exchange="binance",
        market="spot",
        symbols=("BTCUSDT",),
        timeframe="1m",
        date_range=dr,
        columns=("open", "close"),
    )
    assert isinstance(spec.base_dir, Path)
    assert spec.symbols == ("BTCUSDT",)
    assert spec.columns == ("open", "close")

    updated = spec.with_symbols(["ETHUSDT", "BTCUSDT"])
    assert updated.symbols == ("ETHUSDT", "BTCUSDT")

    with pytest.raises(ValueError):
        DataSpec(
            base_dir=tmp_path,
            exchange="binance",
            market="spot",
            symbols=[""],  # type: ignore
            timeframe="1m",
            date_range=dr,
        )

    with pytest.raises(ValueError):
        DataSpec(
            base_dir=tmp_path,
            exchange="binance",
            market="spot",
            symbols=("BTCUSDT",),
            timeframe="",
            date_range=dr,
        )


def test_parquet_loader_load_dataset_filters_and_casts(tmp_path: Path) -> None:
    tz = dt.timezone.utc
    start = dt.datetime(2024, 1, 15, 0, 0, tzinfo=tz)
    end = dt.datetime(2024, 2, 15, 0, 0, tzinfo=tz)

    base_dir = tmp_path
    year_dir = target_path(
        root=base_dir,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        data_type="candle",
        interval="1m",
        d=dt.date(2024, 1, 1),
    )

    jan_rows = pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT", "ETHUSDT", "BTCUSDT"],
            "timeframe": ["1m", "1m", "1m", "1m", "5m"],
            "start_ms": [
                _ms(dt.datetime(2024, 1, 14, 23, 58, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 14, 23, 59, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 15, 0, 0, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 15, 0, 2, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 15, 0, 5, tzinfo=tz)),
            ],
            "end_ms": [
                _ms(dt.datetime(2024, 1, 14, 23, 59, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 15, 0, 0, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 15, 0, 1, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 15, 0, 3, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 15, 0, 10, tzinfo=tz)),
            ],
            "open": [0.9, 1.0, 1.1, 2.0, 3.0],
            "high": [1.0, 1.1, 1.2, 2.1, 3.2],
            "low": [0.8, 0.9, 1.0, 1.8, 2.9],
            "close": [0.95, 1.05, 1.15, 2.05, 3.1],
            "volume": [5.0, 10.0, 8.0, 7.0, 9.0],
            "trades": [1, 2, 3, 4, 5],
            "is_final": [True, True, False, True, True],
        }
    )
    feb_rows = pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "timeframe": ["1m", "1m"],
            "start_ms": [
                _ms(dt.datetime(2024, 2, 14, 23, 58, tzinfo=tz)),
                _ms(dt.datetime(2024, 2, 14, 23, 59, tzinfo=tz)),
            ],
            "end_ms": [
                _ms(dt.datetime(2024, 2, 14, 23, 59, tzinfo=tz)),
                _ms(dt.datetime(2024, 2, 15, 0, 0, tzinfo=tz)),
            ],
            "open": [2.0, 2.1],
            "high": [2.2, 2.3],
            "low": [1.9, 2.0],
            "close": [2.05, 2.15],
            "volume": [11.0, 12.0],
            "trades": [6, 7],
            "is_final": [True, True],
        }
    )

    _write_parquet(year_dir / "BTCUSDT-1m-2024-1.parquet", jan_rows)
    _write_parquet(year_dir / "BTCUSDT-1m-2024-02.parquet", feb_rows)

    spec = DataSpec(
        base_dir=base_dir,
        exchange="binance",
        market="spot",
        symbols=("BTCUSDT",),
        timeframe="1m",
        date_range=DateRange(start=start, end=end),
        columns=("open", "close"),
    )

    loader = ParquetLoader()
    lf = loader.load_dataset(spec)
    assert isinstance(lf, pl.LazyFrame)
    df = lf.collect()

    expected_end_ms = [
        _ms(dt.datetime(2024, 1, 15, 0, 0, tzinfo=tz)),
        _ms(dt.datetime(2024, 2, 14, 23, 59, tzinfo=tz)),
    ]

    assert df.height == 2
    assert set(df.columns) == {
        "symbol",
        "timeframe",
        "start_ms",
        "end_ms",
        "open",
        "close",
    }
    assert df["end_ms"].to_list() == expected_end_ms
    assert df.schema["open"] == pl.Float32
    assert df.schema["close"] == pl.Float32
    assert df.schema["start_ms"] == pl.Int64
    assert df.schema["end_ms"] == pl.Int64


def test_parquet_loader_resample_data_ohlcv() -> None:
    tz = dt.timezone.utc
    rows = pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
            "timeframe": ["1m", "1m", "1m"],
            "start_ms": [
                _ms(dt.datetime(2024, 1, 1, 0, 0, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 1, 0, 1, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 1, 0, 2, tzinfo=tz)),
            ],
            "end_ms": [
                _ms(dt.datetime(2024, 1, 1, 0, 1, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 1, 0, 2, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 1, 0, 3, tzinfo=tz)),
            ],
            "open": [1.0, 1.5, 2.0],
            "high": [2.0, 2.5, 2.2],
            "low": [0.5, 1.0, 1.8],
            "close": [1.5, 2.0, 1.9],
            "volume": [10.0, 12.0, 8.0],
            "trades": [1, 2, 3],
            "is_final": [True, True, True],
        }
    )

    loader = ParquetLoader()
    out = loader.resample_data(rows.lazy(), "3m").collect()

    assert out.height == 2
    assert out["timeframe"].to_list() == ["3m", "3m"]
    assert out["end_ms"].to_list() == [
        _ms(dt.datetime(2024, 1, 1, 0, 3, tzinfo=tz)),
        _ms(dt.datetime(2024, 1, 1, 0, 6, tzinfo=tz)),
    ]

    # Use pytest.approx for Float32 comparisons (Numba compatibility casting)
    first = out.row(0, named=True)
    assert first["open"] == pytest.approx(1.0)
    assert first["high"] == pytest.approx(2.5)
    assert first["low"] == pytest.approx(0.5)
    assert first["close"] == pytest.approx(2.0)
    assert first["volume"] == pytest.approx(22.0)

    second = out.row(1, named=True)
    assert second["open"] == pytest.approx(2.0)
    assert second["high"] == pytest.approx(2.2)
    assert second["low"] == pytest.approx(1.8)
    assert second["close"] == pytest.approx(1.9)
    assert second["volume"] == pytest.approx(8.0)

    # Verify Float32 dtype preservation
    assert out.schema["open"] == pl.Float32
    assert out.schema["volume"] == pl.Float32


def test_parquet_loader_debug_mode_returns_stats(tmp_path: Path) -> None:
    """Test that debug=True returns LoadStats with useful metadata."""
    tz = dt.timezone.utc
    start = dt.datetime(2024, 1, 15, 0, 0, tzinfo=tz)
    end = dt.datetime(2024, 1, 16, 0, 0, tzinfo=tz)

    base_dir = tmp_path
    year_dir = target_path(
        root=base_dir,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        data_type="candle",
        interval="1m",
        d=dt.date(2024, 1, 1),
    )

    rows = pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "timeframe": ["1m", "1m"],
            "start_ms": [
                _ms(dt.datetime(2024, 1, 15, 0, 0, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 15, 0, 1, tzinfo=tz)),
            ],
            "end_ms": [
                _ms(dt.datetime(2024, 1, 15, 0, 1, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 15, 0, 2, tzinfo=tz)),
            ],
            "open": [1.0, 1.1],
            "high": [1.2, 1.3],
            "low": [0.9, 1.0],
            "close": [1.1, 1.2],
            "volume": [10.0, 12.0],
            "trades": [1, 2],
            "is_final": [True, True],
        }
    )
    _write_parquet(year_dir / "BTCUSDT-1m-2024-01.parquet", rows)

    spec = DataSpec(
        base_dir=base_dir,
        exchange="binance",
        market="spot",
        symbols=("BTCUSDT",),
        timeframe="1m",
        date_range=DateRange(start=start, end=end),
    )

    loader = ParquetLoader()

    # Non-debug mode returns just LazyFrame
    result = loader.load_dataset(spec, debug=False)
    assert isinstance(result, pl.LazyFrame)

    # Debug mode returns tuple
    result_debug = loader.load_dataset(spec, debug=True)
    assert isinstance(result_debug, tuple)
    lf, stats = result_debug
    assert isinstance(lf, pl.LazyFrame)
    assert isinstance(stats, LoadStats)

    # Verify stats content
    assert stats.symbols == ("BTCUSDT",)
    assert stats.timeframe == "1m"
    assert stats.paths_found == 1
    assert len(stats.schema_hash) == 8
    assert "time:" in stats.filters_applied[0]
    assert stats.float32_cols  # Should have some float columns


def test_parquet_loader_empty_result_logs_warning(tmp_path: Path, caplog) -> None:
    """Test that loading nonexistent data logs a warning."""
    tz = dt.timezone.utc
    spec = DataSpec(
        base_dir=tmp_path,
        exchange="binance",
        market="spot",
        symbols=("NONEXISTENT",),
        timeframe="1m",
        date_range=DateRange(
            start=dt.datetime(2024, 1, 1, tzinfo=tz),
            end=dt.datetime(2024, 1, 2, tzinfo=tz),
        ),
    )

    loader = ParquetLoader()

    import logging

    with caplog.at_level(logging.WARNING):
        result = loader.load_dataset(spec)

    assert isinstance(result, pl.LazyFrame)
    assert result.collect().height == 0
    assert "No parquet files found" in caplog.text


def test_slice_by_time() -> None:
    """Test time slicing for WFA train/test splits."""
    tz = dt.timezone.utc
    rows = pl.DataFrame(
        {
            "end_ms": [
                _ms(dt.datetime(2024, 1, 1, 0, 0, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 1, 1, 0, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 1, 2, 0, tzinfo=tz)),
                _ms(dt.datetime(2024, 1, 1, 3, 0, tzinfo=tz)),
            ],
            "close": [1.0, 2.0, 3.0, 4.0],
        }
    )

    loader = ParquetLoader()
    start_ms = _ms(dt.datetime(2024, 1, 1, 1, 0, tzinfo=tz))
    end_ms = _ms(dt.datetime(2024, 1, 1, 3, 0, tzinfo=tz))

    sliced = loader.slice_by_time(rows.lazy(), start_ms, end_ms).collect()

    assert sliced.height == 2
    assert sliced["close"].to_list() == [2.0, 3.0]


def test_with_warmup_buffer() -> None:
    """Test warmup buffer for indicator initialization in WFA."""
    tz = dt.timezone.utc
    # Create 10 bars
    rows = pl.DataFrame(
        {
            "end_ms": [_ms(dt.datetime(2024, 1, 1, i, 0, tzinfo=tz)) for i in range(10)],
            "close": [float(i) for i in range(10)],
        }
    )

    loader = ParquetLoader()

    # Target period: bars 5-8, warmup: 3 bars before
    start_ms = _ms(dt.datetime(2024, 1, 1, 5, 0, tzinfo=tz))
    end_ms = _ms(dt.datetime(2024, 1, 1, 8, 0, tzinfo=tz))

    result_lf, actual_start = loader.with_warmup_buffer(
        rows.lazy(), start_ms, end_ms, warmup_bars=3
    )
    result = result_lf.collect()

    # Should have 3 warmup bars (2,3,4) + 3 target bars (5,6,7) = 6 bars
    assert result.height == 6
    assert result["close"].to_list() == [2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
    assert actual_start == start_ms
