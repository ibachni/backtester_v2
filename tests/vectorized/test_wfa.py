"""Tests for the Walk-Forward Analysis (WFA) module."""

import datetime as dt

import polars as pl
import pytest

from backtester.vectorized.engine import CostModel
from backtester.vectorized.wfa import (
    TimeSlicer,
    TimeWindow,
    WalkForwardValidator,
    WFAResult,
    WFAWindowResult,
)

# =============================================================================
# TimeWindow Tests
# =============================================================================


class TestTimeWindow:
    def test_valid_window(self) -> None:
        window = TimeWindow(
            train_start=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
            train_end=dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc),
            test_start=dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc),
            test_end=dt.datetime(2023, 7, 1, tzinfo=dt.timezone.utc),
            warmup_start=dt.datetime(2023, 5, 15, tzinfo=dt.timezone.utc),
            window_id=0,
        )
        assert window.train_days == 151
        assert window.test_days == 30
        assert window.warmup_days == 17

    def test_train_end_before_start_raises(self) -> None:
        with pytest.raises(ValueError, match="train_end must be after train_start"):
            TimeWindow(
                train_start=dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc),
                train_end=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
                test_start=dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc),
                test_end=dt.datetime(2023, 7, 1, tzinfo=dt.timezone.utc),
                warmup_start=dt.datetime(2023, 5, 15, tzinfo=dt.timezone.utc),
                window_id=0,
            )

    def test_test_end_before_start_raises(self) -> None:
        with pytest.raises(ValueError, match="test_end must be after test_start"):
            TimeWindow(
                train_start=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
                train_end=dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc),
                test_start=dt.datetime(2023, 7, 1, tzinfo=dt.timezone.utc),
                test_end=dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc),
                warmup_start=dt.datetime(2023, 5, 15, tzinfo=dt.timezone.utc),
                window_id=0,
            )

    def test_warmup_after_test_start_raises(self) -> None:
        with pytest.raises(ValueError, match="warmup_start cannot be after test_start"):
            TimeWindow(
                train_start=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
                train_end=dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc),
                test_start=dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc),
                test_end=dt.datetime(2023, 7, 1, tzinfo=dt.timezone.utc),
                warmup_start=dt.datetime(2023, 6, 15, tzinfo=dt.timezone.utc),
                window_id=0,
            )

    def test_to_dict(self) -> None:
        window = TimeWindow(
            train_start=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
            train_end=dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc),
            test_start=dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc),
            test_end=dt.datetime(2023, 7, 1, tzinfo=dt.timezone.utc),
            warmup_start=dt.datetime(2023, 5, 15, tzinfo=dt.timezone.utc),
            window_id=0,
        )
        d = window.to_dict()
        assert d["window_id"] == 0
        assert "train_start" in d
        assert "train_days" in d


# =============================================================================
# TimeSlicer Tests
# =============================================================================


class TestTimeSlicer:
    def test_invalid_train_days_raises(self) -> None:
        with pytest.raises(ValueError, match="train_days must be positive"):
            TimeSlicer(train_days=0, test_days=30)

    def test_invalid_test_days_raises(self) -> None:
        with pytest.raises(ValueError, match="test_days must be positive"):
            TimeSlicer(train_days=365, test_days=0)

    def test_invalid_warmup_days_raises(self) -> None:
        with pytest.raises(ValueError, match="warmup_days cannot be negative"):
            TimeSlicer(train_days=365, test_days=30, warmup_days=-1)

    def test_min_days_required(self) -> None:
        slicer = TimeSlicer(train_days=365, test_days=30)
        assert slicer.min_days_required == 395

    def test_date_range_too_short_raises(self) -> None:
        slicer = TimeSlicer(train_days=365, test_days=30)
        start = dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc)  # Only ~150 days

        with pytest.raises(ValueError, match="Date range.*too short"):
            list(slicer.generate(start, end))

    def test_rolling_generates_windows(self) -> None:
        slicer = TimeSlicer(train_days=90, test_days=30, anchored=False)
        start = dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2023, 12, 31, tzinfo=dt.timezone.utc)  # 364 days

        windows = list(slicer.generate(start, end))

        # With 90-day train and 30-day test, rolling by 30 days
        # Should get multiple windows
        assert len(windows) >= 2

        # Check window IDs are sequential
        for i, w in enumerate(windows):
            assert w.window_id == i

        # In rolling mode, train windows should slide
        if len(windows) >= 2:
            assert windows[1].train_start > windows[0].train_start

    def test_anchored_generates_windows(self) -> None:
        slicer = TimeSlicer(train_days=90, test_days=30, anchored=True)
        start = dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2023, 12, 31, tzinfo=dt.timezone.utc)

        windows = list(slicer.generate(start, end))

        assert len(windows) >= 2

        # In anchored mode, all train windows should start at same date
        for w in windows:
            assert w.train_start == start

        # But train_end should grow
        if len(windows) >= 2:
            assert windows[1].train_end > windows[0].train_end

    def test_warmup_days_applied(self) -> None:
        slicer = TimeSlicer(train_days=90, test_days=30, warmup_days=14)
        start = dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc)

        windows = list(slicer.generate(start, end))
        assert len(windows) >= 1

        # Warmup should be before test_start
        for w in windows:
            assert w.warmup_start < w.test_start
            assert w.warmup_days == 14 or w.warmup_start == w.train_start

    def test_count_windows(self) -> None:
        slicer = TimeSlicer(train_days=90, test_days=30)
        start = dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2023, 12, 31, tzinfo=dt.timezone.utc)

        count = slicer.count_windows(start, end)
        windows = list(slicer.generate(start, end))

        assert count == len(windows)


# =============================================================================
# WFAResult Tests
# =============================================================================


class TestWFAResult:
    @pytest.fixture
    def sample_window_results(self) -> list[WFAWindowResult]:
        window1 = TimeWindow(
            train_start=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
            train_end=dt.datetime(2023, 4, 1, tzinfo=dt.timezone.utc),
            test_start=dt.datetime(2023, 4, 1, tzinfo=dt.timezone.utc),
            test_end=dt.datetime(2023, 5, 1, tzinfo=dt.timezone.utc),
            warmup_start=dt.datetime(2023, 3, 15, tzinfo=dt.timezone.utc),
            window_id=0,
        )
        window2 = TimeWindow(
            train_start=dt.datetime(2023, 2, 1, tzinfo=dt.timezone.utc),
            train_end=dt.datetime(2023, 5, 1, tzinfo=dt.timezone.utc),
            test_start=dt.datetime(2023, 5, 1, tzinfo=dt.timezone.utc),
            test_end=dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc),
            warmup_start=dt.datetime(2023, 4, 15, tzinfo=dt.timezone.utc),
            window_id=1,
        )

        return [
            WFAWindowResult(
                window=window1,
                best_params={"window": 10},
                train_metrics={"sharpe": 1.5, "total_return": 0.10},
                test_metrics={"sharpe": 1.2, "total_return": 0.05},
            ),
            WFAWindowResult(
                window=window2,
                best_params={"window": 20},
                train_metrics={"sharpe": 1.8, "total_return": 0.12},
                test_metrics={"sharpe": 1.0, "total_return": 0.03},
            ),
        ]

    def test_n_windows(self, sample_window_results: list[WFAWindowResult]) -> None:
        result = WFAResult(window_results=sample_window_results)
        assert result.n_windows == 2

    def test_all_params(self, sample_window_results: list[WFAWindowResult]) -> None:
        result = WFAResult(window_results=sample_window_results)
        assert result.all_params == [{"window": 10}, {"window": 20}]

    def test_to_dataframe(self, sample_window_results: list[WFAWindowResult]) -> None:
        result = WFAResult(window_results=sample_window_results)
        df = result.to_dataframe()

        assert len(df) == 2
        assert "window_id" in df.columns
        assert "param_window" in df.columns
        assert "train_sharpe" in df.columns
        assert "test_sharpe" in df.columns

    def test_empty_result(self) -> None:
        result = WFAResult(window_results=[])
        assert result.n_windows == 0
        assert result.all_params == []
        assert result.to_dataframe().is_empty()


# =============================================================================
# WalkForwardValidator Tests
# =============================================================================


class TestWalkForwardValidator:
    @pytest.fixture
    def sample_data(self) -> pl.DataFrame:
        """Create sample price data spanning 2 years."""
        # Generate daily data for 2 years
        n_days = 730
        base_date = dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc)

        dates = [base_date + dt.timedelta(days=i) for i in range(n_days)]
        timestamps = [int(d.timestamp() * 1000) for d in dates]

        # Simple random walk for prices
        import random

        random.seed(42)
        prices = [100.0]
        for _ in range(n_days - 1):
            change = random.gauss(0.0005, 0.02)  # Small drift + volatility
            prices.append(prices[-1] * (1 + change))

        return pl.DataFrame(
            {
                "end_ms": timestamps,
                "close": prices,
            }
        )

    @pytest.fixture
    def simple_signal_generator(self):
        """Simple moving average crossover signal generator."""

        def signal_gen(df: pl.DataFrame, params: dict) -> pl.LazyFrame:
            window = params.get("window", 20)
            return df.lazy().with_columns(
                pl.when(pl.col("close") > pl.col("close").rolling_mean(window))
                .then(1)
                .otherwise(-1)
                .alias("signal")
            )

        return signal_gen

    def test_init(self) -> None:
        validator = WalkForwardValidator(
            train_days=180,
            test_days=30,
            warmup_days=20,
        )
        assert validator.slicer.train_days == 180
        assert validator.slicer.test_days == 30
        assert validator.slicer.warmup_days == 20

    def test_run_basic(
        self,
        sample_data: pl.DataFrame,
        simple_signal_generator,
    ) -> None:
        validator = WalkForwardValidator(
            train_days=180,
            test_days=30,
            warmup_days=20,
            cost_model=CostModel(fee_bps=10),
            n_jobs=1,  # Use single thread for testing
        )

        result = validator.run(
            data=sample_data,
            signal_generator=simple_signal_generator,
            param_ranges={"window": [10, 20, 30]},
        )

        assert isinstance(result, WFAResult)
        assert result.n_windows >= 1
        assert len(result.all_params) == result.n_windows

        # Check aggregate metrics are computed
        assert "n_windows" in result.aggregate_metrics
        assert result.aggregate_metrics["n_windows"] == result.n_windows

    def test_run_with_equity(
        self,
        sample_data: pl.DataFrame,
        simple_signal_generator,
    ) -> None:
        validator = WalkForwardValidator(
            train_days=180,
            test_days=30,
            n_jobs=1,
        )

        result = validator.run(
            data=sample_data,
            signal_generator=simple_signal_generator,
            param_ranges={"window": [10, 20]},
            return_equity=True,
        )

        # Check that equity curves are returned
        for wr in result.window_results:
            assert wr.test_equity is not None
            assert "equity" in wr.test_equity.columns

    def test_run_anchored_mode(
        self,
        sample_data: pl.DataFrame,
        simple_signal_generator,
    ) -> None:
        validator = WalkForwardValidator(
            train_days=180,
            test_days=30,
            anchored=True,
            n_jobs=1,
        )

        result = validator.run(
            data=sample_data,
            signal_generator=simple_signal_generator,
            param_ranges={"window": [10, 20]},
        )

        assert result.n_windows >= 1

        # In anchored mode, train_start should be the same for all windows
        train_starts = [wr.window.train_start for wr in result.window_results]
        assert len(set(train_starts)) == 1

    def test_run_with_explicit_dates(
        self,
        sample_data: pl.DataFrame,
        simple_signal_generator,
    ) -> None:
        validator = WalkForwardValidator(
            train_days=90,
            test_days=30,
            n_jobs=1,
        )

        start = dt.datetime(2022, 3, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2022, 12, 1, tzinfo=dt.timezone.utc)

        result = validator.run(
            data=sample_data,
            signal_generator=simple_signal_generator,
            param_ranges={"window": [10, 20]},
            start=start,
            end=end,
        )

        assert result.n_windows >= 1

        # All windows should be within the specified range
        for wr in result.window_results:
            assert wr.window.train_start >= start
            assert wr.window.test_end <= end

    def test_run_random_search(
        self,
        sample_data: pl.DataFrame,
        simple_signal_generator,
    ) -> None:
        validator = WalkForwardValidator(
            train_days=180,
            test_days=30,
            n_jobs=1,
        )

        result = validator.run(
            data=sample_data,
            signal_generator=simple_signal_generator,
            param_ranges={"window": [5, 10, 15, 20, 25, 30, 35, 40]},
            n_random=3,
            seed=42,
        )

        assert result.n_windows >= 1

    def test_date_range_too_short_raises(
        self,
        simple_signal_generator,
    ) -> None:
        # Create very short data
        short_data = pl.DataFrame(
            {
                "end_ms": [
                    int(dt.datetime(2023, 1, i, tzinfo=dt.timezone.utc).timestamp() * 1000)
                    for i in range(1, 31)
                ],
                "close": list(range(100, 130)),
            }
        )

        validator = WalkForwardValidator(
            train_days=180,
            test_days=30,
            n_jobs=1,
        )

        with pytest.raises(ValueError, match="too short|No valid windows"):
            validator.run(
                data=short_data,
                signal_generator=simple_signal_generator,
                param_ranges={"window": [10]},
            )


# =============================================================================
# Integration Tests
# =============================================================================


class TestWFAIntegration:
    def test_full_workflow(self) -> None:
        """Test complete WFA workflow with stitched equity curve."""
        # Generate 2 years of data
        n_days = 730
        base_date = dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc)

        import random

        random.seed(123)
        prices = [100.0]
        for _ in range(n_days - 1):
            prices.append(prices[-1] * (1 + random.gauss(0.0003, 0.015)))

        data = pl.DataFrame(
            {
                "end_ms": [
                    int((base_date + dt.timedelta(days=i)).timestamp() * 1000)
                    for i in range(n_days)
                ],
                "close": prices,
            }
        )

        def signal_gen(df: pl.DataFrame, params: dict) -> pl.LazyFrame:
            fast = params["fast"]
            slow = params["slow"]
            return df.lazy().with_columns(
                pl.when(pl.col("close").rolling_mean(fast) > pl.col("close").rolling_mean(slow))
                .then(1)
                .otherwise(-1)
                .alias("signal")
            )

        validator = WalkForwardValidator(
            train_days=180,
            test_days=30,
            warmup_days=30,
            cost_model=CostModel(fee_bps=10, slippage_bps=5),
            n_jobs=1,
        )

        result = validator.run(
            data=data,
            signal_generator=signal_gen,
            param_ranges={
                "fast": [5, 10, 20],
                "slow": [50, 100],
            },
            return_equity=True,
        )

        # Verify results structure
        assert result.n_windows >= 1
        assert "avg_sharpe" in result.aggregate_metrics or "n_windows" in result.aggregate_metrics

        # Verify each window has valid data
        for wr in result.window_results:
            assert "fast" in wr.best_params
            assert "slow" in wr.best_params
            assert wr.train_metrics is not None
            assert wr.test_metrics is not None

        # Verify to_dataframe works
        df = result.to_dataframe()
        assert len(df) == result.n_windows
        assert "param_fast" in df.columns
        assert "param_slow" in df.columns
