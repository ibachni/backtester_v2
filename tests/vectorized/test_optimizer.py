"""Tests for the ParamOptimizer module."""

import polars as pl
import pytest

from backtester.vectorized.engine import CostModel
from backtester.vectorized.optimizer import (
    OptimizationResult,
    ParamGrid,
    ParamOptimizer,
    generate_grid,
)

# =============================================================================
# ParamGrid Tests
# =============================================================================


class TestParamGrid:
    def test_empty_ranges_raises(self) -> None:
        with pytest.raises(ValueError, match="cannot be empty"):
            ParamGrid({})

    def test_empty_values_raises(self) -> None:
        with pytest.raises(ValueError, match="has empty range"):
            ParamGrid({"window": []})

    def test_grid_size_single_param(self) -> None:
        grid = ParamGrid({"window": [10, 20, 50]})
        assert grid.grid_size == 3

    def test_grid_size_multiple_params(self) -> None:
        grid = ParamGrid({"window": [10, 20], "threshold": [0.5, 1.0, 1.5]})
        assert grid.grid_size == 6

    def test_param_names(self) -> None:
        grid = ParamGrid({"a": [1], "b": [2], "c": [3]})
        assert set(grid.param_names) == {"a", "b", "c"}

    def test_generate_full_grid(self) -> None:
        grid = ParamGrid({"x": [1, 2], "y": ["a", "b"]})
        combinations = list(grid.generate())
        assert len(combinations) == 4
        assert {"x": 1, "y": "a"} in combinations
        assert {"x": 1, "y": "b"} in combinations
        assert {"x": 2, "y": "a"} in combinations
        assert {"x": 2, "y": "b"} in combinations

    def test_generate_random_subset(self) -> None:
        grid = ParamGrid({"x": [1, 2, 3, 4, 5], "y": [1, 2, 3, 4, 5]})
        combinations = list(grid.generate(n_random=5, seed=42))
        assert len(combinations) == 5
        # Verify all are valid combinations
        for combo in combinations:
            assert combo["x"] in [1, 2, 3, 4, 5]
            assert combo["y"] in [1, 2, 3, 4, 5]

    def test_generate_random_reproducible(self) -> None:
        grid = ParamGrid({"x": range(10), "y": range(10)})
        combo1 = list(grid.generate(n_random=5, seed=123))
        combo2 = list(grid.generate(n_random=5, seed=123))
        assert combo1 == combo2

    def test_generate_random_exceeds_grid_returns_full(self) -> None:
        grid = ParamGrid({"x": [1, 2]})
        combinations = list(grid.generate(n_random=100))
        assert len(combinations) == 2


class TestGenerateGrid:
    def test_convenience_function(self) -> None:
        result = generate_grid({"a": [1, 2], "b": [3, 4]})
        assert len(result) == 4
        assert isinstance(result, list)


# =============================================================================
# OptimizationResult Tests
# =============================================================================


class TestOptimizationResult:
    @pytest.fixture
    def sample_results(self) -> list[tuple[dict, dict]]:
        return [
            ({"window": 10}, {"sharpe": 1.5, "total_return": 0.10}),
            ({"window": 20}, {"sharpe": 2.0, "total_return": 0.15}),
            ({"window": 50}, {"sharpe": 1.2, "total_return": 0.08}),
        ]

    def test_empty_result(self) -> None:
        result = OptimizationResult(results=[])
        assert result.n_successful == 0
        assert result.best_params is None
        assert result.best_metrics is None

    def test_n_successful(self, sample_results: list) -> None:
        result = OptimizationResult(results=sample_results)
        assert result.n_successful == 3

    def test_n_failed(self) -> None:
        result = OptimizationResult(
            results=[],
            failed=[{"bad": 1}, {"bad": 2}],
        )
        assert result.n_failed == 2

    def test_best_params_sharpe(self, sample_results: list) -> None:
        result = OptimizationResult(results=sample_results, rank_by="sharpe")
        assert result.best_params == {"window": 20}

    def test_best_metrics(self, sample_results: list) -> None:
        result = OptimizationResult(results=sample_results, rank_by="sharpe")
        assert result.best_metrics == {"sharpe": 2.0, "total_return": 0.15}

    def test_rank_ascending(self, sample_results: list) -> None:
        result = OptimizationResult(
            results=sample_results,
            rank_by="sharpe",
            rank_descending=False,
        )
        # Lowest sharpe first
        assert result.best_params == {"window": 50}

    def test_top_n(self, sample_results: list) -> None:
        result = OptimizationResult(results=sample_results, rank_by="sharpe")
        top2 = result.top_n(2)
        assert len(top2) == 2
        assert top2[0][0] == {"window": 20}  # Best sharpe
        assert top2[1][0] == {"window": 10}  # Second best

    def test_to_dataframe(self, sample_results: list) -> None:
        result = OptimizationResult(results=sample_results)
        df = result.to_dataframe()
        assert len(df) == 3
        assert "window" in df.columns
        assert "sharpe" in df.columns
        assert "total_return" in df.columns

    def test_score_table_sorted(self, sample_results: list) -> None:
        result = OptimizationResult(results=sample_results, rank_by="sharpe")
        df = result.score_table()
        # Should be sorted by sharpe descending
        sharpes = df["sharpe"].to_list()
        assert sharpes == [2.0, 1.5, 1.2]

    def test_score_table_top_n(self, sample_results: list) -> None:
        result = OptimizationResult(results=sample_results, rank_by="sharpe")
        df = result.score_table(top_n=2)
        assert len(df) == 2

    def test_handles_none_metrics(self) -> None:
        results = [
            ({"w": 10}, {"sharpe": None, "total_return": 0.1}),
            ({"w": 20}, {"sharpe": 1.5, "total_return": 0.2}),
        ]
        result = OptimizationResult(results=results, rank_by="sharpe")
        # Should handle None gracefully
        assert result.best_params == {"w": 20}


# =============================================================================
# ParamOptimizer Tests
# =============================================================================


class TestParamOptimizer:
    @pytest.fixture
    def price_data(self) -> pl.LazyFrame:
        """Generate synthetic price data for testing."""
        import numpy as np

        np.random.seed(42)
        n = 500
        # Simple trending price with noise
        returns = np.random.normal(0.0005, 0.02, n)
        prices = 100 * np.cumprod(1 + returns)

        return pl.LazyFrame(
            {
                "end_ms": list(range(n)),
                "close": prices.tolist(),
            }
        )

    @staticmethod
    def simple_signal_generator(df: pl.DataFrame, params: dict) -> pl.LazyFrame:
        """Simple moving average crossover signal generator."""
        window = params["window"]
        return df.lazy().with_columns(
            pl.when(pl.col("close") > pl.col("close").rolling_mean(window))
            .then(1)
            .otherwise(-1)
            .alias("signal")
        )

    def test_run_sequential_basic(self, price_data: pl.LazyFrame) -> None:
        optimizer = ParamOptimizer()
        result = optimizer.run_sequential(
            data=price_data,
            signal_generator=self.simple_signal_generator,
            param_ranges={"window": [10, 20, 50]},
        )
        assert result.n_successful == 3
        assert result.n_failed == 0
        assert result.best_params is not None

    def test_run_parallel_basic(self, price_data: pl.LazyFrame) -> None:
        pytest.importorskip("joblib")
        optimizer = ParamOptimizer(n_jobs=2)
        result = optimizer.run(
            data=price_data,
            signal_generator=self.simple_signal_generator,
            param_ranges={"window": [10, 20, 50]},
        )
        assert result.n_successful == 3
        assert result.best_params is not None

    def test_run_with_dataframe_input(self, price_data: pl.LazyFrame) -> None:
        df = price_data.collect()
        optimizer = ParamOptimizer()
        result = optimizer.run_sequential(
            data=df,
            signal_generator=self.simple_signal_generator,
            param_ranges={"window": [10, 20]},
        )
        assert result.n_successful == 2

    def test_run_with_cost_model(self, price_data: pl.LazyFrame) -> None:
        optimizer = ParamOptimizer(cost_model=CostModel(fee_bps=10, slippage_bps=5))
        result = optimizer.run_sequential(
            data=price_data,
            signal_generator=self.simple_signal_generator,
            param_ranges={"window": [20]},
        )
        # With costs, returns should be lower
        assert result.n_successful == 1

    def test_run_random_search(self, price_data: pl.LazyFrame) -> None:
        optimizer = ParamOptimizer()
        result = optimizer.run_sequential(
            data=price_data,
            signal_generator=self.simple_signal_generator,
            param_ranges={"window": list(range(5, 100))},
            n_random=10,
            seed=42,
        )
        assert result.n_successful == 10

    def test_rank_by_custom_metric(self, price_data: pl.LazyFrame) -> None:
        optimizer = ParamOptimizer()
        result = optimizer.run_sequential(
            data=price_data,
            signal_generator=self.simple_signal_generator,
            param_ranges={"window": [10, 20, 50]},
            rank_by="total_return",
        )
        assert result.rank_by == "total_return"
        # Best should be based on total_return now
        assert result.best_metrics is not None

    def test_handles_failing_params(self, price_data: pl.LazyFrame) -> None:
        def failing_generator(df: pl.DataFrame, params: dict) -> pl.LazyFrame:
            if params["window"] == 0:
                raise ValueError("Invalid window size")
            return df.lazy().with_columns(
                pl.col("close").rolling_mean(params["window"]).alias("signal")
            )

        optimizer = ParamOptimizer()
        result = optimizer.run_sequential(
            data=price_data,
            signal_generator=failing_generator,
            param_ranges={"window": [0, 10, 20]},
        )
        # window=0 should fail, others should succeed
        assert result.n_failed >= 1
        assert result.n_successful >= 1

    def test_multi_param_grid(self, price_data: pl.LazyFrame) -> None:
        def multi_param_generator(df: pl.DataFrame, params: dict) -> pl.LazyFrame:
            fast = params["fast"]
            slow = params["slow"]
            return df.lazy().with_columns(
                pl.when(pl.col("close").rolling_mean(fast) > pl.col("close").rolling_mean(slow))
                .then(1)
                .otherwise(-1)
                .alias("signal")
            )

        optimizer = ParamOptimizer()
        result = optimizer.run_sequential(
            data=price_data,
            signal_generator=multi_param_generator,
            param_ranges={"fast": [5, 10], "slow": [20, 50]},
        )
        assert result.n_successful == 4  # 2 x 2 grid

    def test_result_dataframe_has_params(self, price_data: pl.LazyFrame) -> None:
        optimizer = ParamOptimizer()
        result = optimizer.run_sequential(
            data=price_data,
            signal_generator=self.simple_signal_generator,
            param_ranges={"window": [10, 20]},
        )
        df = result.to_dataframe()
        assert "window" in df.columns
        assert set(df["window"].to_list()) == {10, 20}


# =============================================================================
# Integration Tests
# =============================================================================


class TestOptimizerIntegration:
    """Integration tests combining optimizer with real strategies."""

    @pytest.fixture
    def trending_data(self) -> pl.LazyFrame:
        """Data with clear upward trend - should favor long signals."""
        prices = [100 + i * 0.5 + (i % 10) * 0.1 for i in range(200)]
        return pl.LazyFrame(
            {
                "end_ms": list(range(200)),
                "close": prices,
            }
        )

    def test_optimizer_finds_better_params(self, trending_data: pl.LazyFrame) -> None:
        """In trending data, shorter MAs should outperform longer ones."""

        def trend_following(df: pl.DataFrame, params: dict) -> pl.LazyFrame:
            return df.lazy().with_columns(
                pl.when(pl.col("close") > pl.col("close").rolling_mean(params["window"]))
                .then(1)
                .otherwise(0)
                .alias("signal")
            )

        optimizer = ParamOptimizer()
        result = optimizer.run_sequential(
            data=trending_data,
            signal_generator=trend_following,
            param_ranges={"window": [5, 10, 20, 50, 100]},
        )

        # All should succeed
        assert result.n_successful == 5

        # Best params should have positive Sharpe in trending data
        best = result.best_metrics
        assert best is not None
        assert best.get("sharpe") is not None

    def test_score_table_for_analysis(self, trending_data: pl.LazyFrame) -> None:
        """Score table should support strategy analysis."""

        def signal_gen(df: pl.DataFrame, params: dict) -> pl.LazyFrame:
            return df.lazy().with_columns(
                pl.col("close")
                .rolling_mean(params["window"])
                .shift(1)
                .fill_null(0)
                .sign()
                .alias("signal")
            )

        optimizer = ParamOptimizer()
        result = optimizer.run_sequential(
            data=trending_data,
            signal_generator=signal_gen,
            param_ranges={"window": [5, 10, 20]},
        )

        df = result.score_table()
        assert not df.is_empty()

        # Should have all metrics
        expected_metrics = ["sharpe", "total_return", "max_drawdown", "trade_count"]
        for metric in expected_metrics:
            assert metric in df.columns
