"""Tests for the VectorizedEngine module."""

import polars as pl
import pytest

from backtester.vectorized.engine import (
    BacktestResult,
    CostModel,
    VectorizedEngine,
    quick_backtest,
)

# =============================================================================
# CostModel Tests
# =============================================================================


class TestCostModel:
    def test_zero_costs(self) -> None:
        cost = CostModel()
        assert cost.fee_bps == 0.0
        assert cost.slippage_bps == 0.0
        assert cost.total_bps == 0.0
        assert cost.total_frac == 0.0

    def test_fee_only(self) -> None:
        cost = CostModel(fee_bps=10)
        assert cost.total_bps == 10.0
        assert cost.total_frac == pytest.approx(0.001)

    def test_combined_costs(self) -> None:
        cost = CostModel(fee_bps=10, slippage_bps=5)
        assert cost.total_bps == 15.0
        assert cost.total_frac == pytest.approx(0.0015)

    def test_cost_id(self) -> None:
        cost = CostModel(fee_bps=10, slippage_bps=5)
        assert cost.cost_id() == "fee10.0_slip5.0"

    def test_frozen(self) -> None:
        cost = CostModel(fee_bps=10)
        with pytest.raises(AttributeError):
            cost.fee_bps = 20  # type: ignore[misc]


# =============================================================================
# VectorizedEngine Tests - Basic Functionality
# =============================================================================


class TestVectorizedEngineBasic:
    @pytest.fixture
    def simple_data(self) -> pl.LazyFrame:
        """Simple test data with known returns."""
        return pl.LazyFrame(
            {
                "end_ms": [1, 2, 3, 4, 5],
                "close": [100.0, 110.0, 105.0, 115.0, 120.0],
                "signal": [0, 1, 1, -1, 0],
            }
        )

    def test_missing_price_col_raises(self) -> None:
        lf = pl.LazyFrame({"signal": [1, 0, -1]})
        engine = VectorizedEngine()
        with pytest.raises(ValueError, match="Missing required columns"):
            engine.run(lf)

    def test_missing_signal_col_raises(self) -> None:
        lf = pl.LazyFrame({"close": [100.0, 110.0, 105.0]})
        engine = VectorizedEngine()
        with pytest.raises(ValueError, match="Missing required columns"):
            engine.run(lf)

    def test_run_returns_backtest_result(self, simple_data: pl.LazyFrame) -> None:
        engine = VectorizedEngine()
        result = engine.run(simple_data)
        assert isinstance(result, BacktestResult)
        assert isinstance(result.metrics, dict)

    def test_metrics_keys_present(self, simple_data: pl.LazyFrame) -> None:
        engine = VectorizedEngine()
        result = engine.run(simple_data)
        expected_keys = {
            "sharpe",
            "sortino",
            "total_return",
            "cagr",
            "max_drawdown",
            "volatility",
            "calmar",
            "avg_turnover",
            "trade_count",
            "n_bars",
            "n_bars_in_market",
        }
        assert set(result.metrics.keys()) == expected_keys

    def test_n_bars_correct(self, simple_data: pl.LazyFrame) -> None:
        engine = VectorizedEngine()
        result = engine.run(simple_data)
        assert result.metrics["n_bars"] == 5

    def test_params_stored(self, simple_data: pl.LazyFrame) -> None:
        engine = VectorizedEngine()
        params = {"window": 20, "threshold": 0.5}
        result = engine.run(simple_data, params=params)
        assert result.params == params

    def test_aligned_df_not_returned_by_default(self, simple_data: pl.LazyFrame) -> None:
        engine = VectorizedEngine()
        result = engine.run(simple_data)
        assert result.aligned_df is None

    def test_aligned_df_returned_when_requested(self, simple_data: pl.LazyFrame) -> None:
        engine = VectorizedEngine()
        result = engine.run(simple_data, return_aligned_df=True)
        assert result.aligned_df is not None
        assert isinstance(result.aligned_df, pl.DataFrame)


# =============================================================================
# VectorizedEngine Tests - Position Logic
# =============================================================================


class TestVectorizedEnginePosition:
    def test_position_is_shifted_signal(self) -> None:
        """Position should be signal shifted by 1 (trade at next bar)."""
        lf = pl.LazyFrame(
            {
                "close": [100.0, 100.0, 100.0, 100.0, 100.0],
                "signal": [0, 1, 1, -1, 0],
            }
        )
        engine = VectorizedEngine()
        result = engine.run(lf, return_aligned_df=True)
        df = result.aligned_df
        assert df is not None

        # Position should be: [0, 0, 1, 1, -1] (signal shifted by 1)
        positions = df["position"].to_list()
        assert positions == [0, 0, 1, 1, -1]

    def test_first_position_is_zero(self) -> None:
        """First position should always be 0 (no position before first signal)."""
        lf = pl.LazyFrame(
            {
                "close": [100.0, 110.0],
                "signal": [1, 1],
            }
        )
        engine = VectorizedEngine()
        result = engine.run(lf, return_aligned_df=True)
        df = result.aligned_df
        assert df is not None
        assert df["position"][0] == 0


# =============================================================================
# VectorizedEngine Tests - Returns Calculation
# =============================================================================


class TestVectorizedEngineReturns:
    def test_asset_return_calculation(self) -> None:
        """Asset return = (price[t] / price[t-1]) - 1."""
        lf = pl.LazyFrame(
            {
                "close": [100.0, 110.0, 105.0],
                "signal": [0, 1, 0],
            }
        )
        engine = VectorizedEngine()
        result = engine.run(lf, return_aligned_df=True)
        df = result.aligned_df
        assert df is not None

        asset_returns = df["asset_return"].to_list()
        assert asset_returns[0] == pytest.approx(0.0)  # First return is 0 (filled)
        assert asset_returns[1] == pytest.approx(0.1)  # (110-100)/100
        assert asset_returns[2] == pytest.approx(-0.04545, rel=1e-3)  # (105-110)/110

    def test_gross_return_is_position_times_asset_return(self) -> None:
        """Gross return = position * asset_return."""
        lf = pl.LazyFrame(
            {
                "close": [100.0, 110.0, 121.0],
                "signal": [1, 1, 1],
            }
        )
        engine = VectorizedEngine()
        result = engine.run(lf, return_aligned_df=True)
        df = result.aligned_df
        assert df is not None

        # Positions: [0, 1, 1] (shifted signal)
        # Asset returns: [0, 0.1, 0.1]
        # Gross returns: [0*0, 1*0.1, 1*0.1] = [0, 0.1, 0.1]
        gross_returns = df["gross_return"].to_list()
        assert gross_returns[0] == pytest.approx(0.0)
        assert gross_returns[1] == pytest.approx(0.1)
        assert gross_returns[2] == pytest.approx(0.1)

    def test_short_position_inverts_return(self) -> None:
        """Short position (-1) should invert the return."""
        lf = pl.LazyFrame(
            {
                "close": [100.0, 110.0],  # 10% gain
                "signal": [-1, -1],
            }
        )
        engine = VectorizedEngine()
        result = engine.run(lf, return_aligned_df=True)
        df = result.aligned_df
        assert df is not None

        # Position at bar 1 = -1 (from shifted signal)
        # Asset return at bar 1 = 0.1
        # Gross return = -1 * 0.1 = -0.1
        assert df["gross_return"][1] == pytest.approx(-0.1)


# =============================================================================
# VectorizedEngine Tests - Cost Calculation
# =============================================================================


class TestVectorizedEngineCosts:
    def test_zero_costs_no_deduction(self) -> None:
        """With zero costs, net return equals gross return."""
        lf = pl.LazyFrame(
            {
                "close": [100.0, 110.0],
                "signal": [1, 1],
            }
        )
        engine = VectorizedEngine(cost_model=CostModel())
        result = engine.run(lf, return_aligned_df=True)
        df = result.aligned_df
        assert df is not None

        assert df["cost"].sum() == pytest.approx(0.0)
        assert df["net_return"].to_list() == pytest.approx(df["gross_return"].to_list())

    def test_turnover_calculation(self) -> None:
        """Turnover = abs(position change)."""
        lf = pl.LazyFrame(
            {
                "close": [100.0, 100.0, 100.0, 100.0],
                "signal": [0, 1, -1, 0],  # Positions will be: [0, 0, 1, -1]
            }
        )
        engine = VectorizedEngine()
        result = engine.run(lf, return_aligned_df=True)
        df = result.aligned_df
        assert df is not None

        # Position changes: [0, 0-0=0, 1-0=1, -1-1=-2]
        # Turnover: [0, 0, 1, 2]
        turnovers = df["turnover"].to_list()
        assert turnovers[0] == pytest.approx(0.0)
        assert turnovers[1] == pytest.approx(0.0)
        assert turnovers[2] == pytest.approx(1.0)
        assert turnovers[3] == pytest.approx(2.0)

    def test_cost_applied_on_trade(self) -> None:
        """Cost = turnover * cost_fraction."""
        lf = pl.LazyFrame(
            {
                "close": [100.0, 100.0, 100.0],
                "signal": [0, 1, 1],  # Enter long on bar 2
            }
        )
        # 10 bps fee = 0.001 (0.1%)
        engine = VectorizedEngine(cost_model=CostModel(fee_bps=10))
        result = engine.run(lf, return_aligned_df=True)
        df = result.aligned_df
        assert df is not None

        # Positions: [0, 0, 1]
        # Turnover at bar 2: abs(1 - 0) = 1
        # Cost at bar 2: 1 * 0.001 = 0.001
        costs = df["cost"].to_list()
        assert costs[2] == pytest.approx(0.001)

    def test_net_return_accounts_for_costs(self) -> None:
        """Net return = gross return - cost."""
        lf = pl.LazyFrame(
            {
                "close": [100.0, 110.0, 110.0],  # 10% gain, then flat
                "signal": [1, 1, 1],
            }
        )
        # 100 bps = 1% cost
        engine = VectorizedEngine(cost_model=CostModel(fee_bps=100))
        result = engine.run(lf, return_aligned_df=True)
        df = result.aligned_df
        assert df is not None

        # Bar 1: position=1, asset_return=0.1, turnover=1, cost=0.01
        # Net return = 0.1 - 0.01 = 0.09
        assert df["net_return"][1] == pytest.approx(0.09)


# =============================================================================
# VectorizedEngine Tests - Metrics Calculation
# =============================================================================


class TestVectorizedEngineMetrics:
    def test_total_return_calculation(self) -> None:
        """Total return = final_equity - 1."""
        lf = pl.LazyFrame(
            {
                "close": [100.0, 110.0, 121.0],  # 10%, 10% returns
                "signal": [1, 1, 1],
            }
        )
        engine = VectorizedEngine()
        result = engine.run(lf)

        # Positions: [0, 1, 1]
        # Returns: [0, 0.1, 0.1]
        # Equity: [1, 1.1, 1.21]
        # Total return: 0.21
        assert result.total_return == pytest.approx(0.21, rel=1e-3)

    def test_trade_count(self) -> None:
        """Trade count = number of position changes (turnover > 0)."""
        lf = pl.LazyFrame(
            {
                "close": [100.0] * 5,
                "signal": [0, 1, 1, -1, 0],
                # Positions (shifted): [0, 0, 1, 1, -1]
                # Turnover: [0, 0, 1, 0, 2] -> 2 non-zero entries
            }
        )
        engine = VectorizedEngine()
        result = engine.run(lf)
        assert result.metrics["trade_count"] == 2

    def test_n_bars_in_market(self) -> None:
        """Bars in market = bars with non-zero position."""
        lf = pl.LazyFrame(
            {
                "close": [100.0] * 5,
                "signal": [0, 1, 1, 0, 0],  # Positions: [0, 0, 1, 1, 0]
            }
        )
        engine = VectorizedEngine()
        result = engine.run(lf)
        assert result.metrics["n_bars_in_market"] == 2

    def test_max_drawdown_calculation(self) -> None:
        """Max drawdown should capture worst peak-to-trough decline."""
        # Create a scenario with known drawdown
        lf = pl.LazyFrame(
            {
                "close": [100.0, 110.0, 100.0, 110.0],  # Up 10%, down ~9%, up 10%
                "signal": [1, 1, 1, 1],
            }
        )
        engine = VectorizedEngine()
        result = engine.run(lf)

        # Equity: [1, 1.1, 1.0, 1.1]
        # Peak: [1, 1.1, 1.1, 1.1]
        # Drawdown: [0, 0, 0.1/1.1, 0]
        # Max DD ≈ 0.0909
        assert result.max_drawdown == pytest.approx(0.0909, rel=0.01)

    def test_sharpe_ratio_positive_for_positive_returns(self) -> None:
        """Sharpe should be positive for consistently positive returns."""
        lf = pl.LazyFrame(
            {
                "close": [100.0 * (1.01**i) for i in range(100)],  # 1% daily gains
                "signal": [1] * 100,
            }
        )
        engine = VectorizedEngine()
        result = engine.run(lf)
        assert result.sharpe is not None
        assert result.sharpe > 0

    def test_empty_dataframe_returns_empty_metrics(self) -> None:
        """Empty data should return null metrics."""
        lf = pl.LazyFrame({"close": [], "signal": []})
        engine = VectorizedEngine()
        result = engine.run(lf)
        assert result.metrics["n_bars"] == 0
        assert result.metrics["sharpe"] is None


# =============================================================================
# VectorizedEngine Tests - Custom Column Names
# =============================================================================


class TestVectorizedEngineCustomColumns:
    def test_custom_price_col(self) -> None:
        """Engine should work with custom price column name."""
        lf = pl.LazyFrame(
            {
                "adj_close": [100.0, 110.0],
                "signal": [1, 1],
            }
        )
        engine = VectorizedEngine(price_col="adj_close")
        result = engine.run(lf)
        assert result.metrics["n_bars"] == 2

    def test_custom_signal_col(self) -> None:
        """Engine should work with custom signal column name."""
        lf = pl.LazyFrame(
            {
                "close": [100.0, 110.0],
                "position_signal": [1, 1],
            }
        )
        engine = VectorizedEngine(signal_col="position_signal")
        result = engine.run(lf)
        assert result.metrics["n_bars"] == 2


# =============================================================================
# BacktestResult Tests
# =============================================================================


class TestBacktestResult:
    def test_property_accessors(self) -> None:
        """Result properties should return metrics values."""
        metrics: dict[str, float | int | None] = {
            "sharpe": 1.5,
            "total_return": 0.25,
            "max_drawdown": 0.1,
        }
        result = BacktestResult(metrics=metrics)
        assert result.sharpe == 1.5
        assert result.total_return == 0.25
        assert result.max_drawdown == 0.1

    def test_to_dict(self) -> None:
        """to_dict should return serializable dictionary."""
        metrics: dict[str, float | int | None] = {"sharpe": 1.5}
        params = {"window": 20}
        result = BacktestResult(metrics=metrics, params=params)
        d = result.to_dict()
        assert d["metrics"] == metrics
        assert d["params"] == params


# =============================================================================
# quick_backtest Tests
# =============================================================================


class TestQuickBacktest:
    def test_returns_metrics_dict(self) -> None:
        """quick_backtest should return metrics dictionary."""
        lf = pl.LazyFrame(
            {
                "close": [100.0, 110.0, 115.0],
                "signal": [1, 1, 1],
            }
        )
        metrics = quick_backtest(lf)
        assert isinstance(metrics, dict)
        assert "sharpe" in metrics

    def test_accepts_cost_params(self) -> None:
        """quick_backtest should accept cost parameters."""
        lf = pl.LazyFrame(
            {
                "close": [100.0, 110.0],
                "signal": [1, 1],
            }
        )
        # Without costs
        metrics_no_cost = quick_backtest(lf)
        # With costs
        metrics_with_cost = quick_backtest(lf, fee_bps=100)

        # With high costs, total return should be lower
        assert metrics_with_cost["total_return"] is not None
        assert metrics_no_cost["total_return"] is not None
        assert metrics_with_cost["total_return"] < metrics_no_cost["total_return"]
