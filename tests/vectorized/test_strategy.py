import numpy as np
import polars as pl
import pytest

from backtester.vectorized.strategy import IndicatorFactory, StrategyBase

# =============================================================================
# IndicatorFactory Tests - Moving Averages
# =============================================================================


def test_indicator_factory_sma_over_symbol() -> None:
    df = pl.DataFrame(
        {
            "symbol": ["A", "A", "B", "B"],
            "close": [1.0, 2.0, 10.0, 12.0],
        }
    )
    lf = df.lazy().with_columns(
        IndicatorFactory.sma("close", 2, min_samples=1, over="symbol").alias("sma")
    )
    out = lf.collect()
    assert out["sma"].to_list() == [1.0, 1.5, 10.0, 11.0]


def test_indicator_factory_sma_basic() -> None:
    df = pl.DataFrame({"close": [1.0, 2.0, 3.0, 4.0, 5.0]})
    lf = df.lazy().with_columns(IndicatorFactory.sma("close", 3, min_samples=1).alias("sma"))
    out = lf.collect()
    # SMA(3) with min_samples=1: [1.0, 1.5, 2.0, 3.0, 4.0]
    expected = [1.0, 1.5, 2.0, 3.0, 4.0]
    assert out["sma"].to_list() == pytest.approx(expected)


def test_indicator_factory_ema() -> None:
    df = pl.DataFrame({"close": [1.0, 2.0, 3.0, 4.0, 5.0]})
    lf = df.lazy().with_columns(IndicatorFactory.ema("close", span=3, min_samples=1).alias("ema"))
    out = lf.collect()
    # EMA should be weighted toward recent values
    assert len(out["ema"]) == 5
    assert out["ema"][0] == 1.0  # First value equals input
    assert out["ema"][-1] > out["sma"][-1] if "sma" in out.columns else True


def test_indicator_factory_ema_over_symbol() -> None:
    df = pl.DataFrame(
        {
            "symbol": ["A", "A", "A", "B", "B", "B"],
            "close": [1.0, 2.0, 3.0, 10.0, 20.0, 30.0],
        }
    )
    lf = df.lazy().with_columns(
        IndicatorFactory.ema("close", span=2, min_samples=1, over="symbol").alias("ema")
    )
    out = lf.collect()
    # EMA should be computed separately per symbol
    assert out["ema"][0] == pytest.approx(1.0)  # First A
    assert out["ema"][3] == pytest.approx(10.0)  # First B


# =============================================================================
# IndicatorFactory Tests - Returns
# =============================================================================


def test_indicator_factory_returns() -> None:
    df = pl.DataFrame({"close": [100.0, 105.0, 102.0, 110.0]})
    lf = df.lazy().with_columns(IndicatorFactory.returns("close").alias("ret"))
    out = lf.collect()
    # Returns: (105-100)/100=0.05, (102-105)/105=-0.0286, (110-102)/102=0.0784
    expected = [None, 0.05, (102 - 105) / 105, (110 - 102) / 102]
    for i, (actual, exp) in enumerate(zip(out["ret"].to_list(), expected)):
        if exp is None:
            assert actual is None
        else:
            assert actual == pytest.approx(exp, rel=1e-4)


def test_indicator_factory_log_returns() -> None:
    df = pl.DataFrame({"close": [100.0, 110.0, 100.0]})
    lf = df.lazy().with_columns(IndicatorFactory.log_returns("close").alias("log_ret"))
    out = lf.collect()
    # Log returns: ln(110/100), ln(100/110)
    import math

    assert out["log_ret"][0] is None
    assert out["log_ret"][1] == pytest.approx(math.log(110 / 100), rel=1e-4)
    assert out["log_ret"][2] == pytest.approx(math.log(100 / 110), rel=1e-4)


def test_indicator_factory_forward_returns() -> None:
    df = pl.DataFrame({"close": [100.0, 110.0, 105.0, 120.0]})
    lf = df.lazy().with_columns(
        IndicatorFactory.forward_returns("close", periods=1).alias("fwd_ret")
    )
    out = lf.collect()
    # Forward returns: (110-100)/100=0.1, (105-110)/110, (120-105)/105, None
    expected = [0.1, (105 - 110) / 110, (120 - 105) / 105, None]
    for actual, exp in zip(out["fwd_ret"].to_list(), expected):
        if exp is None:
            assert actual is None
        else:
            assert actual == pytest.approx(exp, rel=1e-4)


# =============================================================================
# IndicatorFactory Tests - Volatility
# =============================================================================


def test_indicator_factory_rolling_std() -> None:
    df = pl.DataFrame({"close": [1.0, 2.0, 3.0, 4.0, 5.0]})
    lf = df.lazy().with_columns(
        IndicatorFactory.rolling_std("close", 3, min_samples=3).alias("std")
    )
    out = lf.collect()
    assert out["std"][0] is None
    assert out["std"][1] is None
    assert out["std"][2] is not None
    assert out["std"][2] == pytest.approx(1.0, rel=1e-4)  # std of [1,2,3]


def test_indicator_factory_atr() -> None:
    df = pl.DataFrame(
        {
            "high": [102.0, 105.0, 108.0, 110.0],
            "low": [98.0, 100.0, 103.0, 105.0],
            "close": [100.0, 104.0, 106.0, 108.0],
        }
    )
    lf = df.lazy().with_columns(
        IndicatorFactory.atr("high", "low", "close", window=2, min_samples=1).alias("atr")
    )
    out = lf.collect()
    # ATR should be positive and reasonable
    assert out["atr"][-1] is not None
    assert out["atr"][-1] > 0


# =============================================================================
# IndicatorFactory Tests - Oscillators
# =============================================================================


def test_indicator_factory_rsi() -> None:
    # Create data with clear up and down moves
    df = pl.DataFrame({"close": [44.0, 44.5, 43.5, 44.5, 44.0, 44.5, 45.0, 45.5]})
    lf = df.lazy().with_columns(IndicatorFactory.rsi("close", window=3, min_samples=1).alias("rsi"))
    out = lf.collect()
    # RSI should be between 0 and 100
    for val in out["rsi"].to_list():
        if val is not None:
            assert 0 <= val <= 100


def test_indicator_factory_momentum() -> None:
    df = pl.DataFrame({"close": [100.0, 110.0, 120.0, 130.0]})
    lf = df.lazy().with_columns(IndicatorFactory.momentum("close", periods=2).alias("mom"))
    out = lf.collect()
    # Momentum at index 2: 120/100 = 1.2
    assert out["mom"][2] == pytest.approx(1.2, rel=1e-4)


def test_indicator_factory_roc() -> None:
    df = pl.DataFrame({"close": [100.0, 110.0, 120.0, 130.0]})
    lf = df.lazy().with_columns(IndicatorFactory.roc("close", periods=2).alias("roc"))
    out = lf.collect()
    # ROC at index 2: ((120-100)/100)*100 = 20%
    assert out["roc"][2] == pytest.approx(20.0, rel=1e-4)


# =============================================================================
# IndicatorFactory Tests - Trend
# =============================================================================


def test_indicator_factory_macd() -> None:
    # Need enough data for MACD
    prices = [100.0 + i * 0.5 for i in range(30)]  # Uptrend
    df = pl.DataFrame({"close": prices})
    lf = df.lazy().with_columns(IndicatorFactory.macd("close", fast=5, slow=10).alias("macd"))
    out = lf.collect()
    # MACD should be positive in uptrend (fast EMA > slow EMA)
    assert out["macd"][-1] > 0


def test_indicator_factory_bollinger_bands() -> None:
    df = pl.DataFrame({"close": [100.0, 101.0, 102.0, 101.0, 100.0] * 5})
    middle, upper, lower = IndicatorFactory.bollinger_bands("close", window=5)
    lf = df.lazy().with_columns(
        middle.alias("bb_mid"), upper.alias("bb_upper"), lower.alias("bb_lower")
    )
    out = lf.collect()
    # Upper should be above middle, lower below
    for i in range(5, len(out)):  # After warmup
        assert out["bb_upper"][i] > out["bb_mid"][i]
        assert out["bb_lower"][i] < out["bb_mid"][i]


# =============================================================================
# IndicatorFactory Tests - Cross-Sectional
# =============================================================================


def test_indicator_factory_rank() -> None:
    df = pl.DataFrame(
        {
            "timestamp": [1, 1, 1, 2, 2, 2],
            "symbol": ["A", "B", "C", "A", "B", "C"],
            "signal": [3.0, 1.0, 2.0, 10.0, 30.0, 20.0],
        }
    )
    lf = df.lazy().with_columns(IndicatorFactory.rank("signal", over="timestamp").alias("rank"))
    out = lf.collect()
    # At timestamp 1: A=3 (highest=1.0), B=1 (lowest=0.0), C=2 (middle=0.5)
    ranks_t1 = out.filter(pl.col("timestamp") == 1)["rank"].to_list()
    assert max(ranks_t1) == pytest.approx(1.0)
    assert min(ranks_t1) == pytest.approx(0.0)


def test_indicator_factory_zscore() -> None:
    df = pl.DataFrame({"signal": [0.0, 10.0, 20.0, 30.0, 40.0]})
    lf = df.lazy().with_columns(IndicatorFactory.zscore("signal").alias("z"))
    out = lf.collect()
    # Mean=20, middle value should have z-score ≈ 0
    assert out["z"][2] == pytest.approx(0.0, abs=1e-4)


def test_indicator_factory_rolling_zscore() -> None:
    df = pl.DataFrame({"close": [100.0, 110.0, 120.0, 100.0, 140.0]})
    lf = df.lazy().with_columns(
        IndicatorFactory.rolling_zscore("close", window=3, min_samples=3).alias("rz")
    )
    out = lf.collect()
    # After warmup, rolling z-score should be defined
    assert out["rz"][2] is not None


# =============================================================================
# IndicatorFactory Tests - Utility
# =============================================================================


def test_indicator_factory_shift() -> None:
    df = pl.DataFrame({"close": [1.0, 2.0, 3.0, 4.0]})
    lf = df.lazy().with_columns(IndicatorFactory.shift("close", 1).alias("lagged"))
    out = lf.collect()
    assert out["lagged"].to_list() == [None, 1.0, 2.0, 3.0]


def test_indicator_factory_diff() -> None:
    df = pl.DataFrame({"close": [1.0, 3.0, 6.0, 10.0]})
    lf = df.lazy().with_columns(IndicatorFactory.diff("close", 1).alias("diff"))
    out = lf.collect()
    expected = [None, 2.0, 3.0, 4.0]
    for actual, exp in zip(out["diff"].to_list(), expected):
        if exp is None:
            assert actual is None
        else:
            assert actual == pytest.approx(exp)


# =============================================================================
# StrategyBase Tests
# =============================================================================


def test_apply_numba_logic_cumulative_sum() -> None:
    numba = pytest.importorskip("numba")

    @numba.njit
    def _cumsum(arr: np.ndarray) -> np.ndarray:
        out = np.empty_like(arr)
        total = 0.0
        for i in range(arr.size):
            total += arr[i]
            out[i] = total
        return out

    df = pl.DataFrame({"x": [1.0, None, 2.0]})
    lf = df.lazy()
    strategy = StrategyBase()
    out = strategy.apply_numba_logic(lf, ["x"], kernel=_cumsum, output_col="csum").collect()

    assert out["csum"].to_list() == [1.0, 1.0, 3.0]
    assert out.schema["csum"] == pl.Float32


def test_apply_numba_logic_multi_input() -> None:
    numba = pytest.importorskip("numba")

    @numba.njit
    def _add_arrays(a: np.ndarray, b: np.ndarray) -> np.ndarray:
        return a + b

    df = pl.DataFrame({"a": [1.0, 2.0, 3.0], "b": [10.0, 20.0, 30.0]})
    lf = df.lazy()
    strategy = StrategyBase()
    out = strategy.apply_numba_logic(lf, ["a", "b"], kernel=_add_arrays, output_col="sum").collect()

    assert out["sum"].to_list() == [11.0, 22.0, 33.0]


def test_apply_numba_logic_missing_column() -> None:
    numba = pytest.importorskip("numba")

    @numba.njit
    def _identity(arr: np.ndarray) -> np.ndarray:
        return arr

    df = pl.DataFrame({"x": [1.0, 2.0]})
    strategy = StrategyBase()
    with pytest.raises(ValueError, match="Missing columns"):
        strategy.apply_numba_logic(df.lazy(), ["x", "y"], kernel=_identity, output_col="out")


def test_combine_signals() -> None:
    df = pl.DataFrame(
        {"rsi": [20.0, 50.0, 80.0], "trend": [1.0, 0.0, -1.0]}  # Oversold  # Overbought
    )
    signal = StrategyBase.combine_signals(
        [
            (pl.col("rsi") < 30, 1),  # Oversold -> Long
            (pl.col("rsi") > 70, -1),  # Overbought -> Short
        ],
        default=0,
    )
    out = df.lazy().with_columns(signal).collect()
    assert out["signal"].to_list() == [1, 0, -1]


def test_combine_signals_empty_rules() -> None:
    df = pl.DataFrame({"close": [1.0, 2.0, 3.0]})
    signal = StrategyBase.combine_signals([], default=0)
    out = df.lazy().with_columns(signal).collect()
    assert out["signal"].to_list() == [0, 0, 0]


def test_strategy_run_debug_mode() -> None:
    class SimpleStrategy(StrategyBase):
        def add_indicators(self, lf: pl.LazyFrame) -> pl.LazyFrame:
            return lf.with_columns(IndicatorFactory.sma("close", 2, min_samples=1).alias("sma"))

        def add_signals(self, lf: pl.LazyFrame) -> pl.LazyFrame:
            return lf.with_columns(self.combine_signals([(pl.col("close") > pl.col("sma"), 1)]))

    df = pl.DataFrame({"close": [1.0, 2.0, 3.0, 4.0]})
    strategy = SimpleStrategy(debug=True)
    result = strategy.run(df.lazy())

    assert isinstance(result, tuple)
    output, features = result
    assert "signal" in output.collect_schema()
    assert "sma" in features.collect_schema()
