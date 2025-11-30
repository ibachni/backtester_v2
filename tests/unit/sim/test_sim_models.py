from decimal import Decimal

import pytest

from backtester.sim.sim_models import FixedBps, MakerTakerBps, SlippageModel
from backtester.types.types import Fill, Liquidity, Market, Side


def _fill(*, side: Side, price: str, qty: str, liquidity: Liquidity) -> Fill:
    return Fill(
        fill_id="f1",
        order_id="o1",
        symbol="BTCUSDT",
        market=Market.SPOT,
        qty=Decimal(qty),
        price=Decimal(price),
        side=side,
        ts=0,
        venue="paper",
        liquidity_flag=liquidity,
        fees_explicit=Decimal("0"),
        rebates=Decimal("0"),
        slippage_components="{}",
        tags=[],
    )


def test_fixed_bps_fee_uses_notional() -> None:
    fill = _fill(side=Side.BUY, price="100", qty="2", liquidity=Liquidity.TAKER)
    fee = FixedBps(bps=10).fee(fill)  # 0.10% of 200 = 0.2
    assert fee == pytest.approx(0.2)


def test_maker_taker_bps_handles_sell_qty() -> None:
    fill = _fill(side=Side.SELL, price="50", qty="-3", liquidity=Liquidity.TAKER)
    fee = MakerTakerBps(maker_bps=5, taker_bps=10).fee(fill)  # 0.10% of 150 = 0.15
    assert fee == pytest.approx(0.15)


def test_slippage_model_adds_and_subtracts_bps() -> None:
    model = SlippageModel(bps=25)  # 0.25%
    buy_px = model.apply(Side.BUY, Decimal("100"))
    sell_px = model.apply(Side.SELL, Decimal("100"))

    assert buy_px == Decimal("100.25")
    assert sell_px == Decimal("99.75")
