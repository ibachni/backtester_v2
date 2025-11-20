import asyncio
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import pytest

from backtester.config.configs import AuditConfig, RunContext
from backtester.core.audit import AuditWriter
from backtester.core.backtest_engine_new import (
    T_ACCOUNT_SNAPSHOT,
    T_CANDLES,
    T_FILLS,
    T_ORDERS_ACK,
    T_ORDERS_CANCELED,
    T_TRADE_EVENT,
)
from backtester.core.bus import Envelope
from backtester.types.types import (
    AckStatus,
    Candle,
    ControlEvent,
    Fill,
    Liquidity,
    Market,
    MarketOrderIntent,
    OrderAck,
    PortfolioSnapshot,
    PositionView,
    Side,
    TimeInForce,
    TradeEvent,
)


@pytest.fixture
def run_ctx():
    return RunContext(
        run_id="test-run-123",
        seed=42,
        git_sha="abc123",
        allow_net=False,
    )


@pytest.fixture
def audit_cfg():
    return AuditConfig(
        log_dir=Path("test_output/audit"),
        capture_market_data=False,
    )


@pytest.fixture
def audit_writer(run_ctx, audit_cfg, tmp_path):
    # Override log_dir to use tmp_path
    cfg = AuditConfig(
        log_dir=Path(str(tmp_path / "audit")),
        capture_market_data=audit_cfg.capture_market_data,
    )
    writer = AuditWriter(run_ctx=run_ctx, cfg=cfg)
    yield writer
    writer.stop()


@pytest.fixture
def sample_fill():
    return Fill(
        fill_id="fill-001",
        order_id="order-001",
        symbol="BTCUSDT",
        market=Market.SPOT,
        qty=Decimal("1.5"),
        price=Decimal("50000.00"),
        side=Side.BUY,
        ts=1700000000000,
        venue="paper",
        liquidity_flag=Liquidity.TAKER,
        fees_explicit=Decimal("0.1"),
        rebates=Decimal("0"),
        slippage_components="",
        tags=["test"],
    )


@pytest.fixture
def sample_candle():
    return Candle(
        symbol="BTCUSDT",
        timeframe="1h",
        start_ms=1700000000000,
        end_ms=1700003600000,
        open=50000,
        high=51000,
        low=49500,
        close=50500,
        volume=100.5,
        trades=150,
        is_final=True,
    )


@pytest.fixture
def sample_order_ack():
    return OrderAck(
        intent_id="intent-001",
        strategy_id="test-strategy",
        component="exec_sim",
        symbol="BTCUSDT",
        side=Side.BUY,
        market=Market.SPOT,
        status=AckStatus.ACCEPTED,
        reason_code=None,
        reason=None,
        exchange_order_id="exchange-123",
        router_order_id="router-456",
        ts_utc=1700000000000,
        venue_ts=1700000000100,
        venue="paper",
        seq=1,
        tags={"key": "value"},
    )


@pytest.fixture
def sample_portfolio_snapshot():
    return PortfolioSnapshot(
        ts=1700000000000,
        base_ccy="USDT",
        cash=Decimal("10000.00"),
        equity=Decimal("12000.00"),
        upnl=Decimal("500.00"),
        rpnl=Decimal("1500.00"),
        gross_exposure=Decimal("5000.00"),
        net_exposure=Decimal("2000.00"),
        fees_paid=Decimal("10.00"),
        positions=(
            PositionView(
                symbol="BTCUSDT",
                qty=Decimal("1.0"),
                avg_cost=Decimal("50000"),
                last_price=Decimal("52000"),
                market_value=Decimal("20000"),
                unrealized_pnl=Decimal("2000"),
                realized_pnl=Decimal("0"),
            ),
        ),
    )


# --- Initialization & Lifecycle ---


def test_init_creates_directory_structure(run_ctx, tmp_path):
    cfg = AuditConfig(log_dir=Path(str(tmp_path / "audit")))
    writer = AuditWriter(run_ctx=run_ctx, cfg=cfg)

    expected_root = tmp_path / "audit" / run_ctx.run_id
    assert expected_root.parent.exists()

    writer.stop()


def test_start_launches_worker_thread(audit_writer):
    audit_writer.start()
    assert audit_writer._worker_thread.is_alive()


def test_stop_terminates_worker_gracefully(audit_writer):
    audit_writer.start()
    audit_writer.stop()
    assert not audit_writer._worker_thread.is_alive()


def test_run_id_property_returns_context_run_id(audit_writer, run_ctx):
    assert audit_writer.run_id == run_ctx.run_id


# --- Event Routing to Streams ---


@pytest.mark.asyncio
async def test_fill_event_routes_to_ledger_jsonl(audit_writer, sample_fill, tmp_path):
    audit_writer.start()

    envelope = Envelope(
        topic=T_FILLS,
        seq=1,
        ts=1700000000000,
        payload=sample_fill,
    )

    await audit_writer.on_event(envelope)
    await asyncio.sleep(0.1)  # Let worker process

    audit_writer.stop()

    ledger_path = tmp_path / "audit" / audit_writer.run_id / "ledger.jsonl"
    assert ledger_path.exists()

    with ledger_path.open() as f:
        record = json.loads(f.readline())

    assert record["fill_id"] == "fill-001"
    assert record["order_id"] == "order-001"
    assert record["symbol"] == "BTCUSDT"
    assert record["qty"] == "1.5"
    assert record["price"] == "50000.00"


@pytest.mark.asyncio
async def test_order_ack_routes_to_ledger_jsonl(audit_writer, sample_order_ack, tmp_path):
    audit_writer.start()

    envelope = Envelope(
        topic=T_ORDERS_ACK,
        seq=1,
        ts=1700000000000,
        payload=sample_order_ack,
    )

    await audit_writer.on_event(envelope)
    await asyncio.sleep(0.1)

    audit_writer.stop()

    ledger_path = tmp_path / "audit" / audit_writer.run_id / "ledger.jsonl"
    assert ledger_path.exists()

    with ledger_path.open() as f:
        record = json.loads(f.readline())

    assert record["intent_id"] == "intent-001"
    assert record["status"] == "AckStatus.ACCEPTED"
    assert record["exchange_order_id"] == "exchange-123"
    assert record["tags"]["key"] == "value"


@pytest.mark.asyncio
async def test_canceled_order_routes_to_ledger_jsonl(audit_writer, tmp_path):
    audit_writer.start()

    intent = MarketOrderIntent(
        symbol="BTCUSDT",
        market=Market.SPOT,
        side=Side.SELL,
        id="order-cancel-001",
        ts_utc=1700000000000,
        qty=Decimal("2.0"),
        strategy_id="test-strategy",
        tif=TimeInForce.GTC,
        reduce_only=False,
        tags={},
    )

    envelope = Envelope(
        topic=T_ORDERS_CANCELED,
        seq=1,
        ts=1700000000000,
        payload=intent,
    )

    await audit_writer.on_event(envelope)
    await asyncio.sleep(0.1)

    audit_writer.stop()

    ledger_path = tmp_path / "audit" / audit_writer.run_id / "ledger.jsonl"
    assert ledger_path.exists()

    with ledger_path.open() as f:
        record = json.loads(f.readline())

    assert record["id"] == "order-cancel-001"
    assert record["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_trade_event_routes_to_ledger_jsonl(audit_writer, tmp_path):
    audit_writer.start()

    trade = TradeEvent(
        ts=1700000000000,
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=1,
        price=50000,
        fee=50,
        venue="paper",
        liquidity_flag=Liquidity.TAKER,
        idempotency_key="trade-001",
    )

    envelope = Envelope(
        topic=T_TRADE_EVENT,
        seq=1,
        ts=1700000000000,
        payload=trade,
    )

    await audit_writer.on_event(envelope)
    await asyncio.sleep(0.1)

    audit_writer.stop()

    ledger_path = tmp_path / "audit" / audit_writer.run_id / "ledger.jsonl"
    assert ledger_path.exists()

    with ledger_path.open() as f:
        record = json.loads(f.readline())

    assert record["idempotency_key"] == "trade-001"
    assert record["qty"] == 1


@pytest.mark.asyncio
async def test_portfolio_snapshot_routes_to_metrics_csv(
    audit_writer, sample_portfolio_snapshot, tmp_path
):
    audit_writer.start()

    envelope = Envelope(
        topic=T_ACCOUNT_SNAPSHOT,
        seq=1,
        ts=1700000000000,
        payload=sample_portfolio_snapshot,
    )

    await audit_writer.on_event(envelope)
    await asyncio.sleep(0.1)

    audit_writer.stop()

    metrics_path = tmp_path / "audit" / audit_writer.run_id / "metrics.csv"
    assert metrics_path.exists()

    with metrics_path.open() as f:
        lines = f.readlines()

    assert len(lines) == 2  # Verify!
    assert "equity" in lines[0]
    assert "12000.00" in lines[1]


@pytest.mark.asyncio
async def test_debug_topics_route_to_debug_jsonl(audit_writer, tmp_path):
    audit_writer.start()

    control = ControlEvent(
        type="eof",
        source="candles",
        ts_utc=1700000000000,
        run_id="test-run-123",
        details={"foo": "bar"},
    )

    envelope = Envelope(
        topic="control",
        seq=1,
        ts=1700000000000,
        payload=control,
    )

    await audit_writer.on_event(envelope)
    await asyncio.sleep(0.1)

    audit_writer.stop()

    debug_path = tmp_path / "audit" / audit_writer.run_id / "debug.jsonl"
    assert debug_path.exists()

    with debug_path.open() as f:
        record = json.loads(f.readline())

    assert record["topic"] == "control"
    assert "ts_wall" in record
    assert record["payload"]["type"] == "eof"


@pytest.mark.asyncio
async def test_candle_not_written_when_capture_data_false(audit_writer, sample_candle, tmp_path):
    audit_writer.start()

    envelope = Envelope(
        topic=T_CANDLES,
        seq=1,
        ts=1700000000000,
        payload=sample_candle,
    )

    await audit_writer.on_event(envelope)
    await asyncio.sleep(0.1)

    audit_writer.stop()

    market_data_path = tmp_path / "audit" / audit_writer.run_id / "market_data.jsonl"
    assert not market_data_path.exists()


@pytest.mark.asyncio
async def test_candle_written_when_capture_data_true(run_ctx, sample_candle, tmp_path):
    cfg = AuditConfig(
        log_dir=Path(str(tmp_path / "audit")),
        capture_market_data=True,
    )
    writer = AuditWriter(run_ctx=run_ctx, cfg=cfg)
    writer.start()

    envelope = Envelope(
        topic=T_CANDLES,
        seq=1,
        ts=1700000000000,
        payload=sample_candle,
    )

    await writer.on_event(envelope)
    await asyncio.sleep(0.1)

    writer.stop()

    market_data_path = tmp_path / "audit" / run_ctx.run_id / "market_data.jsonl"
    assert market_data_path.exists()

    with market_data_path.open() as f:
        record = json.loads(f.readline())

    assert record["topic"] == T_CANDLES
    assert record["payload"]["symbol"] == "BTCUSDT"


# --- Payload Conversion ---


def test_fill_to_payload_contains_all_fields(audit_writer, sample_fill):
    payload = audit_writer.fill_to_payload(sample_fill)

    assert payload["fill_id"] == "fill-001"
    assert payload["order_id"] == "order-001"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["market"] == "Market.SPOT"
    assert payload["qty"] == "1.5"
    assert payload["price"] == "50000.00"
    assert payload["side"] == "buy"
    assert payload["ts"] == 1700000000000
    assert payload["venue"] == "paper"
    assert payload["liquidity_flag"] == "Liquidity.TAKER"
    assert payload["fees_explicit"] == "0.1"
    assert payload["rebates"] == "0"
    assert payload["tags"] == ["test"]


def test_candle_to_payload_contains_all_fields(audit_writer, sample_candle):
    payload = audit_writer.candle_to_payload(sample_candle)

    assert payload["symbol"] == "BTCUSDT"
    assert payload["timeframe"] == "1h"
    assert payload["start_ms"] == 1700000000000
    assert payload["end_ms"] == 1700003600000
    assert payload["open"] == 50000.0
    assert payload["high"] == 51000.0
    assert payload["low"] == 49500.0
    assert payload["close"] == 50500.0
    assert payload["volume"] == 100.5
    assert payload["trades"] == 150
    assert payload["is_final"] is True


def test_portfolio_to_payload_contains_all_fields(audit_writer, sample_portfolio_snapshot):
    payload = audit_writer.portfolio_to_payload(sample_portfolio_snapshot)

    assert payload["ts"] == 1700000000000
    assert payload["base_ccy"] == "USDT"
    assert payload["cash"] == Decimal("10000.00")
    assert payload["equity"] == Decimal("12000.00")
    assert payload["upnl"] == Decimal("500.00")
    assert payload["rpnl"] == Decimal("1500.00")
    assert payload["gross_exposure"] == Decimal("5000.00")
    assert payload["net_exposure"] == Decimal("2000.00")
    assert payload["fees_paid"] == Decimal("10.00")
    assert len(payload["positions"]) == 1


# --- Concurrent Event Processing ---


@pytest.mark.asyncio
async def test_multiple_events_processed_in_order(audit_writer, sample_fill, tmp_path):
    audit_writer.start()

    # Send multiple events
    for i in range(5):
        fill = Fill(
            fill_id=f"fill-{i:03d}",
            order_id=f"order-{i:03d}",
            symbol="BTCUSDT",
            market=Market.SPOT,
            qty=Decimal("1.0"),
            price=Decimal("50000.00"),
            side=Side.BUY,
            ts=1700000000000 + i,
            venue="paper",
            liquidity_flag=Liquidity.TAKER,
            fees_explicit=Decimal("0.1"),
            rebates=Decimal("0"),
            slippage_components="",
            tags=[],
        )

        envelope = Envelope(
            topic=T_FILLS,
            seq=i + 1,
            ts=1700000000000 + i,
            payload=fill,
        )

        await audit_writer.on_event(envelope)

    await asyncio.sleep(0.2)
    audit_writer.stop()

    ledger_path = tmp_path / "audit" / audit_writer.run_id / "ledger.jsonl"

    with ledger_path.open() as f:
        records = [json.loads(line) for line in f]

    assert len(records) == 5
    for i, record in enumerate(records):
        assert record["fill_id"] == f"fill-{i:03d}"
        assert record["order_id"] == f"order-{i:03d}"


@pytest.mark.asyncio
async def test_worker_thread_does_not_crash_on_exception(audit_writer, tmp_path):
    """Worker should log errors but continue processing."""
    audit_writer.start()

    # Create malformed envelope that might cause issues
    @dataclass
    class BadPayload:
        bad_field: object = object()  # Non-serializable

    envelope = Envelope(
        topic="orders.intent",
        seq=1,
        ts=1700000000000,
        payload=BadPayload(),
    )

    # Should not crash
    await audit_writer.on_event(envelope)
    await asyncio.sleep(0.1)

    # Worker should still be alive
    assert audit_writer._worker_thread.is_alive()

    audit_writer.stop()


# --- CSV Writer Initialization & Headers ---


@pytest.mark.asyncio
async def test_csv_headers_written_on_first_record(
    audit_writer, sample_portfolio_snapshot, tmp_path
):
    audit_writer.start()

    envelope = Envelope(
        topic=T_ACCOUNT_SNAPSHOT,
        seq=1,
        ts=1700000000000,
        payload=sample_portfolio_snapshot,
    )

    await audit_writer.on_event(envelope)
    await asyncio.sleep(0.1)

    audit_writer.stop()

    metrics_path = tmp_path / "audit" / audit_writer.run_id / "metrics.csv"

    with metrics_path.open() as f:
        header = f.readline().strip()

    expected_fields = [
        "ts",
        "base_ccy",
        "cash",
        "equity",
        "upnl",
        "rpnl",
        "gross_exposure",
        "net_exposure",
        "fees_paid",
        "positions",
    ]

    for field in expected_fields:
        assert field in header


# --- File Cleanup & Resource Management ---


def test_stop_closes_all_file_handles(audit_writer, tmp_path):
    audit_writer.start()

    # Manually trigger some file opens
    audit_writer._files["test.jsonl"] = (tmp_path / "test.jsonl").open("ab")

    audit_writer.stop()

    # All files should be closed
    for fh in audit_writer._files.values():
        assert fh.closed
