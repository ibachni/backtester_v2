# @dataclass
# class _DummyFeed:
#     clock: SimClock

#     def __post_init__(self) -> None:
#         self._audit = None
#         self._subs: list[str] = []

#     def start(self) -> None:  # pragma: no cover - helper
#         return

#     def stop(self) -> None:  # pragma: no cover - helper
#         return

#     def iter_frames(self):  # pragma: no cover - helper
#         if False:
#             yield


# async def _run_performance_task_flow() -> None:
#     clock = SimClock(start_ms=0)
#     feed = _DummyFeed(clock=clock)
#     engine = BacktestEngine(clock=clock, feed=feed)
#     sub = SimpleNamespace(queue=asyncio.Queue())
#     task = asyncio.create_task(engine.performance_task(sub))

#     candle = Candle(
#         symbol="BTCUSDT",
#         timeframe="1h",
#         start_ms=0,
#         end_ms=3_600_000,
#         open=1.0,
#         high=1.0,
#         low=1.0,
#         close=1.0,
#         volume=1.0,
#         trades=1,
#         is_final=True,
#     )
#     snapshot = PortfolioSnapshot(
#         ts=3_600_000,
#         base_ccy="USDC",
#         cash=Decimal("1000"),
#         equity=Decimal("1000"),
#         upnl=Decimal("0"),
#         rpnl=Decimal("0"),
#         gross_exposure=Decimal("0"),
#         net_exposure=Decimal("0"),
#         fees_paid=Decimal("0"),
#         positions=tuple(),
#     )

#     await sub.queue.put(Envelope(topic=T_CANDLES, seq=1, ts=0, payload=candle))
#     await sub.queue.put(Envelope(topic=T_ACCOUNT_SNAPSHOT, seq=2, ts=0, payload=snapshot))

#     await sub.queue.put(
#         Envelope(
#             topic=T_CONTROL,
#             seq=3,
#             ts=0,
#             payload=ControlEvent(type="eof", source=T_CANDLES, ts_utc=0),
#         )
#     )
#     await asyncio.sleep(0)
#     assert not task.done()

#     final_snapshot = snapshot
#     await sub.queue.put(Envelope(topic=T_ACCOUNT_SNAPSHOT, seq=4, ts=0, payload=final_snapshot))
#     await sub.queue.put(
#         Envelope(
#             topic=T_CONTROL,
#             seq=5,
#             ts=0,
#             payload=ControlEvent(type="eof", source=T_ACCOUNT_SNAPSHOT, ts_utc=0),
#         )
#     )

#     await asyncio.wait_for(task, timeout=1)
#     engine._audit.close()


# def test_performance_task_waits_for_account_flush() -> None:
#     asyncio.run(_run_performance_task_flow())
