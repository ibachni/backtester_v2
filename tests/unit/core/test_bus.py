import asyncio
import time

import pytest

from backtester.adapters.types import Candle
from backtester.core.bus import (
    Bus,
    BusError,
    IfExistsOptions,
    SubscriptionConfig,
    TopicConfig,
    TopicPriority,
)

# --- Topic Management ---


@pytest.mark.asyncio
async def test_register_topic_and_stats_defaults():
    bus = Bus()
    await bus.register_topic("btc.spot")

    # Topic config
    ts = bus._topics["btc.spot"]
    assert ts.name == "btc.spot"
    assert type(ts.config) is TopicConfig
    # ts: default config
    assert ts.config.schema is None
    assert ts.config.priority == TopicPriority.NORMAL
    assert ts.config.coalesce_key is None
    assert ts.config.validate_schema is True

    # Other ts fields
    assert ts.subscribers == set()
    assert ts.high_seq == 0
    assert ts._pub_count == 0
    assert ts._last_publish_utc is None
    assert ts._last_publish_mono is None
    assert ts._ema_rate == 0

    # Stats
    stats = await bus.topic_stats("btc.spot")
    assert stats.name == "btc.spot"
    assert stats.priority == TopicPriority.NORMAL
    assert stats.high_seq == 0
    assert stats.subscribers == 0
    assert stats.publish_count == 0
    assert stats.publish_rate_eps >= 0.0
    assert stats.last_publish_utc is None
    assert stats.low_watermark == 0
    assert stats.high_watermark == 0


@pytest.mark.asyncio
async def test_topic_stats_unknown_raises():
    bus = Bus()
    with pytest.raises(KeyError):
        await bus.topic_stats(topic="unknown.topic")


@pytest.mark.asyncio
async def test_subscribe_unknown_topic_raises_bus_error():
    bus = Bus()
    cfg = SubscriptionConfig(topics={"missing.topic"})
    with pytest.raises(BusError):
        await bus.subscribe(name="test_subscriber", bus_sub_config=cfg)


@pytest.mark.asyncio
async def test_register_topic_validate_ok_same_config():
    bus = Bus()
    await bus.register_topic(
        "btc.spot",
        priority=TopicPriority.HIGH,
        validate_schema=True,
        if_exists=IfExistsOptions.VALIDATE,
    )

    # re-register same topic (under validate)
    await bus.register_topic(
        "btc.spot",
        priority=TopicPriority.HIGH,
        validate_schema=True,
        if_exists=IfExistsOptions.VALIDATE,
    )

    # Stats
    stats = await bus.topic_stats("btc.spot")
    assert stats.name == "btc.spot"


@pytest.mark.asyncio
async def test_register_topic_validate_schema_mismatch_raises():
    bus = Bus()

    await bus.register_topic("btc.spot", schema=int, validate_schema=True)
    with pytest.raises(TypeError):
        await bus.register_topic(
            "btc.spot", schema=float, validate_schema=True, if_exists=IfExistsOptions.VALIDATE
        )


@pytest.mark.asyncio
async def test_register_topic_merge_updates_config():
    # Expected behavior of merge: No none provided values of re-register
    # overwrittes existing topic
    bus = Bus()
    await bus.register_topic(
        "btc.spot",
        schema=None,
        priority=TopicPriority.HIGH,
        validate_schema=False,
        # if_exists can be arbitrary here, since no topic has been registered
        if_exists=IfExistsOptions.IGNORE,
    )

    await bus.register_topic(
        "btc.spot",
        schema=float,
        priority=TopicPriority.LOW,
        if_exists=IfExistsOptions.MERGE,
    )

    ts = bus._topics["btc.spot"]
    # config schema, default buffer size, priority not none, hence overwritten
    assert ts.config.schema is float
    assert ts.config.priority == TopicPriority.LOW
    # validate_schema not provided in re-registering,
    # hence, default value "true" overwrittes "False"
    assert ts.config.validate_schema is True


@pytest.mark.asyncio
async def test_register_topic_ignore_keeps_existing_config():
    bus = Bus()

    await bus.register_topic(
        "btc.spot",
        schema=None,
        priority=TopicPriority.HIGH,
        validate_schema=False,
    )

    await bus.register_topic(
        "btc.spot",
        schema=float,
        priority=TopicPriority.LOW,
        if_exists=IfExistsOptions.IGNORE,
    )

    ts = bus._topics["btc.spot"]
    # config schema, default buffer size, priority not none, hence overwritten
    assert ts.config.schema is None
    assert ts.config.priority == TopicPriority.HIGH
    assert ts.config.validate_schema is False


@pytest.mark.asyncio
async def test_register_then_subscribe_reflects_in_topic_stats():
    bus = Bus()
    await bus.register_topic(
        "btc.spot",
        schema=float,
        priority=TopicPriority.HIGH,
        validate_schema=False,
    )
    strategy_config = SubscriptionConfig({"btc.spot"})
    await bus.subscribe("strategy", strategy_config)
    ts = bus._topics["btc.spot"]

    assert len(ts.subscribers) == 1
    assert ts.high_seq == 0
    assert ts._pub_count == 0
    assert ts._last_publish_utc is None
    assert ts._last_publish_mono is None
    assert ts._ema_rate == 0

    topic_stats = await bus.topic_stats("btc.spot")

    assert topic_stats.subscribers == 1
    assert topic_stats.high_seq == 0
    assert topic_stats.name == "btc.spot"


# --- B) Subscribing / Unsubscribing ---

"""
- Bus state
- Create BusSubConfig
- Create SubScription objects (
    - check init (bus, name, topics (from sub config), queue, enqueued seq, drops, delivered, )
later:
    - check pause/ resume (receive not receive envs)
    - depth
    - lag
- async def subscribing
    - adding two identical subscribers (no effect since .add())
    - check self._subscriptions
    - check self._topics[topic].subscribers
- BusError on topic that has not been registered
-

TODO
Buffer size defaulting
Setup: SubscriptionConfig with buffer_size=None.
Expect: sub.queue.maxsize == BusConfig.default_buffer_size.
Duplicate subscribe name allowed/handled?
Current code allows same name twice; add a test to document behavior
(either allow equals, or decide to enforce uniqueness later).


TODO
validate_schema=False permits any payload
Setup: topic with schema set but validate_schema=False; publish wrong type.
Expect: no error; enqueued as normal.
validate_schema=True rejects wrong payload
You have coverage via test_publish_validate_schema; add negative for nested/multiple payloads if
needed.


"""


def test_creating_bus_sub_config():
    new = SubscriptionConfig(topics={"btc.spot", "eth.spot"}, buffer_size=2048)
    assert new.topics == {"btc.spot", "eth.spot"}
    assert new.buffer_size == 2048


@pytest.mark.asyncio
async def test_subscribing():
    bus = Bus()
    await bus.register_topic(name="btc.spot", priority=TopicPriority.HIGH, validate_schema=True)

    await bus.register_topic(name="eth.spot", priority=TopicPriority.HIGH, validate_schema=True)

    sub_config = SubscriptionConfig(topics={"btc.spot", "eth.spot"}, buffer_size=2048)
    await bus.subscribe(name="strategy", bus_sub_config=sub_config)

    sub = bus._subscriptions
    topics = bus._topics

    # 1. Checking Bus
    assert topics["btc.spot"].subscribers == topics["eth.spot"].subscribers == sub
    assert len(sub) == 1

    # 2. Checking Subscription Object
    sub = bus._subscriptions.pop()
    assert sub.name == "strategy"
    assert sub._closed is False
    assert sub.topics == {"btc.spot", "eth.spot"}


@pytest.mark.asyncio
async def test_subscribing_error_unknown_topic():
    bus = Bus()
    await bus.register_topic(name="btc.spot", priority=TopicPriority.HIGH, validate_schema=True)

    await bus.register_topic(name="eth.spot", priority=TopicPriority.HIGH, validate_schema=True)

    sub_config = SubscriptionConfig(topics={"shib.spot", "eth.spot"}, buffer_size=2048)
    with pytest.raises(BusError):
        await bus.subscribe(name="strategy", bus_sub_config=sub_config)


@pytest.mark.asyncio
async def test_unsubscribing():
    bus = Bus()
    await bus.register_topic(name="btc.spot", priority=TopicPriority.HIGH, validate_schema=True)

    await bus.register_topic(name="eth.spot", priority=TopicPriority.HIGH, validate_schema=True)

    sub_config = SubscriptionConfig(topics={"btc.spot", "eth.spot"}, buffer_size=2048)
    sub_config_eth = SubscriptionConfig(topics={"btc.spot"}, buffer_size=2048)
    sub_strategy = await bus.subscribe(name="strategy", bus_sub_config=sub_config)
    eth_only_strategy = await bus.subscribe(name="eth_only_strategy", bus_sub_config=sub_config_eth)

    await bus.unsubscribe(subscription=eth_only_strategy)

    # has been removed
    assert len(bus._subscriptions) == 1
    # Remaining strategy (in a set)
    assert next(iter(bus._topics["eth.spot"].subscribers)) == sub_strategy

    assert eth_only_strategy._closed is True
    assert eth_only_strategy._close_reason == "unsubscribe"


# --- C) Centralized per-topic sequencing / Publish and fan-out behavior ---
"""
- monontonic per-topic seq
    - publish N messages on one topic
        - Expect: topic_state.high_seq == N
        - Expect: Subscribers have seq 1,...,N in Order
- Independent Sequences across topics
    - each topics seq starts at 1 and increases independently
- Same seq to all subscribers: Two subs on the same topics.
    - For each published messages, same seq values and in FIFO order per sub
- No gaps/dups under concurrency
    - 2+ concurrent producers on the same topics
    - Expect: Multiset of that topic is 1,2,...,N, no gaps or duplicates
    - ordering per sub remains FIFO
- sequence unaffected by consumption
    - drain consuer queue after publishing
    - Expect: topig_state.high_seq remains N; consumption does not change per
    - topic sequence
- Publishing with zero subscribers
    - high_seq increments nevertheless. No enveloped are delivered
    - When subscriver joins later, enqueued_seq starts at 0;
    no retroactive delivery
"""


async def producer(bus: Bus, topic: str, n: int):
    for i in range(0, n):
        await bus.publish(topic=topic, ts_utc=int(time.time()), payload={"price": 100 + i})
        await asyncio.sleep(0.001)


candle = Candle(
    symbol="BTC",
    timeframe="1m",
    start_ms=1700000,
    end_ms=180000,
    open=100,
    high=110,
    low=83,
    close=101,
    volume=1001,
    trades=10,
    is_final=True,
)


@pytest.mark.asyncio
async def test_publish_n_messages():
    bus = Bus()

    # Topic Management
    await bus.register_topic(name="btc.spot", priority=TopicPriority.HIGH, validate_schema=False)
    await bus.register_topic(name="eth.spot", priority=TopicPriority.HIGH, validate_schema=False)
    await bus.register_topic(name="no_sub", priority=TopicPriority.HIGH, validate_schema=False)

    # Sub Configs
    sub_config = SubscriptionConfig(topics={"btc.spot", "eth.spot"}, buffer_size=100)
    sub_config_eth = SubscriptionConfig(topics={"btc.spot"}, buffer_size=100)

    # three Consumers / subscribers
    await bus.subscribe(name="strategy", bus_sub_config=sub_config)
    await bus.subscribe(name="eth_only_strategy", bus_sub_config=sub_config_eth)
    await bus.subscribe(name="other_strategy", bus_sub_config=sub_config_eth)

    # three producers, publishing to "btc.spot", "eth.spot" and "no_sub"
    await asyncio.gather(
        producer(bus, "btc.spot", 10),
        producer(bus, "eth.spot", 5),
        producer(bus, "no_sub", 1),
    )

    # Publish to unknown topic raises error
    with pytest.raises(BusError):
        await asyncio.create_task(producer(bus, "missing_topic", 1))


@pytest.mark.asyncio
async def test_publish_validate_schema():
    bus = Bus()
    await bus.register_topic(
        name="btc.spot", schema=Candle, priority=TopicPriority.HIGH, validate_schema=True
    )
    sub_config = SubscriptionConfig(topics={"btc.spot"}, buffer_size=100)
    await bus.subscribe(name="strategy", bus_sub_config=sub_config)

    # Publish with incorrect Schema
    with pytest.raises(BusError):
        await asyncio.create_task(producer(bus, "btc.spot", 1))

    # Publish with correct schema
    await asyncio.create_task(bus.publish("btc.spot", int(time.time()), candle))


@pytest.mark.asyncio
async def test_publish_per_topic_seq():
    """
    - monontonic per-topic seq
    - publish N messages on one topic
        - Expect: topic_state.high_seq == N
        - Expect: Subscribers have seq 1,...,N in Order
    - Independent Sequences across topics
        - each topics seq starts at 1 and increases independently
    """
    bus = Bus()

    # Topic Management
    await bus.register_topic(name="btc.spot", priority=TopicPriority.HIGH, validate_schema=False)
    await bus.register_topic(name="eth.spot", priority=TopicPriority.HIGH, validate_schema=False)
    await bus.register_topic(name="no_sub", priority=TopicPriority.HIGH, validate_schema=False)

    # Sub Configs
    sub_config = SubscriptionConfig(topics={"btc.spot", "eth.spot"}, buffer_size=100)
    sub_config_eth = SubscriptionConfig(topics={"eth.spot"}, buffer_size=100)

    # three Consumers / subscribers
    sub_strategy = await bus.subscribe(name="strategy", bus_sub_config=sub_config)
    eth_only_strategy = await bus.subscribe(name="eth_only_strategy", bus_sub_config=sub_config_eth)
    other_strategy = await bus.subscribe(name="other_strategy", bus_sub_config=sub_config_eth)

    # three producers, publishing to "btc.spot", "eth.spot" and "no_sub"
    await asyncio.gather(
        producer(bus, "btc.spot", 10),
        producer(bus, "eth.spot", 5),
        producer(bus, "no_sub", 1),
    )

    # check the per_topic seq (even for unsubscribed topics)
    assert bus._topics["btc.spot"].high_seq == 10
    assert bus._topics["eth.spot"].high_seq == 5
    assert bus._topics["no_sub"].high_seq == 1

    # queue parameters
    assert sub_strategy.queue.qsize() == 15
    first_item = sub_strategy.queue.get_nowait()
    assert first_item is not None
    assert first_item.seq == 1
    second_item = sub_strategy.queue.get_nowait()
    assert second_item is not None
    assert second_item.seq == 1
    assert sub_strategy.queue.qsize() == 13
    assert eth_only_strategy.queue.qsize() == 5
    assert other_strategy.queue.qsize() == 5

    # enqueued_seq (what has been delivered)

    assert sub_strategy._enqueued_seq["btc.spot"] == 10
    assert sub_strategy._enqueued_seq["eth.spot"] == 5


@pytest.mark.asyncio
async def test_publish_similar_seq():
    """
    - Same seq to all subscribers: Two subs on the same topics.
    - For each published messages, same seq values and in FIFO order per sub
    - sequence unaffected by consumption
    """
    bus = Bus()

    # Topic Management
    await bus.register_topic(name="btc.spot", priority=TopicPriority.HIGH, validate_schema=False)

    # Sub Configs
    sub_config = SubscriptionConfig(topics={"btc.spot"}, buffer_size=100)

    # two Consumers / subscribers
    sub_strategy = await bus.subscribe(name="strategy", bus_sub_config=sub_config)
    sub_strategy_2 = await bus.subscribe(name="strategy", bus_sub_config=sub_config)

    await asyncio.gather(
        producer(bus, "btc.spot", 10),
    )

    # check the per_topic seq (even for unsubscribed topics)
    assert bus._topics["btc.spot"].high_seq == 10

    # Verify that both subscribers have same sequence and items!
    for i in range(0, 10):
        sub_item = sub_strategy.queue.get_nowait()
        sub_item_2 = sub_strategy_2.queue.get_nowait()
        assert sub_item == sub_item_2

    # High_seq stays unaffected by draining
    assert bus._topics["btc.spot"].high_seq == 10


@pytest.mark.asyncio
async def test_publish_concurrent_publishers_on_same_topic():
    """
    - No gaps/dups under concurrency
    - 2+ concurrent producers on the same topics
    - Expect: Multiset of that topic is 1,2,...,N, no gaps or duplicates
    - ordering per sub remains FIFO
    """
    bus = Bus()

    # Topic Management
    await bus.register_topic(name="btc.spot", priority=TopicPriority.HIGH, validate_schema=False)

    # Sub Configs
    sub_config = SubscriptionConfig(topics={"btc.spot"}, buffer_size=100)

    # one consumer / subscriber
    sub_strategy = await bus.subscribe(name="strategy", bus_sub_config=sub_config)

    # two producers on the same topic
    await asyncio.gather(producer(bus, "btc.spot", 5), producer(bus, "btc.spot", 5))

    for i in range(1, 11):
        sub_item = sub_strategy.queue.get_nowait()
        assert sub_item is not None
        assert sub_item.seq == i


@pytest.mark.asyncio
async def test_publish_late_subscriber():
    """
    - Publishing with zero subscribers
    - high_seq increments nevertheless. No enveloped are delivered
    - When subscriver joins later, enqueued_seq starts at 0; no retroactive delivery
    """
    bus = Bus()

    # Topic Management
    await bus.register_topic(name="btc.spot", priority=TopicPriority.HIGH, validate_schema=False)

    # Sub Configs
    sub_config = SubscriptionConfig(topics={"btc.spot"}, buffer_size=100)

    # producer, no subscriber
    await asyncio.gather(producer(bus, "btc.spot", 5))

    assert bus._topics["btc.spot"].high_seq == 5

    # late subscriber
    sub_strategy = await bus.subscribe(name="strategy", bus_sub_config=sub_config)

    assert sub_strategy.queue.qsize() == 0
    assert sub_strategy._enqueued_seq["btc.spot"] == 0

    await asyncio.gather(producer(bus, "btc.spot", 2))

    assert sub_strategy.queue.qsize() == 2
    # highest sequence number that has been successfully enqueued
    assert sub_strategy._enqueued_seq["btc.spot"] == 7


# --- D) Policy-aware enqueue ---
"""
BLOCK with full queue (no drops)
Setup: sub policy=BLOCK, small buffer (e.g., 1), no consumer; publish 2 messages.
Expect: second publish awaits until space; no drops; enqueued_seq advances per publish; flush never
times out.
DROP_NEWEST with full queue
Setup: sub policy=DROP_NEWEST, buffer=1, publish 2 messages without consuming.
Expect: first enqueued, second dropped; sub._drops==1; bus total_drops increments; on_dropped fired
with reason="drop_newest".
DROP_OLDEST with full queue
Setup: sub policy=DROP_OLDEST, buffer=1, publish 2 messages.
Expect: second publish evicts oldest; sub._drops==1; on_dropped fired with
reason="drop_oldest_evict"; sub.queue contains only the second.
Paused subscriber (logical full)
Setup: sub policy=BLOCK, pause(); publish one message.
Expect: publish does not block; message dropped; sub._drops++ ;
on_dropped reason="paused_drop_newest"; enqueued_seq unchanged.
Sentinel preservation under DROP_OLDEST
Setup: push sentinel None into queue (unsubscribe or mark_closed); set policy=DROP_OLDEST;
try to publish.
Expect: incoming dropped, sentinel reinserted; reason="drop_newest_sentinel_guard".
"""


# --- E) Subscription UC and delivered accounting ---
"""
anext stops on sentinel
Setup: put sentinel; iterate with async for.
Expect: StopAsyncIteration; sub._delivered unchanged.
get() increments delivered
Setup: enqueue 2 envelopes; call get twice; then send sentinel and get again.
Expect: delivered==2; third get returns None; delivered unchanged on sentinel.
poll(timeout)
Setup: empty queue; poll with small timeout.
Expect: returns None; no delivered increment.
Depth vs delivered
Setup: enqueue N; consume k via get/iter; check depth and delivered.
Expect: depth==N−k; delivered==k.
"""

# --- F) Flush barrier ----
"""
Basic flush completion
Setup: publish N to a topic with 1+ subscribers; call flush().
Expect: returns; for each sub, enqueued_seq(topic)==topic.high_seq at snapshot time.
Snapshot semantics (post-snapshot publishes not waited on)
Setup: start flush; concurrently publish after a small delay.
Expect: flush returns after pre-snapshot messages enqueued; later messages may still arrive.
Timeout on stalled progress
Setup: sub policy=BLOCK, pause(); publish 1 (dropped due to pause); call flush(timeout=short).
Expect: flush raises asyncio.TimeoutError (high_seq advanced, sub enqueued_seq did not).
"""

# --- G) Graceful close with sentinel ---
"""
Close drains then signals
Setup: publish N; call close(drain=True); consume until sentinel.
Expect: all N enqueued before sentinel; state CLOSED; no further events delivered.
Sentinel with full queue counts a drop
Setup: fill queue to max; call unsubscribe() or close(); inspect sub._drops and bus totals.
Expect: sub._drops incremented by 1; on_dropped fired with topic "<control>",
reason="sentinel_evict".
Post-close API behavior
Setup: after close, attempt subscribe/publish.
Expect: RuntimeError (“closing/closed”).
"""

# --- H) Telemetry and stats ---
"""
on_dropped hook fires with reasons
Setup: cause drops via DROP_NEWEST, DROP_OLDEST, paused, timeout in BLOCK; register hook collecting
calls.
Expect: hook called with (topic, sub_name, env_or_none, reason) per drop; reasons match code
strings.
on_error hook receives exceptions from on_dropped
Setup: register on_dropped that raises; register on_error capturing errors; trigger a drop.
Expect: on_error called with where="on_drop".
get_stats snapshot sanity
Setup: some publishes and consumption; call get_stats().
Expect: totals consistent: state string, topic/sub counts, subscriber_names match live subs,
total_drops equals sum of per-topic drops; per_sub fields (buffer_size, depth, drops, delivered,
paused, policy) accurate.
topic_stats watermarks and lag map
Setup: two subs on same topic with different backlogs; call topic_stats(topic).
Expect: low_watermark=min(enqueued_seq), high_watermark=high_seq;
max_lag_by_sub[name]==high_seq−sub.enqueued_seq.
"""


# --- I) Subscribe defaults and errors ---
"""
- Buffer size defaulting
- duplicate subscribe name allowed -> do not allow
"""

# --- J) Progress event and flush wakeups ---
"""
publish sets _progress
Setup: start flush loop; publish one message.
Expect: flush wakes promptly (returns without waiting full interval).
"""

# --- K) Unsub behavior ---
"""
Stops delivery and enqueued watermark frozen
Setup: subscribe; publish; unsubscribe; publish more.
Expect: queue has sentinel; no new items; sub._enqueued_seq no longer changes.
"""

# --- L) Return values ---
"""
publish returns assigned seq
Setup: call publish repeatedly; collect return values.
Expect: per-topic seqs strictly increase and match envelope seqs delivered.
"""


# --- M) Stats ---
"""
watermarks returns (low, high) and equals (high, high) with no subscribers.
topic_stats.low_watermark/min(enqueued_seq) and high_watermark==high_seq; lag map matches.
get_stats.subscriber_names equals the names from the snapshot ‘subs’.
"""
