from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import TYPE_CHECKING, Any, AsyncGenerator, Callable, Iterable, Optional, Tuple, Type

from backtester.config.configs import BusConfig, SubscriptionConfig
from backtester.core.clock import Clock, Millis
from backtester.errors.errors import BusError
from backtester.types.topics import T_LOG
from backtester.types.types import LogEvent

if TYPE_CHECKING:
    from backtester.audit.audit import AuditWriter

# --- Data structures and config ---


@dataclass(frozen=True)
class Envelope:
    """Immutable message envelope delivered to subscribers."""

    topic: str
    seq: int
    ts: Millis
    payload: Any
    # priority: int
    # seq: int # per topic, strictly increasing numer.
    # key: str \ None # optional de-dup key


# --- Enum ---


class TopicPriority(IntEnum):
    HIGH = 90
    NORMAL = 50
    LOW = 10


class IfExistsOptions(str, Enum):
    VALIDATE = "validate"
    MERGE = "merge"
    IGNORE = "ignore"


class _BusState(str, Enum):
    """
    Internal enum for lifecycle: Running -> Closing -> Closed
    """

    RUNNING = "RUNNING"  # all APIs are available
    CLOSING = "CLOSING"  # Shutting down, new publishes are refused
    CLOSED = "CLOSED"  # everything is torn down; calls are no-ops or errors


class BackpressurePolicy(str, Enum):
    BLOCK = "block"
    DROP_NEWEST = "drop_newest"
    DROP_OLDEST = "drop_oldest"
    COALESCE = "coalesce"


# --- Dataclasses ---


@dataclass
class TopicConfig:
    schema: Optional[Type[Any]] = None
    priority: TopicPriority = TopicPriority.NORMAL
    coalesce_key: Optional[Callable[[Any], Any]] = None
    validate_schema: bool = False  # toggles enforcement above


# --- Metrics ---


@dataclass
class _TopicState:
    """Internal per-topic state & stats."""

    name: str
    config: TopicConfig = field(default_factory=TopicConfig)
    subscribers: set["Subscription"] = field(default_factory=set)
    # CACHE: Sorted list of subscribers for deterministic iteration in hot path
    _subscribers_ordered: list["Subscription"] = field(default_factory=list)

    created_at_utc: float = field(default_factory=lambda: time.time())
    high_seq: int = 0

    # Observability
    _pub_count: int = 0  # total published on this topic
    _last_publish_utc: Optional[float] = None  # last publish monotonic time

    _last_stats_mono: float = field(default_factory=time.monotonic)
    _last_stats_count: int = 0


@dataclass
class TopicStats:
    """Snapshot of a topic's health & activity."""

    name: str
    priority: TopicPriority
    high_seq: int  # latest sequence number published on this topic. Increments by 1 per publish.
    subscribers: int  # number of active subscribers to this topic
    publish_count: int  # total number of published messages on this topic since the bus started
    publish_rate_eps: float  # publish rate (events per second) (How hot is the topic)
    last_publish_utc: Optional[
        float
    ]  # timestamp of the last published message, None if no messages have been published
    low_watermark: (
        int  # lowest sequence number enqueued by any subscriber (what is the slowest subscriber)
    )
    high_watermark: int  # publish high watermark (last published seq on this topic)
    max_lag_by_sub: dict[
        str, int
    ]  # For each subscriber, the lag (high_watermark - sub.enqueued_seq(topic)). Intuition: How
    # many messages behind is this subscriber?


@dataclass
class SubscriberStats:
    name: str
    topics: list[str]
    buffer_size: int
    depth: int
    drops: int
    delivered: int
    paused: bool
    policy: BackpressurePolicy


@dataclass
class BusStats:
    state: str
    topics: int
    topic_names: list[str]
    subscribers: int
    subscriber_names: list[str]
    total_drops: int
    per_topic: list[TopicStats]
    per_subscriber: list[SubscriberStats]


# --- Subscription object ---


class Subscription:
    """
    Consumers handle on the bus.
    - A single merged queue (asyncio.Queue) holds events from all topics this subscriber cares
    about.

    - One can close and pause the queue;
    - Pausing is like closing, but only temporarily
    """

    def __init__(self, bus: "Bus", name: str, sub_config: SubscriptionConfig) -> None:
        self.bus = bus
        self.name = name
        self.topics: set[str] = set(sub_config.topics)

        # Subscription > Topic > Bus
        eff_buffer_size: int = (
            sub_config.buffer_size
            if sub_config.buffer_size is not None
            else bus._cfg.default_buffer_size
        )
        self.queue: asyncio.Queue[Optional[Envelope]] = asyncio.Queue(maxsize=eff_buffer_size)
        self._policy = BackpressurePolicy.BLOCK

        # Status
        self._paused: bool = False
        self._closed: bool = False
        self._close_reason: Optional[str] = None

        # obs
        # enqueued_seq tracks what has been delivered
        self._enqueued_seq: dict[str, int] = {t: 0 for t in self.topics}
        self._drops: int = 0
        self._delivered: int = 0

    # --- Consumption API (MVP) ---

    def __aiter__(self) -> Subscription:
        """
        DEPRECATED: Use consume() context manager instead.
        This method is unsafe for deterministic backtesting (ADR 021).
        """
        import warnings

        warnings.warn(
            "__aiter__ is deprecated for deterministic backtesting. "
            "Use 'async with sub.consume()' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self

    async def __anext__(self) -> Envelope:
        """
        DEPRECATED: Use consume() context manager instead.
        This method is unsafe for deterministic backtesting (ADR 021).
        """
        import warnings

        warnings.warn(
            "__anext__ is deprecated for deterministic backtesting. "
            "Use 'async with sub.consume()' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        item = await self.queue.get()
        self.queue.task_done()
        if item is None:
            # Shutdown sentinel, raise to end the loop
            raise StopAsyncIteration
        self._delivered += 1
        return item

    async def get(self) -> Optional[Envelope]:
        """
        DEPRECATED: Use consume() context manager instead.

        This method requires manual task_done() management, which is error-prone
        and breaks deterministic backtesting (ADR 021).

        Await one envelope; returns None if sentinel is received.
        """
        import warnings

        warnings.warn(
            "get() is deprecated for deterministic backtesting. "
            "Use 'async with sub.consume()' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        item = await self.queue.get()
        self.queue.task_done()
        if item is None:
            return None
        self._delivered += 1
        return item

    async def poll(self, timeout: Optional[float] = None) -> Optional[Envelope]:
        """
        DEPRECATED: Use consume() context manager instead.

        Wait up to 'timeout' seconds for one envelope; None on timeout or sentinel.
        This method requires manual task_done() management (ADR 021).
        """
        import warnings

        warnings.warn(
            "poll() is deprecated for deterministic backtesting. "
            "Use 'async with sub.consume()' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        try:
            if timeout is None:
                item = await self.queue.get()
            else:
                item = await asyncio.wait_for(self.queue.get(), timeout=timeout)
            self.queue.task_done()
        except asyncio.TimeoutError:
            return None
        if item is None:
            return None
        self._delivered += 1
        return item

    # --- ONLY USE consume; rest deprecated ---

    @asynccontextmanager
    async def consume(self) -> AsyncGenerator[Optional[Envelope], None]:
        """
        Context manager to process a message.
        Acknowledges the task (task_done) only when exiting the block.
        Makes sure that wait_until_idle() does not waint indefinetly.
        """
        item = await self.queue.get()
        try:
            yield item
        finally:
            self.queue.task_done()
            if item is not None:
                self._delivered += 1

    # --- Control ----

    def pause(self) -> None:
        """Temporarily make the mailbox 'unavailable'.
        The bus will apply the policy as if the queue were full."""
        self._paused = True

    def resume(self) -> None:
        """Resume normal deliveries."""
        self._paused = False

    def set_backpressure(self, policy: BackpressurePolicy) -> None:
        """Change policy at runtime."""
        self._policy = policy

    def depth(self) -> int:
        """
        Current number of items queued for this subscriber (not counting items already consumed).
        """
        return self.queue.qsize()

    def lag(self, topic: str, high_seq: int) -> int:
        """
        Sequence lag for a topic relative to a provided high watermark.
        (Bus supplies the high watermark; this method just computes the difference.)
        """
        return max(0, int(high_seq) - int(self._enqueued_seq.get(topic, 0)))

    def enqueued_seq(self, topic: str) -> int:
        return self._enqueued_seq.get(topic, 0)

    async def mark_closed(self, reason: str) -> None:
        """
        Signal shutdown to the consumer with a sentinel None (idempotent).
        Called by the bus (or unsubscribe) to end delivery.
        """

        if not self._closed:
            self._closed = True
            self._close_reason = reason
            try:
                self.queue.put_nowait(None)
            except asyncio.QueueFull:
                try:
                    victim = self.queue.get_nowait()
                    self.queue.task_done()
                    self._drops += 1
                    try:
                        await self.bus._record_drop(
                            "<control>",
                            self.name,
                            victim if isinstance(victim, Envelope) else None,
                            "sentinel_evict",
                        )
                    except Exception:
                        pass
                except asyncio.QueueEmpty:
                    pass
                self.queue.put_nowait(None)

    # --- Hooks used by Bus publisher ---

    async def _enqueue_from_bus(
        self, env: Envelope, *, urgent: bool = False, timeout: Optional[float] = None
    ) -> bool:
        """
        Bus-facing enqueue that honors pause state and the subscriber's backpressure policy.
        Returns true if the envelope was queued, False if it was dropped.

        class BackpressurePolicy(str, Enum):
            BLOCK = "block"
            DROP_NEWEST = "drop_newest"
            DROP_OLDEST = "drop_oldest"
            COALESCE = "coalesce"
        """

        # 1. Closed
        if self._closed:
            self._mark_seen(env)
            return False

        policy = self._policy

        # 2. Paused: Treat paused as "logically full" and apply a non-blocking policy
        if self._paused:
            self._mark_seen(env)
            if policy == BackpressurePolicy.DROP_OLDEST:
                # falls through to DROP_OLDEST behavior below (non-blocking)
                pass
            else:
                # default behavior: drop newest (non-blocking)
                self._drops += 1
                try:
                    await self.bus._record_drop(env.topic, self.name, env, "paused_drop_newest")
                except Exception:
                    pass
                return False

        q = self.queue

        # 3. Space available:
        if not q.full():
            q.put_nowait(env)
            self._enqueued_seq[env.topic] = env.seq
            return True

        # 4. Full path

        if policy == BackpressurePolicy.BLOCK:
            ok = await self._enqueue_on_block(q=q, env=env, timeout=timeout)
            if not ok:
                self._mark_seen(env)
            return ok
        if policy == BackpressurePolicy.DROP_OLDEST:
            # seq update is handled
            return await self._enqueue_on_drop_oldest(q=q, env=env)
        if policy == BackpressurePolicy.DROP_NEWEST:
            self._mark_seen(env)
            return await self._enqueue_on_drop_newest(env=env)
        # TODO Out of scope of MVP, added later:
        # Requires a per-topic key function: remove the oldest existing item with the same key,
        if policy == BackpressurePolicy.COALESCE:
            pass

        # unknown policy, drop defensively
        self._mark_seen(env)
        self._drops += 1
        try:
            await self.bus._record_drop(env.topic, self.name, env, "Unknown policy drop")
        except Exception:
            pass
        return False

    async def enqueue(
        self, env: Envelope, *, urgent: bool = False, timeout: Optional[float] = None
    ) -> bool:
        return await self._enqueue_from_bus(env, urgent=urgent, timeout=timeout)

    async def _enqueue_on_block(
        self, q: asyncio.Queue, env: Envelope, timeout: Optional[float]
    ) -> bool:
        """
        Called by _enqueue_from_bus, in the case of full queue and
        policy == BackpressurePolicy.BLOCK

        Semantics:
            - await timeout, else drop
        """
        try:
            if timeout is None:
                await q.put(env)
            else:
                await asyncio.wait_for(q.put(env), timeout=timeout)
            self._enqueued_seq[env.topic] = env.seq
            return True
        except asyncio.TimeoutError:
            self._drops += 1
            try:
                await self.bus._record_drop(env.topic, self.name, env, "timeout_drop_newest")
            except Exception:
                pass
            return False

    async def _enqueue_on_drop_oldest(self, q: asyncio.Queue, env: Envelope) -> bool:
        victim: Optional[Envelope] = None
        try:
            victim = q.get_nowait()
            self.queue.task_done()
            if victim is None:
                q.put_nowait(None)
                self._drops += 1
                try:
                    await self.bus._record_drop(
                        env.topic, self.name, env, "drop_newest_sentinel_guard"
                    )
                except Exception:
                    pass
                return False
        except asyncio.QueueEmpty:
            pass
        q.put_nowait(env)
        self._enqueued_seq[env.topic] = env.seq
        # Count the eviction as a drop (not the incoming env)
        self._drops += 1
        try:
            await self.bus._record_drop(
                env.topic,
                self.name,
                victim if isinstance(victim, Envelope) else None,
                "drop_oldest_evict",
            )
        except Exception:
            pass
        return True

    async def _enqueue_on_drop_newest(self, env: Envelope) -> bool:
        """
        Drop incoming. Return False.
        """
        self._drops += 1
        try:
            await self.bus._record_drop(env.topic, self.name, env, "drop_newest")
        except Exception:
            pass
        return False

    # --- Sequence Helper ---

    def _mark_seen(
        self,
        env: Envelope,
    ) -> None:
        # must be called, even if dropped!
        if env.seq > self._enqueued_seq.get(env.topic, 0):
            self._enqueued_seq[env.topic] = env.seq


# --- Bus object ---


class Bus:
    """
    - 1. General: Create a bus
    - 2. General: Create topics with register_topic
    - 3. Consumer: Create BusSubscriptionConfigs (per each consumer)
    - 4. Consumer: For each consumer, create a Subscription (using the BusSubConfig)
    - 5. General: Create a Bus (using BusConfig)
        - 5.1. Keeping a set of consumers (Subscriptions)
        - 5.2. Keeping a mapping of str (topic) to set of consumers (Subscription)
        - 5.3. Keeping a mapping of str (topic) to the topic state

    Later additions
    - Middleware layer to inject delays between publish and receipt
    - Shuffle delivery order (out-of-order handling stress test)
    - Drop/fail simulation
    -
    """

    # 1. Add Deterministic per-topic sequencing (owned by the bus)
    # 1.1. Implement per-topic high watermark inetefer per topic

    def __init__(self, cfg: BusConfig, clock: Clock, audit: AuditWriter) -> None:
        self._cfg = cfg
        self._clock = clock
        self._audit = audit
        self._state: _BusState = _BusState.RUNNING

        # Active Subscriptions
        self._subscriptions: set[Subscription] = set()
        self._topics: dict[str, _TopicState] = {}

        # Concurrency control
        self._lock = asyncio.Lock()
        self._progress = asyncio.Event()
        self._progress.set()
        # Publishers call set() when enqueue message; flush loop waits on that event to be
        # signalled, before re-checking whether subs have caught up

        # Telemtry
        self._drops_by_topic: dict[str, int] = {}
        self._drops_by_sub: dict[str, int] = {}
        self._on_error: list[Callable[[str, BaseException], None]] = []
        # (topic, sub_name, env, reason)
        self._on_drop: list[Callable[[str, str, Envelope | None, str], None]] = []
        self._no_sub_once: set[str] = set()

    # --- helpers ---

    async def _record_drop(
        self, topic: str, sub_name: str, env: Envelope | None, reason: str
    ) -> None:
        key = topic
        self._drops_by_topic[key] = self._drops_by_topic.get(key, 0) + 1
        self._drops_by_sub[sub_name] = self._drops_by_sub.get(sub_name, 0) + 1
        for cb in list(self._on_drop):
            try:
                cb(topic, sub_name, env, reason)
            except Exception as e:
                await self._emit_error("on_drop", e)
        payload_obj: Any = getattr(env, "payload", None) if env is not None else None
        order_id = getattr(payload_obj, "id", None) or getattr(payload_obj, "order_id", None)
        await self.emit_log(
            "WARN",
            "BUS_DROP",
            payload={
                "topic": topic,
                "subscriber": sub_name,
                "reason": reason,
                "seq": getattr(env, "seq", None) if env is not None else None,
                "symbol": getattr(payload_obj, "symbol", None),
                "order_id": order_id,
            },
            sim_time=self._clock.now(),
        )

    async def _emit_error(self, where: str, exc: BaseException) -> None:
        for cb in list(self._on_error):
            try:
                cb(where, exc)
            except Exception:
                pass
        await self.emit_log(
            "ERROR",
            "BUS_ERROR",
            payload={"where": where, "error": repr(exc)},
            sim_time=self._clock.now(),
        )

    def _emit_audit(
        self,
        event: str,
        *,
        component: str = "bus",
        level: str = "INFO",
        simple: bool = False,
        sim_time: Optional[int] = None,
        payload: Optional[dict[str, Any]] = None,
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        parent_id: Optional[str] = None,
    ) -> None:
        raise UserWarning("Deprecated; emit via emit_log()")
        if self._audit is None:
            return
        self._audit.emit(
            component=component,
            event=event,
            level=level,
            simple=simple,
            sim_time=sim_time,
            payload=payload or {},
            symbol=symbol,
            order_id=order_id,
            parent_id=parent_id,
        )

    def set_audit(self, audit: AuditWriter) -> None:
        self._audit = audit

    # --- Public hook registration ---

    def on_error(self, callback: Callable[[str, BaseException], None]) -> None:
        """
        Register an error hook: callback(where, exception).
        """
        self._on_error.append(callback)

    def on_dropped(self, callback: Callable[[str, str, Envelope | None, str], None]) -> None:
        """
        Register a drop hook: callback(topic, subscriber_name, envelope_or_none, reason).
        """
        self._on_drop.append(callback)

    async def wait_until_idle(self) -> None:
        """
        Barrier. Blocks until ALL items in ALL queues have been processed.
        """
        try:
            await asyncio.wait_for(
                asyncio.gather(*[sub.queue.join() for sub in self._subscriptions]), timeout=5.0
            )
        except asyncio.TimeoutError:
            print("\nCRITICAL: Bus stuck in wait_until_idle!")
            for sub in self._subscriptions:
                unfinished = sub.queue.qsize()
                if unfinished > 0:
                    print(f" -> STUCK SUBSCRIBER: {sub.name}")
                    print(f"    Unfinished tasks: {unfinished}")
                    print(f"    Queue depth: {sub.queue.qsize()}")
            raise

    async def emit_log(
        self, level: str, msg: str, payload: Optional[dict[str, Any]], sim_time: Optional[int]
    ) -> None:
        """Payload must be JSON serializable"""
        log_event = LogEvent(
            level=level,
            component="BUS",
            msg=msg,
            payload=payload or {},
            sim_time=sim_time,
        )
        await self.publish(topic=T_LOG, ts_utc=sim_time or 0, payload=log_event)

    # --- Topic Management ---

    async def register_topic(
        self,
        name: str,
        *,
        schema: Optional[Type[Any]] = None,
        priority: Optional[TopicPriority] = None,
        coalesce_key: Optional[Callable[[Any], Any]] = None,
        validate_schema: Optional[bool] = True,
        if_exists: str | IfExistsOptions = IfExistsOptions.VALIDATE,  # or "merge" | "ignore"
    ) -> None:
        """
        Declare (or validate) a topic.
        - If topic does not exist, create it with the provided config.
        - If it exists:
          * "validate" (default): assert the provided config is compatible with existing. (schema,
          default_buffer_size must match if provided)
          * "merge": merge non-None fields into existing config.
          * "ignore": keep existing config, ignore provided values.
        """
        if_exists = IfExistsOptions(if_exists)  # raises val error if invalid string
        # collect single deferred log call (level, msg, payload, sim_time)
        deferred_log: Optional[tuple[str, str, dict[str, Any], int]] = None

        async with self._lock:
            existing = self._topics.get(name)
            # 1. Case: Does not exist yet
            if existing is None:
                eff_priority = priority if priority is not None else TopicPriority.NORMAL
                eff_validate = validate_schema if validate_schema is not None else True
                cfg = TopicConfig(
                    schema=schema,
                    priority=eff_priority,
                    coalesce_key=coalesce_key,
                    validate_schema=eff_validate,
                )
                self._topics[name] = _TopicState(name=name, config=cfg)
                deferred_log = (
                    "INFO",
                    "BUS_TOPIC_REGISTERED",
                    {
                        "topic": name,
                        "priority": int(eff_priority),
                        "validate_schema": bool(eff_validate),
                        "schema": getattr(schema, "__name__", None) if schema else None,
                        "coalesce": coalesce_key.__name__ if coalesce_key else None,
                    },
                    self._clock.now(),
                )

            # 2. Case: Already exists; depending on if_exists
            else:
                # 2.1. ignore
                if if_exists == IfExistsOptions.IGNORE:
                    return
                # 2.2. validate
                elif if_exists == IfExistsOptions.VALIDATE:
                    cfg = existing.config
                    if schema is not None and cfg.schema is not None and schema is not cfg.schema:
                        raise TypeError(
                            f"Topic '{name}' schema mismatch: {schema} vs existing {cfg.schema}"
                        )
                    deferred_log = (
                        "INFO",
                        "BUS_TOPIC_VALIDATED",
                        {
                            "topic": name,
                            "schema": getattr(cfg.schema, "__name__", None) if cfg.schema else None,
                        },
                        self._clock.now(),
                    )

                # 2.3. merge
                elif if_exists == IfExistsOptions.MERGE:
                    cfg = existing.config
                    before = {
                        "priority": int(cfg.priority),
                        "validate_schema": bool(cfg.validate_schema),
                        "schema": getattr(cfg.schema, "__name__", None) if cfg.schema else None,
                    }
                    if schema is not None:
                        cfg.schema = schema
                    if priority is not None:
                        cfg.priority = priority
                    if coalesce_key is not None:
                        cfg.coalesce_key = coalesce_key
                    if validate_schema is not None:
                        cfg.validate_schema = validate_schema
                    after = {
                        "priority": int(cfg.priority),
                        "validate_schema": bool(cfg.validate_schema),
                        "schema": getattr(cfg.schema, "__name__", None) if cfg.schema else None,
                    }
                    deferred_log = (
                        "INFO",
                        "BUS_TOPIC_MERGED",
                        {
                            "topic": name,
                            "before": before,
                            "after": after,
                            "coalesce": coalesce_key.__name__ if coalesce_key else None,
                        },
                        self._clock.now(),
                    )
        if deferred_log is not None:
            level, msg, payload, sim_time = deferred_log
            await self.emit_log(level=level, msg=msg, payload=payload, sim_time=sim_time)
            return

    # --- Lifecycle API ---

    async def flush(self, timeout: Optional[float] = None) -> None:
        """
        Block until all messages published this call's snapshot have been
        delivered into every mailbox. Does not wait to process (consume) them.

        Semantics:
        - Snapshot current per-topic high_seq
        - wait until every sub has enqueued >= those seq for each of its topics
        - new messages published AFTER the snapshot are not waited on
        """
        if self._state == _BusState.CLOSED:
            return

        # 1. Take snapshot
        async with self._lock:
            watermark = {t: ts.high_seq for t, ts in self._topics.items()}
            subs = list(self._subscriptions)
        if not watermark or not subs:
            return
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        # 2. Loop until each subscription has that sequence enqueued for the topics it subscribes to
        while True:
            all_caught_up = True
            for sub in subs:
                for topic in sub.topics & watermark.keys():
                    # Compare the enque level of TopicState (published) to enqueue level
                    # of Subscription (delivered)
                    if sub.enqueued_seq(topic) < watermark[topic]:
                        all_caught_up = False
                        break
                if not all_caught_up:
                    break
            if all_caught_up:
                return

            wait_for = self._cfg.flush_check_interval
            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0.0:
                    raise asyncio.TimeoutError("Bus.flush timed out")
                wait_for = min(wait_for, max(0.0, remaining))
            self._progress.clear()
            try:
                await asyncio.wait_for(self._progress.wait(), timeout=wait_for)
            except asyncio.TimeoutError:
                pass

    async def close(
        self, reason: Optional[str] = None, *, drain: bool = True, timeout: Optional[float] = None
    ) -> None:
        """
        Graceful shutdown:
        - Move to closing
        - Optionally flush (drain=True) to deliver all messages already published
        - Signal all subscriptions with a sentinel (None) and mark CLOSED
        Safe to call multiple times.
        """
        # Step 1: Snapshot state without holding the lock across emit_log (publish acquires _lock)
        async with self._lock:
            sub_count = len(self._subscriptions)
            topic_count = len(self._topics)
            already_closed = self._state == _BusState.CLOSED

        if already_closed:
            return

        await self.emit_log(
            "INFO",
            "BUS_CLOSE_START",
            payload={
                "reason": reason or "unspecified",
                "drain": bool(drain),
                "timeout": timeout,
                "subscriptions": sub_count,
                "topics": topic_count,
            },
            sim_time=self._clock.now(),
        )

        # Transition to CLOSING (idempotent guard)
        async with self._lock:
            if self._state == _BusState.CLOSED:
                return
            self._state = _BusState.CLOSING

        # Step 2: optional drain up to snapshot taken *inside* flush()
        if drain:
            try:
                await self.flush(timeout=timeout)  # wait until all have caught up
            except asyncio.TimeoutError:
                # continue shutdown even if not fully drained
                pass

        # Step 3: signal subscribers and finalize
        async with self._lock:
            subs = list(self._subscriptions)
        for sub in subs:
            await sub.mark_closed(reason or "bus.close()")

        async with self._lock:
            self._subscriptions.clear()
            self._topics.clear()
            self._state = _BusState.CLOSED
            # reset
            self._progress.set()

    # --- subscriptions ---

    async def subscribe(self, name: str, bus_sub_config: SubscriptionConfig) -> Subscription:
        """
        name: str (name of the subscription object)
        bus_sub_config: BusSubscriptionConfig (config)
        """
        if self._state != _BusState.RUNNING:
            raise RuntimeError("Cannot subscribe: bus is closing/closed")

        sub = Subscription(self, name, bus_sub_config)

        async with self._lock:
            for t in sub.topics:
                ts = self._topics.get(t)
                if ts is None:
                    raise BusError(f"Topic {t!r} not registered")
                # .add() avoids double registering any consumers
                ts.subscribers.add(sub)
                # Update deterministic cache
                ts._subscribers_ordered = sorted(ts.subscribers, key=lambda s: s.name)
            self._subscriptions.add(sub)

        await self.emit_log(
            "INFO",
            "BUS_SUBSCRIBER_ATTACHED",
            payload={
                "subscriber": name,
                "topics": sorted(sub.topics),
                "buffer_size": sub.queue.maxsize,
                "policy": str(sub._policy),
            },
            sim_time=self._clock.now(),
        )

        return sub

    async def unsubscribe(self, subscription: Subscription, reason: str = "unsubscribe") -> None:
        """
        Detach a subscription. It receives a sentinel and stops getting new events.
        Safe to call multiple times.
        """
        # Mark closed first so no further enqueues stick
        await subscription.mark_closed(reason)
        async with self._lock:
            if subscription in self._subscriptions:
                self._subscriptions.remove(subscription)
            for ts in self._topics.values():
                ts.subscribers.discard(subscription)
                ts._subscribers_ordered = sorted(ts.subscribers, key=lambda s: s.name)

        await self.emit_log(
            "INFO",
            "BUS_SUBSCRIBER_DETACHED",
            payload={
                "subscriber": subscription.name,
                "reason": reason,
                "topics": sorted(subscription.topics),
            },
            sim_time=self._clock.now(),
        )

    # --- publish ---

    async def publish(self, topic: str, ts_utc: Millis, payload: Any) -> int:
        """
        Fan-out to all subscribers of env.topic.
        """
        # Get the list of relevant subscribers for this topic
        if self._state != _BusState.RUNNING:
            raise RuntimeError("Cannot publish: bus is closing/closed")

        if not isinstance(ts_utc, int):
            raise ValueError(f"ts_utc is of type {type(ts_utc)}")

        tstate = self._topics.get(topic)
        if tstate is None:
            raise BusError(f"Publish: Topic {topic} unknown")
        if (
            self._cfg.validate_schema  # disabled per default for speed
            and tstate.config.validate_schema
            and tstate.config.schema is not None
        ):
            if not isinstance(payload, tstate.config.schema):
                msg = (
                    f"Schema {tstate.config.schema} does not align with payload type "
                    f"{type(payload)}"
                )
                raise BusError(msg)

        # 1. Grab subscribers
        async with self._lock:
            tstate = self._topics.get(topic)
            if tstate is None:
                raise BusError(f"Publish: Topic {topic} unknown")

            # we now assume tstate exists and is of type _TopicState
            tstate.high_seq += 1
            seq = tstate.high_seq

            # Update stats (minimal)
            tstate._pub_count += 1
            tstate._last_publish_utc = ts_utc

            # copy subscribers to release lock early
            subscribers = list(tstate._subscribers_ordered)

        if not subscribers and topic not in self._no_sub_once:
            self._no_sub_once.add(topic)
            await self.emit_log(
                "WARN",
                "BUS_PUBLISH_NO_SUBSCRIBERS",
                payload={"topic": topic},
                sim_time=self._clock.now(),
            )

        # The sequence here is topic-based (not mailbox based)
        env = Envelope(topic=topic, seq=seq, ts=Millis(ts_utc), payload=payload)

        # Fan-out through policy-aware enqueue
        any_enqueued = False
        for sub in subscribers:
            ok = await sub.enqueue(env)
            any_enqueued = any_enqueued or ok
        if any_enqueued:
            await asyncio.sleep(0)
            self._progress.set()
        else:
            pass

        return seq

    async def publish_batch(
        self,
        topic: str,
        payloads: Iterable[Any],
        *,
        ts_utc: int,
        urgent: bool = False,
        timeout_per_item: Optional[float] = None,
    ) -> Tuple[int, int]:
        """
        Publish many events onto a single topic efficiently.
        """

        if self._state != _BusState.RUNNING:
            raise RuntimeError("Cannot publish: bus is closing/closed")

        payloads_list = list(payloads)
        n = len(payloads_list)
        if n == 0:
            return (0, 0)

        async with self._lock:
            tstate = self._topics.get(topic)
            if tstate is None:
                raise BusError(f"publish_batch: Topic {topic} unknown")

            if (
                self._cfg.validate_schema
                and tstate.config.validate_schema
                and tstate.config.schema is not None
            ):
                for p in payloads_list:
                    if not isinstance(p, tstate.config.schema):
                        msg = (
                            f"Schema {tstate.config.schema} does not align with payload type "
                            f"{type(p)}"
                        )
                        raise BusError(msg)

            # Reserve a contiguous seq range and snapshot subscribers
            first_seq = tstate.high_seq + 1
            last_seq = tstate.high_seq + n
            tstate.high_seq = last_seq

            # use cached ordered list (determinism + speed)
            subscribers = list(tstate._subscribers_ordered)

            # Stats update (minimal, no syscalls)
            tstate._pub_count += n
            tstate._last_publish_utc = ts_utc

        if not subscribers and topic not in self._no_sub_once:
            self._no_sub_once.add(topic)
            await self.emit_log(
                "WARN",
                "BUS_PUBLISH_NO_SUBSCRIBERS",
                payload={"topic": topic, "batch": True},
                sim_time=self._clock.now(),
            )

        # Build envs
        base_ts = ts_utc
        envs = [
            Envelope(topic=topic, seq=(first_seq + i), ts=int(base_ts), payload=p)
            for i, p in enumerate(payloads_list)
        ]
        any_enqueued = False
        for sub in subscribers:
            for env in envs:
                ok = await sub.enqueue(env, urgent=urgent, timeout=timeout_per_item)
                any_enqueued = any_enqueued or ok

        if any_enqueued:
            await asyncio.sleep(0)
            self._progress.set()
        else:
            await self.emit_log(
                "WARN",
                "BUS_PUBLISH_BATCH_DROPPED",
                payload={
                    "topic": topic,
                    "seq_start": first_seq,
                    "seq_end": last_seq,
                    "count": n,
                    "subscribers": [sub.name for sub in subscribers],
                },
                sim_time=self._clock.now(),
            )

        return (first_seq, last_seq)

    # --- diagnostics & telemetry ---

    async def get_summary_stats(self) -> dict[str, Any]:
        async with self._lock:
            subs = list(self._subscriptions)
            topics = list(self._topics.keys())

        total_delivered = sum(sub._delivered for sub in subs)
        drops_total = sum(self._drops_by_topic.values())

        # Avg throughput: sum of per-topic rates
        topic_rates = []
        for topic in topics:
            stats = await self.topic_stats(topic)
            topic_rates.append(stats.publish_rate_eps)
        avg_throughput_eps = sum(topic_rates) / len(topic_rates) if topic_rates else 0.0

        # Slowest subscriber: highest queue depth
        slowest_sub = max(subs, key=lambda s: s.depth(), default=None)
        slowest_sub_name = slowest_sub.name if slowest_sub else None
        slowest_sub_depth = slowest_sub.queue.maxsize if slowest_sub else 0

        return {
            "total_delivered": total_delivered,
            "drops_total": drops_total,
            "avg_throughput_eps": avg_throughput_eps,
            "slowest_subscriber": slowest_sub_name,
            "slowest_subscriber_depth": slowest_sub_depth,
        }

    async def get_stats(self) -> BusStats:
        """
        Get a global snapshot: state, counts, per-topic & per-subscriber stats.
        Call periodically to assess health.
        """
        # 1. Get the current state
        async with self._lock:
            state = self._state.value if isinstance(self._state, Enum) else str(self._state)
            topic_names = sorted(self._topics.keys())
            subs = list(self._subscriptions)
            drops_total = sum(self._drops_by_topic.values())

        per_topic: list[TopicStats] = []
        topic_stats_tasks = [self.topic_stats(t) for t in topic_names]
        per_topic = await asyncio.gather(*topic_stats_tasks)

        per_sub: list[SubscriberStats] = []
        for s in subs:
            per_sub.append(
                SubscriberStats(
                    name=s.name,
                    topics=sorted(list(s.topics)),
                    buffer_size=s.queue.maxsize,
                    depth=s.depth(),
                    drops=s._drops,
                    delivered=s._delivered,
                    paused=s._paused,
                    policy=s._policy,
                )
            )

        return BusStats(
            state=state,
            topics=len(topic_names),
            topic_names=topic_names,
            subscribers=len(subs),
            subscriber_names=[sub.name for sub in subs],
            total_drops=drops_total,
            per_topic=per_topic,
            per_subscriber=per_sub,
        )

    async def topic_stats(self, topic: str) -> TopicStats:
        """Per-topic snapshot with watermarks and per-subscriber lag."""
        async with self._lock:
            ts = self._topics.get(topic)
            if ts is None:
                raise KeyError(f"Topic '{topic}' not found")

            high_seq = ts.high_seq
            subs = list(ts.subscribers)
            cfg = ts.config
            pub_count = ts._pub_count
            last_pub = ts._last_publish_utc

            # Calculate rate lazily (metrics decoupling)
            now_mono = time.monotonic()
            delta_t = now_mono - ts._last_stats_mono
            delta_count = pub_count - ts._last_stats_count

            rate = 0.0
            if delta_t > 0:
                rate = delta_count / delta_t

            ts._last_stats_mono = now_mono
            ts._last_stats_count = pub_count

            if not subs:
                low_watermark = high_seq
            else:
                low_watermark = min(sub.enqueued_seq(topic) for sub in subs)

        lag_map = {sub.name: (high_seq - sub.enqueued_seq(topic)) for sub in subs}

        return TopicStats(
            name=topic,
            priority=cfg.priority,
            high_seq=high_seq,
            subscribers=len(subs),
            publish_count=pub_count,
            publish_rate_eps=rate,
            last_publish_utc=last_pub,
            low_watermark=low_watermark,
            high_watermark=high_seq,
            max_lag_by_sub=lag_map,
        )

    async def watermarks(self, topic: str) -> tuple[int, int]:
        """
        Quick view of backlog for one topic: returns (low, high)
            - low: min enqueued seq across subscribers (or high if none)
            - high: last published seq on the topic
        """
        async with self._lock:
            ts = self._topics.get(topic)
            if ts is None:
                raise KeyError(f"bus.watermarks(): unknown topic '{topic}'")
            high = ts.high_seq
            subs = list(ts.subscribers)
        low = min((sub.enqueued_seq(topic) for sub in subs), default=high)
        return (low, high)
