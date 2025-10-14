from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Any, Callable, Optional

from backtester.core.clock import Millis

# --- Data structures and config ---


@dataclass(frozen=True)
class Envelope:
    """Immutable message envelope delivered to subscribers."""

    topic: str
    ts: Millis
    payload: Any
    # seq: int # per topic, strictly increasing numer.


@dataclass
class BusConfig:
    """
    Defaults for a single in-process Bus instance.
    More added later, e.g., default backpressure policy, stats hooks.
    """

    default_buffer_size: int = 1000  # per-subscription mailbox size
    flush_check_interval: float = 0.01  # seconds to wait between flush state checks


class _BusState(str, Enum):
    """
    Internal enum for lifecycle: Running -> Closing -> Closed
    """

    RUNNING = "RUNNING"  # all APIs are available
    CLOSING = "CLOSING"  # Shutting down, new publishes are refused
    CLOSED = "CLOSED"  # everything is torn down; calls are no-ops or errors


# --- Snapshots stats for topic, subscriver and bus ---
# to be added latter

# --- Shared pieces ---


class TopicPriority(IntEnum):
    LOW = 10
    NORMAL = 50
    HIGH = 90


@dataclass
class TopicConfig:
    """
    Declarative configuration for a topic.
    - schema: Optional type to validate payloads (e.g., Candle)
    - priority: Scheduling hint (HIGH for risk_alerts; NORMAL default)
    - default_buffer_size: Suggested mailbox size for subscribers to this topic
    - coalesce_key: Optional function(payload) -> hashable key (used later by coalescing
    backpressure)
    - validate_schema: If True, publish() enforces isinstance(payload, schema)

    register_topic (within Bus), created a topic with a given config, if it does not exist.
    Publishing or subscribing to a concrete topic that does not exist will auto-create it with
    default config.
    """

    schema: Optional[type[Any]] = None
    priority: TopicPriority = TopicPriority.NORMAL
    coalesce_key: Optional[Callable[[Any], Any]] = None
    validate_schema: bool = True

    # schema: Optional[type[Any]] = (
    #     None  # Publish() checks isinstance(payload, schema), whenever validate_schema=True.
    #     # Avoids misrouting (Trade to a candles topic)
    # )
    # priority: TopicPriority = TopicPriority.NORMAL  # scheduling / importance hint.
    # default_buffer_size: Optional[int] = (
    #     None  # A per-topic suggested mailbox size for subscribers. When a subscriber joins
    # multiple
    #     # topics without specifying buffer_size, the bus uses the max of the bound topics
    #     # default_buffer_size (or the bus default if none is provided)
    # )
    # coalesce_key: Optional[Callable[[Any], Any]] = (
    #     None  # powers the COALESCE backpressure policy at the subscriber. If the subscriber
    # is set
    #     # to BackpressurePolicy.COALESCE, the bus will keep only the last queued item per key
    # within that topic
    # )
    # validate_schema: bool = True  # toggles the enforcement above. Set to False, if a topic must
    # # occasionally carry different payload types.


class BackpressurePolicy(str, Enum):
    BLOCK = "block"
    DROP_NEWEST = "drop_newest"
    DROP_OLDEST = "drop_oldest"
    COALESCE = "coalesce"


# --- Bus lifecycle ---


def create_bus(config: Optional[BusConfig] = None) -> Bus:
    """
    Factory to build a bus with consistent defaults.
    """
    return Bus.create_bus(config)


# --- Subscription object (Consumer mailbox) ---


class Subscription:
    pass


# --- Bus object ---


class Bus:
    def __init__(self, config: Optional[BusConfig]) -> None:
        self._cfg = config or BusConfig()

    # --- Topic Management ---

    async def register_topic(self) -> None:
        """
        Declare (or validate) a topic.
        - If topic does not exist, create with the provided config
        - If it exists:
            * "validate" (default): assert the provided config is compatible with existing
            * "merge": merge non-None fields into existing config
            * "ignore" keep existi ng config, ignore provided values
        """
        pass

    async def get_topic_config(self) -> None:
        """
        Return a (shallow) copy of the topic's config.
        """
        pass

    # --- Lifecycle API ---

    @classmethod
    def create_bus(cls, config: Optional[BusConfig] = None) -> "Bus":
        """
        Construct and return a Bus instance using the provided config.
        This is a classmethod so the module-level create_bus(...) can delegate to it
        without needing an instance.
        """
        # Minimal concrete implementation: create an instance and attach config for later use.
        return cls(config)

    async def flush(self) -> None:
        """
        Block until all messages published before this call have been delivered into
        every subsriber's mailbox (i.e., enqueued). Does not wait for subscribers
        to consume (process) them.
        """
        pass

    async def close(self) -> None:
        """
        Graceful shutdown
        """
        pass

    # --- Publishing ---

    def publish(self) -> None:
        pass

    def publish_batch(self) -> None:
        pass

    # --- Subscribing & consuming ---

    def subscribe(self) -> None:
        pass

    def unsubscribe(self) -> None:
        pass

    # --- diagnostics & telemetry (delayed)
