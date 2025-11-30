# --- Strategy ---


class StrategyError(Exception):
    """Recoverable strategy error (engine can continue or quarantine the strategy)."""


class StrategyPanic(Exception):
    """Non-recoverable; engine should disable this strategy instance cleanly."""


# --- Data ----


class EmptyArchiveError(RuntimeError):
    """Raised when a ZIP contains no usable CSV rows."""


class SourceError(Exception):
    "Error class connected to the 'Candle Source'."


class FeedError(Exception):
    """Raised for feed-level alignment or configuration errors."""


# --- Engine ---


class EngineError(Exception):
    """
    Raised for any errors related to the functioning og the backtest engine.
    """


# --- Bus ---


class BusError(Exception):
    "Error in connection with the bus"
