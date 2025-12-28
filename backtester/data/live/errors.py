"""
Custom exceptions for the live data feed module.

Exception hierarchy:
- LiveFeedError (base)
  - ConnectionError: WebSocket connection issues
  - SubscriptionError: Stream subscription failures
  - MessageParseError: Invalid/malformed messages
  - HandlerError: Handler processing failures
  - HealthError: Health check failures
  - ConfigurationError: Invalid configuration
"""

from __future__ import annotations

from typing import Any, Optional


class LiveFeedError(Exception):
    """Base exception for all live feed errors."""

    def __init__(
        self,
        message: str,
        *,
        component: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        self.component = component
        self.details = details or {}
        super().__init__(message)

    def __str__(self) -> str:
        parts = [super().__str__()]
        if self.component:
            parts.append(f"[component={self.component}]")
        if self.details:
            parts.append(f"[details={self.details}]")
        return " ".join(parts)


class ConnectionError(LiveFeedError):
    """Raised when WebSocket connection fails or is lost."""

    def __init__(
        self,
        message: str,
        *,
        url: Optional[str] = None,
        reconnect_attempt: int = 0,
        component: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        self.url = url
        self.reconnect_attempt = reconnect_attempt
        details = details or {}
        if url:
            details["url"] = url
        details["reconnect_attempt"] = reconnect_attempt
        super().__init__(message, component=component, details=details)


class SubscriptionError(LiveFeedError):
    """Raised when stream subscription fails."""

    def __init__(
        self,
        message: str,
        *,
        stream: Optional[str] = None,
        symbol: Optional[str] = None,
        component: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        self.stream = stream
        self.symbol = symbol
        details = details or {}
        if stream:
            details["stream"] = stream
        if symbol:
            details["symbol"] = symbol
        super().__init__(message, component=component, details=details)


class MessageParseError(LiveFeedError):
    """Raised when a message cannot be parsed."""

    def __init__(
        self,
        message: str,
        *,
        raw_data: Optional[str] = None,
        expected_type: Optional[str] = None,
        component: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        self.raw_data = raw_data
        self.expected_type = expected_type
        details = details or {}
        if expected_type:
            details["expected_type"] = expected_type
        # Don't include raw_data in details to avoid log spam
        super().__init__(message, component=component, details=details)


class HandlerError(LiveFeedError):
    """Raised when a message handler fails to process a message."""

    def __init__(
        self,
        message: str,
        *,
        handler_name: Optional[str] = None,
        message_type: Optional[str] = None,
        component: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        self.handler_name = handler_name
        self.message_type = message_type
        details = details or {}
        if handler_name:
            details["handler_name"] = handler_name
        if message_type:
            details["message_type"] = message_type
        super().__init__(message, component=component, details=details)


class HealthError(LiveFeedError):
    """Raised when health checks fail or feeds become stale."""

    def __init__(
        self,
        message: str,
        *,
        stale_feeds: Optional[list[str]] = None,
        component: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        self.stale_feeds = stale_feeds or []
        details = details or {}
        if stale_feeds:
            details["stale_feeds"] = stale_feeds
        super().__init__(message, component=component, details=details)


class ConfigurationError(LiveFeedError):
    """Raised when configuration is invalid."""

    def __init__(
        self,
        message: str,
        *,
        field: Optional[str] = None,
        value: Optional[Any] = None,
        component: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        self.field = field
        self.value = value
        details = details or {}
        if field:
            details["field"] = field
        if value is not None:
            details["value"] = str(value)
        super().__init__(message, component=component, details=details)


class RateLimitError(LiveFeedError):
    """Raised when rate limits are exceeded."""

    def __init__(
        self,
        message: str,
        *,
        retry_after_ms: Optional[int] = None,
        component: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        self.retry_after_ms = retry_after_ms
        details = details or {}
        if retry_after_ms is not None:
            details["retry_after_ms"] = retry_after_ms
        super().__init__(message, component=component, details=details)
