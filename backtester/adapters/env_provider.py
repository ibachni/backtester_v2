from __future__ import annotations

import logging
import os

from backtester.ports.secrets_provider import SecretsProvider

_LOGGER = logging.getLogger(__name__)


class MissingSecretError(ValueError):
    """
    Raised when a logical secret cannot be resolved from the environment.
    """

    def __init__(self, secret_name: str) -> None:
        super().__init__(secret_name)
        self.secret_name = secret_name

    def __str__(self) -> str:
        return f"Secret '{self.secret_name}' is unavailable"


class EnvSecretsProvider(SecretsProvider):
    def __init__(
        self,
        prefix: str = "BT_SECRET_",
        allowed: dict[str, str] | None = None,
        fallback_path: str | None = None,
    ) -> None:
        """
        Configure deterministic secret lookup rules for environment-backed secrets.
        """

        if not prefix:
            raise ValueError("Environment prefix must be a non-empty string")
        # storing the environment variable prefix for later use (append to all secret keys)
        self._prefix = prefix
        # define a default mapping of logical scret names to their environment variable suffixes
        base_allowed: dict[str, str] = {
            "exchange_api_key": "EXCHANGE_API_KEY",
            "exchange_api_secret": "EXCHANGE_API_SECRET",
        }
        if allowed:
            base_allowed.update(allowed)
        # store for later use.
        self._allowed = base_allowed
        self._fallback_path = fallback_path

        #
        if fallback_path is not None:
            _LOGGER.debug(
                "env_secrets_fallback_disabled",
                extra={
                    "event": "env_secrets_fallback_disabled",
                    "fallback_path": fallback_path,
                    "reason": "fallback requires ADR before enabling",
                },
            )

    def get(self, secret_name: str) -> str:
        """Resolve a logical secret name to a concrete environment variable value."""

        if secret_name not in self._allowed:
            raise MissingSecretError(secret_name)

        env_suffix = self._allowed[secret_name]
        env_var = f"{self._prefix}{env_suffix}"
        try:
            value = os.environ[env_var]
        except KeyError as exc:  # pragma: no cover - mirrored branch
            raise MissingSecretError(secret_name) from exc

        _LOGGER.debug(
            "secret_resolved",
            extra={
                "event": "secret_resolved",
                "secret_name": secret_name,
                "source": "env",
            },
        )
        _LOGGER.debug(
            "secrets_lookup_total",
            extra={
                "event": "secrets_lookup_total",
                "secret_name": secret_name,
            },
        )
        return value
