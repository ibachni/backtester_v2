import logging

import pytest

from backtester.adapters.env_provider import EnvSecretsProvider, MissingSecretError


@pytest.fixture(autouse=True)
def reset_logging_handlers():
    # Keep test logging deterministic and avoid leaking handlers between tests.
    logging.getLogger("backtester.adapters.env_provider").handlers = []


def test_get_returns_env_value(monkeypatch):
    # modify environment variables
    monkeypatch.setenv("BT_SECRET_EXCHANGE_API_KEY", "super-secret")

    provider = EnvSecretsProvider()

    assert provider.get("exchange_api_key") == "super-secret"


def test_missing_secret_raises_for_unknown_name(monkeypatch):
    monkeypatch.setenv("BT_SECRET_EXCHANGE_API_KEY", "super-secret")
    provider = EnvSecretsProvider()

    with pytest.raises(MissingSecretError) as exc:
        provider.get("nonexistent")

    assert "nonexistent" in str(exc.value)


def test_missing_secret_raises_when_env_absent():
    provider = EnvSecretsProvider()

    with pytest.raises(MissingSecretError) as exc:
        provider.get("exchange_api_key")

    assert "exchange_api_key" in str(exc.value)


def test_custom_prefix_and_allowlist(monkeypatch):
    monkeypatch.setenv("MY_APP_TOKEN", "token-123")
    provider = EnvSecretsProvider(prefix="MY_", allowed={"app_token": "APP_TOKEN"})

    assert provider.get("app_token") == "token-123"
