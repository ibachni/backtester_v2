import pytest

# NOTE: This file is intentionally failing (red) until Config layer is implemented.
# Each test encodes GIVEN/WHEN/THEN from ticket BT-0001.


def test_config_precedence_simple_key():
    # GIVEN layered values for risk.max_position across precedence levels
    # WHEN resolving (not yet implemented)
    # THEN final value should equal highest-precedence (CLI) override
    pytest.fail("Not implemented: precedence resolution")


def test_invalid_required_key_raises():
    # GIVEN missing required 'symbols' key
    # WHEN validating
    # THEN ValueError mentioning 'symbols' is raised
    pytest.fail("Not implemented: validation for required keys")


def test_secrets_redacted_in_manifest(tmp_path):
    # GIVEN secret api_key provided via SecretsProvider
    # WHEN manifest is written
    # THEN api_key stored as '***REDACTED***'
    pytest.fail("Not implemented: secrets redaction in manifest")


def test_config_hash_determinism():
    # GIVEN two resolution sequences with different override order
    # WHEN computing hash
    # THEN hashes match
    pytest.fail("Not implemented: deterministic hash")
