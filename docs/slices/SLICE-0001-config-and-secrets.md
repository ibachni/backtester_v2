---
id: SLICE-0001
title: Config & Secrets Layer
status: planned
ticket: tickets/BT-0001.json
created: 2025-09-23
owner: nicolas
depends_on: [SLICE-0000]
targets: [backtester/ports/config.py, backtester/core/config_loader.py]
contracts: [ConfigProvider, SecretsProvider, RunManifestStore]
ads: [ADR 001, ADR 019]
risk: low
---

# Config & Secrets Layer

## Goal
Provide deterministic, auditable configuration with secure secret handling and layered override precedence.

## Why
Reproducibility and safety require an immutable config snapshot for every run; layering enables ergonomic overrides without code edits.

## Scope
- Typed `Config` dataclass or pydantic model (sections: data, symbols, timeframes, costs, risk limits, runtime SLOs, live endpoints).
- Precedence: defaults < file (YAML/TOML) < env vars < CLI flags.
- `SecretsProvider` (env + optional local secure backend; redaction in logs).
- Manifest capture of effective config hash, Git SHA, start/end times, environment metadata.
- CLI flags: `--config path`, `--set key=value`, `--env-prefix PREFIX_`.

## Out of Scope
External secret managers, multi-environment promotion workflows.

## Deliverables
- Config loader with merge + validation.
- Secrets provider with redaction tests.
- Manifest writing updated to include resolved config + hash.

## Acceptance Criteria
- Identical config/data pair → identical config hash and manifest (excluding timestamps runtime field).
- Invalid/missing required key fails fast with clear error.
- Secrets never appear in manifest or logs (redacted token tests pass).

## Test Plan
- Unit: precedence resolution; invalid value type errors; redaction.
- Property: merging random override orders yields same final hash (commutativity under precedence constraints).

## Risks / Mitigations
- Complex nested merging → keep shallow initially; document extension path.

## Follow-ups
- Add `--print-config` command for transparency.
