# ADR 019: Config & Secrets Management

Status: Accepted
Date: 2025-09-23
Context Version: 1.0

## Context
Mixing configuration directly in code makes reproducibility and diff review harder. Secrets in the repository pose security risk. Need a layered approach that keeps runs deterministic while allowing environment-specific overrides.

## Decision
Primary configuration in a typed YAML or TOML file committed to the repo. Environment variables may override specific keys at runtime (precedence: env > file > defaults). Secrets (API keys, tokens) are never stored in config files—only referenced via environment variable names. No external key-value store initially.

## Consequences
+ Clear diffable history of run parameters.
+ Safe handling of secrets.
− Two places to inspect (file + env overrides) during troubleshooting.

## Alternatives Considered
*Hard-coded constants*: Not auditable.
*External secret manager early*: Adds complexity not needed locally.

## Revisit Criteria
- Multi-user deployment across machines.
- Need hierarchical environment layering (dev/stage/prod).

## Implementation Notes
- Provide a loader that materializes a validated `Config` dataclass.
- Capture final resolved config hash in run manifest.
- Optionally expose `--print-config` to surface resolved values.

## Related
Determinism (001), Safety (002), Risk Rails (018).
