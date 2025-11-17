import copy
import hashlib
import json
from typing import Any, Iterable, Mapping, MutableMapping, Optional, Sequence

from pydantic import ValidationError
from pydantic.type_adapter import TypeAdapter
from pydantic_core import ValidationError as CoreValidationError

from ..core.models import Config, ReturnConfig
from ..core.utility import deep_merge, validation_error_parser
from ..ports.telemetry import Telemetry

"""
Purpose:
    - Merge configuration layers (defaults, file, cli)
"""


"""
# TODO: Introduce a dedicated eception family:
# TODO Catch Errors!
- ConfigResolutionError with: LayeringError, ValidationError, SecretRedactionError
- Each subtype can carry context fields: stage, key_path, raw_message, layer_source
- Normalize Messages immediately when catching lower-level exceptions
(i.e., pydantic, TypeErrors): translate them into a structured payload
({stage, reason_code, detail, hint}). Log once via the telemtry port (config_validation_error)
before bubbling up.
- Aggregate multiple issues per stage (collect multiple errors on missing keys into a single
error message)
- Attach metadata (run_id, config_hash, layer_name, canonical key path)

- Documenting: Error Catalog table (code, stage, trigger, message, mitigation, logged as)

"""

# TODO Collect all such design parameters in a config!
ALLOWED_KEYS: set[str] = set(Config.model_fields.keys())
FIELD_TYPE_ADAPTERS: dict[str, Any] = {
    name: TypeAdapter(field.annotation)
    for name, field in Config.model_fields.items()
    if field.annotation is not Any
}
REDACTED: str = "***REDACTED***"
HASH_INCLUDES_SECRETS: bool = True  # comment rationale: detects secret drift
CONFIG_KEYS_COUNT_MODE: str = "leaf"  # higher computation, more accurate
NUMERIC_STRING_PATHS: set[tuple[str, ...]] = {
    ("risk", "max_position"),
    ("runtime", "seed"),
}
SECRET_KEYS: list[str] = ["secrets.api_key", "secrets.api_secret"]

# TODO 1. resolve any remaining errors.
# TODO 2. Catch errors
# TODO 3. add dedicated run_id, git_sha etc. such that it is created once and can be retrieved


class ConfigLoader:
    def __init__(self, telemetry: Telemetry) -> None:
        # TODO: different JsonlTelemetry instances for different types of logs!
        self.telemetry = telemetry

    # --- pipeline (resolve) ---------------------------------

    def resolve(
        self,
        defaults: Mapping[str, Any],
        file_cfg: Optional[Mapping[str, Any]],
        cli_overrides: Optional[Mapping[str, Any]],
        secret_paths: Optional[Iterable[str | Iterable[str]]] = None,
    ) -> ReturnConfig:
        """
        1. Calls layering.merge_layers
        2. Pass merged dict into schema.normalize and validate
        3. feed validated config into render_config
        4. Build and returns ReturnConfig with the full set of fields:
            - Internal
            - redacted
            - hash
            - keys_count
            - redacted_count
        """
        # 1. Merging layers
        try:
            config: Mapping[str, Any] = Config(**defaults).model_dump()
        except ValidationError as e:
            parsed_error = validation_error_parser(e)
            self.telemetry.log(
                event="config_validation_error",
                layer="layering",
                step="defaults_validation",
                errors=parsed_error,
            )
            missing_paths = sorted(
                {
                    error["path"]
                    for error in parsed_error
                    if error.get("error_type") == "missing"
                    or error.get("message", "").lower().startswith("field required")
                }
            )
            if missing_paths:
                joined = ", ".join(missing_paths)
                raise KeyError(f"Missing required config key(s): {joined}") from e
            raise

        for layer in [file_cfg, cli_overrides]:
            if layer is not None:
                config = deep_merge(config, layer)

        # 2. Pass merged into normalization
        self._validate(config, check_required=True)
        config = self._normalize_config(config)

        # 3. Feed validated config
        internal_config: Mapping[str, Any] = self._sort_mapping(config)
        # hash the sorted and non-redacted config

        config_hash: str = self.compute_hash(internal_config)
        secret_keys = SECRET_KEYS if secret_paths is None else secret_paths
        # create redacted version
        redacted_config: tuple[Mapping[str, Any], int] = self._redact(
            cfg=internal_config, secret_keys=secret_keys
        )
        # count leafs
        config_keys_total: int = len(self._collect_leaves(internal_config))

        self.telemetry.log(
            event="config_resolved",
            config_hash=config_hash,
            hash_includes_secrets=True,
            redacted_config=redacted_config[0],
            redacted_count=redacted_config[1],
            config_keys_total=config_keys_total,
            unexpected_keys_detected=False,
        )

        return ReturnConfig(
            internal_config=internal_config,
            redacted_config=redacted_config[0],
            redacted_count=redacted_config[1],
            config_hash=config_hash,
            config_keys_total=config_keys_total,
        )

    # --- schema (validation/normalization) ------------------

    def _validate(
        self,
        cfg: Mapping[str, Any],
        *,
        check_required: bool = False,
    ) -> None:
        """
        Fail fast to avoid silent misconfig.
        On failure: raises Value Error
        Input: cfg (mapping, shallow or one nested level)
        """
        # 1. Finding unexpected, non-allowed keys by computing the set difference.
        unexpected = set(cfg) - ALLOWED_KEYS
        if unexpected:
            self.telemetry.log(
                event="config_validation_error",
                layer="schema",
                step="unexpected keys",
                errors=[
                    {
                        "code": "CFG-003",
                        "path": None,
                        "message": f"unexpexted keys(s): {', '.join(unexpected)}",
                        "keys": unexpected,
                    }
                ],
            )
            raise KeyError(f"Unexpected config key(s): {unexpected}")

        # 2. For each key value, validate the type.
        for key, value in cfg.items():
            # Get the type annotation defined
            adapter = FIELD_TYPE_ADAPTERS.get(key)
            if adapter is None:
                continue
            try:
                # does the value correspond with the TypeAnnotation defined?
                adapter.validate_python(value)
            except (CoreValidationError, TypeError, ValueError) as exc:
                if isinstance(exc, CoreValidationError):
                    parsed_error = validation_error_parser(exc)
                else:
                    parsed_error = [
                        {
                            "message": str(exc),
                            "path": key,
                            "error_type": exc.__class__.__name__,
                        }
                    ]
                self.telemetry.log(
                    event="config_validation_error",
                    layer="schema",
                    step="key_value_validation",
                    errors=parsed_error,
                )
                raise TypeError(f"Invalid value for '{key}': {exc}") from exc
        # 3. Finding missing keys
        if check_required:
            missing = ALLOWED_KEYS - set(cfg)
            if missing:
                missing_list = ", ".join(sorted(missing))
                self.telemetry.log(
                    event="config_validation_error",
                    layer="schema",
                    step="required_keys",
                    errors=[
                        {  # TODO: Make a list of codes to track!
                            "code": "CFG-003",
                            "path": None,
                            "message": f"Missing required config key(s): {', '.join(missing_list)}",
                            "keys": missing_list,
                        }
                    ],
                )
                raise KeyError(f"Missing required config key(s): {missing_list}")

    def _normalize_config(self, cfg: Mapping[str, Any]) -> dict[str, Any]:
        """
        Return a deep-copied configuration with canonical casing and primitive types
        for known fields. Keeps callers' original mappings untouched.
        """
        normalized: dict[str, Any] = copy.deepcopy(dict(cfg))

        # Normalizing symbols
        symbols = normalized.get("symbols")
        if isinstance(symbols, list):
            normalized_symbols: list[str] = []
            for symbol in symbols:
                if not isinstance(symbol, str):
                    self.telemetry.log(
                        event="config_validation_error",
                        layer="normalization",
                        step="normalized_symbols",
                        errors=[
                            {
                                "code": "CFG-003",
                                "path": None,
                                "message": f"Symbol not a string: {symbol!r}",
                                "keys": symbol,
                            }
                        ],
                    )
                    raise TypeError("All entries in 'symbols' must be strings")
                stripped = symbol.strip()
                if not stripped:
                    self.telemetry.log(
                        event="config_validation_error",
                        layer="normalization",
                        step="normalized_symbols",
                        errors=[
                            {
                                "code": "CFG-003",
                                "path": None,
                                "message": f"Symbol must not contain blank entries: {symbol!r}",
                                "keys": symbol,
                            }
                        ],
                    )
                    raise ValueError("Symbols must not contain blank entries")
                normalized_symbols.append(stripped.upper())
            normalized["symbols"] = normalized_symbols

        for path in NUMERIC_STRING_PATHS:
            parent: MutableMapping[str, Any] | None = (
                normalized if isinstance(normalized, MutableMapping) else None
            )
            for segment in path[:-1]:
                if parent is None or segment not in parent:
                    parent = None
                    break
                next_node = parent[segment]
                if isinstance(next_node, MutableMapping):
                    parent = next_node
                else:
                    parent = None
                    break

            if parent is None:
                continue

            leaf = path[-1]
            value = parent.get(leaf)
            if isinstance(value, str):
                candidate = value.strip()
                if candidate.isdigit():
                    parent[leaf] = int(candidate)

        return normalized

    # --- render (hashing, redaction, manifest packaging) ----

    def _sort_mapping(self, obj: Any) -> Any:
        if isinstance(obj, Mapping):
            return {k: self._sort_mapping(obj[k]) for k in sorted(obj)}
        if isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray)):
            return [self._sort_mapping(item) for item in obj]
        if isinstance(obj, (set, frozenset)):
            # sets have no deterministic order
            return sorted(self._sort_mapping(item) for item in obj)
        return obj

    def compute_hash(self, cfg: Mapping[str, Any]) -> str:
        canonical = self._sort_mapping(cfg)
        payload = json.dumps(canonical, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _normalize_path(self, path: str | Iterable[str]) -> tuple[str, ...]:
        """
        Normalize incoming key specs by translating each entry into a tuple
        of path segments (e.g. "secrets.api_key" → ("secrets", "api_key");
        already-tupled paths left as-is). Reject empty paths early.
        """
        if isinstance(path, str):
            parts = tuple(segment for segment in path.split(".") if segment)
        else:
            parts = tuple(path)
        if not parts:
            raise ValueError("Secret path must contain at least one segment")
        return parts

    def _redact(
        self, cfg: Mapping[str, Any], secret_keys: Iterable[str | Iterable[str]]
    ) -> tuple[Mapping[str, Any], int]:
        """


        For a path like ("runtime", "exchange", "api_key"), you’d traverse:
            parent = redacted_cfg
            parent = parent["runtime"]
            parent = parent["exchange"]
        Now, parent["api_key"] is the field to redact.
        """

        # 1. Normalize the path
        paths = [self._normalize_path(k) for k in secret_keys]

        # 2. Createa deepcopy
        redacted_cfg = copy.deepcopy(cfg)

        # 3. Iterare over secret paths
        count = 0  # tracking how many secrets are redacted
        for path in paths:
            # 3.1. for each path (e.g., "secrets", "api_key"), start at the top-level of the config
            parent = redacted_cfg
            # 3.2. Traverse the parent dictionary (navigate to the dict with secret field,
            # not field itself)
            for segment in path[:-1]:
                # If at any point, the parent is not a dict or the segment is missing,
                # break (skip this path)
                if not isinstance(parent, dict) or segment not in parent:
                    break
                parent = parent[segment]

            # executes if no break was hit.
            # If no break was hit, successful traverse to parent dict happened.
            else:
                # if traversal succeeded, set the value at leaf key to the redaction token.
                leaf = path[-1]
                if isinstance(parent, dict) and leaf in parent:
                    parent[leaf] = REDACTED
                    count += 1
        return redacted_cfg, count

    def _collect_leaves(
        self, tree: Mapping[str, Any], *, _prefix: tuple[str, ...] = ()
    ) -> list[str]:
        """
        Return flattened key paths for every non-mapping value in ``tree``.

        Lists, scalars, and other primitives count as a single leaf keyed by the full
        dotted path (e.g. ``("risk", "max_position")`` → ``"risk.max_position"``).
        """
        leaves: list[str] = []
        for key, value in tree.items():
            path = _prefix + (str(key),)
            if isinstance(value, Mapping):
                leaves.extend(self._collect_leaves(value, _prefix=path))
            else:
                leaves.append(".".join(path))
        return leaves
