from typing import Any, Mapping, Optional


class ConfigLoader:
    def resolve(
        self,
        defaults: Mapping[str, Any],
        file_cfg: Optional[Mapping[str, Any]],
        env_cfg: Optional[Mapping[str, Any]],
        cli_overrides: Optional[Mapping[str, Any]],
    ) -> dict[str, Any]:
        """
        Resolves differing arguments according to hierachy.
        Implements precedence merge (apply layers sequentially)

        Inputs:
        - defaults: static in-repo default config (authoritative baseline).
        - file_cfg: parsed user config file (YAML/TOML → dict) or None.
        - env_cfg: key/value pairs already transformed into nested dict form (after parsing prefix &
        double-underscore path syntax) or None.
        - cli_overrides: dict from parsed --set key=value flags (dot-paths expanded) or None.

        Idea:
        """
        # resolved_config = dict(defaults)
        if file_cfg:
            for key in file_cfg.keys():
                pass

        return {}

    def validate(self, cfg) -> None:
        """
        Checks required keys, such as symbols and risk.max_position.
        Why: Fail fast to avoid silent misconfig.
        """
        pass

    def compute_hash(self, cfg) -> None:
        pass

    def redact(self, cfg, secret_keys) -> None:
        pass
