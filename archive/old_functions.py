"""
# 3. Environment config via BT_ prefixed variables as part of run_noop
  env_cfg = _collect_env_config(os.environ, prefix=ENV_PREFIX)
"""
# def _collect_env_config(
#     environ: Mapping[str, str], *, prefix: str = ENV_PREFIX
# ) -> Mapping[str, Any] | None:
#     """
#     Translate environment variables with ``prefix`` into a nested mapping.

#     Uses ``__`` as the segment separator (e.g. ``BT_RISK__MAX_POSITION`` →
#     ``{"risk": {"max_position": value}}``). Returns ``None`` when no overrides
#     are detected so callers can skip the layer entirely.
#     """
#     overrides: Dict[str, Any] = {}
#     for raw_key, raw_value in environ.items():
#         if not raw_key.startswith(prefix):
#             continue
#         stripped = raw_key[len(prefix) :]
#         if not stripped:
#             continue
#         segments = [segment.lower() for segment in stripped.split("__") if segment]
#         if not segments:
#             continue
#         cursor: Dict[str, Any] = overrides
#         for segment in segments[:-1]:
#             cursor = cursor.setdefault(segment, {})
#         cursor[segments[-1]] = raw_value
#     return overrides or None
