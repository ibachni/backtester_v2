from typing import Any, Mapping

from pydantic import ValidationError


def deep_merge(a: Mapping[str, Any], b: Mapping[str, Any]) -> Mapping[str, Any]:
    out = dict(a)
    for k, v in b.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def validation_error_parser(error: ValidationError):
    parsed_error = [
        {
            "component": "config.schema.defaults",
            "path": ".".join(map(str, err["loc"])),
            "message": err["msg"],
            "error_type": err["type"],
        }
        for err in error.errors()
    ]
    return parsed_error
