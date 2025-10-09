from typing import Any, Dict


def insert_path(tree: Dict[str, Any], dotted_path: str, value: Any) -> None:
    """Populate ``tree`` with ``value`` located at ``dotted_path``."""

    segments = [segment.strip() for segment in dotted_path.split(".") if segment.strip()]
    if not segments:
        raise ValueError("Override keys must contain at least one non-empty segment")

    cursor: Dict[str, Any] = tree
    for segment in segments[:-1]:
        existing = cursor.get(segment)
        if existing is None:
            next_node: Dict[str, Any] = {}
            cursor[segment] = next_node
            cursor = next_node
        elif isinstance(existing, dict):
            cursor = existing
        else:
            raise ValueError(
                (
                    f"Cannot override nested path '{dotted_path}': "
                    f"segment '{segment}' is already a value"
                )
            )

    leaf = segments[-1]
    existing_leaf = cursor.get(leaf)
    if isinstance(existing_leaf, dict):
        raise ValueError(
            (f"Cannot assign value to '{dotted_path}': existing node at '{leaf}' is a mapping")
        )
    cursor[leaf] = value
