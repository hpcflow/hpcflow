from collections.abc import Mapping
from typing import Any


def get_hash(obj):
    """Return a hash from an arbitrarily nested structure dicts, lists, tuples, and
    sets.

    Note the resulting hash is not necessarily stable across sessions or machines.
    """

    if isinstance(obj, set):
        return hash((set, frozenset(get_hash(i) for i in obj)))

    if isinstance(obj, (tuple, list)):
        return hash((type(obj), *(get_hash(i) for i in obj)))

    if isinstance(obj, dict):
        return hash(frozenset((k, get_hash(v)) for k, v in obj.items()))

    return hash(obj)


def freeze(value: Any) -> Any:
    """Recursively convert containers to hashable equivalents.

    Preserves sequence order and raises TypeError if a value cannot be converted to a
    hashable representation.
    """
    if isinstance(value, Mapping):
        return tuple(sorted((k, freeze(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(freeze(v) for v in value)
    if isinstance(value, set):
        return frozenset(freeze(v) for v in value)
    return value
