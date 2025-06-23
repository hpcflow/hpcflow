from contextlib import contextmanager
from pathlib import Path
import sys
from typing import Any


def resolve_path(path):
    """On Windows Python 3.8, 3.9, and 3.10, `Pathlib.resolve` does
    not return an absolute path for non-existant paths, when it should.

    See: https://github.com/python/cpython/issues/82852

    """
    # TODO: this only seems to be used in a test; remove?
    return Path.cwd() / Path(path).resolve()  # cwd is ignored if already absolute


@contextmanager
def override_module_attrs(module_name: str, overrides: dict[str, Any]):
    """Context manager to temporarily override module-level attributes. The module must be
    imported (i.e. within `sys.modules`)."""

    module = sys.modules[module_name]
    original_values = {k: getattr(module, k) for k in overrides}
    try:
        for k, v in overrides.items():
            setattr(module, k, v)
        yield
    finally:
        for k, v in original_values.items():
            setattr(module, k, v)
