"""
Utilities for discovering what persistence store implementation to use.
"""

from __future__ import annotations
from typing import TYPE_CHECKING
from hpcflow.sdk.persistence.json import JSONPersistentStore
from hpcflow.sdk.persistence.zarr import ZarrPersistentStore, ZarrZipPersistentStore

if TYPE_CHECKING:
    from collections.abc import Mapping
    from .base import PersistentStore

# Because of python/mypy#4717, we need to disable an error here:
# mypy: disable-error-code="type-abstract"
_ALL_STORE_CLS: Mapping[str, type[PersistentStore]] = {
    "zarr": ZarrPersistentStore,
    "zip": ZarrZipPersistentStore,
    "json": JSONPersistentStore,
    # "json-single": JSONPersistentStore,  # TODO
}
# Without that, there's literally no way to write the above with a sane type.

#: The persistence formats supported.
ALL_STORE_FORMATS = tuple(_ALL_STORE_CLS)
#: The persistence formats supported for creation.
ALL_CREATE_STORE_FORMATS = tuple(
    k for k, v in _ALL_STORE_CLS.items() if v._features.create
)


def store_cls_from_str(store_format: str) -> type[PersistentStore]:
    """
    Get the class that implements the persistence store from its name.
    """
    try:
        return _ALL_STORE_CLS[store_format]
    except KeyError:
        raise ValueError(f"Store format {store_format!r} not known.")
