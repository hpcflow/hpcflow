"""
Workflow persistence subsystem.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base import PersistentStore


def store_cls_from_str(store_format: str) -> type[PersistentStore]:
    """
    Get the class that implements the persistence store from its name.
    """
    from .discovery import store_cls_from_str as impl

    return impl(store_format)
