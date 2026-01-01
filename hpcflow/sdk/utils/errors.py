from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import TypeVar

    T = TypeVar("T")


class StoredIndexError(IndexError):
    def __init__(self, index: int, message: str = "index out of range"):
        super().__init__(message)
        self.index = index


def get_with_index(lst: list[T], index: int) -> T:
    """A get-item wrapper that catches IndexError and raises a sub-class that stores the
    index."""
    try:
        return lst[index]
    except IndexError:
        raise StoredIndexError(index) from None
