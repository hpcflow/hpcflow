from __future__ import annotations


import numbers
from typing import TYPE_CHECKING, overload, Any, Sequence

import numpy as np

if TYPE_CHECKING:
    from numpy.typing import NDArray


@overload
def get_2D_idx(idx: int, num_cols: int) -> tuple[int, int]: ...


@overload
def get_2D_idx(idx: NDArray, num_cols: int) -> tuple[NDArray, NDArray]: ...


def get_2D_idx(idx: int | NDArray, num_cols: int) -> tuple[int | NDArray, int | NDArray]:
    """Convert a 1D index to a 2D index, assuming items are arranged in a row-major
    order."""
    row_idx = idx // num_cols
    col_idx = idx % num_cols
    return (row_idx, col_idx)


def get_1D_idx(
    row_idx: int | NDArray, col_idx: int | NDArray, num_cols: int
) -> int | NDArray:
    """Convert a 2D (row, col) index into a 1D index, assuming items are arranged in a
    row-major order."""
    return row_idx * num_cols + col_idx


def split_arr(arr: NDArray, metadata_size: int) -> list[tuple[NDArray, NDArray]]:
    """Split a 1D integer array into a list of tuples, each containing a metadata array
    and a data array, where the size of each (metadata + data) sub-array is specified as
    the integer immediately before each (metadata + data) sub-array.

    Parameters
    ----------
    arr
        One dimensional integer array to split.
    metadata_size
        How many elements to include in the metadata array. This can be zero.

    Returns
    -------
    sub_arrs
        List of tuples of integer arrays. The integers that define the sizes of the
        sub-arrays are excluded.

    Examples
    --------
    >>> split_arr(np.array([4, 0, 1, 2, 3, 4, 1, 4, 5, 6]), metadata_size=1)
    [(array([0]), array([1, 2, 3])), (array([1]), array([4, 5, 6]))]

    """
    count = 0
    block_start = 0
    sub_arrs = []
    while count < len(arr):
        size = arr[block_start]
        start = block_start + 1
        end = start + size
        metadata_i = arr[start : start + metadata_size]
        sub_arr_i = arr[start + metadata_size : end]
        sub_arrs.append((metadata_i, sub_arr_i))
        count += size + 1
        block_start = end
    return sub_arrs


def is_primitive_homogeneous(lst: Sequence[Any], length: int | None = None):
    """
    Returns True if all items of `lst` are either numeric, bools, or strings, and `lst`
    can be cast to an array with hyperrectangular (i.e. non-ragged) shape.

    Parameters
    ----------
    lst:
        Sequence to check for homogeneous type and shape.
    length:
        Used for recursive calls to check siblings have equal length.
    """
    try:
        if len(lst) == 0:
            return True
    except TypeError:
        # not a sequence
        return False

    if isinstance(lst, np.ndarray) and lst.dtype != np.object_:
        return True

    if length is not None and len(lst) != length:
        return False

    first = lst[0]

    if isinstance(first, Sequence) and not isinstance(first, str):
        return all(is_primitive_homogeneous(x, length=len(first)) for x in lst)

    if isinstance(first, numbers.Number):
        return all(isinstance(x, numbers.Number) for x in lst)

    elif isinstance(first, str):
        return all(isinstance(x, str) for x in lst)

    elif isinstance(first, bool):
        return all(isinstance(x, bool) for x in lst)

    return False


def resize_preserve_data(arr, new_shape, fill_value=0):
    """
    Resize an N-dimensional NumPy array without losing data alignment.

    Parameters
    ----------
    a : np.ndarray
        The input array to be resized.
    new_shape : tuple of int
        The desired shape of the output array.
    fill_value : scalar, optional
        The value used to fill new entries. Default is 0.

    Returns
    -------
    np.ndarray
        A new array with the given shape and preserved data.
    """
    extended = np.full(tuple(new_shape), fill_value, dtype=arr.dtype)
    slices = [slice(0, dim) for dim in arr.shape]
    extended[tuple(slices)] = arr[tuple(slices)]
    return extended
