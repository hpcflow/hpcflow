from __future__ import annotations

from typing import TYPE_CHECKING, overload

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
