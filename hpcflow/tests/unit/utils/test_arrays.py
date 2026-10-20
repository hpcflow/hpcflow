import numpy as np

from hpcflow.sdk.utils.arrays import (
    get_1D_idx,
    get_2D_idx,
    reshape_max_width,
    split_arr,
)


def test_get_2D_idx():
    assert get_2D_idx(0, num_cols=10) == (0, 0)
    assert get_2D_idx(4, num_cols=10) == (0, 4)
    assert get_2D_idx(9, num_cols=10) == (0, 9)
    assert get_2D_idx(10, num_cols=10) == (1, 0)
    assert get_2D_idx(13, num_cols=10) == (1, 3)
    assert get_2D_idx(20, num_cols=10) == (2, 0)
    arr_r, arr_c = get_2D_idx(np.array([0, 4, 9, 10, 13, 20]), num_cols=10)
    assert np.array_equal(arr_r, np.array([0, 0, 0, 1, 1, 2]))
    assert np.array_equal(arr_c, np.array([0, 4, 9, 0, 3, 0]))


def test_get_1D_idx():
    assert get_1D_idx(*(0, 0), num_cols=10) == 0
    assert get_1D_idx(*(0, 4), num_cols=10) == 4
    assert get_1D_idx(*(0, 9), num_cols=10) == 9
    assert get_1D_idx(*(1, 0), num_cols=10) == 10
    assert get_1D_idx(*(1, 3), num_cols=10) == 13
    assert get_1D_idx(*(2, 0), num_cols=10) == 20

    assert np.array_equal(
        get_1D_idx(
            np.array([0, 0, 0, 1, 1, 2]), np.array([0, 4, 9, 0, 3, 0]), num_cols=10
        ),
        np.array([0, 4, 9, 10, 13, 20]),
    )


def test_split_arr():
    splt = split_arr(np.array([4, 0, 1, 2, 3, 4, 1, 4, 5, 6]), metadata_size=1)
    assert len(splt) == 2
    assert np.array_equal(splt[0][0], np.array([0]))
    assert np.array_equal(splt[0][1], np.array([1, 2, 3]))
    assert np.array_equal(splt[1][0], np.array([1]))
    assert np.array_equal(splt[1][1], np.array([4, 5, 6]))


def test_reshape_max_width_row_interleaved():
    arr = np.array([[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]])
    out, n_blocks, col_map = reshape_max_width(arr, 3, dtype=int, fill=-1)
    assert n_blocks == 2
    assert col_map == {3: 0, 4: 1}
    assert np.array_equal(
        out,
        np.array(
            [
                [0, 1, 2],
                [3, 4, -1],
                [5, 6, 7],
                [8, 9, -1],
            ]
        ),
    )


def test_reshape_max_width_block_major():
    arr = np.array([[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]])
    out, n_blocks, col_map = reshape_max_width(
        arr, 3, dtype=int, fill=-1, block_major=True
    )
    assert n_blocks == 2
    assert col_map == {3: 0, 4: 1}
    assert np.array_equal(
        out,
        np.array(
            [
                [0, 1, 2],
                [5, 6, 7],
                [3, 4, -1],
                [8, 9, -1],
            ]
        ),
    )


def test_reshape_max_width_no_wrap_needed():
    arr = np.array([[0, 1, 2], [3, 4, 5]])
    for block_major in (False, True):
        out, n_blocks, col_map = reshape_max_width(
            arr, 3, dtype=int, fill=-1, block_major=block_major
        )
        assert n_blocks == 1
        assert col_map == {}
        assert np.array_equal(out, arr)
