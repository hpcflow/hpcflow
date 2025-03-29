import numpy as np

from hpcflow.sdk.utils.arrays import get_1D_idx, get_2D_idx, split_arr


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
