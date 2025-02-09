def get_2D_idx(idx, num_cols=10):
    """Convert a 1D index to a 2D index, assuming items are arranged in a row-major
    order."""
    row_idx = idx // num_cols
    col_idx = idx % num_cols
    return (row_idx, col_idx)


def get_1D_idx(row_idx, col_idx, num_cols=10):
    """Convert a 2D (row, col) index into a 1D index, assuming items are arranged in a
    row-major order."""
    return row_idx * num_cols + col_idx
