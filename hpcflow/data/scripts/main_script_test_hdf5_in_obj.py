import h5py  # type: ignore[import-untyped]


def main_script_test_hdf5_in_obj(_input_files):
    # read inputs
    with h5py.File(_input_files["hdf5"], mode="r") as fh:
        a = fh["p1c"].attrs["a"].item()

    # process
    p2 = a + 100

    return {"p2": p2}
