import h5py  # type: ignore


def main_script_test_hdf5_in_obj_2(p2, _input_files):
    # read inputs
    with h5py.File(_input_files["hdf5"], mode="r") as fh:
        a = fh["p1c"].attrs["a"].item()

    # process
    p3 = a + 100

    return {"p3": p3}
