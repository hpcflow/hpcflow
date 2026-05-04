import h5py  # type: ignore[import-untyped]


def main_script_test_hdf5_in_obj_group(_input_files):
    # read inputs
    with h5py.File(_input_files["hdf5"], mode="r") as fh:
        all_a = [p1c_dat.attrs["a"].item() for p1c_dat in fh["p1c"].values()]

    # process
    p2 = sum(all_a) + 100

    return {"p2": p2}
