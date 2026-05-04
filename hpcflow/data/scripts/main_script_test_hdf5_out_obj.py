import h5py  # type: ignore[import-untyped]


def main_script_test_hdf5_out_obj(p1, _output_files):
    # process
    p1c = {"a": p1 + 100}

    # write outputs
    with h5py.File(_output_files["hdf5"], mode="w") as fh:
        p1c_grp = fh.create_group("p1c")
        p1c_grp.attrs["a"] = p1c["a"]
