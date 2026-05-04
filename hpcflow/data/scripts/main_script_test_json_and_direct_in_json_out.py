import json


def main_script_test_json_and_direct_in_json_out(p2, _input_files, _output_files):
    # read inputs
    with _input_files["json"].open("rt") as fh:
        p1 = int(json.load(fh)["p1"])

    # process
    p3 = p1 + p2

    # save outputs
    with _output_files["json"].open("wt") as fh:
        json.dump({"p3": p3}, fh)
