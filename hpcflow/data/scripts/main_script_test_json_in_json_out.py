import json


def main_script_test_json_in_json_out(_input_files, _output_files):
    # read inputs
    with _input_files["json"].open("rt") as fh:
        p1 = int(json.load(fh)["p1"])

    # process
    p2 = p1 + 100

    # save outputs
    with _output_files["json"].open("wt") as fh:
        json.dump({"p2": p2}, fh)
