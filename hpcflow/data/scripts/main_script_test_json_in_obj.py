import json


def main_script_test_json_in_obj(_input_files):
    # read inputs
    with _input_files["json"].open("rt") as fh:
        p1c = json.load(fh)["p1c"]

    # process
    p2 = p1c["a"] + 100

    return {"p2": p2}
