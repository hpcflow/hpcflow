import json


def main_script_test_json_out_obj(p1, _output_files):
    # process
    p1c = {"a": p1 + 100}

    # write outputs
    with _output_files["json"].open("wt") as fh:
        json.dump({"p1c": p1c}, fh)
