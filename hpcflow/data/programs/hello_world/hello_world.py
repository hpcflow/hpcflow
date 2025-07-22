import json
from pathlib import Path
import sys


def hello_world_ins_outs(inputs_JSON_path, outputs_JSON_path):
    print("hello, world")
    with Path(inputs_JSON_path).open("rt") as fp:
        dat = json.load(fp)
        p1 = dat["p1"]
        p2 = dat["p2"]
        p3 = dat["p3"]

    p4 = sum((p1, p2, p3))

    with Path(outputs_JSON_path).open("wt") as fp:
        json.dump({"p4": p4}, fp)


def hello_world():
    print("hello, world")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        hello_world()
    else:
        hello_world_ins_outs(*args)
