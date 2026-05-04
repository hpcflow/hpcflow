from random import randint
from pathlib import Path


def demo_task_1_generate_t1_infile_1(path, p1):
    with Path(path).open("w") as fp:
        fp.write(f"{randint(0, 1e6)}\n")
        fp.write(f"p1: {p1}\n")
