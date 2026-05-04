from random import randint
from pathlib import Path


def demo_task_1_generate_t1_infile_2(path, p2):
    with Path(path).open("w") as fp:
        fp.write(f"{randint(0, 1e6)}\n")
        fp.write(f"p2: {p2}\n")
