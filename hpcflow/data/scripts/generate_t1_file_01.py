from random import randint


def generate_t1_file_01(path, p1):
    with path.open("w") as fp:
        fp.write(f"{randint(0, 1e6)}\n")
        fp.write(f"p1: {p1}\n")
