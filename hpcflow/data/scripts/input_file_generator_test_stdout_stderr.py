import sys


def input_file_generator_test_stdout_stderr(path, p1):
    print(p1)
    print(p1, file=sys.stderr)
    with path.open("w") as fp:
        fp.write(f"{p1}\n")
