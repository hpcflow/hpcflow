import sys


def output_file_parser_test_stdout_stderr(my_output_file, p1):
    print(p1)
    print(p1, file=sys.stderr)
    with my_output_file.open("r") as fp:
        return int(fp.read().strip())
