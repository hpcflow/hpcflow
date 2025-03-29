import sys


def main_script_test_std_out_std_err(stdout_msg, stderr_msg):
    print(stdout_msg)
    print(stderr_msg, file=sys.stderr)
