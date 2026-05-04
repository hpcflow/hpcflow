from pathlib import Path


def demo_task_1_parse_p3(t1_outfile_1):
    with Path(t1_outfile_1).open("r") as fp:
        p3 = int(fp.readline().strip())
    return p3
