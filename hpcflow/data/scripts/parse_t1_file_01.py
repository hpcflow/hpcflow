def parse_t1_file_01(t1_file_01):
    with t1_file_01.open("r") as fp:
        p4 = int(fp.readline().strip())
    return p4
