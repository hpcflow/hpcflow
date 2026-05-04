def output_file_parser_basic(my_output_file):
    with my_output_file.open("r") as fp:
        return int(fp.read().strip())
