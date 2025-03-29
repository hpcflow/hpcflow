def output_file_parser_basic_FAIL(my_output_file):
    # parse the output file, but then delete it, so it cannot be saved (when run with
    # `OFP.save_files=True`):
    with my_output_file.open("r") as fp:
        out = int(fp.read().strip())
    my_output_file.unlink()
    return out
