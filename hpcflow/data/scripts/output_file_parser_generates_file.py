from pathlib import Path


def output_file_parser_generates_file(my_output_file):
    # parse the output file, and also generate a new file that the parser declares via
    # `generates_files`:
    with my_output_file.open("r") as fp:
        out = int(fp.read().strip())
    Path("generated.txt").write_text(f"generated from {out}")
    return out
