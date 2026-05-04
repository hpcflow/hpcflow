def input_file_generator_basic(path, p1):
    with path.open("w") as fp:
        fp.write(f"{p1}\n")
