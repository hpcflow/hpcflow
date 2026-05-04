def output_file_parser_pass_env_spec(my_output_file, env_spec):
    print(env_spec)
    with my_output_file.open("r") as fp:
        return int(fp.read().strip())
