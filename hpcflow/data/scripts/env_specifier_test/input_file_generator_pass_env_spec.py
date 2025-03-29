def input_file_generator_pass_env_spec(path, p1, env_spec):
    print(env_spec)
    with path.open("w") as fp:
        fp.write(f"{p1}\n")
