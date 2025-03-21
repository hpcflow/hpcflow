def main_script_test_pass_env_spec(p1, env_spec):
    # process
    p2 = p1 + 100

    print(env_spec)

    # return outputs
    return {"p2": p2}
