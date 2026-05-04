import os


def main_script_test_random_seed():
    return {"p2": int(os.environ["HPCFLOW_RUN_RANDOM_SEED"])}
