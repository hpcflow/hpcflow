import json
import os


def main_script_test_shell_env_vars(p1):
    with open("env_vars.json", "wt") as fp:
        json.dump(
            {k: v for k, v in os.environ.items() if k.startswith("HPCFLOW")},
            fp,
            indent=4,
        )
    return {"p1": p1 + 1}
