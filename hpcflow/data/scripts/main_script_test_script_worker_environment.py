def main_script_test_script_worker_environment(p1, env_spec):
    """Report worker identity and invocation context before mutating its environment."""
    import os
    import sys

    output = {
        "pid": os.getpid(),
        "executable": sys.executable,
        "group": env_spec["worker_group"],
        "setup_group": os.environ.get("WORKER_GROUP"),
        "run_id": os.environ["HPCFLOW_RUN_ID"],
        "element_idx": os.environ["HPCFLOW_ELEMENT_IDX"],
        "leaked": os.environ.get("WORKER_LEAK"),
    }
    os.environ["WORKER_LEAK"] = str(p1)
    return {"p2": output}
