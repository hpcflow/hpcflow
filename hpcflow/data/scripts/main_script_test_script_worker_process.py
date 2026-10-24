def main_script_test_script_worker_process(p1):
    """Return the interpreter PID to verify reuse across script invocations."""
    import os

    return {"p2": os.getpid()}
