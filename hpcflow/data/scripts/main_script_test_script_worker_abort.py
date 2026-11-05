def main_script_test_script_worker_abort(p1):
    """Block the first element until aborted, and report later worker PIDs."""
    import json
    import os
    from pathlib import Path
    import time

    if os.environ["HPCFLOW_ELEMENT_IDX"] == "0":
        Path(os.environ["WORKER_CONTROL_PATH"]).write_text(
            json.dumps({"pid": os.getpid()}), encoding="utf-8", newline="\n"
        )
        while True:
            time.sleep(0.1)
    return {"p2": os.getpid()}
