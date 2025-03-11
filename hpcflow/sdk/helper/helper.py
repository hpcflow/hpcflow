"""
Implementation of a helper process used to monitor jobs.
"""

from __future__ import annotations
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any, TYPE_CHECKING
import psutil

from hpcflow.sdk.helper.watcher import MonitorController

if TYPE_CHECKING:
    from collections.abc import Callable
    from ..app import BaseApp


DEFAULT_TIMEOUT = 3600  # seconds
DEFAULT_TIMEOUT_CHECK = 60  # seconds
DEFAULT_WATCH_INTERVAL = 10  # seconds


def kill_proc_tree(
    pid: int,
    sig=signal.SIGTERM,
    include_parent: bool = True,
    timeout: float | None = None,
    on_terminate: Callable[[psutil.Process], object] | None = None,
) -> tuple[list[psutil.Process], list[psutil.Process]]:
    """Kill a process tree (including grandchildren) with signal
    `sig` and return a (gone, still_alive) tuple.
    `on_terminate`, if specified, is a callback function which is
    called as soon as a child terminates.

    Returns
    -------
    list[Process]:
        The process and subprocesses that have died.
    list[Process]:
        The process and subprocesses that are still alive.
    """
    assert pid != os.getpid(), "won't kill myself"
    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    if include_parent:
        children.append(parent)
    for p in children:
        try:
            p.send_signal(sig)
        except psutil.NoSuchProcess:
            pass
    return psutil.wait_procs(children, timeout=timeout, callback=on_terminate)


def get_PID_file_path(app: BaseApp) -> Path:
    """Get the path to the file containing the process ID of the helper, if running."""
    return app.user_data_dir / "pid.txt"


def get_watcher_file_path(app: BaseApp) -> Path:
    """Get the path to the watcher file, which contains a list of workflows to watch."""
    return app.user_data_dir / "watch_workflows.txt"


def get_helper_log_path(app: BaseApp) -> Path:
    """Get the log file path for the helper."""
    return app.user_data_dir / "helper.log"


def get_helper_watch_list(app: BaseApp):
    """Get the list of workflows currently being watched by the helper process."""
    watch_file_path = get_watcher_file_path(app)
    if watch_file_path.exists():
        return MonitorController.parse_watch_workflows_file(
            watch_file_path, get_helper_logger(app)
        )
    return None


def start_helper(
    app: BaseApp,
    timeout: timedelta | float = DEFAULT_TIMEOUT,
    timeout_check_interval: timedelta | float = DEFAULT_TIMEOUT_CHECK,
    watch_interval: timedelta | float = DEFAULT_WATCH_INTERVAL,
    logger: logging.Logger | None = None,
):
    """
    Start the helper process.
    """
    PID_file = get_PID_file_path(app)
    if PID_file.is_file():
        with PID_file.open("rt") as fp:
            helper_pid = int(fp.read().strip())
            print(f"Helper already running, with process ID: {helper_pid}")

    else:
        logger = logger or get_helper_logger(app)
        logger.info(
            f"Starting helper with timeout={timeout!r}, timeout_check_interval="
            f"{timeout_check_interval!r} and watch_interval={watch_interval!r}."
        )
        kwargs: dict[str, Any] = {}
        if os.name == "nt":
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)

        if isinstance(timeout, timedelta):
            timeout = timeout.total_seconds()
        if isinstance(timeout_check_interval, timedelta):
            timeout_check_interval = timeout_check_interval.total_seconds()
        if isinstance(watch_interval, timedelta):
            watch_interval = watch_interval.total_seconds()

        args = [
            *app.run_time_info.invocation_command,
            "--config-dir",
            str(app.config.config_directory),
            "helper",
            "run",
            "--timeout",
            str(timeout),
            "--timeout-check-interval",
            str(timeout_check_interval),
            "--watch-interval",
            str(watch_interval),
        ]

        proc = subprocess.Popen(
            args=args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            **kwargs,
        )

        logger.info(f"Writing process ID {proc.pid} to file.")
        try:
            with PID_file.open("wt") as fp:
                fp.write(f"{proc.pid}\n")
        except FileNotFoundError as err:
            logger.error(
                f"Could not write to the PID file {PID_file!r}; killing helper process. "
                f"Exception was: {err!r}"
            )
            proc.kill()
            sys.exit(1)


def restart_helper(
    app: BaseApp,
    timeout: timedelta | float = DEFAULT_TIMEOUT,
    timeout_check_interval: timedelta | float = DEFAULT_TIMEOUT_CHECK,
    watch_interval: timedelta | float = DEFAULT_WATCH_INTERVAL,
):
    """
    Restart the helper process.
    """
    logger = stop_helper(app, return_logger=True)
    start_helper(app, timeout, timeout_check_interval, watch_interval, logger=logger)


def get_helper_PID(app: BaseApp):
    """
    Get the process ID of the helper process.
    """
    PID_file = get_PID_file_path(app)
    if not PID_file.is_file():
        print("Helper not running!")
        return None
    with PID_file.open("rt") as fp:
        helper_pid = int(fp.read().strip())
    return helper_pid, PID_file


def stop_helper(app: BaseApp, return_logger: bool = False):
    """
    Stop the helper process.
    """
    logger = get_helper_logger(app)
    if pid_info := get_helper_PID(app):
        logger.info("Stopping helper.")
        pid, pid_file = pid_info
        kill_proc_tree(pid=pid)
        pid_file.unlink()

        workflow_dirs_file_path = get_watcher_file_path(app)
        logger.info(f"Deleting watcher file: {str(workflow_dirs_file_path)}")
        workflow_dirs_file_path.unlink()

    return logger if return_logger else None


def clear_helper(app: BaseApp):
    """
    Stop the helper or remove any stale information relating to it.
    """
    try:
        stop_helper(app)
    except psutil.NoSuchProcess:
        if pid_info := get_helper_PID(app):
            pid_file = pid_info[1]
            print(f"Removing file {pid_file!r}")
            pid_file.unlink()


def get_helper_uptime(app: BaseApp) -> None | timedelta:
    """
    Get the amount of time that the helper has been running.
    """
    if not (pid_info := get_helper_PID(app)):
        return None
    proc = psutil.Process(pid_info[0])
    create_time = datetime.fromtimestamp(proc.create_time())
    return datetime.now() - create_time


def get_helper_logger(app: BaseApp) -> logging.Logger:
    """
    Get the logger for helper-related messages.
    """
    log_path = get_helper_log_path(app)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    f_handler = RotatingFileHandler(log_path, maxBytes=(5 * 2**20), backupCount=3)
    f_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)

    return logger


def helper_timeout(
    app: BaseApp,
    timeout: timedelta,
    controller: MonitorController,
    logger: logging.Logger,
):
    """Kill the helper due to running duration exceeding the timeout."""

    logger.info(f"Helper exiting due to timeout ({timeout!r}).")
    if pid_info := get_helper_PID(app):
        pid_file = pid_info[1]
        logger.info(f"Deleting PID file: {pid_file!r}.")
        pid_file.unlink()

    logger.info("Stopping all watchers.")
    controller.stop()
    controller.join()

    logger.info(f"Deleting watcher file: {str(controller.workflow_dirs_file_path)}")
    controller.workflow_dirs_file_path.unlink()

    sys.exit(0)


def run_helper(
    app: BaseApp,
    timeout: timedelta | float = DEFAULT_TIMEOUT,
    timeout_check_interval: timedelta | float = DEFAULT_TIMEOUT_CHECK,
    watch_interval: timedelta | float = DEFAULT_WATCH_INTERVAL,
):
    """
    Run the helper core.
    """
    # TODO: when writing to watch_workflows from a workflow, copy, modify and then rename
    # this will be atomic - so there will be only one event fired.
    # Also return a local run ID (the position in the file) to be used in jobscript naming

    # TODO: we will want to set the timeout to be slightly more than the largest allowable
    # walltime in the case of scheduler submissions.

    if not isinstance(timeout, timedelta):
        timeout = timedelta(seconds=timeout)

    if isinstance(timeout_check_interval, timedelta):
        timeout_check_interval_s = timeout_check_interval.total_seconds()
    else:
        timeout_check_interval_s = timeout_check_interval
        timeout_check_interval = timedelta(seconds=timeout_check_interval_s)

    start_time = datetime.now()
    logger = get_helper_logger(app)
    controller = MonitorController(get_watcher_file_path(app), watch_interval, logger)
    timeout_limit = timeout - timeout_check_interval
    try:
        while True:
            if datetime.now() - start_time >= timeout_limit:
                helper_timeout(app, timeout, controller, logger)
            time.sleep(timeout_check_interval_s)

    except KeyboardInterrupt:
        controller.stop()

    controller.join()  # wait for it to stop!
