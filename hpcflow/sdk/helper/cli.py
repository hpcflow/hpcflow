"""
Common Click command line options related to the helper.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

import click

from hpcflow.sdk.helper.helper import (
    DEFAULT_TIMEOUT,
    DEFAULT_TIMEOUT_CHECK,
    DEFAULT_WATCH_INTERVAL,
    get_helper_log_path,
    get_watcher_file_path,
    get_helper_watch_list,
    start_helper,
    stop_helper,
    restart_helper,
    clear_helper,
    run_helper,
    get_helper_PID,
    get_helper_uptime,
)
from hpcflow.sdk.cli_common import _add_doc_from_help

if TYPE_CHECKING:
    from ..app import BaseApp

#: Helper option: ``--timeout``
timeout_option = click.option(
    "--timeout",
    type=click.FLOAT,
    default=DEFAULT_TIMEOUT,
    show_default=True,
    help="Helper timeout in seconds.",
)
#: Helper option: ``--timeout-check-interval``
timeout_check_interval_option = click.option(
    "--timeout-check-interval",
    type=click.FLOAT,
    default=DEFAULT_TIMEOUT_CHECK,
    show_default=True,
    help="Interval between testing if the timeout has been exceeded in seconds.",
)
#: Helper option: ``--watch interval``
watch_interval_option = click.option(
    "--watch-interval",
    type=click.FLOAT,
    default=DEFAULT_WATCH_INTERVAL,
    show_default=True,
    help=(
        "Polling interval for watching workflows (and the workflow watch list) in "
        "seconds."
    ),
)
_add_doc_from_help(timeout_option, timeout_check_interval_option, watch_interval_option)


def get_helper_CLI(app: BaseApp):
    """Generate the CLI to provide some server-like functionality."""

    @click.group()
    def helper():
        pass

    @helper.command()
    @timeout_option
    @timeout_check_interval_option
    @watch_interval_option
    def start(timeout: float, timeout_check_interval: float, watch_interval: float):
        """Start the helper process."""
        start_helper(app, timeout, timeout_check_interval, watch_interval)

    @helper.command()
    def stop():
        """Stop the helper process, if it is running."""
        stop_helper(app)

    @helper.command()
    @timeout_option
    @timeout_check_interval_option
    @watch_interval_option
    def run(timeout: float, timeout_check_interval: float, watch_interval: float):
        """Run the helper functionality."""
        run_helper(app, timeout, timeout_check_interval, watch_interval)

    @helper.command()
    @timeout_option
    @timeout_check_interval_option
    @watch_interval_option
    def restart(timeout: float, timeout_check_interval: float, watch_interval: float):
        """Restart (or start) the helper process."""
        restart_helper(app, timeout, timeout_check_interval, watch_interval)

    @helper.command()
    @click.option("-f", "--file", is_flag=True)
    def pid(file: bool):
        """Get the process ID of the running helper, if running."""
        pid_info = get_helper_PID(app)
        if pid_info:
            pid, pid_file = pid_info
            if file:
                click.echo(f"{pid} ({str(pid_file)})")
            else:
                click.echo(pid)

    @helper.command()
    def clear() -> None:
        """Remove the PID file (and kill the helper process if it exists). This should not
        normally be needed."""
        clear_helper(app)

    @helper.command()
    def uptime() -> None:
        """Get the uptime of the helper process, if it is running."""
        out = get_helper_uptime(app)
        if out:
            click.echo(out)

    @helper.command()
    def log_path() -> None:
        """Get the path to the helper log file (may not exist)."""
        click.echo(get_helper_log_path(app))

    @helper.command()
    def watch_list_path() -> None:
        """Get the path to the workflow watch list file (may not exist)."""
        click.echo(get_watcher_file_path(app))

    @helper.command()
    def watch_list() -> None:
        """Get the list of workflows currently being watched."""
        for wk in get_helper_watch_list(app) or ():
            click.echo(str(wk["path"]))

    return helper
