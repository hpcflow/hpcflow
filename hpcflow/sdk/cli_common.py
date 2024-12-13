"""Click CLI options that are used as decorators in multiple modules."""

from __future__ import annotations
import click

from hpcflow.sdk.core import ALL_TEMPLATE_FORMATS
from hpcflow.sdk.persistence.defaults import DEFAULT_STORE_FORMAT
from hpcflow.sdk.persistence.discovery import ALL_STORE_FORMATS


def sub_tasks_callback(ctx, param, value: str | None) -> list[int] | None:
    """
    Parse subtasks.
    """
    if value:
        return [int(i) for i in value.split(",")]
    else:
        return None


#: Standard option
format_option = click.option(
    "--format",
    type=click.Choice(ALL_TEMPLATE_FORMATS),
    default=None,
    help=(
        'If specified, one of "json" or "yaml". This forces parsing from a '
        "particular format."
    ),
)
#: Standard option
path_option = click.option(
    "--path",
    type=click.Path(exists=True),
    help="The directory path into which the new workflow will be generated.",
)
#: Standard option
name_option = click.option(
    "--name",
    help=(
        "The name of the workflow. If specified, the workflow directory will be "
        "`path` joined with `name`. If not specified the workflow template name "
        "will be used, in combination with a date-timestamp."
    ),
)
#: Standard option
overwrite_option = click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help=(
        "If True and the workflow directory (`path` + `name`) already exists, "
        "the existing directory will be overwritten."
    ),
)
#: Standard option
store_option = click.option(
    "--store",
    type=click.Choice(ALL_STORE_FORMATS),
    help="The persistent store type to use.",
    default=DEFAULT_STORE_FORMAT,
)

#: Standard option
ts_fmt_option = click.option(
    "--ts-fmt",
    help=(
        "The datetime format to use for storing datetimes. Datetimes are always "
        "stored in UTC (because Numpy does not store time zone info), so this "
        "should not include a time zone name."
    ),
)
#: Standard option
ts_name_fmt_option = click.option(
    "--ts-name-fmt",
    help=(
        "The datetime format to use when generating the workflow name, where it "
        "includes a timestamp."
    ),
)

#: Standard option
variables_option = click.option(
    "-v",
    "--var",
    "variables",
    type=(str, str),
    multiple=True,
    help=(
        "Workflow template variable value to be substituted in to the template file or "
        "string. Multiple variable values can be specified."
    ),
)
#: Standard option
js_parallelism_option = click.option(
    "--js-parallelism",
    help=(
        "If True, allow multiple jobscripts to execute simultaneously. Raises if "
        "set to True but the store type does not support the "
        "`jobscript_parallelism` feature. If not set, jobscript parallelism will "
        "be used if the store type supports it."
    ),
    type=click.BOOL,
)
#: Standard option
wait_option = click.option(
    "--wait",
    help=("If True, this command will block until the workflow execution is complete."),
    is_flag=True,
    default=False,
)
#: Standard option
add_to_known_opt = click.option(
    "--add-to-known/--no-add-to-known",
    default=True,
    help="If True, add this submission to the known-submissions file.",
)
#: Standard option
print_idx_opt = click.option(
    "--print-idx",
    help="If True, print the submitted jobscript indices for each submission index.",
    is_flag=True,
    default=False,
)
#: Standard option
tasks_opt = click.option(
    "--tasks",
    help=(
        "List of comma-separated task indices to include in this submission. By default "
        "all tasks are included."
    ),
    callback=sub_tasks_callback,
)
#: Standard option
cancel_opt = click.option(
    "--cancel",
    help="Immediately cancel the submission. Useful for testing and benchmarking.",
    is_flag=True,
    default=False,
)
#: Standard option
submit_status_opt = click.option(
    "--status/--no-status",
    help="If True, display a live status to track submission progress.",
    default=True,
)
#: Standard option
make_status_opt = click.option(
    "--status/--no-status",
    help="If True, display a live status to track workflow creation progress.",
    default=True,
)

#: Standard option
zip_path_opt = click.option(
    "--path",
    default=".",
    help=(
        "Path at which to create the new zipped workflow. If this is an existing "
        "directory, the zip file will be created within this directory. Otherwise, this "
        "path is assumed to be the full file path to the new zip file."
    ),
)
#: Standard option
zip_overwrite_opt = click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="If set, any existing file will be overwritten.",
)
#: Standard option
zip_log_opt = click.option("--log", help="Path to a log file to use during zipping.")
#: Standard option
zip_include_execute_opt = click.option("--include-execute", is_flag=True)
#: Standard option
zip_include_rechunk_backups_opt = click.option("--include-rechunk-backups", is_flag=True)

#: Standard option
unzip_path_opt = click.option(
    "--path",
    default=".",
    help=(
        "Path at which to create the new unzipped workflow. If this is an existing "
        "directory, the new workflow directory will be created within this directory. "
        "Otherwise, this path will represent the new workflow directory path."
    ),
)
#: Standard option
unzip_log_opt = click.option("--log", help="Path to a log file to use during unzipping.")

#: Standard option
rechunk_backup_opt = click.option(
    "--backup/--no-backup",
    default=True,
    help=("First copy a backup of the array to a directory ending in `.bak`."),
)
#: Standard option
rechunk_chunk_size_opt = click.option(
    "--chunk-size",
    type=click.INT,
    default=None,
    help=(
        "New chunk size (array items per chunk). If unset (as by default), the array "
        "will be rechunked to a single chunk array (i.e with a chunk size equal to the "
        "array's shape)."
    ),
)
#: Standard option
rechunk_status_opt = click.option(
    "--status/--no-status",
    default=True,
    help="If True, display a live status to track rechunking progress.",
)


def _add_doc_from_help(*args):
    """
    Attach the ``help`` field of each of its arguments as its ``__doc__``.
    Only necessary because the wrappers in Click don't do this for us.

    :meta private:
    """
    # Yes, this is ugly!
    from types import SimpleNamespace

    for opt in args:
        ns = SimpleNamespace()
        params = getattr(opt(ns), "__click_params__", [])
        if params:
            help = getattr(params[0], "help", "")
            if help:
                opt.__doc__ = f"Click option decorator: {help}"


_add_doc_from_help(
    format_option,
    path_option,
    name_option,
    overwrite_option,
    store_option,
    ts_fmt_option,
    ts_name_fmt_option,
    variables_option,
    js_parallelism_option,
    wait_option,
    add_to_known_opt,
    print_idx_opt,
    tasks_opt,
    cancel_opt,
    submit_status_opt,
    make_status_opt,
    zip_path_opt,
    zip_overwrite_opt,
    zip_log_opt,
    zip_include_execute_opt,
    zip_include_rechunk_backups_opt,
    unzip_path_opt,
    unzip_log_opt,
    rechunk_backup_opt,
    rechunk_chunk_size_opt,
    rechunk_status_opt,
)
