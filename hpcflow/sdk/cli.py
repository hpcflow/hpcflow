import contextlib
import json
import os
import time
from typing import Dict, List
import click
from colorama import init as colorama_init
from termcolor import colored
from rich.pretty import pprint

from hpcflow import __version__, _app_name
from hpcflow.sdk.config.cli import get_config_CLI
from hpcflow.sdk.config.errors import ConfigError
from hpcflow.sdk.core import utils
from hpcflow.sdk.demo.cli import get_demo_software_CLI, get_demo_workflow_CLI
from hpcflow.sdk.cli_common import (
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
    force_arr_opt,
    make_status_opt,
    add_sub_opt,
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
from hpcflow.sdk.helper.cli import get_helper_CLI
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.submission.shells import ALL_SHELLS

string_option = click.option(
    "--string",
    is_flag=True,
    default=False,
    help="Determines if passing a file path or a string.",
)
workflow_ref_type_opt = click.option(
    "--ref-type",
    "-r",
    type=click.Choice(["assume-id", "id", "path"]),
    default="assume-id",
)


class ErrorPropagatingClickContext(click.Context):
    """A click Context class that passes on exception information.

    Using the standard `click.Context` class, exceptions raised when using a resource specified
    with `ctx.with_resource(my_ctx_manager())` are not passed on to the `__exit__` method of
    `my_ctx_manager`. See: https://github.com/pallets/click/issues/2447.

    Examples
    --------
    >>> @click.group()
    ... @click.pass_context
    ... def cli(ctx):
    ...     ctx.with_resource(my_context_manager())
    ... cli.context_class = ErrorPropagatingClickContext

    """

    def __exit__(self, exc_type, exc_value, tb):
        self._depth -= 1
        if self._depth == 0:
            self._exit_stack.__exit__(exc_type, exc_value, tb)
            self._exit_stack = contextlib.ExitStack()
        click.core.pop_context()


@contextlib.contextmanager
def redirect_std_to_file_click(file, mode: str = "a"):
    def ignore(exc):
        """Do not intercept Click's `Exit` exception when the exit code is zero."""
        if type(exc) is click.exceptions.Exit:
            if exc.exit_code == 0:
                return True
            return exc.exit_code
        return 1  # default exit code

    with utils.redirect_std_to_file(file=file, ignore=ignore):
        yield


def parse_jobscript_wait_spec(jobscripts: str) -> Dict[int, List[int]]:
    sub_js_idx_dct = {}
    for sub_i in jobscripts.split(";"):
        sub_idx_str, js_idx_lst_str = sub_i.split(":")
        sub_js_idx_dct[int(sub_idx_str)] = [int(i) for i in js_idx_lst_str.split(",")]
    return sub_js_idx_dct


def _make_API_CLI(app):
    """Generate the CLI for the main functionality."""

    @click.command(name="make")
    @click.argument("template_file_or_str")
    @string_option
    @format_option
    @path_option
    @name_option
    @overwrite_option
    @store_option
    @ts_fmt_option
    @ts_name_fmt_option
    @variables_option
    @make_status_opt
    @add_sub_opt
    def make_workflow(
        template_file_or_str,
        string,
        format,
        path,
        name,
        overwrite,
        store,
        ts_fmt=None,
        ts_name_fmt=None,
        variables=None,
        status=True,
        add_submission=False,
    ):
        """Generate a new {app_name} workflow.

        TEMPLATE_FILE_OR_STR is either a path to a template file in YAML or JSON
        format, or a YAML/JSON string.

        """
        wk = app.make_workflow(
            template_file_or_str=template_file_or_str,
            is_string=string,
            template_format=format,
            path=path,
            name=name,
            overwrite=overwrite,
            store=store,
            ts_fmt=ts_fmt,
            ts_name_fmt=ts_name_fmt,
            variables=dict(variables),
            status=status,
            add_submission=add_submission,
        )
        click.echo(wk.path)

    @click.command(name="go")
    @click.argument("template_file_or_str")
    @string_option
    @format_option
    @path_option
    @name_option
    @overwrite_option
    @store_option
    @ts_fmt_option
    @ts_name_fmt_option
    @variables_option
    @js_parallelism_option
    @wait_option
    @add_to_known_opt
    @print_idx_opt
    @tasks_opt
    @cancel_opt
    @submit_status_opt
    def make_and_submit_workflow(
        template_file_or_str,
        string,
        format,
        path,
        name,
        overwrite,
        store,
        ts_fmt=None,
        ts_name_fmt=None,
        variables=None,
        js_parallelism=None,
        wait=False,
        add_to_known=True,
        print_idx=False,
        tasks=None,
        cancel=False,
        status=True,
    ):
        """Generate and submit a new {app_name} workflow.

        TEMPLATE_FILE_OR_STR is either a path to a template file in YAML or JSON
        format, or a YAML/JSON string.

        """
        # TODO: allow submitting a persistent workflow via this command?
        out = app.make_and_submit_workflow(
            template_file_or_str=template_file_or_str,
            is_string=string,
            template_format=format,
            path=path,
            name=name,
            overwrite=overwrite,
            store=store,
            ts_fmt=ts_fmt,
            ts_name_fmt=ts_name_fmt,
            variables=dict(variables),
            JS_parallelism=js_parallelism,
            wait=wait,
            add_to_known=add_to_known,
            return_idx=print_idx,
            tasks=tasks,
            cancel=cancel,
            status=status,
        )
        if print_idx:
            click.echo(out[1])

    @click.command(context_settings={"ignore_unknown_options": True})
    @click.argument("py_test_args", nargs=-1, type=click.UNPROCESSED)
    @click.pass_context
    def test(ctx, py_test_args):
        """Run {app_name} test suite.

        PY_TEST_ARGS are arguments passed on to Pytest.

        """
        ctx.exit(app.run_tests(*py_test_args))

    @click.command(context_settings={"ignore_unknown_options": True})
    @click.argument("py_test_args", nargs=-1, type=click.UNPROCESSED)
    @click.pass_context
    def test_hpcflow(ctx, py_test_args):
        """Run hpcFlow test suite.

        PY_TEST_ARGS are arguments passed on to Pytest.

        """
        ctx.exit(app.run_hpcflow_tests(*py_test_args))

    commands = [
        make_workflow,
        make_and_submit_workflow,
        test,
    ]
    for cmd in commands:
        if cmd.help:
            cmd.help = cmd.help.format(app_name=app.name)

    if app.name != "hpcFlow":
        # `test_hpcflow` is the same as `test` for the hpcflow app no need to add both:
        commands.append(test_hpcflow)

    return commands


def _make_workflow_submission_jobscript_CLI(app):
    """Generate the CLI for interacting with existing workflow submission
    jobscripts."""

    @click.group(name="js")
    @click.pass_context
    @click.argument("js_idx", type=click.INT)
    def jobscript(ctx, js_idx):
        """Interact with existing {app_name} workflow submission jobscripts.

        JS_IDX is the jobscript index within the submission object.

        """
        ctx.obj["jobscript"] = ctx.obj["submission"].jobscripts[js_idx]

    @jobscript.command(name="res")
    @click.pass_context
    def resources(ctx):
        """Get resources associated with this jobscript."""
        click.echo(ctx.obj["jobscript"].resources.__dict__)

    @jobscript.command(name="deps")
    @click.pass_context
    def dependencies(ctx):
        """Get jobscript dependencies."""
        click.echo(ctx.obj["jobscript"].dependencies)

    @jobscript.command()
    @click.pass_context
    def path(ctx):
        """Get the file path to the jobscript."""
        click.echo(ctx.obj["jobscript"].jobscript_path)

    @jobscript.command()
    @click.pass_context
    def show(ctx):
        """Show the jobscript file."""
        with ctx.obj["jobscript"].jobscript_path.open("rt") as fp:
            click.echo(fp.read())

    jobscript.help = jobscript.help.format(app_name=app.name)

    return jobscript


def _make_workflow_submission_CLI(app):
    """Generate the CLI for interacting with existing workflow submissions."""

    @click.group(name="sub")
    @click.pass_context
    @click.argument("sub_idx", type=click.INT)
    def submission(ctx, sub_idx):
        """Interact with existing {app_name} workflow submissions.

        SUB_IDX is the submission index.

        """
        ctx.obj["submission"] = ctx.obj["workflow"].submissions[sub_idx]

    @submission.command("status")
    @click.pass_context
    def status(ctx):
        """Get the submission status."""
        click.echo(ctx.obj["submission"].status.name.lower())

    @submission.command("submitted-js")
    @click.pass_context
    def submitted_JS(ctx):
        """Get a list of jobscript indices that have been submitted."""
        click.echo(ctx.obj["submission"].submitted_jobscripts)

    @submission.command("outstanding-js")
    @click.pass_context
    def outstanding_JS(ctx):
        """Get a list of jobscript indices that have not yet been submitted."""
        click.echo(ctx.obj["submission"].outstanding_jobscripts)

    @submission.command("needs-submit")
    @click.pass_context
    def needs_submit(ctx):
        """Check if this submission needs submitting."""
        click.echo(ctx.obj["submission"].needs_submit)

    @submission.command("get-active-jobscripts")
    @click.pass_context
    def get_active_jobscripts(ctx):
        """Show active jobscripts and their jobscript-element states."""
        pprint(ctx.obj["submission"].get_active_jobscripts(as_json=True))

    submission.help = submission.help.format(app_name=app.name)
    submission.add_command(_make_workflow_submission_jobscript_CLI(app))

    return submission


def _make_workflow_CLI(app):
    """Generate the CLI for interacting with existing workflows."""

    @click.group()
    @click.argument("workflow_ref")
    @workflow_ref_type_opt
    @click.pass_context
    def workflow(ctx, workflow_ref, ref_type):
        """Interact with existing {app_name} workflows.

        WORKFLOW_REF is the path to, or local ID of, an existing workflow.

        """
        workflow_path = app._resolve_workflow_reference(workflow_ref, ref_type)
        wk = app.Workflow(workflow_path)
        ctx.obj["workflow"] = wk

    @workflow.command(name="submit")
    @js_parallelism_option
    @wait_option
    @add_to_known_opt
    @print_idx_opt
    @tasks_opt
    @cancel_opt
    @submit_status_opt
    @click.pass_context
    def submit_workflow(
        ctx,
        js_parallelism=None,
        wait=False,
        add_to_known=True,
        print_idx=False,
        tasks=None,
        cancel=False,
        status=True,
    ):
        """Submit the workflow."""
        out = ctx.obj["workflow"].submit(
            JS_parallelism=js_parallelism,
            wait=wait,
            add_to_known=add_to_known,
            return_idx=print_idx,
            tasks=tasks,
            cancel=cancel,
            status=status,
        )
        if print_idx:
            click.echo(out)

    @workflow.command(name="add-submission")
    @js_parallelism_option
    @tasks_opt
    @force_arr_opt
    @submit_status_opt
    @click.pass_context
    def add_submission(
        ctx,
        js_parallelism=None,
        tasks=None,
        force_array=False,
        status=True,
    ):
        """Add a new submission to the workflow, but do not submit."""
        ctx.obj["workflow"].add_submission(
            JS_parallelism=js_parallelism,
            tasks=tasks,
            force_array=force_array,
            status=status,
        )

    @workflow.command(name="wait")
    @click.option(
        "-j",
        "--jobscripts",
        help=(
            "Wait for only these jobscripts to finish. Jobscripts should be specified by "
            "their submission index, followed by a colon, followed by a comma-separated "
            "list of jobscript indices within that submission (no spaces are allowed). "
            "To specify jobscripts across multiple submissions, use a semicolon to "
            "separate patterns like these."
        ),
    )
    @click.pass_context
    def wait(ctx, jobscripts):
        js_spec = parse_jobscript_wait_spec(jobscripts) if jobscripts else None
        ctx.obj["workflow"].wait(sub_js=js_spec)

    @workflow.command(name="abort-run")
    @click.option("--submission", type=click.INT, default=-1)
    @click.option("--task", type=click.INT)
    @click.option("--element", type=click.INT)
    @click.pass_context
    def abort_run(ctx, submission, task, element):
        """Abort the specified run."""
        ctx.obj["workflow"].abort_run(
            submission_idx=submission,
            task_idx=task,
            element_idx=element,
        )

    @workflow.command(name="get-param")
    @click.argument("index", type=click.INT)
    @click.pass_context
    def get_parameter(ctx, index):
        """Get a parameter value by data index."""
        click.echo(ctx.obj["workflow"].get_parameter_data(index))

    @workflow.command(name="get-param-source")
    @click.argument("index", type=click.INT)
    @click.pass_context
    def get_parameter_source(ctx, index):
        """Get a parameter source by data index."""
        click.echo(ctx.obj["workflow"].get_parameter_source(index))

    @workflow.command(name="get-all-params")
    @click.pass_context
    def get_all_parameters(ctx):
        """Get all parameter values."""
        click.echo(ctx.obj["workflow"].get_all_parameter_data())

    @workflow.command(name="is-param-set")
    @click.argument("index", type=click.INT)
    @click.pass_context
    def is_parameter_set(ctx, index):
        """Check if a parameter specified by data index is set."""
        click.echo(ctx.obj["workflow"].is_parameter_set(index))

    @workflow.command(name="show-all-status")
    @click.pass_context
    def show_all_EAR_statuses(ctx):
        """Show the submission status of all workflow EARs."""
        ctx.obj["workflow"].show_all_EAR_statuses()

    @workflow.command(name="zip")
    @zip_path_opt
    @zip_overwrite_opt
    @zip_log_opt
    @zip_include_execute_opt
    @zip_include_rechunk_backups_opt
    @click.pass_context
    def zip_workflow(
        ctx,
        path,
        overwrite,
        log,
        include_execute,
        include_rechunk_backups,
    ):
        """Generate a copy of the workflow in the zip file format in the current working
        directory."""
        click.echo(
            ctx.obj["workflow"].zip(
                path=path,
                overwrite=overwrite,
                log=log,
                include_execute=include_execute,
                include_rechunk_backups=include_rechunk_backups,
            )
        )

    @workflow.command(name="unzip")
    @unzip_path_opt
    @unzip_log_opt
    @click.pass_context
    def unzip_workflow(ctx, path, log):
        """Generate a copy of the zipped workflow in the submittable Zarr format in the
        current working directory."""
        click.echo(ctx.obj["workflow"].unzip(path=path, log=log))

    @workflow.command(name="rechunk")
    @rechunk_backup_opt
    @rechunk_chunk_size_opt
    @rechunk_status_opt
    @click.pass_context
    def rechunk(ctx, backup, chunk_size, status):
        """Rechunk metadata/runs and parameters/base arrays."""
        ctx.obj["workflow"].rechunk(backup=backup, chunk_size=chunk_size, status=status)

    @workflow.command(name="rechunk-runs")
    @rechunk_backup_opt
    @rechunk_chunk_size_opt
    @rechunk_status_opt
    @click.pass_context
    def rechunk_runs(ctx, backup, chunk_size, status):
        """Rechunk the metadata/runs array."""
        ctx.obj["workflow"].rechunk_runs(
            backup=backup, chunk_size=chunk_size, status=status
        )

    @workflow.command(name="rechunk-parameter-base")
    @rechunk_backup_opt
    @rechunk_chunk_size_opt
    @rechunk_status_opt
    @click.pass_context
    def rechunk_parameter_base(ctx, backup, chunk_size, status):
        """Rechunk the parameters/base array."""
        ctx.obj["workflow"].rechunk_parameter_base(
            backup=backup, chunk_size=chunk_size, status=status
        )

    workflow.help = workflow.help.format(app_name=app.name)

    workflow.add_command(_make_workflow_submission_CLI(app))

    return workflow


def _make_submission_CLI(app):
    """Generate the CLI for submission related queries."""

    def OS_info_callback(ctx, param, value):
        if not value or ctx.resilient_parsing:
            return
        pprint(app.get_OS_info())
        ctx.exit()

    @click.group()
    @click.option(
        "--os-info",
        help="Print information about the operating system.",
        is_flag=True,
        is_eager=True,
        expose_value=False,
        callback=OS_info_callback,
    )
    @click.pass_context
    def submission(ctx):
        """Submission-related queries."""
        pass

    @submission.command("shell-info")
    @click.argument("shell_name", type=click.Choice(ALL_SHELLS))
    @click.option("--exclude-os", is_flag=True, default=False)
    @click.pass_context
    def shell_info(ctx, shell_name, exclude_os):
        """Show information about the specified shell, such as the version."""
        pprint(app.get_shell_info(shell_name, exclude_os))
        ctx.exit()

    @submission.group("scheduler")
    @click.argument("scheduler_name")
    @click.pass_context
    def scheduler(ctx, scheduler_name):
        ctx.obj["scheduler_obj"] = app.get_scheduler(scheduler_name, os.name)

    @scheduler.command()
    @click.pass_context
    def get_login_nodes(ctx):
        scheduler = ctx.obj["scheduler_obj"]
        pprint(scheduler.get_login_nodes())

    @submission.command()
    @click.option(
        "as_json",
        "--json",
        is_flag=True,
        default=False,
        help="Do not format and only show JSON-compatible information.",
    )
    @click.pass_context
    def get_known(ctx, as_json=False):
        """Print known-submissions information as a formatted Python object."""
        out = app.get_known_submissions(as_json=as_json)
        if as_json:
            click.echo(json.dumps(out))
        else:
            pprint(out)

    return submission


def _make_internal_CLI(app):
    """Generate the CLI for internal use."""

    @click.group()
    def internal(help=True):
        """Internal CLI to be invoked by scripts generated by the app."""
        pass

    @internal.command()
    def get_invoc_cmd():
        """Get the invocation command for this app instance."""
        click.echo(app.run_time_info.invocation_command)

    @internal.command()
    @click.pass_context
    @click.option("--raise", "raise_opt", is_flag=True)
    @click.option("--click-exit-code", type=click.INT)
    @click.option("--sleep", type=click.INT)
    def noop(ctx, raise_opt, click_exit_code, sleep):
        """Used only in CLI tests."""
        if raise_opt:
            raise ValueError("internal noop raised!")
        elif click_exit_code is not None:
            ctx.exit(click_exit_code)
        elif sleep:
            time.sleep(sleep)

    @internal.group()
    @click.argument("path", type=click.Path(exists=True))
    @click.pass_context
    def workflow(ctx, path):
        wk = app.Workflow(path)
        ctx.obj["workflow"] = wk

    @workflow.command()
    @click.pass_context
    @click.argument("submission_idx", type=click.INT)
    @click.argument("jobscript_idx", type=click.INT)
    @click.argument("block_idx", type=click.INT)
    @click.argument("block_action_idx", type=click.INT)
    @click.argument("run_id", type=click.INT)
    def execute_run(
        ctx,
        submission_idx: int,
        jobscript_idx: int,
        block_idx: int,
        block_action_idx: int,
        run_id: int,
    ):
        app.CLI_logger.info(f"execute commands for EAR ID {run_id!r}.")
        ctx.obj["workflow"].execute_run(
            submission_idx=submission_idx,
            block_act_key=(jobscript_idx, block_idx, block_action_idx),
            run_ID=run_id,
        )

    @workflow.command()
    @click.pass_context
    @click.argument("submission_idx", type=click.INT)
    @click.argument("jobscript_idx", type=click.INT)
    def execute_combined_runs(
        ctx,
        submission_idx: int,
        jobscript_idx: int,
    ):
        app.CLI_logger.info(
            f"execute command for combined scripts of jobscript {jobscript_idx}."
        )
        ctx.obj["workflow"].execute_combined_runs(
            submission_idx=submission_idx,
            jobscript_idx=jobscript_idx,
        )

    @workflow.command()
    @click.pass_context
    @click.argument("name")
    @click.argument("value")
    @click.argument("ear_id", type=click.INT)
    @click.argument("cmd_idx", type=click.INT)
    @click.option("--stderr", is_flag=True, default=False)
    def save_parameter(
        ctx,
        name: str,
        value: str,
        ear_id: int,
        cmd_idx: int,
        stderr: bool,
    ):
        app.CLI_logger.info(
            f"save parameter {name!r} for EAR ID {ear_id!r} and command index "
            f"{cmd_idx!r} (stderr={stderr!r})"
        )
        app.CLI_logger.debug(f"save parameter value is: {value!r}")
        wk = ctx.obj["workflow"]
        with wk._store.cached_load():
            value = wk.process_shell_parameter_output(
                name=name,
                value=value,
                EAR_ID=ear_id,
                cmd_idx=cmd_idx,
                stderr=stderr,
            )
            app.CLI_logger.debug(f"save parameter processed value is: {value!r}")
            wk.save_parameter(name=name, value=value, EAR_ID=ear_id)

    @workflow.command()
    @click.pass_context
    @click.argument("ear_id", type=click.INT)
    @click.argument("loop_names", type=click.STRING, nargs=-1)
    def check_loop(ctx, loop_names: List[str], ear_id: int):
        """Check if an iteration has met its loop's termination condition."""
        app.CLI_logger.info(f"check_loop for loops {loop_names!r} and EAR ID {ear_id!r}.")
        ctx.obj["workflow"].check_loop_termination(ear_id, loop_names)

    # TODO: in general, maybe the workflow command group can expose the simple Workflow
    # properties; maybe use a decorator on the Workflow property object to signify
    # inclusion?

    return internal


def _make_template_components_CLI(app):
    @click.command()
    def tc(help=True):
        """For showing template component data."""
        pprint(app.template_components)

    return tc


def _make_show_CLI(app):
    def show_legend_callback(ctx, param, value):
        if not value or ctx.resilient_parsing:
            return
        app.show_legend()
        ctx.exit()

    @click.command()
    @click.option(
        "-r",
        "--max-recent",
        default=3,
        help="The maximum number of inactive submissions to show.",
    )
    @click.option(
        "--no-update",
        is_flag=True,
        default=False,
        help=(
            "If True, do not update the known-submissions file to remove workflows that "
            "are no longer running."
        ),
    )
    @click.option(
        "-f",
        "--full",
        is_flag=True,
        default=False,
        help="Allow multiple lines per workflow submission.",
    )
    @click.option(
        "--legend",
        help="Display the legend for the `show` command output.",
        is_flag=True,
        is_eager=True,
        expose_value=False,
        callback=show_legend_callback,
    )
    def show(max_recent, full, no_update):
        """Show information about running and recently active workflows."""
        app.show(max_recent=max_recent, full=full, no_update=no_update)

    return show


def _make_zip_CLI(app):
    @click.command(name="zip")
    @click.argument("workflow_ref")
    @zip_path_opt
    @zip_overwrite_opt
    @zip_log_opt
    @zip_include_execute_opt
    @zip_include_rechunk_backups_opt
    @workflow_ref_type_opt
    def zip_workflow(
        workflow_ref,
        path,
        overwrite,
        log,
        include_execute,
        include_rechunk_backups,
        ref_type,
    ):
        """Generate a copy of the specified workflow in the zip file format in the
        current working directory.

        WORKFLOW_REF is the local ID (that provided by the `show` command}) or the
        workflow path.
        """
        workflow_path = app._resolve_workflow_reference(workflow_ref, ref_type)
        wk = app.Workflow(workflow_path)
        click.echo(
            wk.zip(
                path=path,
                overwrite=overwrite,
                log=log,
                include_execute=include_execute,
                include_rechunk_backups=include_rechunk_backups,
            )
        )

    return zip_workflow


def _make_unzip_CLI(app):
    @click.command(name="unzip")
    @click.argument("workflow_path")
    @unzip_path_opt
    @unzip_log_opt
    def unzip_workflow(workflow_path, path, log):
        """Generate a copy of the specified zipped workflow in the submittable Zarr
        format in the current working directory.

        WORKFLOW_PATH is path of the zip file to unzip.

        """
        wk = app.Workflow(workflow_path)
        click.echo(wk.unzip(path=path, log=log))

    return unzip_workflow


def _make_cancel_CLI(app):
    @click.command()
    @click.argument("workflow_ref")
    @workflow_ref_type_opt
    def cancel(workflow_ref, ref_type):
        """Stop all running jobscripts of the specified workflow.

        WORKFLOW_REF is the local ID (that provided by the `show` command}) or the
        workflow path.

        """
        app.cancel(workflow_ref, ref_type)

    return cancel


def _make_rechunk_CLI(app):
    @click.command(name="rechunk")
    @click.argument("workflow_ref")
    @workflow_ref_type_opt
    @rechunk_backup_opt
    @rechunk_chunk_size_opt
    @rechunk_status_opt
    def rechunk(workflow_ref, ref_type, backup, chunk_size, status):
        """Rechunk metadata/runs and parameters/base arrays.

        WORKFLOW_REF is the local ID (that provided by the `show` command}) or the
        workflow path.

        """
        workflow_path = app._resolve_workflow_reference(workflow_ref, ref_type)
        wk = app.Workflow(workflow_path)
        wk.rechunk(backup=backup, chunk_size=chunk_size, status=status)

    return rechunk


def _make_open_CLI(app):
    @click.group(name="open")
    def open_file():
        """Open a file (for example {app_name}'s log file) using the default
        application."""

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def log(path=False):
        """Open the {app_name} log file."""
        file_path = app.config.get("log_file_path")
        if path:
            click.echo(file_path)
        else:
            utils.open_file(file_path)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def config(path=False):
        """Open the {app_name} config file, or retrieve it's path."""
        file_path = app.config.get("config_file_path")
        if path:
            click.echo(file_path)
        else:
            utils.open_file(file_path)

    @open_file.command()
    @click.option("--name")
    @click.option("--path", is_flag=True, default=False)
    def env_source(name=None, path=False):
        """Open a named environment sources file, or the first one."""
        sources = app.config.get("environment_sources")
        if not sources:
            raise ValueError("No environment sources specified in the config file.")
        file_paths = []
        if not name:
            file_paths = [sources[0]]
        else:
            for i in sources:
                if i.name == name:
                    file_paths.append(i)
        if not file_paths:
            raise ValueError(
                f"No environment source named {name!r} could be found; available "
                f"environment source files have names: {[i.name for i in sources]!r}"
            )

        assert len(file_paths) < 5  # don't open a stupid number of files
        for i in file_paths:
            if path:
                click.echo(i)
            else:
                utils.open_file(i)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def known_subs(path=False):
        """Open the known-submissions text file."""
        file_path = app.known_subs_file_path
        if path:
            click.echo(file_path)
        else:
            utils.open_file(file_path)

    @open_file.command()
    @click.argument("workflow_ref")
    @click.option("--path", is_flag=True, default=False)
    @workflow_ref_type_opt
    def workflow(workflow_ref, ref_type, path=False):
        """Open a workflow directory using, for example, File Explorer on Windows."""
        workflow_path = app._resolve_workflow_reference(workflow_ref, ref_type)
        if path:
            click.echo(workflow_path)
        else:
            utils.open_file(workflow_path)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def user_data_dir(path=False):
        dir_path = app._ensure_user_data_dir()
        if path:
            click.echo(dir_path)
        else:
            utils.open_file(dir_path)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def user_cache_dir(path=False):
        dir_path = app._ensure_user_cache_dir()
        if path:
            click.echo(dir_path)
        else:
            utils.open_file(dir_path)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def user_runtime_dir(path=False):
        dir_path = app._ensure_user_runtime_dir()
        if path:
            click.echo(dir_path)
        else:
            utils.open_file(dir_path)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def user_data_hostname_dir(path=False):
        dir_path = app._ensure_user_data_hostname_dir()
        if path:
            click.echo(dir_path)
        else:
            utils.open_file(dir_path)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def user_cache_hostname_dir(path=False):
        dir_path = app._ensure_user_cache_hostname_dir()
        if path:
            click.echo(dir_path)
        else:
            utils.open_file(dir_path)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def demo_data_cache_dir(path=False):
        dir_path = app._ensure_demo_data_cache_dir()
        if path:
            click.echo(dir_path)
        else:
            utils.open_file(dir_path)

    open_file.help = open_file.help.format(app_name=app.name)
    log.help = log.help.format(app_name=app.name)
    config.help = config.help.format(app_name=app.name)

    return open_file


def _make_demo_data_CLI(app):
    """Generate the CLI for interacting with example data files that are used in demo
    workflows."""

    def list_callback(ctx, param, value):
        if not value or ctx.resilient_parsing:
            return
        # TODO: format with Rich with a one-line description
        click.echo("\n".join(app.list_demo_data_files()))
        ctx.exit()

    def cache_all_callback(ctx, param, value):
        if not value or ctx.resilient_parsing:
            return
        app.cache_all_demo_data_files()
        ctx.exit()

    @click.group()
    @click.option(
        "-l",
        "--list",
        help="Print available example data files.",
        is_flag=True,
        is_eager=True,
        expose_value=False,
        callback=list_callback,
    )
    def demo_data():
        """Interact with builtin demo data files."""

    @demo_data.command("copy")
    @click.argument("file_name")
    @click.argument("destination")
    def copy_demo_data(file_name, destination):
        """Copy a demo data file to the specified location."""
        app.copy_demo_data(file_name=file_name, dst=destination)

    @demo_data.command("cache")
    @click.option(
        "--all",
        help="Cache all demo data files.",
        is_flag=True,
        is_eager=True,
        expose_value=False,
        callback=cache_all_callback,
    )
    @click.argument("file_name")
    def cache_demo_data(file_name):
        """Ensure a demo data file is in the demo data cache."""
        app.cache_demo_data_file(file_name)

    return demo_data


def _make_manage_CLI(app):
    """Generate the CLI for infrequent app management tasks."""

    @click.group()
    def manage():
        """Infrequent app management tasks.

        App config is not loaded.

        """
        pass

    @manage.command()
    @click.option(
        "--config-dir",
        help="The directory containing the config file to be reset.",
    )
    def reset_config(config_dir):
        """Reset the configuration file to defaults.

        This can be used if the current configuration file is invalid."""
        app.reset_config(config_dir)

    @manage.command()
    @click.option(
        "--config-dir",
        help="The directory containing the config file whose path is to be returned.",
    )
    def get_config_path(config_dir):
        """Print the config file path without loading the config.

        This can be used instead of `{app_name} open config --path` if the config file
        is invalid, because this command does not load the config.

        """
        click.echo(app.get_config_path(config_dir))

    @manage.command("clear-known-subs")
    def clear_known_subs():
        """Delete the contents of the known-submissions file."""
        app.clear_known_submissions_file()

    @manage.command("clear-temp-dir")
    def clear_runtime_dir():
        """Delete all files in the user runtime directory."""
        app.clear_user_runtime_dir()

    @manage.command("clear-cache")
    @click.option("--hostname", is_flag=True, default=False)
    def clear_cache(hostname):
        """Delete the app cache directory."""
        if hostname:
            app.clear_user_cache_hostname_dir()
        else:
            app.clear_user_cache_dir()

    @manage.command("clear-demo-data-cache")
    def clear_demo_data_cache():
        """Delete the app demo data cache directory."""
        app.clear_demo_data_cache_dir()

    return manage


def make_cli(app):
    """Generate the root CLI for the app."""

    colorama_init(autoreset=True)

    def run_time_info_callback(ctx, param, value):
        app.run_time_info.from_CLI = True
        if not value or ctx.resilient_parsing:
            return
        app.run_time_info.show()
        ctx.exit()

    @click.group(name=app.name)
    @click.version_option(
        version=app.version,
        package_name=app.name,
        prog_name=app.name,
        help=f"Show the version of {app.name} and exit.",
    )
    @click.version_option(
        __version__,
        "--hpcflow-version",
        help="Show the version of hpcflow and exit.",
        package_name="hpcflow",
        prog_name=_app_name,
    )
    @click.help_option()
    @click.option(
        "--run-time-info",
        help="Print run-time information!",
        is_flag=True,
        is_eager=True,
        expose_value=False,
        callback=run_time_info_callback,
    )
    @click.option("--config-dir", help="Set the configuration directory.")
    @click.option("--config-key", help="Set the configuration invocation key.")
    @click.option(
        "--with-config",
        help="Override a config item in the config file",
        nargs=2,
        multiple=True,
    )
    @click.option(
        "--timeit",
        help=(
            "Time function pathways as the code executes and write out a summary at the "
            "end. Only functions decorated by `TimeIt.decorator` are included."
        ),
        is_flag=True,
    )
    @click.option(
        "--timeit-file",
        help=(
            "Time function pathways as the code executes and write out a summary at the "
            "end to a text file given by this file path. Only functions decorated by "
            "`TimeIt.decorator` are included."
        ),
    )
    @click.option(
        "--std-stream",
        help="File to redirect standard output and error to, and to print exceptions to.",
    )
    @click.pass_context
    def new_CLI(
        ctx,
        config_dir,
        config_key,
        with_config,
        timeit,
        timeit_file,
        std_stream: str,
    ):
        ctx.ensure_object(dict)

        if std_stream:
            ctx.with_resource(redirect_std_to_file_click(std_stream))

        app.run_time_info.from_CLI = True
        TimeIt.active = timeit or timeit_file
        TimeIt.file_path = timeit_file
        if ctx.invoked_subcommand != "manage":
            # load the config
            overrides = {kv[0]: kv[1] for kv in with_config}
            try:
                app.load_config(
                    config_dir=config_dir,
                    config_key=config_key,
                    **overrides,
                )
            except ConfigError as err:
                click.echo(f"{colored(err.__class__.__name__, 'red')}: {err}")
                ctx.exit(1)

    @new_CLI.result_callback()
    def post_execution(*args, **kwargs):
        if TimeIt.active:
            TimeIt.summarise_string()

    @new_CLI.command()
    @click.argument("name")
    @click.option("--use-current-env", is_flag=True, default=False)
    @click.option("--setup", type=click.STRING)
    @click.option("--env-source-file", type=click.STRING)
    def configure_env(name, use_current_env, setup=None, env_source_file=None):
        """Configure an app environment, using, for example, the currently activated
        Python environment."""
        app.configure_env(
            name=name,
            setup=setup,
            executables=None,
            use_current_env=use_current_env,
            env_source_file=env_source_file,
        )

    new_CLI.context_class = ErrorPropagatingClickContext

    new_CLI.__doc__ = app.description
    new_CLI.add_command(get_config_CLI(app))
    new_CLI.add_command(get_demo_software_CLI(app))
    new_CLI.add_command(get_demo_workflow_CLI(app))
    new_CLI.add_command(get_helper_CLI(app))
    new_CLI.add_command(_make_demo_data_CLI(app))
    new_CLI.add_command(_make_manage_CLI(app))
    new_CLI.add_command(_make_workflow_CLI(app))
    new_CLI.add_command(_make_submission_CLI(app))
    new_CLI.add_command(_make_internal_CLI(app))
    new_CLI.add_command(_make_template_components_CLI(app))
    new_CLI.add_command(_make_show_CLI(app))
    new_CLI.add_command(_make_open_CLI(app))
    new_CLI.add_command(_make_cancel_CLI(app))
    new_CLI.add_command(_make_zip_CLI(app))
    new_CLI.add_command(_make_unzip_CLI(app))
    new_CLI.add_command(_make_rechunk_CLI(app))
    for cli_cmd in _make_API_CLI(app):
        new_CLI.add_command(cli_cmd)

    return new_CLI
