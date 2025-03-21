"""
Command line interface implementation.
"""

from __future__ import annotations
import contextlib
import json
import os
import time
import click
from colorama import init as colorama_init
from termcolor import colored  # type: ignore
from typing import TYPE_CHECKING
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
    cancel_status_opt,
    list_js_max_js_opt,
    list_js_jobscripts_opt,
    list_task_js_max_js_opt,
    list_task_js_task_names_opt,
    list_js_width_opt,
    jobscript_std_array_idx_opt,
    _add_doc_from_help,
)
from hpcflow.sdk.helper.cli import get_helper_CLI
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.core.workflow import Workflow
from hpcflow.sdk.submission.shells import ALL_SHELLS
from hpcflow.sdk.submission.jobscript import Jobscript
from hpcflow.sdk.submission.submission import Submission
from hpcflow.sdk.submission.schedulers.sge import SGEPosix

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Literal
    from .app import BaseApp

#: Standard option
string_option = click.option(
    "--string",
    is_flag=True,
    default=False,
    help="Determines if passing a file path or a string.",
)
#: Standard option
workflow_ref_type_opt = click.option(
    "--ref-type",
    "-r",
    type=click.Choice(("assume-id", "id", "path")),
    default="assume-id",
    help="How to interpret a reference, as an ID, a path, or to guess.",
)

#: Get the current workflow from the context.
_pass_workflow = click.make_pass_decorator(Workflow)
#: Get the current submission from the context.
_pass_submission = click.make_pass_decorator(Submission)
#: Get the current jobscript from the context.
_pass_js = click.make_pass_decorator(Jobscript)

_add_doc_from_help(string_option, workflow_ref_type_opt)


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


def parse_jobscript_wait_spec(jobscripts: str) -> dict[int, list[int]]:
    """
    Parse a jobscript wait specification.
    """
    sub_js_idx_dct = {}
    for sub_i in jobscripts.split(";"):
        sub_idx_str, js_idx_lst_str = sub_i.split(":")
        sub_js_idx_dct[int(sub_idx_str)] = [int(i) for i in js_idx_lst_str.split(",")]
    return sub_js_idx_dct


def _set_help_name(cmd: click.Group | click.Command, app: BaseApp):
    """
    Update the help string of the command to contain the name of the application.
    """
    if cmd.help:
        cmd.help = cmd.help.format(app_name=app.name)


def _make_API_CLI(app: BaseApp):
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
        template_file_or_str: str,
        string: bool,
        format: Literal["json", "yaml"] | None,
        path: Path | None,
        name: str | None,
        overwrite: bool,
        store: str,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        variables: list[tuple[str, str]] | None = None,
        status: bool = True,
        add_submission: bool = False,
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
            variables=dict(variables) if variables is not None else None,
            status=status,
            add_submission=add_submission,
        )
        assert isinstance(wk, Workflow)
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
        template_file_or_str: str,
        string: bool,
        format: Literal["json", "yaml"] | None,
        path: Path | None,
        name: str | None,
        overwrite: bool,
        store: str,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        variables: list[tuple[str, str]] | None = None,
        js_parallelism: bool | None = None,
        wait: bool = False,
        add_to_known: bool = True,
        print_idx: bool = False,
        tasks: list[int] | None = None,
        cancel: bool = False,
        status: bool = True,
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
            variables=dict(variables) if variables is not None else None,
            JS_parallelism=js_parallelism,
            wait=wait,
            add_to_known=add_to_known,
            return_idx=print_idx,
            tasks=tasks,
            cancel=cancel,
            status=status,
        )
        if print_idx:
            assert isinstance(out, tuple)
            click.echo(out[1])

    @click.command(context_settings={"ignore_unknown_options": True})
    @click.argument("py_test_args", nargs=-1, type=click.UNPROCESSED)
    @click.pass_context
    def test(ctx: click.Context, py_test_args: list[str]):
        """Run {app_name} test suite.

        PY_TEST_ARGS are arguments passed on to Pytest.

        """
        ctx.exit(app.run_tests(*py_test_args))

    @click.command(context_settings={"ignore_unknown_options": True})
    @click.argument("py_test_args", nargs=-1, type=click.UNPROCESSED)
    @click.pass_context
    def test_hpcflow(ctx: click.Context, py_test_args: list[str]):
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
        _set_help_name(cmd, app)

    if app.name != "hpcFlow":
        # `test_hpcflow` is the same as `test` for the hpcflow app no need to add both:
        commands.append(test_hpcflow)

    return commands


def _make_workflow_submission_jobscript_CLI(app: BaseApp):
    """Generate the CLI for interacting with existing workflow submission
    jobscripts."""

    @click.group(name="js")
    @_pass_submission
    @click.pass_context
    @click.argument("js_idx", type=click.INT)
    def jobscript(ctx: click.Context, sb: Submission, js_idx: int):
        """Interact with existing {app_name} workflow submission jobscripts.

        JS_IDX is the jobscript index within the submission object.

        """
        ctx.obj = sb.jobscripts[js_idx]

    @jobscript.command(name="res")
    @_pass_js
    def resources(job: Jobscript):
        """Get resources associated with this jobscript."""
        click.echo(job.resources.__dict__)

    @jobscript.command(name="deps")
    @_pass_js
    def dependencies(job: Jobscript):
        """Get jobscript dependencies."""
        click.echo(job.dependencies)

    @jobscript.command()
    @_pass_js
    def path(job: Jobscript):
        """Get the file path to the jobscript."""
        click.echo(job.jobscript_path)

    @jobscript.command()
    @_pass_js
    def show(job: Jobscript):
        """Show the jobscript file."""
        with job.jobscript_path.open("rt") as fp:
            click.echo(fp.read())

    @jobscript.command()
    @jobscript_std_array_idx_opt
    @_pass_js
    def stdout(job: Jobscript, array_idx: int):
        """Print the contents of the standard output stream file."""
        job.print_stdout(array_idx=array_idx)

    @jobscript.command()
    @jobscript_std_array_idx_opt
    @_pass_js
    def stderr(job: Jobscript, array_idx: int):
        """Print the contents of the standard error stream file."""
        job.print_stderr(array_idx=array_idx)

    _set_help_name(jobscript, app)
    return jobscript


def _make_workflow_submission_CLI(app: BaseApp):
    """Generate the CLI for interacting with existing workflow submissions."""

    @click.group(name="sub")
    @_pass_workflow
    @click.pass_context
    @click.argument("sub_idx", type=click.INT)
    def submission(ctx: click.Context, wf: Workflow, sub_idx: int):
        """Interact with existing {app_name} workflow submissions.

        SUB_IDX is the submission index.

        """
        ctx.obj = wf.submissions[sub_idx]

    @submission.command("status")
    @_pass_submission
    def status(sb: Submission):
        """Get the submission status."""
        click.echo(sb.status.name.lower())

    @submission.command("submitted-js")
    @_pass_submission
    def submitted_JS(sb: Submission):
        """Get a list of jobscript indices that have been submitted."""
        click.echo(sb.submitted_jobscripts)

    @submission.command("outstanding-js")
    @_pass_submission
    def outstanding_JS(sb: Submission):
        """Get a list of jobscript indices that have not yet been submitted."""
        click.echo(sb.outstanding_jobscripts)

    @submission.command("needs-submit")
    @_pass_submission
    def needs_submit(sb: Submission):
        """Check if this submission needs submitting."""
        click.echo(sb.needs_submit)

    @submission.command("get-active-jobscripts")
    @_pass_submission
    def get_active_jobscripts(sb: Submission):
        """Show active jobscripts and their jobscript-element states."""
        pprint(sb.get_active_jobscripts(as_json=True))

    @submission.command()
    @_pass_submission
    def get_scheduler_job_IDs(sb: Submission):
        """Print jobscript scheduler job IDs."""
        job_IDs = sb.get_scheduler_job_IDs()
        if job_IDs:
            print("\n".join(job_IDs))

    @submission.command()
    @_pass_submission
    def get_process_IDs(sb: Submission):
        """Print jobscript process IDs."""
        proc_IDs = sb.get_process_IDs()
        if proc_IDs:
            print("\n".join(str(i) for i in proc_IDs))

    @submission.command()
    @list_js_max_js_opt
    @list_js_jobscripts_opt
    @list_js_width_opt
    @_pass_submission
    def list_jobscripts(
        sb: Submission, max_js: int | None, jobscripts: str | None, width: int | None
    ):
        """Print a table listing jobscripts and associated information."""
        jobscripts_ = [int(i) for i in jobscripts.split(",")] if jobscripts else None
        sb.list_jobscripts(max_js=max_js, jobscripts=jobscripts_, width=width)

    @submission.command()
    @list_task_js_max_js_opt
    @list_task_js_task_names_opt
    @list_js_width_opt
    @_pass_submission
    def list_task_jobscripts(
        sb: Submission,
        max_js: int | None,
        task_names: str | None,
        width: int | None,
    ):
        """Print a table listing tasks and their associated jobscripts."""
        task_names_ = list(task_names.split(",")) if task_names else None
        sb.list_task_jobscripts(task_names=task_names_, max_js=max_js, width=width)

    _set_help_name(submission, app)
    submission.add_command(_make_workflow_submission_jobscript_CLI(app))
    return submission


def _make_workflow_CLI(app: BaseApp):
    """Generate the CLI for interacting with existing workflows."""

    @click.group()
    @click.argument("workflow_ref")
    @workflow_ref_type_opt
    @click.pass_context
    def workflow(ctx: click.Context, workflow_ref: str, ref_type: str | None):
        """Interact with existing {app_name} workflows.

        WORKFLOW_REF is the path to, or local ID of, an existing workflow.

        """
        workflow_path = app._resolve_workflow_reference(workflow_ref, ref_type)
        ctx.obj = app.Workflow(workflow_path)

    @workflow.command(name="submit")
    @js_parallelism_option
    @wait_option
    @add_to_known_opt
    @print_idx_opt
    @tasks_opt
    @cancel_opt
    @submit_status_opt
    @_pass_workflow
    def submit_workflow(
        wf: Workflow,
        js_parallelism: bool | None = None,
        wait: bool = False,
        add_to_known: bool = True,
        print_idx: bool = False,
        tasks: list[int] | None = None,
        cancel: bool = False,
        status: bool = True,
    ):
        """Submit the workflow."""
        out = wf.submit(
            JS_parallelism=js_parallelism,
            wait=wait,
            add_to_known=add_to_known,
            return_idx=True,
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
    @_pass_workflow
    def wait(wf: Workflow, jobscripts: str | None):
        js_spec = parse_jobscript_wait_spec(jobscripts) if jobscripts else None
        wf.wait(sub_js=js_spec)

    @workflow.command(name="abort-run")
    @click.option("--submission", type=click.INT, default=-1)
    @click.option("--task", type=click.INT)
    @click.option("--element", type=click.INT)
    @_pass_workflow
    def abort_run(wf: Workflow, submission: int, task: int, element: int):
        """Abort the specified run."""
        wf.abort_run(
            submission_idx=submission,
            task_idx=task,
            element_idx=element,
        )

    @workflow.command(name="get-param")
    @click.argument("index", type=click.INT)
    @_pass_workflow
    def get_parameter(wf: Workflow, index: int):
        """Get a parameter value by data index."""
        click.echo(wf.get_parameter_data(index))

    @workflow.command(name="get-param-source")
    @click.argument("index", type=click.INT)
    @_pass_workflow
    def get_parameter_source(wf: Workflow, index: int):
        """Get a parameter source by data index."""
        click.echo(wf.get_parameter_source(index))

    @workflow.command(name="get-all-params")
    @_pass_workflow
    def get_all_parameters(wf: Workflow):
        """Get all parameter values."""
        click.echo(wf.get_all_parameter_data())

    @workflow.command(name="is-param-set")
    @click.argument("index", type=click.INT)
    @_pass_workflow
    def is_parameter_set(wf: Workflow, index: int):
        """Check if a parameter specified by data index is set."""
        click.echo(wf.is_parameter_set(index))

    @workflow.command(name="show-all-status")
    @_pass_workflow
    def show_all_EAR_statuses(wf: Workflow):
        """Show the submission status of all workflow EARs."""
        wf.show_all_EAR_statuses()

    @workflow.command(name="zip")
    @zip_path_opt
    @zip_overwrite_opt
    @zip_log_opt
    @zip_include_execute_opt
    @zip_include_rechunk_backups_opt
    @_pass_workflow
    def zip_workflow(
        wf: Workflow,
        path: str,
        overwrite: bool,
        log: str | None,
        include_execute: bool,
        include_rechunk_backups: bool,
    ):
        """Generate a copy of the workflow in the zip file format in the current working
        directory."""
        click.echo(
            wf.zip(
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
    @_pass_workflow
    def unzip_workflow(wf: Workflow, path: str, log: str | None):
        """Generate a copy of the zipped workflow in the submittable Zarr format in the
        current working directory."""
        click.echo(wf.unzip(path=path, log=log))

    @workflow.command(name="rechunk")
    @rechunk_backup_opt
    @rechunk_chunk_size_opt
    @rechunk_status_opt
    @_pass_workflow
    def rechunk(wf: Workflow, backup: bool, chunk_size: int, status: bool):
        """Rechunk metadata/runs and parameters/base arrays."""
        wf.rechunk(backup=backup, chunk_size=chunk_size, status=status)

    @workflow.command(name="rechunk-runs")
    @rechunk_backup_opt
    @rechunk_chunk_size_opt
    @rechunk_status_opt
    @_pass_workflow
    def rechunk_runs(wf: Workflow, backup: bool, chunk_size: int, status: bool):
        """Rechunk the metadata/runs array."""
        wf.rechunk_runs(backup=backup, chunk_size=chunk_size, status=status)

    @workflow.command(name="rechunk-parameter-base")
    @rechunk_backup_opt
    @rechunk_chunk_size_opt
    @rechunk_status_opt
    @_pass_workflow
    def rechunk_parameter_base(wf: Workflow, backup: bool, chunk_size: int, status: bool):
        """Rechunk the parameters/base array."""
        wf.rechunk_parameter_base(backup=backup, chunk_size=chunk_size, status=status)

    @workflow.command()
    @_pass_workflow
    def get_scheduler_job_IDs(wf: Workflow):
        """Print jobscript scheduler job IDs from all submissions of this workflow."""
        job_IDs = wf.get_scheduler_job_IDs()
        if job_IDs:
            print("\n".join(job_IDs))

    @workflow.command()
    @_pass_workflow
    def get_process_IDs(wf: Workflow):
        """Print jobscript process IDs from all submissions of this workflow."""
        proc_IDs = wf.get_process_IDs()
        if proc_IDs:
            print("\n".join(str(i) for i in proc_IDs))

    @workflow.command()
    @click.option(
        "--sub-idx",
        type=click.INT,
        default=0,
        help="Submission index whose jobscripts are to be shown.",
    )
    @list_js_max_js_opt
    @list_js_jobscripts_opt
    @list_js_width_opt
    @_pass_workflow
    def list_jobscripts(
        wf: Workflow,
        sub_idx: int,
        max_js: int | None,
        jobscripts: str | None,
        width: int | None,
    ):
        """Print a table listing jobscripts and associated information from the specified
        submission."""
        jobscripts_ = [int(i) for i in jobscripts.split(",")] if jobscripts else None
        wf.list_jobscripts(
            sub_idx=sub_idx, max_js=max_js, jobscripts=jobscripts_, width=width
        )

    @workflow.command()
    @click.option(
        "--sub-idx",
        type=click.INT,
        default=0,
        help="Submission index whose tasks are to be shown.",
    )
    @list_task_js_max_js_opt
    @list_task_js_task_names_opt
    @list_js_width_opt
    @_pass_workflow
    def list_task_jobscripts(
        wf: Workflow,
        sub_idx: int,
        max_js: int | None,
        task_names: str | None,
        width: int | None,
    ):
        """Print a table listing tasks and their associated jobscripts from the specified
        submission."""
        task_names_ = list(task_names.split(",")) if task_names else None
        wf.list_task_jobscripts(
            sub_idx=sub_idx, task_names=task_names_, max_js=max_js, width=width
        )

    _set_help_name(workflow, app)
    workflow.add_command(_make_workflow_submission_CLI(app))
    return workflow


def _make_submission_CLI(app: BaseApp):
    """Generate the CLI for submission related queries."""

    def OS_info_callback(ctx: click.Context, param, value: bool):
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
    def submission():
        """Submission-related queries."""
        pass

    @submission.command("shell-info")
    @click.argument("shell_name", type=click.Choice(list(ALL_SHELLS)))
    @click.option("--exclude-os", is_flag=True, default=False)
    @click.pass_context
    def shell_info(ctx: click.Context, shell_name: str, exclude_os: bool):
        """Show information about the specified shell, such as the version."""
        pprint(app.get_shell_info(shell_name, exclude_os))
        ctx.exit()

    @submission.group("scheduler")
    @click.argument("scheduler_name")
    @click.pass_context
    def scheduler(ctx: click.Context, scheduler_name: str):
        ctx.obj = app.get_scheduler(scheduler_name, os.name)

    pass_scheduler = click.make_pass_decorator(SGEPosix)

    @scheduler.command()
    @pass_scheduler
    def get_login_nodes(scheduler: SGEPosix):
        pprint(scheduler.get_login_nodes())

    @submission.command()
    @click.option(
        "as_json",
        "--json",
        is_flag=True,
        default=False,
        help="Do not format and only show JSON-compatible information.",
    )
    def get_known(as_json: bool = False):
        """Print known-submissions information as a formatted Python object."""
        out = app.get_known_submissions(as_json=as_json)
        if as_json:
            click.echo(json.dumps(out))
        else:
            pprint(out)

    return submission


def _make_internal_CLI(app: BaseApp):
    """Generate the CLI for internal use."""

    @click.group()
    def internal(help: bool = True):  # TEMP
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
    def workflow(ctx: click.Context, path: Path):
        """"""
        ctx.obj = app.Workflow(path)

    @workflow.command()
    @_pass_workflow
    @click.argument("submission_idx", type=click.INT)
    @click.argument("jobscript_idx", type=click.INT)
    @click.argument("block_idx", type=click.INT)
    @click.argument("block_action_idx", type=click.INT)
    @click.argument("run_id", type=click.INT)
    def execute_run(
        wf: Workflow,
        submission_idx: int,
        jobscript_idx: int,
        block_idx: int,
        block_action_idx: int,
        run_id: int,
    ):
        app.CLI_logger.info(f"execute commands for EAR ID {run_id!r}.")
        wf.execute_run(
            submission_idx=submission_idx,
            block_act_key=(jobscript_idx, block_idx, block_action_idx),
            run_ID=run_id,
        )

    @workflow.command()
    @_pass_workflow
    @click.argument("submission_idx", type=click.INT)
    @click.argument("jobscript_idx", type=click.INT)
    def execute_combined_runs(
        wf: Workflow,
        submission_idx: int,
        jobscript_idx: int,
    ):
        app.CLI_logger.info(
            f"execute command for combined scripts of jobscript {jobscript_idx}."
        )
        wf.execute_combined_runs(
            submission_idx=submission_idx,
            jobscript_idx=jobscript_idx,
        )

    @workflow.command()
    @_pass_workflow
    @click.argument("name")
    @click.argument("value")
    @click.argument("ear_id", type=click.INT)
    @click.argument("cmd_idx", type=click.INT)
    @click.option("--stderr", is_flag=True, default=False)
    def save_parameter(
        wf: Workflow,
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
        with wf._store.cached_load():
            value = wf.process_shell_parameter_output(
                name=name,
                value=value,
                EAR_ID=ear_id,
                cmd_idx=cmd_idx,
                stderr=stderr,
            )
            app.CLI_logger.debug(f"save parameter processed value is: {value!r}")
            wf.save_parameter(name=name, value=value, EAR_ID=ear_id)

    # TODO: in general, maybe the workflow command group can expose the simple Workflow
    # properties; maybe use a decorator on the Workflow property object to signify
    # inclusion?

    return internal


def _make_template_components_CLI(app: BaseApp):
    @click.command()
    def tc(help: bool = True):
        """For showing template component data."""
        pprint(app.template_components)

    return tc


def _make_show_CLI(app: BaseApp):
    def show_legend_callback(ctx: click.Context, param, value: bool):
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
    def show(max_recent: int, full: bool, no_update: bool):
        """Show information about running and recently active workflows."""
        app.show(max_recent=max_recent, full=full, no_update=no_update)

    return show


def _make_zip_CLI(app: BaseApp):
    @click.command(name="zip")
    @click.argument("workflow_ref")
    @zip_path_opt
    @zip_overwrite_opt
    @zip_log_opt
    @zip_include_execute_opt
    @zip_include_rechunk_backups_opt
    @workflow_ref_type_opt
    def zip_workflow(
        workflow_ref: str,
        path: str,
        overwrite: bool,
        log: str | None,
        include_execute: bool,
        include_rechunk_backups: bool,
        ref_type: str | None,
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


def _make_unzip_CLI(app: BaseApp):
    @click.command(name="unzip")
    @click.argument("workflow_path")
    @unzip_path_opt
    @unzip_log_opt
    def unzip_workflow(workflow_path: str, path: str, log: str | None):
        """Generate a copy of the specified zipped workflow in the submittable Zarr
        format in the current working directory.

        WORKFLOW_PATH is path of the zip file to unzip.

        """
        wk = app.Workflow(workflow_path)
        click.echo(wk.unzip(path=path, log=log))

    return unzip_workflow


def _make_cancel_CLI(app: BaseApp):
    @click.command()
    @click.argument("workflow_ref")
    @workflow_ref_type_opt
    @cancel_status_opt
    def cancel(workflow_ref: str, ref_type: str | None, status: bool):
        """Stop all running jobscripts of the specified workflow.

        WORKFLOW_REF is the local ID (that provided by the `show` command}) or the
        workflow path.

        """
        app.cancel(workflow_ref=workflow_ref, ref_is_path=ref_type, status=status)

    return cancel


def _make_rechunk_CLI(app: BaseApp):
    @click.command(name="rechunk")
    @click.argument("workflow_ref")
    @workflow_ref_type_opt
    @rechunk_backup_opt
    @rechunk_chunk_size_opt
    @rechunk_status_opt
    def rechunk(
        workflow_ref: str,
        ref_type: str | None,
        backup: bool,
        chunk_size: int,
        status: bool,
    ):
        """Rechunk metadata/runs and parameters/base arrays.

        WORKFLOW_REF is the local ID (that provided by the `show` command}) or the
        workflow path.

        """
        workflow_path = app._resolve_workflow_reference(workflow_ref, ref_type)
        wk = app.Workflow(workflow_path)
        wk.rechunk(backup=backup, chunk_size=chunk_size, status=status)

    return rechunk


def _make_open_CLI(app: BaseApp):
    @click.group(name="open")
    def open_file():
        """Open a file (for example {app_name}'s log file) using the default
        application."""

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def log(path: bool = False):
        """Open the {app_name} log file."""
        file_path = app.config.log_file_path
        if path:
            click.echo(file_path)
        else:
            utils.open_file(file_path)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def config(path: bool = False):
        """Open the {app_name} config file, or retrieve it's path."""
        file_path = app.config.config_file_path
        if path:
            click.echo(file_path)
        else:
            utils.open_file(file_path)

    @open_file.command()
    @click.option("--name")
    @click.option("--path", is_flag=True, default=False)
    def env_source(name: str | None = None, path: bool = False):
        """Open a named environment sources file, or the first one."""
        if not (sources := app.config.environment_sources):
            raise ValueError("No environment sources specified in the config file.")
        if not name:
            file_paths = [sources[0]]
        else:
            file_paths = [pth for pth in sources if pth.name == name]
        if not file_paths:
            raise ValueError(
                f"No environment source named {name!r} could be found; available "
                f"environment source files have names: {[pth.name for pth in sources]!r}"
            )

        assert len(file_paths) < 5  # don't open a stupid number of files
        for pth in file_paths:
            if path:
                click.echo(pth)
            else:
                utils.open_file(pth)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def known_subs(path: bool = False):
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
    def workflow(workflow_ref: str, ref_type: str | None, path: bool = False):
        """Open a workflow directory using, for example, File Explorer on Windows."""
        workflow_path = app._resolve_workflow_reference(workflow_ref, ref_type)
        if path:
            click.echo(workflow_path)
        else:
            utils.open_file(workflow_path)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def user_data_dir(path: bool = False):
        dir_path = app._ensure_user_data_dir()
        if path:
            click.echo(dir_path)
        else:
            utils.open_file(dir_path)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def user_cache_dir(path: bool = False):
        dir_path = app._ensure_user_cache_dir()
        if path:
            click.echo(dir_path)
        else:
            utils.open_file(dir_path)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def user_runtime_dir(path: bool = False):
        dir_path = app._ensure_user_runtime_dir()
        if path:
            click.echo(dir_path)
        else:
            utils.open_file(dir_path)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def user_data_hostname_dir(path: bool = False):
        dir_path = app._ensure_user_data_hostname_dir()
        if path:
            click.echo(dir_path)
        else:
            utils.open_file(dir_path)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def user_cache_hostname_dir(path: bool = False):
        dir_path = app._ensure_user_cache_hostname_dir()
        if path:
            click.echo(dir_path)
        else:
            utils.open_file(dir_path)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def demo_data_cache_dir(path: bool = False):
        dir_path = app._ensure_demo_data_cache_dir()
        if path:
            click.echo(dir_path)
        else:
            utils.open_file(dir_path)

    _set_help_name(open_file, app)
    _set_help_name(log, app)
    _set_help_name(config, app)
    return open_file


def _make_demo_data_CLI(app: BaseApp):
    """Generate the CLI for interacting with example data files that are used in demo
    workflows."""

    def list_callback(ctx: click.Context, param, value: bool):
        if not value or ctx.resilient_parsing:
            return
        # TODO: format with Rich with a one-line description
        click.echo("\n".join(app.list_demo_data_files()))
        ctx.exit()

    def cache_all_callback(ctx: click.Context, param, value: bool):
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
    def copy_demo_data(file_name: str, destination: str):
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
    def cache_demo_data(file_name: str):
        """Ensure a demo data file is in the demo data cache."""
        app.cache_demo_data_file(file_name)

    return demo_data


def _make_manage_CLI(app: BaseApp):
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
    def reset_config(config_dir: str):
        """Reset the configuration file to defaults.

        This can be used if the current configuration file is invalid."""
        app.reset_config(config_dir)

    @manage.command()
    @click.option(
        "--config-dir",
        help="The directory containing the config file whose path is to be returned.",
    )
    def get_config_path(config_dir: str):
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
    def clear_cache(hostname: bool):
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


def make_cli(app: BaseApp):
    """Generate the root CLI for the app."""

    colorama_init(autoreset=True)

    def run_time_info_callback(ctx: click.Context, param, value: bool):
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
        ctx: click.Context,
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
    def configure_env(
        name: str,
        use_current_env: bool,
        setup: list[str] | None = None,
        env_source_file: str | None = None,
    ):
        """Configure an app environment, using, for example, the currently activated
        Python environment."""
        app.configure_env(
            name=name,
            setup=setup,
            executables=None,
            use_current_env=use_current_env,
            env_source_file=None if env_source_file is None else Path(env_source_file),
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
