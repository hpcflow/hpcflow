"""Prepare and launch diagnostic executions without recording workflow results."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
from typing import TYPE_CHECKING

import numpy as np

from ..core.enums import EARStatus
from ..core.errors import NotSubmitMachineError
from ..core.workflow import load_workflow_config
from .enums import JobscriptElementState
from .schedulers import QueuedScheduler
from .schedulers.sge import SGEPosix
from .schedulers.slurm import SlurmPosix
from .shells.powershell import WindowsPowerShell

if TYPE_CHECKING:
    from ..core.actions import ElementActionRun
    from ..core.workflow import Workflow
    from .jobscript import Jobscript
    from .shells.base import Shell


def _quote(shell: Shell, value: str) -> str:
    if isinstance(shell, WindowsPowerShell):
        return "'" + value.replace("'", "''") + "'"
    return shlex.quote(value)


def _launch_line(shell: Shell, command: list[str]) -> str:
    prefix = "& " if isinstance(shell, WindowsPowerShell) else "exec "
    return prefix + " ".join(_quote(shell, word) for word in command)


def _location(
    workflow: Workflow, run: ElementActionRun
) -> tuple[Jobscript, int, int, int]:
    if run.submission_idx is None:
        raise ValueError(f"Run {run.id_} has not been assigned to a submission.")
    for job in workflow.submissions[run.submission_idx].jobscripts:
        for block_idx, block in enumerate(job.blocks):
            matches = np.argwhere(block.EAR_ID == run.id_)
            if len(matches):
                action_idx, element_idx = map(int, matches[0])
                return job, block_idx, action_idx, element_idx
    raise ValueError(f"Cannot locate the jobscript for run {run.id_}.")


def _interrupted(workflow: Workflow, run: ElementActionRun) -> bool:
    job, block_idx, _, element_idx = _location(workflow, run)
    try:
        states = job.get_active_states()
    except (
        OSError,
        ValueError,
        RuntimeError,
        subprocess.SubprocessError,
        NotSubmitMachineError,
    ) as error:
        workflow._app.logger.warning(
            "Cannot ascertain activity for run %s; treating it as interrupted: %s",
            run.id_,
            error,
        )
        return True
    state = states.get(block_idx, {}).get(element_idx)
    return state not in {
        JobscriptElementState.pending,
        JobscriptElementState.waiting,
        JobscriptElementState.configuring,
        JobscriptElementState.running,
    }


def select_debug_run(
    workflow: Workflow, run_id: int | None, first_failed: bool
) -> ElementActionRun:
    """Select an explicit run or the lowest-ID errored/interrupted attempt."""
    if (run_id is not None) == first_failed:
        raise ValueError("Specify exactly one of --run-id and --first-failed.")
    if run_id is not None:
        if not 0 <= run_id < workflow.num_EARs:
            raise ValueError(f"Unknown run ID {run_id}.")
        return workflow.get_EARs_from_IDs([run_id])[0]
    for run in sorted(workflow.get_all_EARs(), key=lambda item: item.id_):
        if run.status is EARStatus.error or (
            run.status is EARStatus.running and _interrupted(workflow, run)
        ):
            return run
    raise ValueError("No errored or interrupted runs were found.")


@dataclass(frozen=True)
class DebugRun:
    """Editable debug artifacts; launching these never updates submission metadata."""

    run_id: int
    directory: Path
    commands_path: Path
    jobscript_path: Path
    script_path: Path | None
    jobscript: Jobscript
    direct: bool

    def execute(self) -> str | int:
        """Run commands synchronously, or submit the independent diagnostic job."""
        shell = self.jobscript.shell
        scheduler = None if self.direct else self.jobscript.scheduler
        run = self.jobscript.workflow.get_EARs_from_IDs([self.run_id])[0]
        environment = dict(os.environ)
        secret_names = run.get_environment().secrets
        for name in secret_names:
            environment[name] = self.jobscript._app.get_secret(name)
        if self.direct or not isinstance(scheduler, QueuedScheduler):
            path = self.commands_path if self.direct else self.jobscript_path
            command = shell.get_command_file_launch_command(shell.prepare_JS_path(path))
            with (
                (self.directory / "stdout.txt").open("wt", newline="\n") as stdout,
                (self.directory / "stderr.txt").open("wt", newline="\n") as stderr,
            ):
                return subprocess.run(
                    command,
                    cwd=self.directory,
                    stdout=stdout,
                    stderr=stderr,
                    env=environment,
                ).returncode
        command = scheduler.get_submit_command(
            shell, shell.prepare_JS_path(self.jobscript_path), {}
        )
        if secret_names:
            if isinstance(scheduler, SlurmPosix):
                command[1:1] = ["--export=ALL," + ",".join(secret_names)]
            elif isinstance(scheduler, SGEPosix):
                command[1:1] = ["-v", ",".join(secret_names)]
            else:
                raise ValueError(
                    "This scheduler cannot forward debug environment secrets."
                )
        result = subprocess.run(
            command,
            cwd=self.directory,
            capture_output=True,
            text=True,
            env=environment,
        )
        if result.returncode or result.stderr:
            raise RuntimeError(
                f"Debug submission failed (exit code {result.returncode}): "
                f"{result.stderr or result.stdout}"
            )
        reference = scheduler.parse_submission_output(result.stdout.strip())
        if not reference:
            raise RuntimeError(
                f"Cannot determine debug job ID from submission output: {result.stdout!r}"
            )
        return reference


@load_workflow_config
def prepare_debug_run(
    workflow: Workflow,
    *,
    run_id: int | None = None,
    first_failed: bool = False,
    direct: bool = False,
) -> DebugRun:
    """Generate an independent enactment in the original submission's tmp directory."""
    if not Path(workflow.path).is_dir():
        raise ValueError(
            "Debug execution requires an unpacked, local workflow directory."
        )
    run = select_debug_run(workflow, run_id, first_failed)
    job, block_idx, action_idx, element_idx = _location(workflow, run)
    shell = job.shell
    submission = job.submission
    commands, _ = run.compose_commands(submission.environments, shell)
    if not commands.strip():
        raise ValueError(f"Run {run.id_} has no commands to debug.")

    submission.tmp_path.mkdir(parents=True, exist_ok=True)
    directory = Path(
        tempfile.mkdtemp(prefix=f"debug_run_{run.id_}_", dir=submission.tmp_path)
    )
    work_dir = directory / "work"
    original_dir = run.get_directory()
    if original_dir is not None and original_dir.is_dir():
        shutil.copytree(original_dir, work_dir)
    else:
        work_dir.mkdir()
    for value in run.get("input_files", {}).values():
        if value:
            for source in value if isinstance(value, list) else [value]:
                source = Path(source)
                destination = work_dir / source.name
                if not destination.exists():
                    shutil.copy2(source, destination)

    previous_dir = Path.cwd()
    try:
        os.chdir(work_dir)
        with run.raise_on_failure_threshold():
            if run.action.script:
                run.write_script_data_in_files((job.index, block_idx, action_idx))
            if run.action.has_program:
                run.write_program_data_in_files((job.index, block_idx, action_idx))
    finally:
        os.chdir(previous_dir)

    script_path = None
    if run.action.script:
        if run.is_snippet_script:
            snippet_path = run.action.get_snippet_script_path(
                run.action.script, run.env_spec
            )
            assert snippet_path is not None
            script_path = directory / run.get_script_artifact_name()
            source = run.action.compose_source(snippet_path, debug=True)
            script_path.write_text(source, encoding="utf-8", newline="\n")
        else:
            source_path = work_dir / run.action.script
            if not source_path.is_file():
                raise FileNotFoundError(f"Script {source_path} is not available.")
            script_path = directory / source_path.name
            shutil.copy2(source_path, script_path)

    app_caps = workflow._app.package_name.upper()
    environment = {
        "WK_PATH": str(Path(workflow.path).resolve()),
        "WK_PATH_ARG": str(Path(workflow.path).resolve()),
        "SUB_IDX": str(submission.index),
        "JS_IDX": str(job.index),
        "BLOCK_IDX": str(block_idx),
        "BLOCK_ACT_IDX": str(action_idx),
        "BLOCK_ELEM_IDX": str(element_idx),
        "JS_ELEM_IDX": str(
            sum(block.num_elements for block in job.blocks[:block_idx]) + element_idx
        ),
        "RUN_ID": str(run.id_),
        "RUN_IDX": str(run.index),
        "TASK_IDX": str(run.task.index),
        "TASK_INSERT_ID": str(run.task.insert_ID),
        "ELEMENT_IDX": str(run.element.index),
        "ELEMENT_ID": str(run.element.id_),
        "ELEMENT_ITER_IDX": str(run.element_iteration.index),
        "ELEMENT_ITER_ID": str(run.element_iteration.id_),
        "ELEMENT_ITER_LOOP_IDX": ";".join(
            f"{key}={value}" for key, value in run.element_iteration.loop_idx.items()
        ),
        "RUN_RANDOM_SEED": str(run.resources.random_seed),
        "RUN_RNG_SPAWN_KEY": ",".join(map(str, run.resources.rng_spawn_key or [])),
        "SUB_SCRIPTS_DIR": str(directory),
        "SUB_TMP_DIR": str(directory),
        "SUB_LOG_DIR": str(directory),
        "SUB_STD_DIR": str(directory),
        "RUN_STD_PATH": str(directory / "app_std.txt"),
        "RUN_LOG_PATH": str(directory / "app.log"),
        "LOG_PATH": str(directory / "app.log"),
        "TIMEIT": "False",
        "JS_FUNCS_PATH": str(directory / f"functions{shell.JS_EXT}"),
    }
    for name in ("num_threads", "num_cores", "num_MPI_ranks"):
        if (value := getattr(run.resources, name)) is not None:
            environment[f"RUN_{name.upper()}"] = str(value)
    if script_path is not None:
        environment.update(
            {
                "RUN_SCRIPT_PATH": str(script_path),
                "RUN_SCRIPT_NAME": script_path.name,
                "RUN_SCRIPT_NAME_NO_EXT": script_path.stem,
                "RUN_SCRIPT_DIR": str(script_path.parent),
            }
        )
    if run.action.has_program and (program_path := run.program_path_actual):
        environment.update(
            {
                "RUN_PROGRAM_PATH": str(program_path),
                "RUN_PROGRAM_NAME": program_path.name,
                "RUN_PROGRAM_NAME_NO_EXT": program_path.stem,
                "RUN_PROGRAM_DIR": str(program_path.parent),
            }
        )

    (directory / f"functions{shell.JS_EXT}").write_text("", newline="\n")
    powershell = isinstance(shell, WindowsPowerShell)
    setup = ["$ErrorActionPreference = 'Stop'"] if powershell else ["set -e"]
    for key, value in environment.items():
        quoted = _quote(shell, value)
        setup.append(
            f"$env:{app_caps}_{key} = {quoted}"
            if powershell
            else f"export {app_caps}_{key}={quoted}"
        )
    setup.append(
        f"Set-Location -LiteralPath {_quote(shell, str(work_dir))}"
        if powershell
        else f"cd {_quote(shell, str(work_dir))}"
    )
    commands_path = directory / f"commands{shell.JS_EXT}"
    commands_path.write_text(
        "\n".join(setup)
        + "\n"
        + shell.format_commands_file(workflow._app.package_name, commands),
        encoding="utf-8",
        newline="\n",
    )

    scheduler = None if direct else job.scheduler
    shebang = shell.JS_SHEBANG.format(
        shebang=" ".join(
            (scheduler.shebang_executable if scheduler else None)
            or shell.shebang_executable
        )
    )
    header = shebang + "\n"
    if not direct and isinstance(scheduler, QueuedScheduler):
        header += scheduler.format_directives(
            job.resources,
            num_elements=1,
            is_array=False,
            sub_idx=submission.index,
            js_idx=job.index,
            std_dir=str(directory),
        )
    jobscript_path = directory / f"jobscript{shell.JS_EXT}"
    jobscript_path.write_text(
        header
        + "\n"
        + _launch_line(
            shell,
            shell.get_command_file_launch_command(shell.prepare_JS_path(commands_path)),
        )
        + ("\nexit $LASTEXITCODE\n" if powershell else "\n"),
        encoding="utf-8",
        newline="\n",
    )
    return DebugRun(
        run.id_, directory, commands_path, jobscript_path, script_path, job, direct
    )
