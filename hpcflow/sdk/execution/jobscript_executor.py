from __future__ import annotations

import asyncio
from collections.abc import Iterator
import contextlib
from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
import time
from typing import TYPE_CHECKING

from hpcflow.sdk.core import (
    ABORT_EXIT_CODE,
    NO_COMMANDS_EXIT_CODE,
    NO_PROGRAM_EXIT_CODE,
    SKIPPED_EXIT_CODE,
)
from hpcflow.sdk.core.actions import ElementActionRun
from hpcflow.sdk.core.app_aware import AppAware
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.errors import (
    OutputFileParserNoOutputError,
    UnsetParameterDataErrorBase,
)
from hpcflow.sdk.core.skip_reason import SkipReason

from hpcflow.sdk.core.utils import redirect_std_to_file
from hpcflow.sdk.config.config import load_workflow_config
from hpcflow.sdk.execution.run_executor import RunExecutor
from hpcflow.sdk.execution.server import JobscriptServer
from hpcflow.sdk.instrumentation import TimeIt
from hpcflow.sdk.utils.deferred_file import DeferredFileWriter
from hpcflow.sdk.wait.completion import JobscriptCompletion
from hpcflow.sdk.wait.protocol import WaitWakeup
from hpcflow.sdk.wait.run_wait import RunWaitEvent, RunWaitState
from hpcflow.sdk.wait.wait_client import WaitClient

if TYPE_CHECKING:
    from hpcflow.sdk.core.types import BlockActionKey

# TODO: add a `hpcflow manage test-compute-connectivity` to check which if
# any directions are communicable between host (i.e. login node) and
# compute node
# TODO: add a e.g. `hpcflow configure cambridge-csd3`, which can take
# some default config and scheduler options for demo-workflows, and
# wait timeouts, scheduler query rate limit etc

# TODO: should also handle SIGTERM in JobscriptExecutor, to allow us
# to mark the jobscript as complete and send sigterm to sub-processes
#
# maybe want to store some exit status of the jobscript somewhere,
# e.g. success or timelimit or cancelled etc
# could also store on startup an `i_active_[jobID]` file in simpler nested
# dir with so that if a node crashes and the job is requeued/restarted,
# we know about it (when the new job starts it can then know it's a
# restart by checking for an `i_active_*` file); also if a job has an active
# file but no record in the squeue then we know it has crashed


@dataclass(frozen=True)
class JobscriptRun:
    element_idx: int
    run_id: int
    run_std_path: Path
    run_log_path: Path | None


@dataclass(frozen=True)
class JobscriptAction:
    block_idx: int
    action_idx: int
    block_action_key: tuple[int, int, int]
    runs: tuple[JobscriptRun, ...]


@dataclass(frozen=True)
class JobscriptRunResult:
    js_run: JobscriptRun
    exit_code: int | None
    skipped: bool = False
    command_time: float | None = None
    new_run_skips: frozenset[int] = frozenset()
    timeit: TimeIt | None = None


@dataclass(frozen=True)
class FinalizedRun:
    run_id: int
    run_dir: Path | None
    exit_code: int
    success: bool


class JobscriptExecutor(AppAware):
    """Facilitates execution of a workflow submission jobscript.

    The WorkflowExecution class:
    - owns jobscript execution lifetime
    - owns JobscriptServer
    - owns active_executors: mapping run IDs to RunExecutor instances
    - updates run persistence state
    - aggregates execution timings
    """

    def __init__(
        self,
        workflow,
        submission_idx,
        jobscript_idx,
        array_idx: int | None = None,
    ):
        self.workflow = workflow

        self.submission_idx = submission_idx
        self.submission = workflow.submissions[submission_idx]

        self.jobscript_idx = jobscript_idx
        self.jobscript = self.submission.jobscripts[jobscript_idx]

        self.array_idx = array_idx

        self.active_executors: dict[int, RunExecutor] = {}

        self.jobscript_server: JobscriptServer | None = None

        self.runs: dict[int, ElementActionRun] = {}

        self.jobscript_std: DeferredFileWriter | None = None

    @property
    def logger(self):
        return self._app.execution_logger

    @property
    def store(self):
        return self.workflow._store

    @property
    def template(self):
        # used by the loads_config decorator
        return self.workflow.template

    @property
    def submissions(self):
        return self.workflow.submissions

    @property
    def submissions_path(self):
        return self.workflow.submissions_path

    @property
    def app_caps(self):
        return self._app.package_name.upper()

    @property
    def jobscript_std_path(self) -> Path:
        return self.jobscript.get_app_std_path()

    @load_workflow_config
    def execute(self):
        asyncio.run(self._execute())

    async def _start_server(self) -> None:
        self.jobscript_server = JobscriptServer(self)
        port_number = await self.jobscript_server.start()
        # TODO: save port number to `js_at_exec_md` data?

    async def _stop_server(self) -> None:
        if self.jobscript_server is None:
            return

        await self.jobscript_server.stop()
        self.jobscript_server = None

    def _update_cached_run_skips(self, run_skips: dict[int, int]) -> None:
        """Update cached runs with newly persisted skip reasons."""
        for run_id, skip_reason in run_skips.items():
            self.runs[run_id]._skip = skip_reason

    def _write_to_jobscript_app_std(self, message: str) -> None:
        with self.jobscript_std_path.open("a", encoding="utf-8") as fp:
            fp.write(f"{message}\n")
            fp.flush()

    async def _execute(self):
        self.runs = self.workflow.get_EARs_from_IDs(
            self.jobscript.all_EAR_IDs,
            as_dict=True,
        )
        await self._start_server()

        try:
            for action in self._iter_actions():
                results = await self._execute_action(action)
                new_run_skips = await self._finish_action(action=action, results=results)
                self._update_cached_run_skips(new_run_skips)

        finally:
            if TimeIt.is_active():
                self._write_to_jobscript_app_std("")  # newline before summary tree

            await self._stop_server()
            self.jobscript_std = None

            self.logger.debug(
                "marking jobscript complete: %s", self.completion.completion_path
            )
            if self.array_idx is None:
                self.completion.mark_complete()
            else:
                self.completion.mark_array_item_complete(self.array_idx)

            await self._notify_waiters()

    def _iter_actions(self) -> Iterator[JobscriptAction]:

        for block in self.jobscript.blocks:
            block_run_ids = block.EAR_ID

            if self.array_idx is None:
                element_indices = tuple(range(block.num_elements))
            else:
                element_indices = (self.array_idx,)

            for action_idx in range(block.num_actions):
                block_action_key = (self.jobscript_idx, block.index, action_idx)
                runs = []
                for element_idx in element_indices:
                    run_id = block_run_ids[action_idx, element_idx]
                    run = self.runs[run_id]
                    runs.append(
                        JobscriptRun(
                            element_idx=element_idx,
                            run_id=block_run_ids[action_idx, element_idx],
                            run_std_path=run.get_app_std_path(),
                            run_log_path=(
                                run.get_app_log_path()
                                if run.resources.write_app_logs
                                else None
                            ),
                        )
                    )

                yield JobscriptAction(
                    block_idx=block.index,
                    action_idx=action_idx,
                    block_action_key=block_action_key,
                    runs=tuple(runs),
                )

    async def _execute_action(
        self, action: JobscriptAction
    ) -> tuple[JobscriptRunResult, ...]:

        results: list[JobscriptRunResult] = []
        try:
            for js_run in action.runs:
                with self._app.log.temporary_file_logger(js_run.run_log_path):
                    result = await self._execute_run(action=action, js_run=js_run)
                results.append(result)
                for run_id in result.new_run_skips:
                    self.runs[run_id]._skip = SkipReason.LOOP_TERMINATION.value

                # accumulate command time on the jobscript TimeIt:
                if (
                    result.command_time is not None
                    and (timeit := TimeIt.current()) is not None
                ):
                    timeit.run_command_time = (
                        timeit.run_command_time or 0
                    ) + result.command_time

        except Exception:
            if results:
                new_run_skips = await self._finish_action(action, tuple(results))
                self._update_cached_run_skips(new_run_skips)
            raise

        return tuple(results)

    @TimeIt.decorator
    async def set_run_start(
        self, run_id: int, run_dir: Path | None, port_number: int | None
    ) -> None:
        """Set the start time on an EAR."""
        self.logger.debug(f"Setting start for EAR ID {run_id!r}")
        with self.store.cached_load(), self.workflow.batch_update():
            self.store.set_EAR_start(run_id, run_dir, port_number)

        waiter_ids = self.run_wait_state.complete(run_id, RunWaitEvent.START)
        await self._notify_waiters(waiter_ids)

    def _timeit_run_start(
        self, run: ElementActionRun, run_timeit: TimeIt | None
    ) -> float:
        run_wall_start = time.perf_counter()
        timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        run_pars = run.parents()
        self._write_to_jobscript_app_std(
            f"TimeIt: run_id={run.id_} run-start {timestamp} "
            f"task={run_pars['task']} action={run_pars['action']} "
            f"element={run_pars['element']} "
            f"iteration={run_pars['iteration']} "
            f"loop={run_pars['loop']} "
        )
        return run_wall_start

    def _timeit_run_end(
        self, run_ID: int, run_wall_start: float, status: str = "completed"
    ) -> None:
        if not TimeIt.is_active():
            return

        elapsed = time.perf_counter() - run_wall_start
        timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        self._write_to_jobscript_app_std(
            f"TimeIt: run_id={run_ID} run-end   {timestamp} status={status} "
            f"elapsed={elapsed:.3f}s",
        )

    async def _execute_run(
        self, action: JobscriptAction, js_run: JobscriptRun
    ) -> JobscriptRunResult:
        """Execute a single element-action run."""
        if TimeIt.current() is None:
            return await self._execute_run_inner(
                action=action, js_run=js_run, run_timeit=None
            )

        run = self.runs[js_run.run_id]

        run_timeit = TimeIt(
            title=f"TimeIt: Run {js_run.run_id} execution",
            file_path=js_run.run_std_path,
            file_mode="a",
            file_preamble=run.get_run_std_preamble(),
            prepend=True,
        )
        with run_timeit.activate():
            result = await self._execute_run_inner(
                action=action,
                js_run=js_run,
                run_timeit=run_timeit,
            )

        return result

    async def _execute_run_inner(
        self,
        action: JobscriptAction,
        js_run: JobscriptRun,
        run_timeit: TimeIt | None,
    ) -> JobscriptRunResult:
        """Execute a single element-action run."""

        def _ignore_unset_parameter_data(exc: BaseException) -> bool:
            return isinstance(exc, UnsetParameterDataErrorBase)

        run = self.runs[js_run.run_id]

        command_time = None
        new_run_skips = set()
        run_std_preamble = run.get_run_std_preamble()

        with redirect_std_to_file(js_run.run_std_path, preamble=run_std_preamble):

            if run_timeit:
                run_wall_start = self._timeit_run_start(run, run_timeit)

            # A scheduler may have requeued the job after this run had already
            # started or completed. A skipped run is an exception: its skipped
            # state may have been propagated by an upstream run, but this
            # jobscript still needs to process it.
            run_status = run.status
            if run_status not in EARStatus.get_executable_states():
                print(
                    f"Run {js_run.run_id} is already running, or in a terminal "
                    f"state: {run_status!r}."
                )
                return JobscriptRunResult(
                    js_run=js_run,
                    exit_code=None,
                    timeit=run_timeit,
                )

            run_dir = None
            if run.action.requires_dir:
                run_dir = run.get_directory()
                assert run_dir is not None

                self.logger.debug(
                    "Changing directory to run execution directory: %s.",
                    run_dir,
                )
                os.chdir(run_dir)

            if not self._apply_group_task_conditions(run):
                run._skip = SkipReason.TASK_CONDITION_NOT_MET.value

        #
        # Prepare and execute
        #

        has_commands = False
        commands_file_path = None

        if run.skip:
            exit_code = SKIPPED_EXIT_CODE

        else:
            try:
                with redirect_std_to_file(
                    js_run.run_std_path,
                    preamble=run_std_preamble,
                    ignore=_ignore_unset_parameter_data,
                ):
                    commands_file_path = self._ensure_run_files(
                        action=action,
                        run=run,
                        run_dir=run_dir,
                    )

            except UnsetParameterDataErrorBase:
                assert self.jobscript_server is not None
                assert self.jobscript_server.port_number is not None
                await self.set_run_start(
                    run.id_,
                    run_dir,
                    port_number=self.jobscript_server.port_number,
                )
                new_run_skips.update(self._check_loop_termination(run))

                return JobscriptRunResult(
                    js_run=js_run,
                    exit_code=1,
                    new_run_skips=frozenset(new_run_skips),
                    timeit=run_timeit,
                )

            if commands_file_path:
                has_commands = True
                assert isinstance(commands_file_path, Path)
                try:
                    exit_code, command_time = await self._execute_run_commands(
                        action=action,
                        js_run=js_run,
                        run=run,
                        run_dir=run_dir,
                        commands_file_path=commands_file_path,
                    )
                except ValueError:
                    return JobscriptRunResult(
                        js_run=js_run,
                        exit_code=NO_PROGRAM_EXIT_CODE,
                        timeit=run_timeit,
                    )

            elif run.action.jinja_template:
                exit_code = 0

            else:
                exit_code = NO_COMMANDS_EXIT_CODE

        #
        # Finalise
        #

        with redirect_std_to_file(
            js_run.run_std_path,
            preamble=run_std_preamble,
        ):
            if has_commands:
                new_run_skips.update(self._check_loop_termination(run))

            if run_timeit is not None:
                assert run_wall_start is not None
                self._timeit_run_end(run_ID=run.id_, run_wall_start=run_wall_start)
                run_timeit.run_command_time = command_time

        return JobscriptRunResult(
            js_run=js_run,
            exit_code=exit_code,
            skipped=bool(run.skip),
            command_time=command_time,
            new_run_skips=frozenset(new_run_skips),
            timeit=run_timeit,
        )

    def _get_run_environment(
        self,
        action: JobscriptAction,
        js_run: JobscriptRun,
        run: ElementActionRun,
    ) -> dict[str, str]:

        assert self.jobscript_server is not None
        assert self.jobscript_server.port_number is not None

        loop_idx_str = ";".join(
            f"{key}={value}" for key, value in run.element_iteration.loop_idx.items()
        )
        rng_spawn_key_str = ",".join(
            str(key) for key in run.resources.rng_spawn_key or []
        )

        app_caps = self.app_caps
        run_log_path = str(js_run.run_log_path) if js_run.run_log_path is not None else ""
        add_env = {
            f"{app_caps}_WK_PATH": str(self.workflow.path),
            f"{app_caps}_SUB_IDX": str(self.submission_idx),
            f"{app_caps}_SUB_SCRIPTS_DIR": str(self.submission.scripts_path),
            f"{app_caps}_JS_CONTROL_PORT": str(self.jobscript_server.port_number),
            f"{app_caps}_JS_IDX": str(self.jobscript_idx),
            f"{app_caps}_BLOCK_IDX": str(action.block_idx),
            f"{app_caps}_BLOCK_ACT_IDX": str(action.action_idx),
            f"{app_caps}_BLOCK_ELEM_IDX": str(js_run.element_idx),
            f"{app_caps}_RUN_STD_PREAMBLE": run.get_run_std_preamble(),
            f"{app_caps}_RUN_STD_PATH": str(js_run.run_std_path),
            f"{app_caps}_RUN_LOG_PATH": run_log_path,
            f"{app_caps}_RUN_ID": str(run.id_),
            f"{app_caps}_RUN_IDX": str(run.index),
            f"{app_caps}_RUN_RANDOM_SEED": str(run.resources.random_seed),
            f"{app_caps}_RUN_RNG_SPAWN_KEY": rng_spawn_key_str,
            f"{app_caps}_TASK_IDX": str(run.task.index),
            f"{app_caps}_TASK_INSERT_ID": str(run.task.insert_ID),
            f"{app_caps}_ELEMENT_IDX": str(run.element.index),
            f"{app_caps}_ELEMENT_ID": str(run.element.id_),
            f"{app_caps}_ELEMENT_ITER_IDX": str(run.element_iteration.index),
            f"{app_caps}_ELEMENT_ITER_ID": str(run.element_iteration.id_),
            f"{app_caps}_ELEMENT_ITER_LOOP_IDX": loop_idx_str,
            f"{app_caps}_TIMEIT": str(int(TimeIt.is_active())),
        }
        if (num_threads := run.resources.num_threads) is not None:
            add_env[f"{app_caps}_RUN_NUM_THREADS"] = str(num_threads)

        if (num_cores := run.resources.num_cores) is not None:
            add_env[f"{app_caps}_RUN_NUM_CORES"] = str(num_cores)

        if (num_MPI_ranks := run.resources.num_MPI_ranks) is not None:
            add_env[f"{app_caps}_RUN_NUM_MPI_RANKS"] = str(num_MPI_ranks)

        if run.action.script:
            if run.is_snippet_script:
                script_artifact_name = run.get_script_artifact_name()
                script_dir = self.submission.scripts_path
                script_name = script_artifact_name
            else:
                # not a snippet script; expect the script in the run execute
                # directory (i.e. created by a previous action)
                script_dir = Path.cwd()
                script_name = run.action.script
            script_name_no_ext = Path(script_name).stem
            add_env.update(
                {
                    f"{app_caps}_RUN_SCRIPT_NAME": script_name,
                    f"{app_caps}_RUN_SCRIPT_NAME_NO_EXT": script_name_no_ext,
                    f"{app_caps}_RUN_SCRIPT_DIR": str(script_dir),
                    f"{app_caps}_RUN_SCRIPT_PATH": str(script_dir / script_name),
                }
            )

        if program_path := run.program_path_actual:
            add_env.update(
                {
                    f"{app_caps}_RUN_PROGRAM_NAME": program_path.name,
                    f"{app_caps}_RUN_PROGRAM_NAME_NO_EXT": program_path.stem,
                    f"{app_caps}_RUN_PROGRAM_DIR": str(program_path.parent),
                    f"{app_caps}_RUN_PROGRAM_PATH": str(program_path),
                }
            )

        env = {**dict(os.environ), **add_env}

        self.logger.debug(f"Run environment variables:")
        for k, v in env.items():
            if k.startswith(app_caps):
                self.logger.debug(f"{k} = {v!r}")

        secrets_env = {}
        for secret_key in run.get_environment().secrets:
            self.logger.debug(f"Adding secret environment variable {secret_key!r}")
            secrets_env[secret_key] = self._app.get_secret(secret_key)

        env.update(secrets_env)

        return env

    def _ensure_run_files(
        self,
        action: JobscriptAction,
        run: ElementActionRun,
        run_dir: Path | None,
    ) -> Path | bool:
        """Ensure files required to execute a run have been written."""

        commands_file_path = None
        try:
            with run.raise_on_failure_threshold() as unset_params:
                if run.action.script:
                    run.write_script_data_in_files(action.block_action_key)

                if run.action.has_program:
                    run.write_program_data_in_files(action.block_action_key)

                commands_file_path = self.ensure_commands_file(run)

        except UnsetParameterDataErrorBase:
            self.logger.debug(
                "Unset parameter threshold satisfied (or unset parameters found "
                "while writing the commands file), so not attempting run %s. "
                "unset_params=%r.",
                run.id_,
                unset_params,
            )
            raise

        # Sufficient parameter data exists to execute the run, but there may still
        # be unset parameters below the configured failure threshold.
        if unset_params:
            self.logger.debug(
                "Some unset parameters found, but no unset-parameter thresholds "
                "were met for run %s: unset_params=%r.",
                run.id_,
                unset_params,
            )

        # TODO: pass unset_params to the script environment where required.

        if run.action.jinja_template_or_template_path:
            run.write_jinja_template()

        return commands_file_path

    def ensure_commands_file(self, run: ElementActionRun) -> Path | bool:
        """Ensure a commands file exists for the specified run."""

        self._app.persistence_logger.debug("JobscriptExecutor.ensure_commands_file")

        if run.commands_file_ID is None:
            # No commands to write.
            return False

        with self.store.cached_load():
            # First check for a commands file specific to this run.
            commands_file_name = f"{run.id_}{self.jobscript.shell.JS_EXT}"
            commands_file_path = self.submission.commands_path / commands_file_name

            if not commands_file_path.is_file():
                # Runs can share a commands file. Check for the file belonging to
                # the root run ID.
                commands_file_name = (
                    f"{run.commands_file_ID}{self.jobscript.shell.JS_EXT}"
                )
                commands_file_path = self.submission.commands_path / commands_file_name

            if not commands_file_path.is_file():
                # No existing commands file is available, so generate one for
                # this run.
                try:
                    commands_file_path = run.try_write_commands(
                        jobscript=self.jobscript,
                        environments=self.submission.environments,
                        raise_on_unset=True,
                        timeit=self.submission.timeit,
                    )
                except OutputFileParserNoOutputError:
                    # The action may exist solely to save files and therefore
                    # legitimately have no commands to execute.
                    return False

        return commands_file_path

    async def _execute_run_commands(
        self,
        action: JobscriptAction,
        js_run: JobscriptRun,
        run: ElementActionRun,
        run_dir: Path | None,
        commands_file_path: Path,
    ) -> tuple[int, float | None]:
        """Execute the commands for a single element-action run."""

        if not commands_file_path.is_file():
            raise RuntimeError(f"Command file {commands_file_path!r} does not exist.")

        cmd = self.jobscript.shell.get_command_file_launch_command(
            str(commands_file_path)
        )

        env = self._get_run_environment(action=action, js_run=js_run, run=run)

        self.logger.info(
            "Executing run %s (block=%s, action=%s, element=%s).",
            js_run.run_id,
            action.block_idx,
            action.action_idx,
            js_run.element_idx,
        )
        self.logger.debug("Command: %s", cmd)

        executor = self._app.RunExecutor(cmd=cmd, env=env)
        self.active_executors[js_run.run_id] = executor

        command_time = None
        if TimeIt.is_active():
            command_start = time.perf_counter()

        try:
            assert self.jobscript_server is not None
            assert self.jobscript_server.port_number is not None

            await self.set_run_start(
                run_id=run.id_,
                run_dir=run_dir,
                port_number=self.jobscript_server.port_number,
            )
            exit_code = await executor.run()
        finally:
            self.active_executors.pop(js_run.run_id, None)

        if run_timeit := TimeIt.current():
            run_timeit.child_orchestration_time += sum(executor.child_orchestration_times)
            run_timeit.child_work_time += sum(executor.child_work_times)

        if TimeIt.is_active():
            command_time = time.perf_counter() - command_start

        self.logger.info(
            "Run %s finished with exit code %s.",
            js_run.run_id,
            exit_code,
        )

        return exit_code, command_time

    async def _finish_action(
        self,
        action: JobscriptAction,
        results: tuple[JobscriptRunResult, ...],
    ) -> dict[int, int]:

        finalized_runs: list[FinalizedRun] = []
        new_run_skips: dict[int, int] = {}

        with self.store.cached_load(), self.workflow.batch_update():

            for result in results:
                if result.exit_code is None:
                    continue

                run = self.runs[result.js_run.run_id]

                timeit_ctx = (
                    result.timeit.activate()
                    if result.timeit is not None
                    else contextlib.nullcontext()
                )

                with (
                    timeit_ctx,
                    self._app.log.temporary_file_logger(result.js_run.run_log_path),
                    redirect_std_to_file(result.js_run.run_std_path),
                ):
                    finalized_run, run_skips = self._finalize_run(
                        block_act_key=action.block_action_key,
                        run=run,
                        exit_code=result.exit_code,
                    )

                finalized_runs.append(finalized_run)
                new_run_skips.update(run_skips)

                if result.timeit is not None:
                    result.timeit.summarise_string()

            if finalized_runs:
                self.store.set_run_ends(finalized_runs)

        # after persistence commits, notify any waiters:
        for finalized_run in finalized_runs:
            waiter_ids = set(
                self.run_wait_state.complete(finalized_run.run_id, RunWaitEvent.END)
            )
            # run-end implies run-start:
            waiter_ids.update(
                self.run_wait_state.complete(finalized_run.run_id, RunWaitEvent.START)
            )
            await self._notify_waiters(tuple(waiter_ids))

        return new_run_skips

    @TimeIt.decorator
    def _finalize_run(
        self,
        block_act_key: BlockActionKey,
        run: ElementActionRun,
        exit_code: int,
    ) -> tuple[FinalizedRun, dict[int, int]]:
        """Finalize a completed run and return its final state."""

        param_id: int | list[int] | None
        success = exit_code == 0

        self.logger.info(
            "Finalizing run %s with exit_code=%s, success=%s, "
            "skip=%r, and skip_reason=%r.",
            run.id_,
            exit_code,
            success,
            run.skip,
            run.skip_reason,
        )
        new_run_skips: dict[int, int] = {}
        run_dir = None
        if not run.skip:
            run_dir = run.get_directory()
            is_aborted = False

            if run.action.abortable and exit_code == ABORT_EXIT_CODE:
                # An abortable run is considered successful so that the workflow
                # can continue.
                is_aborted = True
                success = True

            if run_dir:
                assert isinstance(run_dir, Path)

                #
                # Input-file-generator files
                #

                for IFG_i in run.action.input_file_generators:
                    inp_file = IFG_i.input_file

                    self.logger.debug(
                        "Saving EAR input file %r for EAR ID %r.",
                        inp_file.label,
                        run.id_,
                    )

                    param_id = run.data_idx[f"input_files.{inp_file.label}"]

                    file_paths = inp_file.value(directory=run_dir)

                    for path_i in (
                        file_paths if isinstance(file_paths, list) else [file_paths]
                    ):
                        full_path = run_dir.joinpath(path_i)

                        if not full_path.exists():
                            self.logger.debug(
                                "Expected input file %r does not exist, so "
                                "setting run to an error state (if not aborted).",
                                path_i,
                            )

                            if not is_aborted and success:
                                success = False
                                exit_code = 1

                        else:
                            self.workflow._set_file(
                                param_id=param_id,
                                store_contents=True,
                                is_input=False,
                                path=full_path,
                            )

                #
                # Script-generated parameter data
                #

                if run.action.script_data_out_has_files:
                    self.logger.info("Saving script-generated parameters.")

                    try:
                        run._param_save(
                            "script",
                            block_act_key,
                            run_dir,
                        )
                    except FileNotFoundError:
                        self.logger.debug(
                            "Script did not generate an expected output "
                            "parameter file (block_act_key=%r), so setting "
                            "run to an error state (if not aborted).",
                            block_act_key,
                        )

                        if not is_aborted and success:
                            success = False
                            exit_code = 1

                #
                # Program-generated parameter data
                #

                if run.action.program_data_out_has_files:
                    self.logger.info("Saving program-generated parameters.")

                    try:
                        run._param_save(
                            "program",
                            block_act_key,
                            run_dir,
                        )
                    except FileNotFoundError:
                        self.logger.debug(
                            "Program did not generate an expected output "
                            "parameter file (block_act_key=%r), so setting "
                            "run to an error state (if not aborted).",
                            block_act_key,
                        )

                        if not is_aborted and success:
                            success = False
                            exit_code = 1

                #
                # Action-level save files
                #

                for save_file_j in run.action.save_files:
                    self.logger.debug(
                        "Saving file %r for EAR ID %r.",
                        save_file_j.label,
                        run.id_,
                    )

                    try:
                        param_id = run.data_idx[f"output_files.{save_file_j.label}"]
                    except KeyError:
                        # The file need not be a defined output-file parameter.
                        param_id = None

                    file_paths = save_file_j.value(directory=run_dir)

                    self.logger.debug(
                        "Saving output file paths: %r",
                        file_paths,
                    )

                    for path_i in (
                        file_paths if isinstance(file_paths, list) else [file_paths]
                    ):
                        full_path = run_dir.joinpath(path_i)

                        if not full_path.exists():
                            self.logger.debug(
                                "Expected file to save %r does not exist, so "
                                "setting run to an error state (if not aborted).",
                                path_i,
                            )

                            if not is_aborted and success:
                                success = False
                                exit_code = 1

                        else:
                            self.workflow._set_file(
                                param_id=param_id,
                                store_contents=True,
                                is_input=False,
                                path=full_path,
                                clean_up=(save_file_j in run.action.clean_up),
                            )

                #
                # Output-file-parser save files
                #

                for OFP_i in run.action.output_file_parsers:
                    self.logger.info(
                        "Saving files from OFP: %r.",
                        OFP_i,
                    )

                    for save_file_j in OFP_i._save_files:
                        self.logger.debug(
                            "Saving EAR output file %r for EAR ID %r.",
                            save_file_j.label,
                            run.id_,
                        )

                        try:
                            param_id = run.data_idx[f"output_files.{save_file_j.label}"]
                        except KeyError:
                            param_id = None

                        file_paths = save_file_j.value(directory=run_dir)

                        self.logger.debug(
                            "Saving EAR output file paths: %r",
                            file_paths,
                        )

                        for path_i in (
                            file_paths if isinstance(file_paths, list) else [file_paths]
                        ):
                            full_path = run_dir.joinpath(path_i)

                            if not full_path.exists():
                                self.logger.debug(
                                    "Expected output file parser "
                                    "`save_files` file %r does not exist, "
                                    "so setting run to an error state "
                                    "(if not aborted).",
                                    path_i,
                                )

                                if not is_aborted and success:
                                    success = False
                                    exit_code = 1

                            else:
                                self.workflow._set_file(
                                    param_id=param_id,
                                    store_contents=True,
                                    is_input=False,
                                    path=full_path,
                                    clean_up=(save_file_j in OFP_i.clean_up),
                                )

        else:
            self.logger.info(
                "Run was skipped: reason=%r.",
                run.skip_reason,
            )

        #
        # Propagate failure
        #

        if (
            run.resources.skip_downstream_on_failure
            and not success
            and run.skip_reason is not SkipReason.LOOP_TERMINATION
        ):
            for dependent_run_id in run.get_dependent_EARs(as_objects=False):
                self.logger.info(
                    "Setting EAR ID %r to skip because it depends on "
                    "EAR ID %r, which exited with exit code %r.",
                    dependent_run_id,
                    run.id_,
                    exit_code,
                )
                skip_reason = SkipReason.UPSTREAM_FAILURE.value
                self.store.set_EAR_skip({dependent_run_id: skip_reason})
                new_run_skips[dependent_run_id] = skip_reason

        #
        # Apply task conditions
        #

        new_run_skips.update(self._apply_task_conditions(run))

        return (
            FinalizedRun(
                run_id=run.id_,
                run_dir=run_dir,
                exit_code=exit_code,
                success=success,
            ),
            new_run_skips,
        )

    @TimeIt.decorator
    def _apply_task_conditions(self, run):
        """When a run has ended, check if any non-group task conditions are defined that
        depend on the run's parameters, and if so, skip any runs of those tasks if the
        condition is not met. Group task conditions cannot be evaluated until we start the
        run of the task where the condition is defined (see
        `_apply_group_task_conditions`)."""

        self.logger.info(
            f"checking workflow non-group task conditions for run {run.id_!r} with task "
            f"ID {run.task.insert_ID!r}."
        )

        conditions_not_met = []
        for task_ID_i, condition in self.workflow._get_task_conditions().items():
            if (
                not condition.group_name
                and condition.test(run=run, task_ID=task_ID_i) is False
            ):
                conditions_not_met.append(task_ID_i)

        # for each condition that is not met, skip all runs of the task that are dependent
        # on the current run:
        new_skips: dict[int, int] = {}
        for not_met_task_ID in conditions_not_met:
            dep_skips = {
                dep_id: SkipReason.TASK_CONDITION_NOT_MET
                for dep_id in run.get_dependent_EARs(task_insert_ID=not_met_task_ID)
            }
            self.workflow.set_EAR_skip(dep_skips)
            new_skips.update({k: v.value for k, v in dep_skips.items()})

        return new_skips

    @TimeIt.decorator
    def _apply_group_task_conditions(self, run) -> bool:
        """Before a run starts, check if any group task conditions are defined in the
        run's task, and if so, skip the run if the condition is not met.

        Non-group task conditions can be evaluated earlier (see
        `_apply_task_conditions`).

        Returns False if the run was set to skip, otherwise returns True.
        """

        self.logger.info(
            f"checking workflow group task conditions for run ID {run._id!r}"
        )

        for task_ID_i, condition in self.workflow._get_task_conditions().items():

            if condition.group_name:
                # get dependencies of this run that belong to the condition's source task:
                run_deps = [
                    dep
                    for dep in run.get_EAR_dependencies(as_objects=True)
                    if dep.task.insert_ID == condition.source_task_ID
                ]
                if not run_deps:
                    return True

                if condition.test_group(runs=run_deps, task_ID=task_ID_i) is False:
                    # skip this run:
                    self.workflow.set_EAR_skip(
                        {run.id_: SkipReason.TASK_CONDITION_NOT_MET}
                    )
                    return False
        return True

    @TimeIt.decorator
    def _check_loop_termination(self, run: ElementActionRun) -> set[int]:
        """Check if we need to terminate a loop if this is the last action of the loop
        iteration for this element, and set downstream iteration runs to skip."""

        elem_iter = run.element_iteration
        task = elem_iter.task
        check_loops = []
        to_skip = set()
        for loop_name in elem_iter.loop_idx:
            self.logger.info(f"checking loop termination of loop {loop_name!r}.")
            loop = self.workflow.loops.get(loop_name)
            if (
                loop.template.termination
                and task.insert_ID == loop.template.termination_task_insert_ID
                and run.element_action.action_idx == max(elem_iter.actions)
            ):
                check_loops.append(loop_name)
                # TODO: test with condition actions
                if loop.test_termination(elem_iter):
                    self.logger.info(
                        f"loop {loop_name!r} termination condition met for run "
                        f"ID {run.id_!r}."
                    )
                    to_skip.update(loop.skip_downstream_iterations(elem_iter))
        return to_skip

    @property
    def completion(self) -> JobscriptCompletion:
        return JobscriptCompletion(
            self.submissions_path,
            self.submission_idx,
            self.jobscript_idx,
            num_jobscripts=len(self.submission.jobscripts),
            array_indices=self.jobscript.array_indices,
        )

    @property
    def run_wait_state(self) -> RunWaitState:
        return RunWaitState(
            submissions_path=self.submissions_path,
            submission_idx=self.submission_idx,
            jobscript_idx=self.jobscript_idx,
        )

    @staticmethod
    def get_wait_endpoints_path(
        submissions_path: Path,
        submission_idx: int,
        jobscript_idx: int,
    ) -> Path:
        """Get the path to a directory containing files whose names are wait-endpoint IDs,
        and whose contents are the registered endpoints that should be notified upon
        jobscript completion."""
        return (
            submissions_path
            / str(submission_idx)
            / "js_state"
            / str(jobscript_idx)
            / "endpoints"
        )

    @property
    def wait_endpoints_path(self) -> Path:
        return self.get_wait_endpoints_path(
            self.submissions_path, self.submission_idx, self.jobscript_idx
        )

    def _get_wait_endpoints(self, waiter_ids: tuple[str, ...] | None = None) -> list[str]:
        if not self.wait_endpoints_path.exists():
            return []

        if waiter_ids is None:
            endpoint_paths = tuple(self.wait_endpoints_path.iterdir())
        else:
            endpoint_paths = tuple(
                self.wait_endpoints_path / waiter_id for waiter_id in waiter_ids
            )
        return [
            path.read_text(encoding="utf-8").strip()
            for path in endpoint_paths
            if path.is_file()
        ]

    async def _notify_waiters(self, waiter_ids: tuple[str, ...] | None = None) -> None:
        """Notify any registered wait-endpoints that an event has occurred."""

        if not (endpoints := self._get_wait_endpoints(waiter_ids)):
            return

        self.logger.debug("Notifying jobscript wait endpoints: %r", endpoints)
        notification = WaitWakeup(self.submission_idx, self.jobscript_idx)
        results = await asyncio.gather(
            *(WaitClient(endpoint).notify(notification) for endpoint in endpoints),
            return_exceptions=True,
        )
        for endpoint, result in zip(endpoints, results):
            if result is not True:
                self.logger.debug("Could not notify workflow waiter at %s", endpoint)
