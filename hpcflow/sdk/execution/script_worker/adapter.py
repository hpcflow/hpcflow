"""Off-production adapter between a precomposed callable and one workflow run."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import ExitStack, redirect_stderr, redirect_stdout
import contextvars
import os
from pathlib import Path
import time
import traceback
from typing import Any, TYPE_CHECKING

from hpcflow.sdk.execution.script_worker.protocol import (
    ExecuteScriptRequest,
    ScriptExecutionResult,
)
from hpcflow.sdk.execution.script_worker.service import ScriptWorkerServiceError
from hpcflow.sdk.instrumentation import TimeIt
from hpcflow.sdk.utils.deferred_file import DeferredFileWriter

if TYPE_CHECKING:
    from hpcflow.sdk.core.actions import ElementActionRun


class ScriptWorkerInvocation:
    """Bind a locally selected callable and explicit context to one registered run.

    This adapter does not prepare directories/files, orchestrate runs, finalize
    files, or discover a callable from a wire message.
    """

    def __init__(
        self,
        request: ExecuteScriptRequest,
        run: ElementActionRun,
        function: Callable[..., Any],
        *,
        environment: Mapping[str, str],
        std_path: Path,
        log_path: Path | None = None,
    ) -> None:
        """Bind one protocol identity, persisted run, callable, and execution context."""
        if request.run_id != run.id_:
            raise ValueError("Script-worker request does not match the bound run.")
        if not run.action.script_is_python_snippet or not run.is_snippet_script:
            raise ValueError("Script-worker adapter requires a Python snippet run.")
        if run.jobscript_idx is None:
            raise ValueError("Script-worker run must have an assigned jobscript.")
        if run.submission_idx is None:
            raise ValueError("Script-worker run must have an assigned submission.")
        jobscript = run.workflow.submissions[run.submission_idx].jobscripts[
            run.jobscript_idx
        ]
        try:
            block = jobscript.blocks[request.block_idx]
            run_id = block.EAR_ID[request.action_idx, request.element_idx]
        except IndexError as exc:
            raise ValueError(
                "Script-worker request is outside the run's jobscript."
            ) from exc
        if run_id != run.id_:
            raise ValueError("Script-worker block/action/element does not match the run.")
        self.request = request
        self.run = run
        self.function = function
        self.environment = dict(environment)
        self.std_path = std_path.resolve()
        self.log_path = log_path.resolve() if log_path is not None else None

    def __call__(
        self, environment: Mapping[str, str] | None = None
    ) -> ScriptExecutionResult:
        """Apply per-run overrides to the worker baseline in an isolated context."""
        # Isolate ContextVar changes, including app timing activation, per invocation.
        return contextvars.copy_context().run(
            self._execute,
            (
                self.environment
                if environment is None
                else {**self.environment, **environment}
            ),
        )

    def _execute(self, environment: Mapping[str, str]) -> ScriptExecutionResult:
        """Run the callable, persist direct outputs, and restore process context."""
        start = time.perf_counter()
        work_time = 0.0
        run = self.run
        workflow = run.workflow
        app = run._app
        jobscript_idx = run.jobscript_idx
        assert jobscript_idx is not None
        block_key = (jobscript_idx, self.request.block_idx, self.request.action_idx)
        cwd = Path.cwd()
        process_environment = dict(os.environ)
        run_environment = dict(environment)

        def restore_process() -> None:
            try:
                os.chdir(cwd)
            finally:
                os.environ.clear()
                os.environ.update(process_environment)

        try:
            with ExitStack() as stack:
                stack.callback(restore_process)
                os.environ.clear()
                os.environ.update(run_environment)
                directory = run.get_directory()
                if run.action.requires_dir:
                    if directory is None:
                        raise RuntimeError(f"No prepared directory for run {run.id_}.")
                    os.chdir(directory)
                stack.enter_context(app.log.temporary_file_logger(self.log_path))
                stream = stack.enter_context(
                    DeferredFileWriter(
                        self.std_path, mode="a", preamble=run.get_run_std_preamble()
                    )
                )
                stack.enter_context(redirect_stdout(stream))
                stack.enter_context(redirect_stderr(stream))
                stack.enter_context(TimeIt(title="Script-worker invocation").activate())

                exit_code = 0
                try:
                    with (
                        run.raise_on_failure_threshold(),
                        workflow._store.cache_ctx(),
                        workflow._store.parameters_metadata_cache(),
                        workflow._store.parameters_array_cache(),
                    ):
                        kwargs = run.get_py_script_func_kwargs(
                            raise_on_unset=False,
                            add_script_files=True,
                            blk_act_key=block_key,
                        )
                    work_start = time.perf_counter()
                    try:
                        output = self.function(**kwargs)
                    finally:
                        work_time = time.perf_counter() - work_start

                    outputs: Mapping[str, Any]
                    if run.action.is_OFP:
                        parameter = run.action.output_file_parsers[0].output
                        if parameter is None:
                            raise RuntimeError("Output parser has no declared output.")
                        outputs = {parameter.typ: output}
                    elif run.action.script_data_out_has_direct:
                        if not isinstance(output, Mapping):
                            raise TypeError("Direct script outputs must be a mapping.")
                        outputs = output
                    else:
                        outputs = {}
                    with workflow._store.parameters_metadata_cache():
                        for name, value in outputs.items():
                            workflow.save_parameter(
                                name=f"outputs.{name}", value=value, EAR_ID=run.id_
                            )
                except Exception:
                    app.logger.exception(
                        "Script-worker invocation failed: %r", self.request
                    )
                    traceback.print_exc()
                    exit_code = 1
        except Exception as exc:
            # Setup/teardown failures cannot safely leave this interpreter reusable.
            raise ScriptWorkerServiceError(
                f"Could not establish or restore context for run {run.id_}."
            ) from exc

        return ScriptExecutionResult(
            run_id=self.request.run_id,
            block_idx=self.request.block_idx,
            action_idx=self.request.action_idx,
            element_idx=self.request.element_idx,
            exit_code=exit_code,
            work_time=work_time,
            orchestration_time=max(0.0, time.perf_counter() - start - work_time),
        )
