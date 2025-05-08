"""
A direct job "scheduler" that just runs immediate subprocesses.
"""

from __future__ import annotations
import shutil
import signal
from typing import overload, cast, TYPE_CHECKING
from typing_extensions import override, TypeAlias
import psutil

from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.submission.enums import JobscriptElementState
from hpcflow.sdk.submission.schedulers import Scheduler

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from typing import Any, ClassVar
    from ...config.types import SchedulerConfigDescriptor
    from ..jobscript import Jobscript
    from ..shells.base import Shell

DirectRef: TypeAlias = "tuple[int, list[str]]"


def _is_process_cmdline_equal(proc: psutil.Process, cmdline: list[str]) -> bool:
    """Check if the `cmdline` of a psutil `Process` is equal to the specified
    `cmdline`."""
    try:
        if proc.cmdline() == cmdline:
            return True
        else:
            return False
    except (psutil.NoSuchProcess, psutil.ZombieProcess):
        # process no longer exists or, on unix, process has completed but still has a
        # record
        return False


class DirectScheduler(Scheduler[DirectRef]):
    """
    A direct scheduler, that just runs jobs immediately as direct subprocesses.

    The correct subclass (:py:class:`DirectPosix` or :py:class:`DirectWindows`) should
    be used to create actual instances.

    """

    @classmethod
    @override
    def process_resources(
        cls, resources, scheduler_config: SchedulerConfigDescriptor
    ) -> None:
        """Perform scheduler-specific processing to the element resources.

        Note
        ----
        This mutates `resources`.
        """
        return

    @override
    def get_submit_command(
        self,
        shell: Shell,
        js_path: str,
        deps: dict[Any, tuple[Any, ...]],
    ) -> list[str]:
        """
        Get the concrete submission command.
        """
        return shell.get_direct_submit_command(js_path)

    @staticmethod
    def __kill_processes(
        procs: list[psutil.Process],
        sig: signal.Signals = signal.SIGTERM,
        timeout: float | None = None,
        on_terminate: Callable[[psutil.Process], object] | None = None,
    ):
        all_procs: list[psutil.Process] = []
        for process in procs:
            all_procs.append(process)
            all_procs.extend(process.children(recursive=True))

        for process in all_procs:
            try:
                process.send_signal(sig)
            except psutil.NoSuchProcess:
                pass
        _, alive = psutil.wait_procs(all_procs, timeout=timeout, callback=on_terminate)
        for process in alive:
            process.kill()

    @staticmethod
    def __get_jobscript_processes(js_refs: list[DirectRef]) -> list[psutil.Process]:
        procs: list[psutil.Process] = []
        for p_id, p_cmdline in js_refs:
            try:
                proc_i = psutil.Process(p_id)
            except psutil.NoSuchProcess:
                # process might have completed already
                continue
            if _is_process_cmdline_equal(proc_i, p_cmdline):
                procs.append(proc_i)
        return procs

    @overload
    @override
    @classmethod
    def wait_for_jobscripts(cls, js_refs: list[DirectRef]) -> None: ...

    @overload
    @classmethod
    def wait_for_jobscripts(
        cls,
        js_refs: list[DirectRef],
        *,
        callback: Callable[[psutil.Process], None],
    ) -> list[psutil.Process]: ...

    @classmethod
    def wait_for_jobscripts(
        cls,
        js_refs: list[DirectRef],
        *,
        callback: Callable[[psutil.Process], None] | None = None,
    ) -> list[psutil.Process] | None:
        """Wait until the specified jobscripts have completed."""
        procs = cls.__get_jobscript_processes(js_refs)
        (gone, alive) = psutil.wait_procs(procs, callback=callback)
        assert not alive
        return gone if callback else None

    @override
    def get_job_state_info(
        self, *, js_refs: Sequence[DirectRef] | None = None
    ) -> Mapping[str, JobscriptElementState]:
        """Query the scheduler to get the states of all of this user's jobs, optionally
        filtering by specified job IDs.

        Jobs that are not in the scheduler's status output will not appear in the output
        of this method."""
        info: dict[str, JobscriptElementState] = {}
        for p_id, p_cmdline in js_refs or ():
            if self.is_jobscript_active(p_id, p_cmdline):
                # as far as the "scheduler" is concerned, all elements are running:
                info[str(p_id)] = JobscriptElementState.running

        return info

    @override
    def cancel_jobs(
        self,
        js_refs: list[DirectRef],
        jobscripts: list[Jobscript] | None = None,
    ):
        """
        Cancel some jobs.
        """

        js_proc_id: dict[int, Jobscript]

        def callback(proc: psutil.Process):
            try:
                js_proc_id[proc.pid]
            except KeyError:
                # child process of one of the jobscripts
                self._app.submission_logger.debug(
                    f"jobscript child process ({proc.pid}) killed"
                )
                return

        procs = self.__get_jobscript_processes(js_refs)
        self._app.submission_logger.info(
            f"cancelling {self.__class__.__name__} jobscript processes: {procs}."
        )
        js_proc_id = {i.pid: jobscripts[idx] for idx, i in enumerate(procs) if jobscripts}
        self.__kill_processes(procs, timeout=3, on_terminate=callback)
        print(f"Cancelled {len(procs)} jobscript{'s' if len(procs) > 1 else ''}.")
        self._app.submission_logger.info("jobscripts cancel command executed.")

    def is_jobscript_active(self, process_ID: int, process_cmdline: list[str]):
        """Query if a jobscript is running.

        Note that a "running" jobscript might be waiting on upstream jobscripts to
        complete.

        """
        try:
            proc = psutil.Process(process_ID)
        except psutil.NoSuchProcess:
            return False
        return _is_process_cmdline_equal(proc, process_cmdline)

    def get_std_out_err_filename(self, js_idx: int, **kwargs) -> str:
        """File name of combined standard output and error streams."""
        return f"js_{js_idx}_std.log"

    def get_stdout_filename(self, js_idx: int, **kwargs) -> str:
        """File name of the standard output stream file."""
        return f"js_{js_idx}_stdout.log"

    def get_stderr_filename(self, js_idx: int, **kwargs) -> str:
        """File name of the standard error stream file."""
        return f"js_{js_idx}_stderr.log"


@hydrate
class DirectPosix(DirectScheduler):
    """
    A direct scheduler for POSIX systems.

    """


@hydrate
class DirectWindows(DirectScheduler):
    """
    A direct scheduler for Windows.

    """

    @override
    def get_submit_command(
        self, shell: Shell, js_path: str, deps: dict[Any, tuple[Any, ...]]
    ) -> list[str]:
        cmd = super().get_submit_command(shell, js_path, deps)
        # `Start-Process` (see `Jobscript._launch_direct_js_win`) seems to resolve the
        # executable, which means the process's `cmdline` might look different to what we
        # record; so let's resolve it ourselves:
        cmd[0] = cast("str", shutil.which(cmd[0]))
        return cmd
