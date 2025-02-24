from pathlib import Path
import shutil
import signal
from typing import Callable, Dict, List, Optional, Tuple

import psutil
from hpcflow.sdk.submission.jobscript_info import JobscriptElementState

from hpcflow.sdk.submission.schedulers import NullScheduler
from hpcflow.sdk.submission.shells.base import Shell


class DirectScheduler(NullScheduler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def process_resources(cls, resources, scheduler_config: Dict) -> None:
        """Perform scheduler-specific processing to the element resources.

        Note: this mutates `resources`.

        """
        return

    def get_submit_command(
        self,
        shell: Shell,
        js_path: str,
        deps: List[Tuple],
    ) -> List[str]:
        return shell.get_direct_submit_command(js_path)

    @staticmethod
    def _kill_processes(
        procs: List[psutil.Process],
        sig=signal.SIGTERM,
        timeout=None,
        on_terminate=None,
    ):
        all_procs = []
        for i in procs:
            all_procs.append(i)
            all_procs.extend(i.children(recursive=True))

        for i in all_procs:
            try:
                i.send_signal(sig)
            except psutil.NoSuchProcess:
                pass
        gone, alive = psutil.wait_procs(all_procs, timeout=timeout, callback=on_terminate)
        for p in alive:
            p.kill()

    @staticmethod
    def _get_jobscript_processes(js_refs: List[Tuple[int, List[str]]]):
        procs = []
        for p_id, p_cmdline in js_refs:
            try:
                proc_i = psutil.Process(p_id)
            except psutil.NoSuchProcess:
                # process might have completed already
                continue
            try:
                if proc_i.cmdline() == p_cmdline:
                    # additional check this is the same process that we submitted
                    procs.append(proc_i)
            except psutil.ZombieProcess:
                # on unix: process has completed but still has a record
                continue
        return procs

    @classmethod
    def wait_for_jobscripts(
        cls,
        js_refs: List[Tuple[int, List[str]]],
        callback: Optional[Callable] = None,
    ) -> None:
        """Wait until the specified jobscripts have completed."""
        procs = cls._get_jobscript_processes(js_refs)
        (gone, alive) = psutil.wait_procs(procs, callback=callback)
        assert not alive
        return gone

    def get_job_state_info(
        self,
        js_refs: List[Tuple[int, List[str]]],
    ) -> Dict[int, JobscriptElementState]:
        """Return a running status for jobscript references that are currently active."""
        info = {}
        for p_id, p_cmdline in js_refs:
            is_active = self.is_jobscript_active(p_id, p_cmdline)
            if is_active:
                # as far as the "scheduler" is concerned, all elements of all blocks are
                # running:
                info[p_id] = JobscriptElementState.running

        return info

    def cancel_jobs(
        self,
        js_refs: List[Tuple[int, List[str]]],
        jobscripts: List = None,
    ):
        def callback(proc):
            try:
                js = js_proc_id[proc.pid]
            except KeyError:
                # child process of one of the jobscripts
                self.app.submission_logger.debug(
                    f"jobscript child process ({proc.pid}) killed"
                )
                return

        procs = self._get_jobscript_processes(js_refs)
        self.app.submission_logger.info(
            f"cancelling {self.__class__.__name__} jobscript processes: {procs}."
        )
        js_proc_id = {i.pid: jobscripts[idx] for idx, i in enumerate(procs)}
        self._kill_processes(procs, timeout=3, on_terminate=callback)
        print(f"Cancelled {len(procs)} jobscript{'s' if len(procs) > 1 else ''}.")
        self.app.submission_logger.info(f"jobscripts cancel command executed.")

    def is_jobscript_active(self, process_ID: int, process_cmdline: List[str]):
        """Query if a jobscript is running.

        Note that a "running" jobscript might be waiting on upstream jobscripts to
        complete.

        """
        try:
            proc = psutil.Process(process_ID)
        except psutil.NoSuchProcess:
            return False

        if proc.cmdline() == process_cmdline:
            return True
        else:
            return False

    def get_std_out_err_filename(
        self, js_idx: int, job_ID: None = None, array_idx: None = None
    ):
        """File name of combined standard output and error streams."""
        return f"js_{js_idx}_std.log"

    def get_stdout_filename(
        self, js_idx: int, job_ID: None = None, array_idx: None = None
    ):
        return f"js_{js_idx}_stdout.log"

    def get_stderr_filename(
        self, js_idx: int, job_ID: None = None, array_idx: None = None
    ):
        return f"js_{js_idx}_stderr.log"


class DirectPosix(DirectScheduler):
    _app_attr = "app"
    DEFAULT_SHELL_EXECUTABLE = "/bin/bash"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DirectWindows(DirectScheduler):
    _app_attr = "app"
    DEFAULT_SHELL_EXECUTABLE = "powershell.exe"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_submit_command(
        self, shell: Shell, js_path: str, deps: List[Tuple]
    ) -> List[str]:
        cmd = super().get_submit_command(shell, js_path, deps)
        # `Start-Process` (see `Jobscript._launch_direct_js_win`) seems to resolve the
        # executable, which means the process's `cmdline` might look different to what we
        # record; so let's resolve it ourselves:
        cmd[0] = shutil.which(cmd[0])
        return cmd
