from __future__ import annotations

import asyncio
import os
import struct
from typing import TYPE_CHECKING


from hpcflow.sdk.core import ABORT_EXIT_CODE
from hpcflow.sdk.core.app_aware import AppAware

if TYPE_CHECKING:
    from asyncio.subprocess import Process


class RunExecutor(AppAware):
    """Execute and control a single subprocess.

    A RunExecutor has the lifetime of one run command subprocess (currently; this will
    change).

    External control requests are routed to this object by JobscriptServer/
    JobscriptExecutor.

    The RunExecutor class:
    - owns one subprocess
    - starts/waits for that subprocess
    - accepts abort signals
    - terminates the subprocess, if required
    - records child timing messages
    - does NOT know about ZMQ/jobscript sequencing

    """

    def __init__(
        self,
        cmd: list[str],
        env: dict[str, str],
    ):
        self.cmd = cmd
        self.env = env

        self.process: Process | None = None
        self.return_code: int | None = None

        self._abort_event = asyncio.Event()

        self.child_orchestration_times: list[float] = []
        self.child_work_times: list[float] = []

    @property
    def logger(self):
        return self._app.execution_logger.getChild("executor")

    @property
    def pid(self) -> int | None:
        return None if self.process is None else self.process.pid

    @property
    def is_running(self) -> bool:
        return self.process is not None and self.return_code is None

    def abort(self) -> None:
        """Request that the running subprocess be aborted.

        This method does not itself kill the subprocess. It signals the asynchronous
        runner, which owns subprocess termination.
        """
        self._abort_event.set()

    async def run(self) -> int:

        self.logger.debug(f"starting subprocess: {self.cmd!r}")
        self.process = await asyncio.create_subprocess_exec(*self.cmd, env=self.env)
        self.logger.debug(f"subprocess created pid={self.process.pid}")

        process_task = asyncio.create_task(self.process.wait())
        abort_task = asyncio.create_task(self._abort_event.wait())

        self.logger.debug("waiting for process OR abort")
        try:
            done, pending = await asyncio.wait(
                {process_task, abort_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            self.logger.debug(
                f"wait finished; process_done={process_task.done()}, "
                f"abort_done={abort_task.done()}"
            )
            if process_task in done:
                self.return_code = self._normalise_return_code(process_task.result())
                self.logger.debug(f"process returned {self.return_code}")

            else:
                self.logger.debug("abort requested")
                await self._terminate_process()
                self.return_code = ABORT_EXIT_CODE

        finally:
            for task in (process_task, abort_task):
                if not task.done():
                    task.cancel()

            await asyncio.gather(process_task, abort_task, return_exceptions=True)

        self.logger.debug("finished")

        assert self.return_code is not None
        return self.return_code

    async def _terminate_process(self) -> None:
        """Terminate the subprocess and wait for it to exit.

        This currently kills only the process created directly by Executor. Process-tree
        termination should eventually be implemented here.
        """

        if self.process is None:
            return

        if self.process.returncode is not None:
            return

        self.logger.debug(f"killing subprocess PID {self.process.pid}.")
        self.process.kill()

        await self.process.wait()

    @staticmethod
    def _normalise_return_code(return_code: int) -> int:
        """Normalise platform-specific subprocess return codes."""

        if return_code and os.name == "nt":
            # Windows return codes are defined as unsigned 32-bit integers. Convert values
            # representing negative exit codes back to their signed representation.
            return struct.unpack("i", struct.pack("I", return_code))[0]

        return return_code

    @classmethod
    def _from_noop_cmd(
        cls,
        *args: str,
        env: dict[str, str] | None = None,
    ):
        """For testing only: generate a run executor that executes the app's internal
        noop command, which takes various arguments."""
        cmd = [
            *cls._app.run_time_info.invocation_command,
            "internal",
            "noop",
            *args,
        ]
        return cls(
            cmd=cmd,
            env=env or os.environ.copy(),
        )
