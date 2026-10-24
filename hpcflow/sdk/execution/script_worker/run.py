"""Per-run control for an invocation in a reusable script worker."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping

from hpcflow.sdk.core import ABORT_EXIT_CODE
from hpcflow.sdk.execution.script_worker.client import ScriptWorkerClient
from hpcflow.sdk.execution.script_worker.protocol import ExecuteScriptRequest
from hpcflow.sdk.utils.async_utils import await_cleanup


class ScriptWorkerRun:
    """Expose synchronous abort signalling for one worker invocation."""

    def __init__(
        self,
        client: ScriptWorkerClient,
        request: ExecuteScriptRequest,
        environment: Mapping[str, str],
    ) -> None:
        """Bind a reusable client to one request and its environment overrides."""
        self.client = client
        self.request = request
        self.environment = environment
        self.return_code: int | None = None
        self.child_orchestration_times: list[float] = []
        self.child_work_times: list[float] = []
        self._abort_event = asyncio.Event()

    @property
    def pid(self) -> int | None:
        """Return the owned worker process PID, if it has started."""
        process = self.client.worker.process
        return process.pid if process is not None else None

    @property
    def is_running(self) -> bool:
        """Return whether this invocation still owns a live worker."""
        return self.return_code is None and self.client.worker.is_running

    def abort(self) -> None:
        """Signal the invocation owner without interrupting the control server."""
        self._abort_event.set()

    async def run(self) -> int:
        """Execute once, killing and reaping the worker on abort or cancellation."""
        if self._abort_event.is_set():
            self.return_code = ABORT_EXIT_CODE
            return self.return_code

        async def execute() -> int:
            if not self.client.worker.is_running:
                await self.client.start()
            result = await self.client.execute(self.request, environment=self.environment)
            self.child_orchestration_times.append(result.orchestration_time)
            self.child_work_times.append(result.work_time)
            return result.exit_code

        invocation = asyncio.create_task(execute())
        abort = asyncio.create_task(self._abort_event.wait())
        try:
            done, _ = await asyncio.wait(
                (invocation, abort), return_when=asyncio.FIRST_COMPLETED
            )
            if invocation in done:
                self.return_code = invocation.result()
            else:
                invocation.cancel()
                await await_cleanup(asyncio.gather(invocation, return_exceptions=True))
                self.return_code = ABORT_EXIT_CODE
        finally:

            async def cleanup() -> None:
                abort.cancel()
                if not invocation.done():
                    invocation.cancel()
                results = await asyncio.gather(invocation, abort, return_exceptions=True)
                if self.return_code is None or self.return_code == ABORT_EXIT_CODE:
                    await self.client.abort()
                error = results[0]
                if isinstance(error, BaseException) and not isinstance(
                    error, asyncio.CancelledError
                ):
                    raise error

            await await_cleanup(asyncio.create_task(cleanup()))
        return self.return_code
