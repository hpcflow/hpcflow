from __future__ import annotations

import asyncio

from hpcflow.sdk.core.app_aware import AppAware
from hpcflow.sdk.utils.async_utils import await_cleanup


class ScriptWorkerExecutor(AppAware):
    """Execute and control a persistent script worker process."""

    _TERMINATE_TIMEOUT = 5.0
    _KILL_TIMEOUT = 5.0

    def __init__(self, cmd: list[str], env: dict[str, str]) -> None:
        self.cmd = cmd
        self.env = env
        self.process: asyncio.subprocess.Process | None = None
        self._lifecycle_lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        return self.process is not None and self.process.returncode is None

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self.is_running:
                raise RuntimeError("Script worker is already running.")

            if self.process is not None:
                await await_cleanup(asyncio.create_task(self._shutdown(force=True)))

            spawn = asyncio.create_task(
                asyncio.create_subprocess_exec(*self.cmd, env=self.env)
            )
            try:
                self.process = await asyncio.shield(spawn)
            except asyncio.CancelledError:
                # Spawn may have created a child before returning its process handle.
                async def abort_start() -> None:
                    self.process = await spawn
                    await self._shutdown(force=True)

                await await_cleanup(asyncio.create_task(abort_start()))
                raise

    async def stop(self) -> None:
        """Terminate and reap the worker, escalating to kill after a bounded wait."""
        await await_cleanup(asyncio.create_task(self._shutdown_locked(force=False)))

    async def abort(self) -> None:
        """Kill and reap the worker before propagating cancellation."""
        await await_cleanup(asyncio.create_task(self._shutdown_locked(force=True)))

    async def _shutdown_locked(self, *, force: bool) -> None:
        async with self._lifecycle_lock:
            await self._shutdown(force=force)

    async def _shutdown(self, *, force: bool) -> None:
        if self.process is None:
            return

        process = self.process
        if not force and process.returncode is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass  # The child exited between the status check and signal.
            try:
                await asyncio.wait_for(process.wait(), timeout=self._TERMINATE_TIMEOUT)
            except asyncio.TimeoutError:
                pass  # Escalate to kill below.
            else:
                self.process = None
                return

        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass  # Still reap a child that exited before the signal.
        try:
            await asyncio.wait_for(process.wait(), timeout=self._KILL_TIMEOUT)
        except asyncio.TimeoutError as exc:
            raise asyncio.TimeoutError(
                f"Script worker PID {process.pid} did not exit after kill."
            ) from exc
        self.process = None
