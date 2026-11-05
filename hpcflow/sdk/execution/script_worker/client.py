"""Single-flight, process-aware client for an isolated script worker."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Mapping, TypeVar

import zmq
import zmq.asyncio

from hpcflow.sdk.execution.script_worker.protocol import (
    ExecuteScriptRequest,
    InvalidScriptWorkerMessageError,
    ScriptExecutionResult,
    ScriptWorkerError,
    ScriptWorkerPing,
    ScriptWorkerReady,
    ScriptWorkerRequest,
    ScriptWorkerResponse,
    ScriptWorkerShutdownAck,
    ShutdownScriptWorker,
)
from hpcflow.sdk.execution.script_worker.executor import ScriptWorkerExecutor
from hpcflow.sdk.execution.script_worker.service import SCRIPT_WORKER_ENDPOINT_ENV
from hpcflow.sdk.utils.async_utils import (
    await_cleanup,
    recv_json,
    send_json,
    wait_for_cancellable,
)

ResponseT = TypeVar("ResponseT", bound=ScriptWorkerResponse)


class ScriptWorkerExitedError(RuntimeError):
    """Raised when the worker exits before completing the expected exchange."""

    pass


class ScriptWorkerRejectedError(ValueError):
    """The worker rejected a request without executing its handler."""


def _validate_result_identity(
    request: ScriptWorkerRequest, response: ScriptWorkerResponse
) -> None:
    """Ensure a result response echoes the complete execution request identity."""
    if isinstance(request, ExecuteScriptRequest) and isinstance(
        response, ScriptExecutionResult
    ):
        if (
            response.run_id,
            response.block_idx,
            response.action_idx,
            response.element_idx,
        ) != (request.run_id, request.block_idx, request.action_idx, request.element_idx):
            raise InvalidScriptWorkerMessageError(
                "Script-worker result identity does not match the request."
            )


class ScriptWorkerClient:
    """Own one private transport and its worker process, with no automatic retries."""

    def __init__(
        self,
        cmd: list[str],
        env: dict[str, str],
        *,
        startup_timeout: float = 10.0,
        shutdown_timeout: float = 5.0,
        cwd: Path | None = None,
    ) -> None:
        """Create a client and process owner for one worker process."""
        self.worker = ScriptWorkerExecutor(cmd, dict(env), cwd=cwd)
        self.startup_timeout = startup_timeout
        self.shutdown_timeout = shutdown_timeout
        self.endpoint: str | None = None
        self._socket: zmq.asyncio.Socket | None = None
        self._request_lock = asyncio.Lock()

    async def start(self) -> None:
        """Bind private loopback transport, start the child, and await readiness."""
        async with self._request_lock:
            if self._socket is not None or self.worker.is_running:
                raise RuntimeError("Script worker client is already started.")
            socket = zmq.asyncio.Context.instance().socket(zmq.REQ)
            self._socket = socket
            socket.setsockopt(zmq.LINGER, 0)
            socket.setsockopt(zmq.IMMEDIATE, 1)
            try:
                deadline = asyncio.get_running_loop().time() + self.startup_timeout
                port = socket.bind_to_random_port("tcp://127.0.0.1")
                self.endpoint = f"tcp://127.0.0.1:{port}"
                self.worker.env[SCRIPT_WORKER_ENDPOINT_ENV] = self.endpoint
                await wait_for_cancellable(
                    self.worker.start(), timeout=self.startup_timeout
                )
                await self._request(
                    ScriptWorkerPing(),
                    ScriptWorkerReady,
                    timeout=max(0.0, deadline - asyncio.get_running_loop().time()),
                )
            except (Exception, asyncio.CancelledError):
                await self.abort()
                raise

    async def execute(
        self,
        request: ExecuteScriptRequest,
        *,
        environment: Mapping[str, str] | None = None,
    ) -> ScriptExecutionResult:
        """Send per-run environment overrides and wait without an implicit RPC deadline."""
        async with self._request_lock:
            if environment is not None:
                request = ExecuteScriptRequest(
                    request.run_id,
                    request.block_idx,
                    request.action_idx,
                    request.element_idx,
                    environment,
                )
            return await self._request(request, ScriptExecutionResult)

    async def stop(self) -> None:
        """Request idle shutdown; abort an occupied exchange instead of hanging."""
        if self._request_lock.locked():
            await self.abort()
            return
        async with self._request_lock:
            if self._socket is None:
                await self.abort()
                return
            try:
                await self._request(
                    ShutdownScriptWorker(),
                    ScriptWorkerShutdownAck,
                    timeout=self.shutdown_timeout,
                )
                process = self.worker.process
                if process is not None:
                    await wait_for_cancellable(
                        process.wait(), timeout=self.shutdown_timeout
                    )
                    if process.returncode != 0:
                        raise ScriptWorkerExitedError(
                            f"Script worker PID {process.pid} exited with code "
                            f"{process.returncode} after shutdown acknowledgement."
                        )
                await self.worker.stop()
            except (Exception, asyncio.CancelledError):
                await self.abort()
                raise
            finally:
                self._close_socket()

    async def abort(self) -> None:
        """Close transport and forcibly stop/reap the worker process."""
        await await_cleanup(asyncio.create_task(self._abort()))

    async def _abort(self) -> None:
        """Perform the transport and process cleanup used by abort."""
        self._close_socket()
        await self.worker.abort()

    def _close_socket(self) -> None:
        """Close the current socket and clear its endpoint."""
        if self._socket is not None:
            self._socket.close(linger=0)
            self._socket = None
        self.endpoint = None

    async def _request(
        self,
        request: ScriptWorkerRequest,
        expected: type[ResponseT],
        *,
        timeout: float | None = None,
    ) -> ResponseT:
        """Exchange one request while also observing process exit and timeouts."""
        socket = self._socket
        process = self.worker.process
        if socket is None or process is None:
            raise RuntimeError("Script worker client is not ready.")

        async def exchange() -> ScriptWorkerResponse:
            await send_json(socket, request.to_dict())
            try:
                data = await recv_json(socket)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise InvalidScriptWorkerMessageError(
                    "Script-worker response is not valid JSON."
                ) from exc
            return ScriptWorkerResponse.from_dict(data)

        exchange_task = asyncio.create_task(exchange())
        exit_task = asyncio.create_task(process.wait())
        try:
            done, _ = await asyncio.wait(
                (exchange_task, exit_task),
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                raise asyncio.TimeoutError(
                    f"Script-worker {request.command} timed out after {timeout}s."
                )
            if exchange_task not in done:
                # A final reply may already be in transit when the child exits.
                try:
                    await asyncio.wait_for(asyncio.shield(exchange_task), timeout=0.1)
                except asyncio.TimeoutError as exc:
                    raise ScriptWorkerExitedError(
                        f"Script worker PID {process.pid} exited with code "
                        f"{process.returncode} before replying."
                    ) from exc
            response = exchange_task.result()
            if isinstance(response, ScriptWorkerError):
                raise ScriptWorkerRejectedError(response.error)
            if not isinstance(response, expected):
                raise InvalidScriptWorkerMessageError(
                    f"Expected {expected.__name__}, received {type(response).__name__}."
                )
            _validate_result_identity(request, response)
            return response
        except ScriptWorkerRejectedError:
            raise
        except (Exception, asyncio.CancelledError):
            await self.abort()
            raise
        finally:
            for task in (exchange_task, exit_task):
                if not task.done():
                    task.cancel()
            try:
                await await_cleanup(
                    asyncio.gather(exchange_task, exit_task, return_exceptions=True)
                )
            except asyncio.CancelledError:
                await self.abort()
                raise
