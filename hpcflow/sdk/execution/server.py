from __future__ import annotations

import asyncio
import os
from typing import Any, TYPE_CHECKING
import warnings

import zmq
import zmq.asyncio

from hpcflow.sdk.core.app_aware import AppAware
from hpcflow.sdk.execution.protocol import (
    JobscriptRequest,
    PingRequest,
    StatusRequest,
    AbortRequest,
    TimeItRequest,
    ErrorResponse,
    InvalidRequestError,
    JobscriptResponse,
    OkResponse,
    RunStatus,
    StatusResponse,
)
from hpcflow.sdk.utils.async_utils import recv_json

if TYPE_CHECKING:
    from hpcflow.sdk.execution.jobscript_executor import JobscriptExecutor


class JobscriptServer(AppAware):
    """ZeroMQ control server for one executing jobscript (or jobscript-element for a
    scheduled array job).

    The server has the lifetime of execute_jobscript().

    WorkflowExecution owns the active Executor instances; this server routes incoming
    requests to them.

    This class:
    - owns a ZMQ socket
    - owns the control protocol
    - translates JSON to typed requests
    - routes requests to active Executors
    - returns types responses
    - does NOT execute runs

    """

    def __init__(
        self,
        executor: JobscriptExecutor,
        *,
        bind_address: str = "tcp://*",
    ):
        self.execution = executor
        self.bind_address = bind_address

        self.context = zmq.asyncio.Context.instance()
        self.socket: zmq.asyncio.Socket | None = None
        self._run_task: asyncio.Task[None] | None = None

        self.port_number: int | None = None

    @property
    def logger(self):
        return self.execution._app.execution_logger.getChild("server")

    async def start(self) -> int:
        if self.socket is not None:
            raise RuntimeError("JobscriptServer is already running")

        self.socket = self.context.socket(zmq.REP)
        self.port_number = self.socket.bind_to_random_port(self.bind_address)
        self._run_task = asyncio.create_task(self.run())
        self.logger.info(f"JobscriptServer started on port {self.port_number}")

        return self.port_number

    async def stop(self) -> None:
        if self._run_task is None:
            return

        self._run_task.cancel()

        try:
            await self._run_task
        except asyncio.CancelledError:
            pass
        finally:
            self._run_task = None
            self.port_number = None

        self.logger.info("JobscriptServer stopped")

    async def run(self) -> None:
        if self.socket is None:
            raise RuntimeError("JobscriptServer.start() must be called first")

        try:
            while True:
                data = await recv_json(self.socket)
                try:
                    request = JobscriptRequest.from_dict(data)
                    response = self._handle_request(request)

                except InvalidRequestError as exc:
                    response = ErrorResponse(error=str(exc))

                except Exception:
                    self.logger.exception("Error handling jobscript control request.")
                    response = ErrorResponse(error="Internal server error")

                await self.socket.send_json(response.to_dict())

        except asyncio.CancelledError:
            raise

        finally:
            self.socket.close(linger=0)
            self.socket = None
            self.port_number = None

    def _handle_request(self, request: JobscriptRequest) -> JobscriptResponse:
        """Handle a single control request."""

        if isinstance(request, PingRequest):
            return self._handle_ping(request)

        if isinstance(request, StatusRequest):
            return self._handle_status(request)

        if isinstance(request, AbortRequest):
            return self._handle_abort(request)

        if isinstance(request, TimeItRequest):
            return self._handle_timeit(request)

        raise TypeError(f"Unsupported request type {type(request)!r}.")

    def _handle_ping(self, request: PingRequest) -> OkResponse:
        return OkResponse()

    def _handle_status(self, request: StatusRequest) -> StatusResponse:
        return StatusResponse(
            runs=[
                RunStatus(
                    run_id=run_id,
                    pid=executor.pid,
                    running=executor.is_running,
                )
                for run_id, executor in self.execution.active_executors.items()
            ]
        )

    def _handle_abort(self, request: AbortRequest) -> JobscriptResponse:
        executor = self.execution.active_executors.get(request.run_id)

        if executor is None:
            return ErrorResponse(error=f"Run {request.run_id!r} is not active.")

        executor.abort()
        return OkResponse()

    def _handle_timeit(self, request: TimeItRequest) -> JobscriptResponse:
        """Receive timing information from a child process."""

        executor = self.execution.active_executors.get(request.run_id)

        if executor is None:
            return ErrorResponse(error=f"Run {request.run_id!r} is not active.")

        if request.orchestration_time is not None:
            executor.child_orchestration_times.append(request.orchestration_time)

        if request.work_time is not None:
            executor.child_work_times.append(request.work_time)

        return OkResponse()
