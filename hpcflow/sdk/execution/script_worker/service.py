"""Isolated service for explicitly registered script-worker invocations."""

from __future__ import annotations

from collections.abc import Callable, Mapping
import json
import logging
import time

import zmq

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

SCRIPT_WORKER_ENDPOINT_ENV = "HPCFLOW_SCRIPT_WORKER_ENDPOINT"


class ScriptWorkerServiceError(RuntimeError):
    """Signal a fatal service or context failure that forbids worker reuse."""


class ScriptWorkerService:
    """Serve pre-registered synchronous invocations, without workflow orchestration.

    Handlers own their input/output serialization and per-run context. Production
    adapters are deliberately not provided yet.
    """

    def __init__(
        self,
        endpoint: str,
        invocations: Mapping[
            ExecuteScriptRequest, Callable[..., int | ScriptExecutionResult]
        ],
    ) -> None:
        """Create a synchronous service with an explicit request-to-handler map."""
        self.endpoint = endpoint
        self.invocations = dict(invocations)
        self.logger = logging.getLogger(__name__)

    def run(self) -> None:
        """Connect to the client endpoint and serve requests until shutdown."""
        # A synchronous REP socket lets handlers run on the worker's main thread.
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.setsockopt(zmq.LINGER, 1000)
        try:
            socket.connect(self.endpoint)
            while True:
                request = None
                try:
                    data = socket.recv_json()
                    request = ScriptWorkerRequest.from_dict(data)
                except (
                    json.JSONDecodeError,
                    UnicodeDecodeError,
                    InvalidScriptWorkerMessageError,
                ) as exc:
                    self.logger.warning("Rejected script-worker request: %s", exc)
                    response: ScriptWorkerResponse = ScriptWorkerError(str(exc))
                else:
                    response = self._handle(request)
                socket.send_json(response.to_dict())
                if isinstance(request, ShutdownScriptWorker):
                    return
        except Exception:
            self.logger.exception("Script-worker service failed.")
            raise
        finally:
            socket.close()
            context.term()

    def _handle(self, request: ScriptWorkerRequest) -> ScriptWorkerResponse:
        """Dispatch a validated request to its pre-registered handler."""
        if isinstance(request, ScriptWorkerPing):
            return ScriptWorkerReady()
        if isinstance(request, ShutdownScriptWorker):
            return ScriptWorkerShutdownAck()
        if not isinstance(request, ExecuteScriptRequest):
            raise TypeError(f"Unsupported script-worker request: {type(request)!r}")

        start = time.perf_counter()
        handler = self.invocations.get(request)
        if handler is None:
            self.logger.warning("Rejected unregistered script invocation: %r", request)
            return ScriptWorkerError(f"Unregistered script invocation: {request!r}")

        work_start = time.perf_counter()
        try:
            if request.environment is None:
                exit_code = handler()
            else:
                exit_code = handler(request.environment)
        except ScriptWorkerServiceError:
            raise
        except Exception:
            self.logger.exception("Script invocation failed: %r", request)
            exit_code = 1
        work_end = time.perf_counter()
        if isinstance(exit_code, ScriptExecutionResult):
            identity = ExecuteScriptRequest(
                exit_code.run_id,
                exit_code.block_idx,
                exit_code.action_idx,
                exit_code.element_idx,
            )
            if identity != request:
                raise ScriptWorkerServiceError(
                    "Adapter returned mismatched run identity."
                )
            return exit_code
        # Invalid handler results are service bugs, not successful script results.
        return ScriptExecutionResult(
            run_id=request.run_id,
            block_idx=request.block_idx,
            action_idx=request.action_idx,
            element_idx=request.element_idx,
            exit_code=exit_code,
            work_time=work_end - work_start,
            orchestration_time=(work_start - start) + (time.perf_counter() - work_end),
        )
