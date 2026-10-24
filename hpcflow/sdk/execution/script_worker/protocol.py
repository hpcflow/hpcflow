"""Versioned JSON messages exchanged with persistent script workers."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import math
from typing import Any, ClassVar, Mapping

SCRIPT_WORKER_PROTOCOL_VERSION = 1


class InvalidScriptWorkerMessageError(ValueError):
    """Raised when a script-worker request or response violates its schema."""


def _script_worker_fields(data: Any, discriminator: str) -> tuple[str, dict[str, Any]]:
    """Validate a message envelope and return its discriminator and payload."""
    if not isinstance(data, dict):
        raise InvalidScriptWorkerMessageError("Message must be a JSON object.")

    version = data.get("version")
    if type(version) is not int or version != SCRIPT_WORKER_PROTOCOL_VERSION:
        raise InvalidScriptWorkerMessageError(
            f"Unsupported script-worker protocol version {version!r}."
        )
    kind = data.get(discriminator)
    if not isinstance(kind, str):
        raise InvalidScriptWorkerMessageError(f"Invalid {discriminator} {kind!r}.")
    return kind, {
        key: value for key, value in data.items() if key not in ("version", discriminator)
    }


def _validate_script_identity(
    run_id: int, block_idx: int, action_idx: int, element_idx: int
) -> None:
    """Reject invalid or boolean-valued coordinates in a script-worker identity."""
    for name, value in (
        ("run_id", run_id),
        ("block_idx", block_idx),
        ("action_idx", action_idx),
        ("element_idx", element_idx),
    ):
        if type(value) is not int or value < 0:
            raise InvalidScriptWorkerMessageError(
                f"'{name}' must be a nonnegative integer."
            )


@dataclass(frozen=True)
class ScriptWorkerRequest:
    """Base class for requests sent to a script worker."""

    command: ClassVar[str]

    def to_dict(self) -> dict[str, Any]:
        """Serialize the request with its protocol version and command."""
        return {
            "version": SCRIPT_WORKER_PROTOCOL_VERSION,
            "command": self.command,
            **asdict(self),
        }

    @classmethod
    def from_dict(cls, data: Any) -> ScriptWorkerRequest:
        """Parse and validate a request dictionary."""
        command, kwargs = _script_worker_fields(data, "command")
        request_cls = SCRIPT_WORKER_REQUEST_TYPES.get(command)
        if request_cls is None:
            raise InvalidScriptWorkerMessageError(f"Unknown command {command!r}.")
        try:
            return request_cls(**kwargs)
        except TypeError as exc:
            raise InvalidScriptWorkerMessageError(
                f"Invalid {command!r} request: {exc}"
            ) from exc


@dataclass(frozen=True)
class ScriptWorkerPing(ScriptWorkerRequest):
    """Request that checks whether the worker is ready."""

    command: ClassVar[str] = "ping"


@dataclass(frozen=True)
class ExecuteScriptRequest(ScriptWorkerRequest):
    """Request execution for one run and one jobscript block coordinate."""

    command: ClassVar[str] = "execute"

    run_id: int
    block_idx: int
    action_idx: int
    element_idx: int
    environment: Mapping[str, str] | None = field(
        default=None, compare=False, hash=False, repr=False
    )

    def __post_init__(self) -> None:
        """Validate the run, block, action, and element identity."""
        _validate_script_identity(
            self.run_id, self.block_idx, self.action_idx, self.element_idx
        )
        if self.environment is not None:
            if not isinstance(self.environment, Mapping):
                raise InvalidScriptWorkerMessageError(
                    "'environment' must be an object of string keys and values."
                )
            if any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in self.environment.items()
            ):
                raise InvalidScriptWorkerMessageError(
                    "'environment' must contain only string keys and values."
                )
            object.__setattr__(self, "environment", dict(self.environment))

    def to_dict(self) -> dict[str, Any]:
        """Serialize identity and optional per-run environment context."""
        data = super().to_dict()
        if self.environment is None:
            data.pop("environment")
        return data


@dataclass(frozen=True)
class ShutdownScriptWorker(ScriptWorkerRequest):
    """Request orderly shutdown after the current request."""

    command: ClassVar[str] = "shutdown"


@dataclass(frozen=True)
class ScriptWorkerResponse:
    """Base class for responses returned by a script worker."""

    response: ClassVar[str]
    ok: ClassVar[bool] = True

    def to_dict(self) -> dict[str, Any]:
        """Serialize the response with its protocol version and status."""
        return {
            "version": SCRIPT_WORKER_PROTOCOL_VERSION,
            "response": self.response,
            "ok": self.ok,
            **asdict(self),
        }

    @classmethod
    def from_dict(cls, data: Any) -> ScriptWorkerResponse:
        """Parse and validate a response dictionary."""
        response, kwargs = _script_worker_fields(data, "response")
        response_cls = SCRIPT_WORKER_RESPONSE_TYPES.get(response)
        if response_cls is None:
            raise InvalidScriptWorkerMessageError(f"Unknown response {response!r}.")
        ok = kwargs.pop("ok", None)
        if type(ok) is not bool or ok != response_cls.ok:
            raise InvalidScriptWorkerMessageError(f"Invalid 'ok' for {response!r}.")
        try:
            return response_cls(**kwargs)
        except TypeError as exc:
            raise InvalidScriptWorkerMessageError(
                f"Invalid {response!r} response: {exc}"
            ) from exc


@dataclass(frozen=True)
class ScriptWorkerReady(ScriptWorkerResponse):
    """Acknowledge a readiness ping."""

    response: ClassVar[str] = "ready"


@dataclass(frozen=True)
class ScriptExecutionResult(ScriptWorkerResponse):
    """Describe invocation completion without implying run finalization success."""

    response: ClassVar[str] = "result"

    run_id: int
    block_idx: int
    action_idx: int
    element_idx: int
    exit_code: int
    work_time: float
    orchestration_time: float

    def __post_init__(self) -> None:
        """Validate the request identity, exit code, and measured durations."""
        _validate_script_identity(
            self.run_id, self.block_idx, self.action_idx, self.element_idx
        )
        if type(self.exit_code) is not int:
            raise InvalidScriptWorkerMessageError("'exit_code' must be an integer.")
        for name, value in (
            ("work_time", self.work_time),
            ("orchestration_time", self.orchestration_time),
        ):
            if (
                type(value) not in (int, float)
                or value < 0
                or (isinstance(value, float) and not math.isfinite(value))
            ):
                raise InvalidScriptWorkerMessageError(
                    f"'{name}' must be a finite nonnegative number."
                )


@dataclass(frozen=True)
class ScriptWorkerShutdownAck(ScriptWorkerResponse):
    """Acknowledge an orderly shutdown request."""

    response: ClassVar[str] = "shutdown"


@dataclass(frozen=True)
class ScriptWorkerError(ScriptWorkerResponse):
    """Report a rejected request or protocol-level error."""

    response: ClassVar[str] = "error"
    ok: ClassVar[bool] = False

    error: str

    def __post_init__(self) -> None:
        """Require a nonempty explanation for the protocol error."""
        if not isinstance(self.error, str) or not self.error.strip():
            raise InvalidScriptWorkerMessageError("'error' must be a nonempty string.")


SCRIPT_WORKER_REQUEST_TYPES: dict[str, type[ScriptWorkerRequest]] = {
    cls.command: cls
    for cls in (ScriptWorkerPing, ExecuteScriptRequest, ShutdownScriptWorker)
}
SCRIPT_WORKER_RESPONSE_TYPES: dict[str, type[ScriptWorkerResponse]] = {
    cls.response: cls
    for cls in (
        ScriptWorkerReady,
        ScriptExecutionResult,
        ScriptWorkerShutdownAck,
        ScriptWorkerError,
    )
}
