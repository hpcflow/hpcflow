"""Run execution protocol."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, ClassVar


class InvalidRequestError(ValueError):
    pass


@dataclass(frozen=True)
class JobscriptRequest:
    command: ClassVar[str]

    @classmethod
    def from_dict(cls, data: Any) -> JobscriptRequest:
        if not isinstance(data, dict):
            raise InvalidRequestError("Request must be a JSON object.")

        command = data.get("command")
        if not isinstance(command, str):
            raise InvalidRequestError(f"Unknown command {command!r}.")

        request_cls = REQUEST_TYPES.get(command)
        if request_cls is None:
            raise InvalidRequestError(f"Unknown command {command!r}.")

        kwargs = {key: value for key, value in data.items() if key != "command"}

        try:
            return request_cls(**kwargs)
        except TypeError as exc:
            raise InvalidRequestError(f"Invalid {command!r} request: {exc}") from exc

    def to_dict(self) -> dict[str, Any]:
        return {
            "command": self.command,
            **asdict(self),
        }


@dataclass(frozen=True)
class RunRequest(JobscriptRequest):
    run_id: int

    def __post_init__(self) -> None:
        if type(self.run_id) is not int:
            raise InvalidRequestError("'run_id' must be an integer.")


@dataclass(frozen=True)
class PingRequest(JobscriptRequest):
    command: ClassVar[str] = "ping"


@dataclass(frozen=True)
class StatusRequest(JobscriptRequest):
    command: ClassVar[str] = "status"


@dataclass(frozen=True)
class AbortRequest(RunRequest):
    command: ClassVar[str] = "abort"

    run_id: int


@dataclass(frozen=True)
class TimeItRequest(RunRequest):
    command: ClassVar[str] = "timeit"

    run_id: int
    orchestration_time: float | None = None
    work_time: float | None = None

    def __post_init__(self) -> None:

        super().__post_init__()

        self._validate_time("orchestration_time", self.orchestration_time)
        self._validate_time("work_time", self.work_time)

    def _validate_time(self, name, value):
        if value is None:
            return
        if type(value) not in (int, float):
            raise InvalidRequestError(f"'{name}' must be a number.")


REQUEST_TYPES = {
    PingRequest.command: PingRequest,
    StatusRequest.command: StatusRequest,
    AbortRequest.command: AbortRequest,
    TimeItRequest.command: TimeItRequest,
}


@dataclass(frozen=True)
class JobscriptResponse:
    ok: bool = field(init=False)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OkResponse(JobscriptResponse):
    ok: bool = field(default=True, init=False)


@dataclass(frozen=True)
class ErrorResponse(JobscriptResponse):
    error: str
    ok: bool = field(default=False, init=False)


@dataclass(frozen=True)
class RunStatus:
    run_id: int
    pid: int | None
    running: bool


@dataclass(frozen=True)
class StatusResponse(JobscriptResponse):
    runs: list[RunStatus]
    ok: bool = field(default=True, init=False)
