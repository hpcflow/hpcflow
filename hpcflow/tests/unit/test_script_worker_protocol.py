import json
import math
from typing import Any, TypedDict

import pytest

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


class ScriptIdentity(TypedDict):
    run_id: int
    block_idx: int
    action_idx: int
    element_idx: int


IDENTITY: ScriptIdentity = {
    "run_id": 7,
    "block_idx": 0,
    "action_idx": 2,
    "element_idx": 3,
}


@pytest.mark.parametrize(
    ("message", "payload"),
    [
        (ScriptWorkerPing(), {"version": 1, "command": "ping"}),
        (
            ExecuteScriptRequest(**IDENTITY),
            {"version": 1, "command": "execute", **IDENTITY},
        ),
        (ShutdownScriptWorker(), {"version": 1, "command": "shutdown"}),
        (
            ScriptWorkerReady(),
            {"version": 1, "response": "ready", "ok": True},
        ),
        (
            ScriptExecutionResult(
                **IDENTITY, exit_code=1, work_time=0.25, orchestration_time=0.5
            ),
            {
                "version": 1,
                "response": "result",
                "ok": True,
                **IDENTITY,
                "exit_code": 1,
                "work_time": 0.25,
                "orchestration_time": 0.5,
            },
        ),
        (
            ScriptWorkerShutdownAck(),
            {"version": 1, "response": "shutdown", "ok": True},
        ),
        (
            ScriptWorkerError("Unknown action."),
            {"version": 1, "response": "error", "ok": False, "error": "Unknown action."},
        ),
    ],
)
def test_worker_message_wire_shape_and_json_round_trip(message, payload) -> None:
    assert message.to_dict() == payload
    decoder = (
        ScriptWorkerRequest
        if isinstance(message, ScriptWorkerRequest)
        else ScriptWorkerResponse
    )
    assert decoder.from_dict(json.loads(json.dumps(payload, allow_nan=False))) == message


def test_worker_protocol_compatibility_import() -> None:
    from hpcflow.sdk.execution.protocol import (
        ExecuteScriptRequest as CompatibilityExecuteScriptRequest,
    )

    assert CompatibilityExecuteScriptRequest is ExecuteScriptRequest


@pytest.mark.parametrize("decoder", [ScriptWorkerRequest, ScriptWorkerResponse])
@pytest.mark.parametrize("payload", [None, [], "ping", 1, True])
def test_worker_messages_require_json_objects(decoder, payload: Any) -> None:
    with pytest.raises(InvalidScriptWorkerMessageError, match="JSON object"):
        decoder.from_dict(payload)


@pytest.mark.parametrize(
    ("decoder", "payload"),
    [
        (ScriptWorkerRequest, {"command": "ping"}),
        (ScriptWorkerResponse, {"response": "ready", "ok": True}),
    ],
)
@pytest.mark.parametrize("version", [None, True, 1.0, "1", 0, 2, -1])
def test_worker_messages_reject_missing_or_unsupported_version(
    decoder, payload: dict[str, Any], version: Any
) -> None:
    if version is not None:
        payload = {**payload, "version": version}
    with pytest.raises(InvalidScriptWorkerMessageError, match="version"):
        decoder.from_dict(payload)


@pytest.mark.parametrize(
    ("decoder", "payload"),
    [
        (ScriptWorkerRequest, {"version": 1}),
        (ScriptWorkerRequest, {"version": 1, "command": "banana"}),
        (ScriptWorkerRequest, {"version": 1, "command": True}),
        (ScriptWorkerRequest, {"version": 1, "command": []}),
        (ScriptWorkerRequest, {"version": 1, "command": "ping", "extra": 0}),
        (
            ScriptWorkerRequest,
            {"version": 1, "command": "execute", **IDENTITY, "code": ""},
        ),
        (ScriptWorkerResponse, {"version": 1, "response": "banana", "ok": True}),
        (ScriptWorkerResponse, {"version": 1, "response": [], "ok": True}),
        (ScriptWorkerResponse, {"version": 1, "response": "ready"}),
        (ScriptWorkerResponse, {"version": 1, "response": "ready", "ok": False}),
        (ScriptWorkerResponse, {"version": 1, "response": "ready", "ok": 1}),
        (
            ScriptWorkerResponse,
            {"version": 1, "response": "ready", "ok": True, "extra": 0},
        ),
        (
            ScriptWorkerResponse,
            {"version": 1, "response": "error", "ok": True, "error": "x"},
        ),
        (
            ScriptWorkerResponse,
            {"version": 1, "response": "result", "ok": True, **IDENTITY},
        ),
        (ScriptWorkerResponse, {"version": 1, "response": "error", "ok": False}),
    ],
)
def test_worker_messages_reject_unknown_fields_types_and_incomplete_payloads(
    decoder, payload: Any
) -> None:
    with pytest.raises(InvalidScriptWorkerMessageError):
        decoder.from_dict(payload)


@pytest.mark.parametrize("name", list(IDENTITY))
@pytest.mark.parametrize("value", [True, False, -1, 1.0, "1", None, [], {}])
@pytest.mark.parametrize("result", [False, True])
def test_execution_identity_requires_nonnegative_integers(
    name: str, value: Any, result: bool
) -> None:
    identity = {**IDENTITY, name: value}
    constructor: type[ScriptExecutionResult] | type[ExecuteScriptRequest]
    if result:
        constructor = ScriptExecutionResult
        kwargs = {**identity, "exit_code": 0, "work_time": 0, "orchestration_time": 0}
    else:
        constructor = ExecuteScriptRequest
        kwargs = identity
    with pytest.raises(InvalidScriptWorkerMessageError, match=name):
        constructor(**kwargs)
    decoder = ScriptWorkerResponse if result else ScriptWorkerRequest
    tag = {"response": "result", "ok": True} if result else {"command": "execute"}
    with pytest.raises(InvalidScriptWorkerMessageError, match=name):
        decoder.from_dict({"version": 1, **tag, **kwargs})


@pytest.mark.parametrize("name", list(IDENTITY))
@pytest.mark.parametrize("result", [False, True])
def test_execution_requires_every_identity_field(name: str, result: bool) -> None:
    payload: dict[str, Any]
    decoder: type[ScriptWorkerResponse] | type[ScriptWorkerRequest]
    if result:
        payload = {
            "version": 1,
            "response": "result",
            "ok": True,
            **IDENTITY,
            "exit_code": 0,
            "work_time": 0,
            "orchestration_time": 0,
        }
        decoder = ScriptWorkerResponse
    else:
        payload = {"version": 1, "command": "execute", **IDENTITY}
        decoder = ScriptWorkerRequest
    del payload[name]
    with pytest.raises(InvalidScriptWorkerMessageError):
        decoder.from_dict(payload)


@pytest.mark.parametrize("exit_code", [0, 1, -1, -123124, 4294967295])
def test_result_accepts_integer_exit_codes_without_normalizing(exit_code: int) -> None:
    result = ScriptExecutionResult(
        **IDENTITY, exit_code=exit_code, work_time=0, orchestration_time=0
    )
    assert result.exit_code == exit_code


@pytest.mark.parametrize("exit_code", [True, False, 0.0, "0", None])
def test_result_rejects_noninteger_exit_codes(exit_code: Any) -> None:
    kwargs = {**IDENTITY, "exit_code": exit_code, "work_time": 0, "orchestration_time": 0}
    with pytest.raises(InvalidScriptWorkerMessageError, match="exit_code"):
        ScriptExecutionResult(**kwargs)
    with pytest.raises(InvalidScriptWorkerMessageError, match="exit_code"):
        ScriptWorkerResponse.from_dict(
            {"version": 1, "response": "result", "ok": True, **kwargs}
        )


@pytest.mark.parametrize("name", ["work_time", "orchestration_time"])
@pytest.mark.parametrize(
    "value", [-0.1, math.nan, math.inf, -math.inf, True, "1", None, []]
)
def test_result_requires_finite_nonnegative_timings(name: str, value: Any) -> None:
    kwargs = {
        **IDENTITY,
        "exit_code": 0,
        "work_time": 0,
        "orchestration_time": 0,
        name: value,
    }
    with pytest.raises(InvalidScriptWorkerMessageError, match=name):
        ScriptExecutionResult(**kwargs)
    with pytest.raises(InvalidScriptWorkerMessageError, match=name):
        ScriptWorkerResponse.from_dict(
            {"version": 1, "response": "result", "ok": True, **kwargs}
        )


@pytest.mark.parametrize("error", ["", " \n", None, 1, True, []])
def test_protocol_error_requires_nonempty_description(error: Any) -> None:
    with pytest.raises(InvalidScriptWorkerMessageError, match="error"):
        ScriptWorkerError(error)
    with pytest.raises(InvalidScriptWorkerMessageError, match="error"):
        ScriptWorkerResponse.from_dict(
            {"version": 1, "response": "error", "ok": False, "error": error}
        )


def test_worker_and_jobscript_protocols_are_separate() -> None:
    from hpcflow.sdk.execution.protocol import (
        InvalidRequestError,
        JobscriptRequest,
        PingRequest,
    )

    assert PingRequest().to_dict() == {"command": "ping"}
    with pytest.raises(InvalidRequestError):
        JobscriptRequest.from_dict(ExecuteScriptRequest(**IDENTITY).to_dict())
    with pytest.raises(InvalidScriptWorkerMessageError):
        ScriptWorkerRequest.from_dict(PingRequest().to_dict())
    with pytest.raises(InvalidScriptWorkerMessageError):
        ScriptWorkerResponse.from_dict(ScriptWorkerPing().to_dict())
    with pytest.raises(InvalidScriptWorkerMessageError):
        ScriptWorkerRequest.from_dict(ScriptWorkerReady().to_dict())
