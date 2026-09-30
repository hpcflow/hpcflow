import asyncio
import os
import sys

import pytest

import pytest_asyncio
import zmq
import zmq.asyncio

from hpcflow.app import app as hf
from hpcflow.sdk.core import ABORT_EXIT_CODE
from hpcflow.sdk.execution.protocol import (
    AbortRequest,
    ErrorResponse,
    InvalidRequestError,
    JobscriptRequest,
    OkResponse,
    PingRequest,
    RunStatus,
    StatusRequest,
    StatusResponse,
    TimeItRequest,
)


@pytest.mark.asyncio
async def test_executor_success():
    executor = hf.RunExecutor._from_noop_cmd()
    return_code = await executor.run()

    assert return_code == 0
    assert executor.return_code == 0
    assert not executor.is_running


@pytest.mark.asyncio
async def test_executor_nonzero_exit():
    executor = hf.RunExecutor._from_noop_cmd("--exit-code", "7")
    return_code = await executor.run()

    assert return_code == 7
    assert executor.return_code == 7
    assert not executor.is_running


@pytest.mark.asyncio
async def test_executor_abort():
    executor = hf.RunExecutor._from_noop_cmd("--sleep", "60")
    task = asyncio.create_task(executor.run())

    while executor.pid is None:
        await asyncio.sleep(0)

    executor.abort()

    return_code = await asyncio.wait_for(task, timeout=5)

    assert return_code == ABORT_EXIT_CODE
    assert not executor.is_running


def test_ping_request_to_dict():
    request = PingRequest()
    assert request.to_dict() == {"command": "ping"}


def test_ping_request_from_dict():
    request = JobscriptRequest.from_dict({"command": "ping"})
    assert request == PingRequest()


def test_ok_response_to_dict():
    response = OkResponse()
    assert response.to_dict() == {"ok": True}


class FakeExecution:
    def __init__(self):
        self._app = hf
        self.active_executors = {}


@pytest_asyncio.fixture
async def jobscript_server():
    execution = FakeExecution()
    server = hf.JobscriptServer(execution)

    port = await server.start()
    task = asyncio.create_task(server.run())

    try:
        yield server, execution, port
    finally:
        task.cancel()
        await asyncio.gather(
            task,
            return_exceptions=True,
        )


async def send_request(port, request):
    """Helper to deliberately talk over ZMQ rather than calling server methods, so we can
    test the protocol boundary.
    """
    context = zmq.asyncio.Context.instance()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)

    try:
        socket.connect(f"tcp://127.0.0.1:{port}")
        await socket.send_json(request.to_dict())
        return await asyncio.wait_for(socket.recv_json(), timeout=2)
    finally:
        socket.close()


@pytest.mark.asyncio
async def test_ping(jobscript_server):
    _, _, port = jobscript_server
    response = await send_request(port, PingRequest())
    assert response == {"ok": True}


@pytest.mark.asyncio
async def test_status_no_active_runs(jobscript_server):
    _, _, port = jobscript_server
    response = await send_request(port, StatusRequest())
    assert response == {"ok": True, "runs": []}


@pytest.mark.asyncio
async def test_abort_unknown_run(jobscript_server):
    _, _, port = jobscript_server
    response = await send_request(port, AbortRequest(run_id=6))
    assert response["ok"] is False
    assert "6" in response["error"]


@pytest.mark.asyncio
async def test_unknown_command(jobscript_server):
    _, _, port = jobscript_server
    context = zmq.asyncio.Context.instance()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    try:
        socket.connect(f"tcp://127.0.0.1:{port}")
        await socket.send_json({"command": "banana"})
        response = await asyncio.wait_for(socket.recv_json(), timeout=2)
    finally:
        socket.close()

    assert response["ok"] is False
    assert "banana" in response["error"]


@pytest.mark.asyncio
async def test_server_survives_invalid_request(jobscript_server):
    _, _, port = jobscript_server
    context = zmq.asyncio.Context.instance()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    try:
        socket.connect(f"tcp://127.0.0.1:{port}")
        await socket.send_json({"command": "banana"})
        response = await asyncio.wait_for(socket.recv_json(), timeout=2)
        assert response["ok"] is False

        # same server should still handle another request:
        await socket.send_json(PingRequest().to_dict())
        response = await asyncio.wait_for(socket.recv_json(), timeout=2)
        assert response == {"ok": True}

    finally:
        socket.close()


@pytest.mark.asyncio
async def test_status_running_executor(jobscript_server):
    _, execution, port = jobscript_server
    run_id = 8
    executor = hf.RunExecutor._from_noop_cmd("--sleep", "60")
    execution.active_executors[run_id] = executor
    executor_task = asyncio.create_task(executor.run())
    try:
        # wait until the subprocess has actually started.
        while executor.pid is None:
            await asyncio.sleep(0)

        response = await send_request(port, StatusRequest())

        assert response == {
            "ok": True,
            "runs": [
                {
                    "run_id": run_id,
                    "pid": executor.pid,
                    "running": True,
                }
            ],
        }

    finally:
        executor.abort()
        await asyncio.wait_for(executor_task, timeout=5)
        execution.active_executors.pop(run_id, None)


@pytest.mark.asyncio
async def test_abort_running_executor(jobscript_server):
    _, execution, port = jobscript_server
    run_id = 2348
    executor = hf.RunExecutor._from_noop_cmd("--sleep", "60")
    execution.active_executors[run_id] = executor
    executor_task = asyncio.create_task(executor.run())

    try:
        # ensure the subprocess is running before requesting the abort.
        while executor.pid is None:
            await asyncio.sleep(0)

        response = await send_request(port, AbortRequest(run_id=run_id))
        assert response == {"ok": True}

        return_code = await asyncio.wait_for(executor_task, timeout=5)

        assert return_code == ABORT_EXIT_CODE
        assert executor.return_code == ABORT_EXIT_CODE
        assert not executor.is_running

    finally:
        if not executor_task.done():
            executor.abort()
            await asyncio.wait_for(executor_task, timeout=5)
        execution.active_executors.pop(run_id, None)


@pytest.mark.asyncio
async def test_timeit_running_executor(jobscript_server):
    _, execution, port = jobscript_server
    run_id = 9834
    executor = hf.RunExecutor._from_noop_cmd("--sleep", "60")
    execution.active_executors[run_id] = executor
    executor_task = asyncio.create_task(executor.run())

    try:
        while executor.pid is None:
            await asyncio.sleep(0)

        response = await send_request(
            port,
            TimeItRequest(run_id=run_id, orchestration_time=1.25, work_time=0.75),
        )

        assert response == {"ok": True}
        assert executor.child_orchestration_times == [1.25]
        assert executor.child_work_times == [0.75]

    finally:
        if not executor_task.done():
            executor.abort()
            await asyncio.wait_for(executor_task, timeout=5)
        execution.active_executors.pop(run_id, None)


@pytest.mark.asyncio
async def test_timeit_orchestration_time_only(jobscript_server):
    _, execution, port = jobscript_server
    run_id = 445
    executor = hf.RunExecutor._from_noop_cmd("--sleep", "60")
    execution.active_executors[run_id] = executor
    executor_task = asyncio.create_task(executor.run())

    try:
        while executor.pid is None:
            await asyncio.sleep(0)

        response = await send_request(
            port,
            TimeItRequest(run_id=run_id, orchestration_time=1.25),
        )

        assert response == {"ok": True}
        assert executor.child_orchestration_times == [1.25]
        assert executor.child_work_times == []

    finally:
        if not executor_task.done():
            executor.abort()
            await asyncio.wait_for(executor_task, timeout=5)
        execution.active_executors.pop(run_id, None)


@pytest.mark.asyncio
async def test_timeit_work_time_only(jobscript_server):
    _, execution, port = jobscript_server
    run_id = 755
    executor = hf.RunExecutor._from_noop_cmd("--sleep", "60")
    execution.active_executors[run_id] = executor
    executor_task = asyncio.create_task(executor.run())

    try:
        while executor.pid is None:
            await asyncio.sleep(0)

        response = await send_request(
            port,
            TimeItRequest(run_id=run_id, work_time=0.75),
        )

        assert response == {"ok": True}
        assert executor.child_orchestration_times == []
        assert executor.child_work_times == [0.75]

    finally:
        if not executor_task.done():
            executor.abort()
            await asyncio.wait_for(executor_task, timeout=5)
        execution.active_executors.pop(run_id, None)


@pytest.mark.asyncio
async def test_timeit_unknown_run(jobscript_server):
    _, _, port = jobscript_server
    response = await send_request(
        port,
        TimeItRequest(run_id=444, orchestration_time=1.25, work_time=0.75),
    )
    assert response["ok"] is False
    assert "444" in response["error"]


def test_abort_request_rejects_invalid_run_id():
    with pytest.raises(InvalidRequestError):
        AbortRequest(run_id="42")


def test_abort_request_rejects_bool_run_id():
    with pytest.raises(InvalidRequestError):
        AbortRequest(run_id=True)


@pytest.mark.parametrize(
    "field, value",
    [
        ("orchestration_time", "1.25"),
        ("work_time", "0.75"),
        ("orchestration_time", True),
        ("work_time", False),
    ],
)
def test_timeit_request_rejects_invalid_times(field, value):
    kwargs = {"run_id": 42, field: value}
    with pytest.raises(InvalidRequestError):
        TimeItRequest(**kwargs)


def test_request_from_dict_rejects_missing_command():
    with pytest.raises(InvalidRequestError):
        JobscriptRequest.from_dict({})


def test_request_from_dict_rejects_unknown_command():
    with pytest.raises(InvalidRequestError):
        JobscriptRequest.from_dict({"command": "banana"})


def test_request_from_dict_rejects_missing_run_id():
    with pytest.raises(InvalidRequestError):
        JobscriptRequest.from_dict({"command": "abort"})


def test_request_from_dict_rejects_unexpected_fields():
    with pytest.raises(InvalidRequestError):
        JobscriptRequest.from_dict(
            {
                "command": "abort",
                "run_id": 42,
                "banana": "hello",
            }
        )


@pytest.mark.parametrize(
    "data, expected_type",
    [
        ({"command": "ping"}, PingRequest),
        ({"command": "status"}, StatusRequest),
        (
            {"command": "abort", "run_id": 42},
            AbortRequest,
        ),
        (
            {
                "command": "timeit",
                "run_id": 42,
                "work_time": 1.5,
            },
            TimeItRequest,
        ),
    ],
)
def test_request_from_dict_dispatches_to_request_type(data, expected_type):
    request = JobscriptRequest.from_dict(data)
    assert isinstance(request, expected_type)


def test_error_response_to_dict():
    response = ErrorResponse(error="Something went wrong.")
    assert response.to_dict() == {"ok": False, "error": "Something went wrong."}


def test_status_response_to_dict():
    response = StatusResponse(runs=[RunStatus(run_id=42, pid=1234, running=True)])
    assert response.to_dict() == {
        "ok": True,
        "runs": [{"run_id": 42, "pid": 1234, "running": True}],
    }


def test_status_response_allows_missing_pid():
    response = StatusResponse(runs=[RunStatus(run_id=42, pid=None, running=False)])
    assert response.to_dict() == {
        "ok": True,
        "runs": [{"run_id": 42, "pid": None, "running": False}],
    }


@pytest.mark.asyncio
async def test_server_stops_cleanly():
    execution = FakeExecution()
    server = hf.JobscriptServer(execution)

    await server.start()
    server_task = asyncio.create_task(server.run())

    # Give server.run() a chance to start and reach recv_json().
    await asyncio.sleep(0)

    assert server.socket is not None
    assert not server_task.done()

    server_task.cancel()

    await asyncio.gather(server_task, return_exceptions=True)

    assert server_task.cancelled()
    assert server.socket is None


@pytest.mark.asyncio
async def test_server_run_requires_start():
    execution = FakeExecution()
    server = hf.JobscriptServer(execution)
    with pytest.raises(RuntimeError, match=r"start\(\) must be called first"):
        await server.run()
