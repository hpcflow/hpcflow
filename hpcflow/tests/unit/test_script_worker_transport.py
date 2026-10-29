import asyncio
from collections.abc import AsyncIterator, Callable
import os
from pathlib import Path
import sys
from textwrap import dedent

import pytest
import pytest_asyncio
import zmq
import zmq.asyncio

from hpcflow.sdk.execution.script_worker.protocol import (
    ExecuteScriptRequest,
    InvalidScriptWorkerMessageError,
    ScriptExecutionResult,
    ScriptWorkerError,
    ScriptWorkerPing,
    ScriptWorkerReady,
    ScriptWorkerResponse,
    ShutdownScriptWorker,
)
from hpcflow.sdk.execution.script_worker.client import (
    ScriptWorkerClient,
    ScriptWorkerExitedError,
    ScriptWorkerRejectedError,
)
from hpcflow.sdk.execution.script_worker.service import SCRIPT_WORKER_ENDPOINT_ENV
from hpcflow.sdk.execution.script_worker.run import ScriptWorkerRun
from hpcflow.sdk.core import ABORT_EXIT_CODE
from hpcflow.sdk.utils.async_utils import recv_json, send_json
from hpcflow.tests.unit.utils.test_patches import asyncio_timeout


@pytest_asyncio.fixture
async def clients(
    tmp_path: Path,
) -> AsyncIterator[Callable[..., ScriptWorkerClient]]:
    if getattr(sys, "frozen", False):
        pytest.skip("Worker subprocess tests require Python.")
    script = tmp_path / "worker.py"
    script.write_text(
        dedent("""\
        import os
        from pathlib import Path
        import time
        import zmq
        from hpcflow.sdk.execution.script_worker.protocol import (
            ExecuteScriptRequest, ScriptExecutionResult,
            ScriptWorkerReady, ScriptWorkerShutdownAck,
        )
        from hpcflow.sdk.execution.script_worker.service import (
            SCRIPT_WORKER_ENDPOINT_ENV, ScriptWorkerService,
        )

        def execute(environment=None):
            path = Path(os.environ["COUNTER"])
            with path.open("a") as fp:
                fp.write(os.environ.get("LABEL", "one") + "\\n")
            time.sleep(float(os.environ.get("DELAY", "0")))
            if os.environ.get("MODE") == "invalid_return":
                return None
            return 0

        def failure():
            raise ValueError("test script failure")

        def exit_worker(environment=None):
            os._exit(9)

        mode = os.environ.get("MODE", "")
        if mode == "exit":
            raise SystemExit(8)
        if mode == "silent":
            time.sleep(60)
        elif mode in ("bad_ready", "bad_reply", "wrong_identity", "drop_reply",
                      "ignore_shutdown", "ack_nonzero"):
            context = zmq.Context()
            socket = context.socket(zmq.REP)
            socket.setsockopt(zmq.LINGER, 1000)
            socket.connect(os.environ[SCRIPT_WORKER_ENDPOINT_ENV])
            socket.recv_json()
            if mode == "bad_ready":
                socket.send_json({"version": 1, "response": "shutdown", "ok": True})
            else:
                socket.send_json(ScriptWorkerReady().to_dict())
                socket.recv_json()
                if mode == "bad_reply":
                    socket.send(b"not json")
                elif mode == "wrong_identity":
                    socket.send_json(ScriptExecutionResult(
                        999, 0, 0, 0, 0, 0.0, 0.0,
                    ).to_dict())
                elif mode == "drop_reply":
                    Path(os.environ["COUNTER"]).write_text("once\\n")
                elif mode == "ack_nonzero":
                    socket.send_json(ScriptWorkerShutdownAck().to_dict())
                    socket.close()
                    context.term()
                    raise SystemExit(4)
            time.sleep(60)
        else:
            ScriptWorkerService(
                os.environ[SCRIPT_WORKER_ENDPOINT_ENV],
                {
                    ExecuteScriptRequest(7, 0, 0, 0): execute,
                    ExecuteScriptRequest(8, 0, 0, 1): execute,
                    ExecuteScriptRequest(9, 0, 1, 0): failure,
                    ExecuteScriptRequest(10, 0, 2, 0): exit_worker,
                },
            ).run()
        """),
        encoding="utf-8",
        newline="\n",
    )
    instances: list[ScriptWorkerClient] = []

    def create(**env: str) -> ScriptWorkerClient:
        client = ScriptWorkerClient(
            [sys.executable, str(script)],
            {**os.environ, "COUNTER": str(tmp_path / "count.txt"), **env},
            startup_timeout=5,
            shutdown_timeout=2,
        )
        instances.append(client)
        return client

    try:
        yield create
    finally:
        for instance in instances:
            await instance.abort()


async def wait_for_file(path: Path) -> None:
    async with asyncio_timeout(5):
        while not path.exists():
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_run_abort_during_real_worker_readiness_cleans_up(clients):
    client = clients(MODE="silent")
    invocation = ScriptWorkerRun(client, ExecuteScriptRequest(7, 0, 0, 0), {})
    task = asyncio.create_task(invocation.run())
    try:
        async with asyncio_timeout(5):
            while client.worker.process is None:
                await asyncio.sleep(0.01)
        process = client.worker.process
        invocation.abort()
        assert await asyncio.wait_for(task, timeout=10) == ABORT_EXIT_CODE
        assert process.returncode is not None
        assert client.worker.process is None
        assert client.endpoint is None
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_run_worker_death_is_error_without_retry(clients, tmp_path):
    client = clients()
    invocation = ScriptWorkerRun(client, ExecuteScriptRequest(10, 0, 2, 0), {})
    with pytest.raises(ScriptWorkerExitedError, match="exited with code"):
        await asyncio.wait_for(invocation.run(), timeout=10)
    assert invocation.return_code is None
    assert client.worker.process is None
    assert client.endpoint is None
    assert not (tmp_path / "count.txt").exists()
    following = ScriptWorkerRun(client, ExecuteScriptRequest(7, 0, 0, 0), {})
    assert await following.run() == 0
    assert (tmp_path / "count.txt").read_text().splitlines() == ["one"]


@pytest.mark.asyncio
async def test_run_abort_preserves_other_worker_and_restarts_only_for_next_run(
    clients, tmp_path
):
    first_count = tmp_path / "first.txt"
    second_count = tmp_path / "second.txt"
    first = clients(DELAY="60", COUNTER=str(first_count))
    second = clients(COUNTER=str(second_count))
    await asyncio.gather(first.start(), second.start())
    first_process = first.worker.process
    second_process = second.worker.process
    assert first_process is not None and second_process is not None
    invocation = ScriptWorkerRun(first, ExecuteScriptRequest(7, 0, 0, 0), {})
    task = asyncio.create_task(invocation.run())
    try:
        await wait_for_file(first_count)
        invocation.abort()
        assert await asyncio.wait_for(task, timeout=10) == ABORT_EXIT_CODE
        assert first_process.returncode is not None
        assert first.worker.process is None
        assert first.endpoint is None
        assert second.worker.process is second_process
        assert second_process.returncode is None
        assert (await second.execute(ExecuteScriptRequest(7, 0, 0, 0))).exit_code == 0
        assert first_count.read_text().splitlines() == ["one"]
        first.worker.env["DELAY"] = "0"
        next_run = ScriptWorkerRun(first, ExecuteScriptRequest(8, 0, 0, 1), {})
        assert await next_run.run() == 0
        assert first.worker.process is not first_process
        assert first_count.read_text().splitlines() == ["one", "one"]
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_readiness_sequential_execution_and_graceful_stop(clients, tmp_path):
    client = clients()
    await client.start()
    process = client.worker.process
    assert process is not None and client.worker.is_running
    assert client.endpoint.startswith("tcp://127.0.0.1:")
    with pytest.raises(RuntimeError, match="already"):
        await client.start()
    for request in (ExecuteScriptRequest(7, 0, 0, 0), ExecuteScriptRequest(8, 0, 0, 1)):
        result = await client.execute(request)
        assert isinstance(result, ScriptExecutionResult)
        assert (result.run_id, result.element_idx) == (
            request.run_id,
            request.element_idx,
        )
        assert result.exit_code == 0
        assert result.work_time >= 0 and result.orchestration_time >= 0
        assert client.worker.process is process
    assert (tmp_path / "count.txt").read_text().splitlines() == ["one", "one"]
    await client.stop()
    assert process.returncode == 0
    assert client.worker.process is None
    assert client.endpoint is None
    await client.stop()
    await client.abort()


@pytest.mark.asyncio
async def test_script_failure_is_result_and_worker_survives(clients):
    client = clients()
    await client.start()
    result = await client.execute(ExecuteScriptRequest(9, 0, 1, 0))
    assert result.exit_code == 1
    assert client.worker.is_running
    assert (await client.execute(ExecuteScriptRequest(7, 0, 0, 0))).exit_code == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invocation",
    [
        ExecuteScriptRequest(7, 1, 0, 0),
        ExecuteScriptRequest(7, 0, 1, 0),
        ExecuteScriptRequest(7, 0, 0, 1),
        ExecuteScriptRequest(99, 0, 0, 0),
    ],
)
async def test_unknown_identity_rejected_without_invocation(
    clients, tmp_path, invocation
):
    client = clients()
    await client.start()
    with pytest.raises(ScriptWorkerRejectedError):
        await client.execute(invocation)
    assert not (tmp_path / "count.txt").exists()
    assert (await client.execute(ExecuteScriptRequest(7, 0, 0, 0))).exit_code == 0


@pytest.mark.asyncio
async def test_concurrent_requests_are_serialized(clients, tmp_path):
    client = clients(DELAY="0.05")
    await client.start()
    results = await asyncio.gather(
        client.execute(ExecuteScriptRequest(7, 0, 0, 0)),
        client.execute(ExecuteScriptRequest(8, 0, 0, 1)),
    )
    assert [result.run_id for result in results] == [7, 8]
    assert len((tmp_path / "count.txt").read_text().splitlines()) == 2


@pytest.mark.asyncio
async def test_multiple_workers_have_independent_endpoints_and_environments(
    clients, tmp_path
):
    first = clients(LABEL="first", COUNTER=str(tmp_path / "first.txt"))
    second = clients(LABEL="second", COUNTER=str(tmp_path / "second.txt"))
    await asyncio.gather(first.start(), second.start())
    assert first.endpoint != second.endpoint
    await first.execute(ExecuteScriptRequest(7, 0, 0, 0))
    await first.stop()
    assert second.worker.is_running
    await second.execute(ExecuteScriptRequest(7, 0, 0, 0))
    assert (tmp_path / "first.txt").read_text().strip() == "first"
    assert (tmp_path / "second.txt").read_text().strip() == "second"


@pytest.mark.asyncio
async def test_process_exit_during_execution_is_not_a_script_result(clients):
    client = clients()
    await client.start()
    process = client.worker.process
    with pytest.raises(ScriptWorkerExitedError, match="9"):
        await asyncio.wait_for(client.execute(ExecuteScriptRequest(10, 0, 2, 0)), 5)
    assert process is not None and process.returncode == 9
    assert client.worker.process is None
    assert client.endpoint is None


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["exit", "silent"])
async def test_startup_failure_reaps_process_and_closes_endpoint(clients, mode):
    client = clients(MODE=mode)
    client.startup_timeout = 1
    error = ScriptWorkerExitedError if mode == "exit" else asyncio.TimeoutError
    with pytest.raises(error):
        await client.start()
    assert client.worker.process is None
    assert client.endpoint is None


@pytest.mark.asyncio
async def test_cancelled_execution_is_not_retried_and_worker_is_reaped(clients, tmp_path):
    client = clients(DELAY="60")
    await client.start()
    process = client.worker.process
    task = asyncio.create_task(client.execute(ExecuteScriptRequest(7, 0, 0, 0)))
    await wait_for_file(tmp_path / "count.txt")
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert process is not None and process.returncode is not None
    assert client.endpoint is None
    assert (tmp_path / "count.txt").read_text().splitlines() == ["one"]
    with pytest.raises(RuntimeError, match="ready"):
        await client.execute(ExecuteScriptRequest(7, 0, 0, 0))


@pytest.mark.asyncio
async def test_stop_while_busy_aborts_instead_of_waiting_for_reply(clients, tmp_path):
    client = clients(DELAY="60")
    await client.start()
    task = asyncio.create_task(client.execute(ExecuteScriptRequest(7, 0, 0, 0)))
    await wait_for_file(tmp_path / "count.txt")
    await asyncio.wait_for(client.stop(), 5)
    with pytest.raises((ScriptWorkerExitedError, zmq.ZMQError)):
        await task
    assert client.worker.process is None
    assert client.endpoint is None


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["bad_ready", "bad_reply", "wrong_identity"])
async def test_invalid_reply_poisoning_closes_transport_and_reaps(clients, mode):
    client = clients(MODE=mode)
    if mode == "bad_ready":
        with pytest.raises(InvalidScriptWorkerMessageError):
            await client.start()
    else:
        await client.start()
        error = ValueError if mode == "bad_reply" else InvalidScriptWorkerMessageError
        with pytest.raises(error):
            await client.execute(ExecuteScriptRequest(7, 0, 0, 0))
    assert client.worker.process is None
    assert client.endpoint is None


@pytest.mark.asyncio
async def test_lost_reply_after_side_effect_is_not_retried(clients, tmp_path):
    client = clients(MODE="drop_reply")
    await client.start()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(client.execute(ExecuteScriptRequest(7, 0, 0, 0)), 0.5)
    assert (tmp_path / "count.txt").read_text() == "once\n"
    assert client.worker.process is None
    assert client.endpoint is None


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["ignore_shutdown", "ack_nonzero"])
async def test_shutdown_failure_is_reported_and_worker_reaped(clients, mode):
    client = clients(MODE=mode)
    client.shutdown_timeout = 0.1
    await client.start()
    error = asyncio.TimeoutError if mode == "ignore_shutdown" else ScriptWorkerExitedError
    with pytest.raises(error):
        await client.stop()
    assert client.worker.process is None
    assert client.endpoint is None


@pytest.mark.asyncio
async def test_invalid_handler_return_is_service_failure_not_exit_zero(clients):
    client = clients(MODE="invalid_return")
    await client.start()
    with pytest.raises(ScriptWorkerExitedError):
        await client.execute(ExecuteScriptRequest(7, 0, 0, 0))


@pytest.mark.asyncio
async def test_cancelled_queued_request_does_not_abort_active_invocation(
    clients, tmp_path
):
    client = clients(DELAY="0.2")
    await client.start()
    first = asyncio.create_task(client.execute(ExecuteScriptRequest(7, 0, 0, 0)))
    await wait_for_file(tmp_path / "count.txt")
    queued = asyncio.create_task(client.execute(ExecuteScriptRequest(8, 0, 0, 1)))
    await asyncio.sleep(0)
    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    assert (await first).exit_code == 0
    assert client.worker.is_running
    assert (tmp_path / "count.txt").read_text().splitlines() == ["one"]


@pytest.mark.asyncio
async def test_cancelled_startup_reaps_and_allows_explicit_restart(clients):
    client = clients(MODE="silent")
    start = asyncio.create_task(client.start())
    async with asyncio_timeout(5):
        while client.worker.process is None:
            await asyncio.sleep(0.01)
    start.cancel()
    with pytest.raises(asyncio.CancelledError):
        await start
    assert client.worker.process is None
    assert client.endpoint is None
    client.worker.env["MODE"] = ""
    await client.start()
    assert (await client.execute(ExecuteScriptRequest(7, 0, 0, 0))).exit_code == 0


@pytest.mark.asyncio
async def test_spawn_failure_closes_endpoint(clients, tmp_path):
    client = clients()
    client.worker.cmd = [str(tmp_path / "missing-interpreter")]
    with pytest.raises(OSError):
        await client.start()
    assert client.worker.process is None
    assert client.endpoint is None


@pytest.mark.asyncio
async def test_cancelled_graceful_shutdown_still_reaps_worker(clients):
    client = clients(MODE="ignore_shutdown")
    await client.start()
    process = client.worker.process
    stop = asyncio.create_task(client.stop())
    await asyncio.sleep(0.05)
    stop.cancel()
    await asyncio.sleep(0)
    stop.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stop
    assert process is not None and process.returncode is not None
    assert client.worker.process is None
    assert client.endpoint is None


@pytest.mark.asyncio
async def test_cancellation_while_draining_exchange_tasks_reaps_worker(
    clients, monkeypatch
):
    client = clients()
    await client.start()
    process = client.worker.process
    assert process is not None
    original_wait = process.wait
    entered = asyncio.Event()
    release = asyncio.Event()

    async def wait():
        try:
            return await original_wait()
        except asyncio.CancelledError:
            entered.set()
            await release.wait()
            raise

    monkeypatch.setattr(process, "wait", wait)
    task = asyncio.create_task(client.execute(ExecuteScriptRequest(7, 0, 0, 0)))
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert process.returncode is not None
    assert client.worker.process is None
    assert client.endpoint is None


@pytest.mark.asyncio
async def test_long_execution_has_no_implicit_short_rpc_timeout(clients):
    client = clients(DELAY="0.2")
    await client.start()
    client.startup_timeout = 0.01
    assert (await client.execute(ExecuteScriptRequest(7, 0, 0, 0))).exit_code == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload", [b"not json", b"[]", b'{"version":2,"command":"ping"}']
)
async def test_service_rejects_bad_wire_message_then_handles_next_request(
    clients, payload
):
    client = clients()
    # Exercise the service over an independent raw REQ socket, without the client decoder.
    socket = zmq.asyncio.Context.instance().socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    port = socket.bind_to_random_port("tcp://127.0.0.1")
    client.worker.env[SCRIPT_WORKER_ENDPOINT_ENV] = f"tcp://127.0.0.1:{port}"
    await client.worker.start()
    try:
        await send_json(socket, ScriptWorkerPing().to_dict())
        await asyncio.wait_for(recv_json(socket), 5)
        await socket.send(payload)
        response = await asyncio.wait_for(recv_json(socket), 5)
        assert isinstance(ScriptWorkerResponse.from_dict(response), ScriptWorkerError)
        await send_json(socket, ScriptWorkerPing().to_dict())
        assert isinstance(
            ScriptWorkerResponse.from_dict(await asyncio.wait_for(recv_json(socket), 5)),
            ScriptWorkerReady,
        )
        await send_json(socket, ShutdownScriptWorker().to_dict())
        await asyncio.wait_for(recv_json(socket), 5)
        process = client.worker.process
        assert process is not None
        assert await asyncio.wait_for(process.wait(), 5) == 0
    finally:
        socket.close(linger=0)
