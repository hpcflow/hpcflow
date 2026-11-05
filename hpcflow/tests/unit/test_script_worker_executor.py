import asyncio
from collections.abc import AsyncIterator
import os
from pathlib import Path
import sys
from typing import cast
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

from hpcflow.app import app as hf
from hpcflow.sdk.execution.script_worker.client import ScriptWorkerClient
from hpcflow.sdk.execution.script_worker.executor import ScriptWorkerExecutor
from hpcflow.tests.unit.utils.test_patches import asyncio_timeout


@pytest_asyncio.fixture
async def real_workers(tmp_path: Path) -> AsyncIterator[list[ScriptWorkerExecutor]]:
    if getattr(sys, "frozen", False):
        pytest.skip("These subprocess tests require a Python interpreter.")
    workers = []
    for label in ("one", "two"):
        env = dict(os.environ, WORKER_LABEL=label)
        workers.append(
            ScriptWorkerExecutor(
                [
                    sys.executable,
                    "-c",
                    "import os, pathlib, sys, time; "
                    "pathlib.Path(sys.argv[1]).write_text(os.environ['WORKER_LABEL']); "
                    "time.sleep(60)",
                    str(tmp_path / label),
                ],
                env,
            )
        )
    try:
        yield workers
    finally:
        for worker in workers:
            process = worker.process
            if process is not None:
                if process.returncode is None:
                    process.kill()
                await asyncio.wait_for(process.wait(), timeout=5)


async def wait_until_ready(path: Path) -> str:
    async with asyncio_timeout(5):
        while not path.exists() or not path.read_text():
            await asyncio.sleep(0.01)
    return path.read_text()


@pytest.mark.asyncio
async def test_worker_stays_alive_stops_and_restarts(
    real_workers: list[ScriptWorkerExecutor], tmp_path: Path
) -> None:
    worker = real_workers[0]
    assert not worker.is_running
    await worker.stop()
    await worker.abort()
    await worker.start()
    process = worker.process
    assert process is not None
    assert await wait_until_ready(tmp_path / "one") == "one"
    assert worker.is_running
    with pytest.raises(RuntimeError, match="already running"):
        await worker.start()

    await worker.stop()
    assert process.returncode is not None
    assert worker.process is None
    assert not worker.is_running
    await worker.stop()

    (tmp_path / "one").unlink()
    await worker.start()
    restarted = worker.process
    assert restarted is not None and restarted is not process
    assert await wait_until_ready(tmp_path / "one") == "one"
    await worker.abort()
    assert restarted.returncode is not None
    assert worker.process is None
    await worker.abort()


@pytest.mark.asyncio
async def test_workers_keep_independent_environments_and_cleanup(
    real_workers: list[ScriptWorkerExecutor], tmp_path: Path
) -> None:
    first, second = real_workers
    await asyncio.gather(first.start(), second.start())
    assert await wait_until_ready(tmp_path / "one") == "one"
    assert await wait_until_ready(tmp_path / "two") == "two"
    assert first.process is not None and second.process is not None
    assert first.process.pid != second.process.pid

    await first.abort()
    assert not first.is_running
    assert second.is_running
    executor = hf.JobscriptExecutor.__new__(hf.JobscriptExecutor)
    executor.script_workers = {
        "first": cast(ScriptWorkerClient, first),
        "second": cast(ScriptWorkerClient, second),
    }
    await executor._stop_script_workers()
    assert executor.script_workers == {}
    assert not second.is_running


@pytest.mark.asyncio
async def test_worker_unexpected_exit_can_be_reaped_and_restarted(
    real_workers: list[ScriptWorkerExecutor],
) -> None:
    worker = real_workers[0]
    worker.cmd = [sys.executable, "-c", "raise SystemExit(7)"]
    await worker.start()
    process = worker.process
    assert process is not None
    assert await asyncio.wait_for(process.wait(), timeout=5) == 7
    assert not worker.is_running
    await worker.start()
    assert worker.process is not None and worker.process is not process
    await worker.stop()
    assert worker.process is None


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_jobscript_reaps_real_workers_on_exception_or_cancellation(
    real_workers: list[ScriptWorkerExecutor],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cancel: bool,
) -> None:
    await asyncio.gather(*(worker.start() for worker in real_workers))
    await wait_until_ready(tmp_path / "one")
    await wait_until_ready(tmp_path / "two")
    processes = [worker.process for worker in real_workers]
    executor = hf.JobscriptExecutor.__new__(hf.JobscriptExecutor)
    executor.workflow = Mock()
    executor.jobscript = Mock(all_EAR_IDs=[])
    executor.array_idx = None
    executor.script_workers = {
        name: cast(ScriptWorkerClient, worker)
        for name, worker in zip(("one", "two"), real_workers)
    }
    completion = Mock()
    monkeypatch.setattr(
        hf.JobscriptExecutor, "completion", property(lambda _: completion)
    )
    monkeypatch.setattr(executor, "_start_server", AsyncMock())
    monkeypatch.setattr(executor, "_stop_server", AsyncMock())
    monkeypatch.setattr(executor, "_iter_actions", lambda: iter((Mock(),)))
    entered = asyncio.Event()

    async def execute_action(action):
        entered.set()
        if cancel:
            await asyncio.Event().wait()
        raise RuntimeError("action failed")

    def notified():
        assert all(
            process is not None and process.returncode is not None
            for process in processes
        )
        completion.mark_complete.assert_called_once()

    notify = AsyncMock(side_effect=notified)
    monkeypatch.setattr(executor, "_notify_waiters", notify)
    monkeypatch.setattr(executor, "_execute_action", execute_action)
    task = asyncio.create_task(executor._execute())
    await entered.wait()
    if cancel:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(RuntimeError, match="action failed"):
            await task
    assert executor.script_workers == {}
    assert all(worker.process is None for worker in real_workers)
    notify.assert_awaited_once()


def mock_process() -> Mock:
    process = Mock()
    process.pid = 123
    process.returncode = None
    process.wait = AsyncMock(return_value=0)
    return process


@pytest.mark.asyncio
async def test_spawn_error_is_reported_and_start_can_be_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = mock_process()
    spawn = AsyncMock(side_effect=[OSError("cannot spawn"), process])
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    worker = ScriptWorkerExecutor(["python"], {})
    with pytest.raises(OSError, match="cannot spawn"):
        await worker.start()
    assert worker.process is None
    await worker.start()
    await worker.abort()


@pytest.mark.asyncio
async def test_concurrent_starts_do_not_spawn_two_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    process = mock_process()

    async def spawn(*args, **kwargs):
        entered.set()
        await release.wait()
        return process

    factory = AsyncMock(side_effect=spawn)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", factory)
    worker = ScriptWorkerExecutor(["python"], {})
    first = asyncio.create_task(worker.start())
    await entered.wait()
    second = asyncio.create_task(worker.start())
    await asyncio.sleep(0)
    release.set()
    results = await asyncio.gather(first, second, return_exceptions=True)
    await worker.abort()
    assert results[0] is None
    assert isinstance(results[1], RuntimeError)
    assert factory.await_count == 1


@pytest.mark.asyncio
async def test_cancelled_start_reaps_process_created_during_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    process = mock_process()

    async def spawn(*args, **kwargs):
        entered.set()
        await release.wait()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    worker = ScriptWorkerExecutor(["python"], {})
    task = asyncio.create_task(worker.start())
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    process.kill.assert_called_once()
    process.wait.assert_awaited_once()
    assert worker.process is None


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["stop", "abort"])
async def test_cancelled_shutdown_reaps_before_propagating_cancellation(
    method: str,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    process = mock_process()

    async def wait():
        entered.set()
        await release.wait()
        return 0

    process.wait.side_effect = wait
    worker = ScriptWorkerExecutor(["python"], {})
    worker.process = process
    task = asyncio.create_task(getattr(worker, method)())
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    pending_until_reaped = not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert pending_until_reaped
    assert worker.process is None


@pytest.mark.asyncio
async def test_stop_escalates_to_kill_with_bounded_waits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ScriptWorkerExecutor, "_TERMINATE_TIMEOUT", 0.01, raising=False)
    monkeypatch.setattr(ScriptWorkerExecutor, "_KILL_TIMEOUT", 0.05, raising=False)
    killed = asyncio.Event()
    process = mock_process()
    process.kill.side_effect = killed.set

    async def wait():
        await killed.wait()
        return 0

    process.wait.side_effect = wait
    worker = ScriptWorkerExecutor(["python"], {})
    worker.process = process
    await asyncio.wait_for(worker.stop(), timeout=0.5)
    process.terminate.assert_called_once()
    process.kill.assert_called_once()
    assert worker.process is None


@pytest.mark.asyncio
async def test_unreaped_process_timeout_is_reported_and_handle_retained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ScriptWorkerExecutor, "_KILL_TIMEOUT", 0.01, raising=False)
    process = mock_process()
    process.wait.side_effect = asyncio.Event().wait
    worker = ScriptWorkerExecutor(["python"], {})
    worker.process = process
    with pytest.raises(asyncio.TimeoutError, match="123"):
        await asyncio.wait_for(worker.abort(), timeout=0.5)
    assert worker.process is process


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["stop", "abort"])
async def test_exit_between_status_check_and_signal_is_reaped(method: str) -> None:
    process = mock_process()

    def already_exited():
        process.returncode = 0
        raise ProcessLookupError

    process.terminate.side_effect = already_exited
    process.kill.side_effect = already_exited
    worker = ScriptWorkerExecutor(["python"], {})
    worker.process = process
    await getattr(worker, method)()
    process.wait.assert_awaited_once()
    assert worker.process is None


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["stop", "abort"])
async def test_signal_errors_are_reported_and_handle_retained(method: str) -> None:
    process = mock_process()
    process.terminate.side_effect = PermissionError("cannot terminate")
    process.kill.side_effect = PermissionError("cannot kill")
    worker = ScriptWorkerExecutor(["python"], {})
    worker.process = process
    with pytest.raises(PermissionError, match="cannot"):
        await getattr(worker, method)()
    assert worker.process is process


@pytest.mark.asyncio
async def test_concurrent_shutdowns_reap_once() -> None:
    process = mock_process()
    worker = ScriptWorkerExecutor(["python"], {})
    worker.process = process
    await asyncio.gather(worker.stop(), worker.abort(), worker.stop())
    assert worker.process is None
    process.terminate.assert_called_once()
    process.kill.assert_not_called()
    process.wait.assert_awaited_once()
