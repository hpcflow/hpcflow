import asyncio

import pytest

from hpcflow.sdk.utils.async_utils import (
    await_cleanup,
    run_coroutine_sync,
    wait_for_cancellable,
)


def test_run_coroutine_sync():
    async def func():
        await asyncio.sleep(0)
        return 1234

    assert run_coroutine_sync(func()) == 1234


def test_run_coroutine_sync_exception():
    class TestError(Exception):
        pass

    async def func():
        raise TestError("test")

    with pytest.raises(TestError, match="test"):
        run_coroutine_sync(func())


@pytest.mark.asyncio
async def test_run_coroutine_sync_with_running_event_loop():
    async def func():
        await asyncio.sleep(0)
        return 1234

    assert run_coroutine_sync(func()) == 1234


@pytest.mark.asyncio
async def test_await_cleanup_returns_result() -> None:
    assert await await_cleanup(asyncio.create_task(asyncio.sleep(0, result=123))) == 123


@pytest.mark.asyncio
@pytest.mark.parametrize("fail", [False, True])
async def test_await_cleanup_defers_repeated_cancellation_and_surfaces_errors(
    fail: bool,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def cleanup() -> None:
        entered.set()
        await release.wait()
        finished.set()
        if fail:
            raise RuntimeError("cleanup failed")

    task = asyncio.create_task(await_cleanup(asyncio.create_task(cleanup())))
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    if fail:
        with pytest.raises(RuntimeError, match="cleanup failed"):
            await task
    else:
        with pytest.raises(asyncio.CancelledError):
            await task
    assert finished.is_set()


@pytest.mark.asyncio
async def test_await_cleanup_propagates_cancelled_cleanup_task() -> None:
    cleanup = asyncio.create_task(asyncio.sleep(0))
    cleanup.cancel()
    with pytest.raises(asyncio.CancelledError):
        await await_cleanup(cleanup)


@pytest.mark.asyncio
async def test_wait_for_cancellable_returns_result() -> None:
    assert await wait_for_cancellable(asyncio.sleep(0, result=123), timeout=5) == 123


@pytest.mark.asyncio
async def test_wait_for_cancellable_times_out_and_cancels_inner() -> None:
    inner = asyncio.create_task(asyncio.sleep(10))
    with pytest.raises(asyncio.TimeoutError):
        await wait_for_cancellable(inner, timeout=0.01)
    assert inner.cancelled()


@pytest.mark.asyncio
async def test_wait_for_cancellable_propagates_cancellation_racing_completion() -> None:
    # `asyncio.wait_for` swallows this cancellation before Python 3.12.
    started = asyncio.Event()

    async def inner() -> str:
        started.set()
        await asyncio.sleep(0)
        return "done"

    task = asyncio.create_task(wait_for_cancellable(inner(), timeout=5))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
