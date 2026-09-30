import asyncio

import pytest

from hpcflow.sdk.utils.async_utils import run_coroutine_sync


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
