import asyncio
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from hpcflow.sdk.utils.patches import resolve_path


def test_absolute_path():
    # TODO: seems to be unused
    assert resolve_path("my_file_path").is_absolute()


if sys.version_info >= (3, 11):
    asyncio_timeout = asyncio.timeout
else:

    @asynccontextmanager
    async def asyncio_timeout(delay: float | None) -> AsyncIterator[None]:
        """Python 3.10 compatibility shim for asyncio.timeout.

        TODO: remove when Python 3.10 support is dropped and use asyncio.timeout directly.
        """
        if delay is None:
            yield
            return

        task = asyncio.current_task()
        assert task is not None

        loop = asyncio.get_running_loop()
        timed_out = False

        def cancel_task() -> None:
            nonlocal timed_out
            timed_out = True
            task.cancel()

        handle = loop.call_later(delay, cancel_task)

        try:
            yield
        except asyncio.CancelledError as exc:
            if timed_out:
                raise asyncio.TimeoutError from exc
            raise
        finally:
            handle.cancel()
