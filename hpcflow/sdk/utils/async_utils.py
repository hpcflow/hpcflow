from __future__ import annotations

import asyncio
from collections.abc import Coroutine, Iterator
from contextlib import contextmanager
import os
from threading import Event, Thread
from typing import Any, TypeVar
import warnings

import zmq.asyncio

T = TypeVar("T")


async def await_cleanup(task: asyncio.Future[T]) -> T:
    """Finish a scheduled cleanup before propagating caller cancellation.

    The cleanup must bound its own waits. Cleanup failures take precedence over
    cancellation so that failures to release resources remain visible.
    """
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True

    result = task.result()
    if cancelled:
        raise asyncio.CancelledError
    return result


def run_coroutine_sync(coro: Coroutine[Any, Any, T]) -> T:
    """Run a coroutine synchronously in a dedicated thread.

    The coroutine is run in its own asyncio event loop, allowing this function to be
    called when the calling thread already has a running event loop, such as in a Jupyter
    notebook.

    If the calling thread receives ``KeyboardInterrupt``, the coroutine is cancelled and
    this function waits for cancellation and asynchronous cleanup to complete before
    re-raising the interrupt.
    """

    ready = Event()  # set when we are ready to join the thread

    loop: asyncio.AbstractEventLoop | None = None
    task: asyncio.Task[T] | None = None
    result: T | None = None
    exception: BaseException | None = None

    def run() -> None:
        nonlocal loop, task, result, exception

        async def runner() -> None:
            nonlocal loop, task, result

            loop = asyncio.get_running_loop()
            task = asyncio.current_task()
            ready.set()
            result = await coro

        try:
            asyncio.run(runner())
        except BaseException as exc:
            exception = exc
            ready.set()

    thread = Thread(target=run)
    thread.start()

    # don't enter the interruptible join loop until either the task has been established
    # or startup has failed:
    ready.wait()

    try:
        while thread.is_alive():
            thread.join(timeout=1)

    except KeyboardInterrupt:
        if loop is not None and task is not None:
            loop.call_soon_threadsafe(task.cancel)

        # allow the coroutine's finally blocks and other asynchronous cleanup to finish
        # before returning control to the caller:
        while thread.is_alive():
            thread.join(timeout=1)

        raise

    if exception is not None:
        raise exception

    return result  # type: ignore[return-value]


@contextmanager
def _zmq_compatibility_warnings() -> Iterator[None]:
    with warnings.catch_warnings():
        if os.name == "nt":
            warnings.filterwarnings(
                "ignore",
                message=(
                    r"Proactor event loop does not implement add_reader "
                    r"family of methods required for zmq\..*"
                ),
                category=RuntimeWarning,
            )
        yield


async def send_json(socket: zmq.asyncio.Socket, data: Any) -> None:
    """Send JSON, suppressing pyzmq's Windows selector-thread fallback warning."""
    with _zmq_compatibility_warnings():
        await socket.send_json(data)


async def recv_json(socket: zmq.asyncio.Socket) -> Any:
    """Receive JSON, suppressing pyzmq's Windows selector-thread fallback warning.

    Keep the Proactor event loop because asyncio subprocesses require it.
    """
    assert socket is not None
    with _zmq_compatibility_warnings():
        return await socket.recv_json()
