from __future__ import annotations

import asyncio
from collections.abc import Coroutine
import os
from threading import Event, Thread
from typing import Any, TypeVar
import warnings

import zmq.asyncio

T = TypeVar("T")


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


async def recv_json(socket: zmq.asyncio.Socket) -> Any:
    """Receive JSON on the socket, filtering an event-loop warning on Windows.

    Notes
    -----
    On Windows, pyzmq falls back to a selector thread when used with the Proactor
    event loop. We retain the Proactor loop because asyncio subprocess support
    requires it, and suppress pyzmq's compatibility warning here so that it does not
    leak into jobscript stderr.

    """
    assert socket is not None

    if os.name != "nt":
        return await socket.recv_json()

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=(
                r"Proactor event loop does not implement add_reader "
                r"family of methods required for zmq\..*"
            ),
            category=RuntimeWarning,
        )
        return await socket.recv_json()
