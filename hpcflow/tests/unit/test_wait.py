import asyncio
import errno
import logging

import pytest

from hpcflow.sdk.wait.protocol import WaitWakeup
from hpcflow.sdk.wait.run_wait import RunWaitEvent, RunWaitState
from hpcflow.sdk.wait.wait_client import WaitClient
from hpcflow.sdk.wait.wait_server import WaitServer


@pytest.mark.asyncio
async def test_wait_notification():
    server = WaitServer(advertise_host="127.0.0.1")
    endpoint = await server.start()
    client = WaitClient(endpoint)
    notification = WaitWakeup(submission_idx=2, jobscript_idx=3)
    notify_task = asyncio.create_task(client.notify(notification))
    received = await server.wait(timeout=1)
    assert received == notification
    assert await notify_task is True
    await server.stop()


@pytest.mark.asyncio
async def test_wait_timeout():
    server = WaitServer(advertise_host="127.0.0.1")
    await server.start()
    try:
        assert await server.wait(timeout=0.01) is None
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_wait_client_timeout():
    # TODO: acquire/release a random port
    client = WaitClient("tcp://127.0.0.1:54321", timeout=0.01)
    received = await client.notify(WaitWakeup(submission_idx=2, jobscript_idx=3))
    assert received is False


def test_register_run_waiter(tmp_path):
    """Test we can register a run-end waiter."""
    state = RunWaitState(tmp_path, 0, 0)
    assert not state.register(123, RunWaitEvent.END, "A")
    assert (state.get_waiters_path(123, RunWaitEvent.END) / "A").is_file()


def test_multiple_run_waiters(tmp_path):
    """Check we can register multiple run waiters on a given run."""
    state = RunWaitState(tmp_path, 0, 0)
    state.register(123, RunWaitEvent.END, "A")
    state.register(123, RunWaitEvent.END, "B")
    assert set(state.get_waiter_ids(123, RunWaitEvent.END)) == {
        "A",
        "B",
    }


def test_complete_run_event(tmp_path):
    state = RunWaitState(tmp_path, 0, 0)
    state.register(123, RunWaitEvent.END, "A")
    state.register(123, RunWaitEvent.END, "B")
    waiter_ids = state.complete(123, RunWaitEvent.END)

    assert set(waiter_ids) == {"A", "B"}
    assert state.is_complete(123, RunWaitEvent.END)
    assert not state.get_event_path(123, RunWaitEvent.END).exists()


def test_complete_run_event_survives_einval_cleanup(tmp_path, monkeypatch, caplog):
    state = RunWaitState(tmp_path, 0, 0)
    state.register(123, RunWaitEvent.END, "A")
    state.register(123, RunWaitEvent.END, "B")
    event_path = state.get_event_path(123, RunWaitEvent.END)

    def fail_claim(src, dst):
        assert state.is_complete(123, RunWaitEvent.END)
        raise OSError(errno.EINVAL, "Invalid argument")

    monkeypatch.setattr("hpcflow.sdk.wait.utils.os.replace", fail_claim)
    with caplog.at_level(logging.WARNING, logger="hpcflow.sdk.wait.utils"):
        assert set(state.complete(123, RunWaitEvent.END)) == {"A", "B"}

    assert state.is_complete(123, RunWaitEvent.END)
    assert state.register(123, RunWaitEvent.END, "C")
    assert event_path.is_dir()
    records = [
        record for record in caplog.records if record.name == "hpcflow.sdk.wait.utils"
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    assert str(event_path) in records[0].getMessage()
    assert "Invalid argument" in records[0].getMessage()


def test_register_completed_run_event(tmp_path):
    state = RunWaitState(tmp_path, 0, 0)

    state.register(123, RunWaitEvent.END, "A")
    state.complete(123, RunWaitEvent.END)

    assert state.register(123, RunWaitEvent.END, "B")
    assert not (state.get_waiters_path(123, RunWaitEvent.END) / "B").exists()


def test_start_and_end_are_independent(tmp_path):
    state = RunWaitState(tmp_path, 0, 0)

    state.register(123, RunWaitEvent.START, "A")
    state.register(123, RunWaitEvent.END, "B")

    assert state.complete(123, RunWaitEvent.START) == ("A",)

    assert state.is_complete(123, RunWaitEvent.START)
    assert not state.is_complete(123, RunWaitEvent.END)
    assert state.get_waiter_ids(123, RunWaitEvent.END) == ("B",)
