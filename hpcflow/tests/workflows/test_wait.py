from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING

import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.test_utils import (
    almost_submit,
    launch_forced_array_item,
    wait_for_array_item_completion,
)
from hpcflow.sdk.execution.jobscript_executor import JobscriptExecutor
from hpcflow.sdk.persistence.utils import atomic_write
from hpcflow.sdk.wait.protocol import WaitWakeup
from hpcflow.sdk.wait.run_wait import RunWaitEvent, RunWaitState
from hpcflow.sdk.wait.wait_client import WaitClient
from hpcflow.sdk.wait.wait_server import WaitServer
from hpcflow.tests.workflows.conftest import (
    assert_wk_1_repeats_success,
    assert_workflow_1_success,
)
from hpcflow.tests.unit.utils.test_patches import asyncio_timeout

if TYPE_CHECKING:
    from hpcflow.sdk.submission.jobscript import Jobscript


def fake_jobscript_submission(jobscript: Jobscript):
    """Make a jobscript look submitted."""
    jobscript._set_process_ID(12345 + jobscript.index)


def assert_no_wait_endpoints(
    workflow,
    sub_idx: int,
    js_idx: int,
) -> None:
    endpoints_path = JobscriptExecutor.get_wait_endpoints_path(
        workflow.submissions_path,
        sub_idx,
        js_idx,
    )
    assert not endpoints_path.exists() or not any(endpoints_path.iterdir())


@pytest.mark.asyncio
async def test_wait_server_start():
    server = WaitServer(advertise_host=hf.config.server_advertise_host)
    try:
        endpoint = await asyncio.wait_for(server.start(), timeout=1)
        assert endpoint
    finally:
        await server.stop()


async def wait_for_endpoint(
    workflow,
    sub_idx: int,
    js_idx: int,
    *,
    wait_task: asyncio.Task | None = None,
    timeout: float = 1,
) -> str:
    """Wait for a workflow waiter endpoint to be registered for a jobscript.

    Poll the jobscript's wait-endpoints directory until an endpoint is registered
    and return its value. If `wait_task` is provided, fail if the task completes
    before registering an endpoint. Raise `TimeoutError` if no endpoint is
    registered within `timeout` seconds.

    """
    endpoints_path = JobscriptExecutor.get_wait_endpoints_path(
        workflow.submissions_path,
        sub_idx,
        js_idx,
    )

    async with asyncio_timeout(timeout):
        while True:
            if wait_task is not None and wait_task.done():
                await wait_task
                raise AssertionError(
                    "Wait task completed before registering an endpoint."
                )

            if endpoints_path.exists():
                endpoint_paths = list(endpoints_path.iterdir())
                if endpoint_paths:
                    return endpoint_paths[0].read_text(encoding="utf-8")

            await asyncio.sleep(0.01)


async def wait_for_run_waiter(
    state: RunWaitState,
    run_id: int,
    event: RunWaitEvent,
    *,
    wait_task: asyncio.Task,
) -> None:
    while not state.get_waiter_ids(run_id, event):
        if wait_task.done():
            await wait_task
            raise AssertionError("Run wait completed before its waiter was registered.")
        await asyncio.sleep(0.001)


@pytest.mark.asyncio
async def test_wait_already_complete(workflow_1_add_sub):
    """Test wait returns immediately when the completion marker already exists."""
    sub = workflow_1_add_sub.submissions[0]
    js = sub.jobscripts[0]
    fake_jobscript_submission(js)
    js.completion_obj.mark_complete()
    await workflow_1_add_sub._wait(sub_js={sub.index: [js.index]}, quiet=True)
    assert_no_wait_endpoints(workflow_1_add_sub, sub.index, js.index)


@pytest.mark.asyncio
async def test_wait_completion_marker_fallback(workflow_1_add_sub, monkeypatch):
    """Test wait discovers completion from the filesystem without a ZMQ notification."""

    sub = workflow_1_add_sub.submissions[0]
    js = sub.jobscripts[0]
    fake_jobscript_submission(js)

    # query the filesystem frequently so the fallback path is quick to test.
    monkeypatch.setattr(workflow_1_add_sub, "WAIT_STATE_CHECK_INTERVAL", 0.01)

    wait_task = asyncio.create_task(
        workflow_1_add_sub._wait(sub_js={sub.index: [js.index]}, quiet=True)
    )

    # ensure the waiter is actually waiting before marking the jobscript complete.
    await wait_for_endpoint(
        workflow_1_add_sub,
        sub.index,
        js.index,
        wait_task=wait_task,
    )

    # do not send a ZMQ notification. The periodic filesystem check must discover this
    # marker:
    js.completion_obj.mark_complete()

    async with asyncio_timeout(1):
        await wait_task

    assert_no_wait_endpoints(workflow_1_add_sub, sub.index, js.index)


@pytest.mark.asyncio
async def test_wait_zmq_notification(
    workflow_1_add_sub,
    monkeypatch,
):
    """Test a ZMQ notification wakes the waiter without waiting for fallback polling."""

    sub = workflow_1_add_sub.submissions[0]
    js = sub.jobscripts[0]
    fake_jobscript_submission(js)

    # make filesystem fallback much longer than the test timeout. This means the test can
    # only finish promptly if the ZMQ notification wakes it:
    monkeypatch.setattr(workflow_1_add_sub, "WAIT_STATE_CHECK_INTERVAL", 60)

    wait_task = asyncio.create_task(
        workflow_1_add_sub._wait(sub_js={sub.index: [js.index]}, quiet=True)
    )

    endpoint = await wait_for_endpoint(
        workflow_1_add_sub,
        sub.index,
        js.index,
        wait_task=wait_task,
    )

    # the completion marker remains authoritative:
    js.completion_obj.mark_complete()

    # the ZMQ notification is just the fast wake-up mechanism:
    notification = WaitWakeup(
        submission_idx=sub.index,
        jobscript_idx=js.index,
    )
    assert await WaitClient(endpoint).notify(notification)

    async with asyncio_timeout(1):
        await wait_task

    assert_no_wait_endpoints(workflow_1_add_sub, sub.index, js.index)


@pytest.mark.asyncio
async def test_wait_cancellation_cleans_up(workflow_1_add_sub):
    """Test cancelling wait removes its registered endpoint."""
    sub = workflow_1_add_sub.submissions[0]
    js = sub.jobscripts[0]
    fake_jobscript_submission(js)

    wait_task = asyncio.create_task(
        workflow_1_add_sub._wait(sub_js={sub.index: [js.index]}, quiet=True)
    )

    # ensure cancellation happens while _wait() is actually waiting.
    await wait_for_endpoint(
        workflow_1_add_sub,
        sub.index,
        js.index,
        wait_task=wait_task,
    )

    wait_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await wait_task

    assert_no_wait_endpoints(workflow_1_add_sub, sub.index, js.index)


@pytest.mark.asyncio
async def test_wait_multiple_jobscripts(workflow_2_sub):
    """Test completion of one jobscript does not cause wait to return while another
    requested jobscript is still pending."""
    sub = workflow_2_sub
    workflow = sub.workflow
    js_0, js_1 = sub.jobscripts[:2]

    fake_jobscript_submission(js_0)
    fake_jobscript_submission(js_1)

    wait_task = asyncio.create_task(
        workflow._wait(sub_js={sub.index: [js_0.index, js_1.index]}, quiet=True)
    )

    endpoint_0 = await wait_for_endpoint(
        workflow,
        sub.index,
        js_0.index,
        wait_task=wait_task,
    )
    await wait_for_endpoint(
        workflow,
        sub.index,
        js_1.index,
        wait_task=wait_task,
    )

    # complete only the first jobscript.
    js_0.completion_obj.mark_complete()

    await WaitClient(endpoint_0).notify(
        WaitWakeup(submission_idx=sub.index, jobscript_idx=js_0.index)
    )

    # the second jobscript is still pending.
    await asyncio.sleep(0.05)
    assert not wait_task.done()

    # now complete the second.
    js_1.completion_obj.mark_complete()

    # either endpoint can wake the common WaitServer.
    endpoint_1 = await wait_for_endpoint(
        workflow,
        sub.index,
        js_1.index,
        wait_task=wait_task,
    )
    await WaitClient(endpoint_1).notify(
        WaitWakeup(submission_idx=sub.index, jobscript_idx=js_1.index)
    )

    async with asyncio_timeout(1):
        await wait_task

    assert_no_wait_endpoints(workflow, sub.index, js_0.index)
    assert_no_wait_endpoints(workflow, sub.index, js_1.index)


@pytest.mark.asyncio
async def test_multiple_waiters(workflow_1_add_sub):
    sub = workflow_1_add_sub.submissions[0]
    js = sub.jobscripts[0]
    fake_jobscript_submission(js)

    sub_js = {sub.index: [js.index]}

    wait_task_1 = asyncio.create_task(workflow_1_add_sub._wait(sub_js=sub_js, quiet=True))
    wait_task_2 = asyncio.create_task(workflow_1_add_sub._wait(sub_js=sub_js, quiet=True))

    endpoints_path = JobscriptExecutor.get_wait_endpoints_path(
        workflow_1_add_sub.submissions_path,
        sub.index,
        js.index,
    )

    async with asyncio_timeout(1):
        while not endpoints_path.exists() or len(list(endpoints_path.iterdir())) != 2:
            await asyncio.sleep(0.01)

    endpoints = [path.read_text(encoding="utf-8") for path in endpoints_path.iterdir()]

    js.completion_obj.mark_complete()

    notification = WaitWakeup(submission_idx=sub.index, jobscript_idx=js.index)

    for endpoint in endpoints:
        assert await WaitClient(endpoint).notify(notification)

    async with asyncio_timeout(1):
        await asyncio.gather(wait_task_1, wait_task_2)

    assert_no_wait_endpoints(workflow_1_add_sub, sub.index, js.index)


@pytest.mark.asyncio
async def test_wait_unsubmitted_jobscript(workflow_1_add_sub):
    sub = workflow_1_add_sub.submissions[0]
    js = sub.jobscripts[0]
    with pytest.raises(RuntimeError, match="submitted"):
        await workflow_1_add_sub._wait(sub_js={sub.index: [js.index]}, quiet=True)


@pytest.mark.integration
def test_workflow_submit_then_wait(workflow_1):
    workflow_1.submit(wait=False, status=False, add_to_known=False, quiet=True)
    workflow_1.wait(quiet=True)
    assert_workflow_1_success(workflow_1)


@pytest.mark.integration
def test_workflow_submit_with_wait(workflow_1):
    workflow_1.submit(wait=True, status=False, add_to_known=False, quiet=True)
    assert_workflow_1_success(workflow_1)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_workflow_wait_with_running_event_loop(workflow_1):
    """Test synchronous wait works when the calling thread has a running event loop."""
    workflow_1.submit(wait=False, status=False, add_to_known=False)
    # this is deliberately the synchronous public API. The current thread already has a
    # running asyncio event loop, as it would in Jupyter:
    workflow_1.wait(quiet=True)
    assert_workflow_1_success(workflow_1)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_workflow_submit_with_wait_with_running_event_loop(workflow_1):
    """Test submit(wait=True) works when the calling thread has a running event loop."""
    workflow_1.submit(wait=True, status=False, add_to_known=False, quiet=True)
    assert_workflow_1_success(workflow_1)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_wait_for_forced_array_execution(wk_1_repeats_2):
    """Test waiting for a job array to complete.

    Execute a forced direct array out of order and verify that the workflow waiter remains
    blocked as individual array items complete. Once the final array item completes,
    verify that the array completion state is consolidated to the jobscript-level
    completion marker and the waiter is released.

    """
    prepared = almost_submit(wk_1_repeats_2, force_array=True, status=False)
    sub = wk_1_repeats_2.submissions[-1]
    js = sub.jobscripts[0]

    assert js.array_indices is not None

    # the forced direct array is deliberately not submitted through Jobscript.submit(),
    # so give _wait() the submitted-state indication it normally gets from submission.
    js._set_process_ID(os.getpid())

    completion = js.completion_obj
    array_indices = tuple(reversed(js.array_indices))

    wait_task = asyncio.create_task(
        wk_1_repeats_2._wait(sub_js={sub.index: [js.index]}, quiet=True)
    )

    await wait_for_endpoint(
        wk_1_repeats_2,
        sub.index,
        js.index,
        wait_task=wait_task,
    )

    assert not wait_task.done()

    for array_idx in array_indices[:-1]:
        launch_forced_array_item(js, prepared, array_idx)
        await wait_for_array_item_completion(completion, array_idx)

        assert not completion.is_complete()
        assert not wait_task.done()

    final_idx = array_indices[-1]
    launch_forced_array_item(js, prepared, final_idx)

    await asyncio.wait_for(wait_task, timeout=10)

    assert completion.is_complete()
    assert not completion.array_path.exists()
    assert_wk_1_repeats_success(wk_1_repeats_2)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_wait_for_already_completed_forced_array(wk_1_repeats_2):
    """Test waiting for a forced direct array that has already completed."""

    prepared = almost_submit(wk_1_repeats_2, force_array=True, status=False)
    sub = wk_1_repeats_2.submissions[-1]
    js = sub.jobscripts[0]

    assert js.array_indices is not None

    # the forced direct array is deliberately not submitted through Jobscript.submit(),
    # so give _wait() the submitted-state indication it normally gets from submission.
    js._set_process_ID(os.getpid())

    completion = js.completion_obj

    for array_idx in reversed(js.array_indices):
        launch_forced_array_item(js, prepared, array_idx)
        await wait_for_array_item_completion(completion, array_idx)

    assert completion.is_complete()

    await asyncio.wait_for(
        wk_1_repeats_2._wait(
            sub_js={sub.index: [js.index]},
            quiet=True,
        ),
        timeout=1,
    )

    assert_wk_1_repeats_success(wk_1_repeats_2)


@pytest.mark.integration
def test_wait_for_runs_end(workflow_1):
    workflow_1.submit(wait=False, add_to_known=False, status=False)
    workflow_1.wait_for_runs([0], event=RunWaitEvent.END)
    run = workflow_1.get_EARs_from_IDs([0])[0]
    assert run.status in EARStatus.get_terminal_states()


@pytest.mark.integration
def test_wait_for_multiple_runs_end(wk_1_repeats_2):
    wk_1_repeats_2.submit(wait=False, add_to_known=False, status=False)
    wk_1_repeats_2.wait_for_runs([0, 1], event=RunWaitEvent.END)
    runs = wk_1_repeats_2.get_EARs_from_IDs([0, 1])
    assert all(run.status in EARStatus.get_terminal_states() for run in runs)


@pytest.mark.integration
def test_wait_for_runs_already_ended(workflow_1):
    workflow_1.submit(wait=True, add_to_known=False, status=False)
    # there is no active executor to send a wake-up here. This must return from the
    # authoritative run status check:
    workflow_1.wait_for_runs([0], event=RunWaitEvent.END)
    run = workflow_1.get_EARs_from_IDs([0])[0]
    assert run.status in EARStatus.get_terminal_states()


@pytest.mark.integration
def test_wait_for_run_start_already_ended(workflow_1):
    workflow_1.submit(wait=True, add_to_known=False, status=False)
    workflow_1.wait_for_runs([0], event=RunWaitEvent.START)


@pytest.mark.integration
def test_wait_for_run_start_when_skipped(wk_skip):
    wk_skip.submit(wait=True, add_to_known=False, status=False)
    run = wk_skip.get_EARs_from_IDs([1])[0]
    assert run.status is EARStatus.skipped
    wk_skip.wait_for_runs([1], event=RunWaitEvent.START)


@pytest.mark.asyncio
async def test_wait_for_runs_completion_marker_fallback(workflow_1_add_sub, monkeypatch):
    """Test run wait discovers completion from the filesystem without a ZMQ
    notification."""

    sub = workflow_1_add_sub.submissions[0]
    js = sub.jobscripts[0]
    fake_jobscript_submission(js)

    run = workflow_1_add_sub.get_EARs_from_IDs([0])[0]

    # query the filesystem frequently so the fallback path is quick to test.
    monkeypatch.setattr(workflow_1_add_sub, "WAIT_STATE_CHECK_INTERVAL", 0.01)

    wait_task = asyncio.create_task(
        workflow_1_add_sub._wait_for_runs(
            run_ids=[run.id_],
            event=RunWaitEvent.END,
            quiet=True,
        )
    )

    run_wait_state = workflow_1_add_sub._get_run_wait_state(run)

    # ensure the run waiter has actually been registered before completing it.
    await wait_for_run_waiter(
        run_wait_state,
        run.id_,
        RunWaitEvent.END,
        wait_task=wait_task,
    )

    # do not send a ZMQ notification; the periodic filesystem check must discover this
    # completion marker:
    run_wait_state.complete(run.id_, RunWaitEvent.END)

    async with asyncio_timeout(1):
        await wait_task

    assert run_wait_state.get_waiter_ids(run.id_, RunWaitEvent.END) == ()
    assert_no_wait_endpoints(workflow_1_add_sub, sub.index, js.index)


@pytest.mark.asyncio
async def test_wait_for_runs_zmq_notification(workflow_1_add_sub, monkeypatch):
    """Test a ZMQ notification wakes a run waiter without fallback polling."""

    sub = workflow_1_add_sub.submissions[0]
    js = sub.jobscripts[0]
    fake_jobscript_submission(js)

    run = workflow_1_add_sub.get_EARs_from_IDs([0])[0]

    # make filesystem fallback much longer than the test timeout; this means the test can
    # only finish promptly if the ZMQ notification wakes it:
    monkeypatch.setattr(workflow_1_add_sub, "WAIT_STATE_CHECK_INTERVAL", 60)

    wait_task = asyncio.create_task(
        workflow_1_add_sub._wait_for_runs(
            run_ids=[run.id_],
            event=RunWaitEvent.END,
            quiet=True,
        )
    )

    endpoint = await wait_for_endpoint(
        workflow_1_add_sub,
        sub.index,
        js.index,
        wait_task=wait_task,
    )

    run_wait_state = workflow_1_add_sub._get_run_wait_state(run)

    await wait_for_run_waiter(
        run_wait_state,
        run.id_,
        RunWaitEvent.END,
        wait_task=wait_task,
    )

    # The filesystem completion marker remains authoritative:
    waiter_ids = run_wait_state.complete(run.id_, RunWaitEvent.END)

    assert len(waiter_ids) == 1

    # The ZMQ notification is just the fast wake-up mechanism:
    notification = WaitWakeup(submission_idx=sub.index, jobscript_idx=js.index)
    assert await WaitClient(endpoint).notify(notification)

    async with asyncio_timeout(1):
        await wait_task

    assert_no_wait_endpoints(workflow_1_add_sub, sub.index, js.index)


@pytest.mark.asyncio
async def test_wait_for_multiple_runs(wk_1_reps_2_add_sub, monkeypatch):
    """Test wait_for_runs waits until all requested runs are complete."""

    sub = wk_1_reps_2_add_sub.submissions[0]
    js = sub.jobscripts[0]
    fake_jobscript_submission(js)

    runs = wk_1_reps_2_add_sub.get_EARs_from_IDs([0, 1])

    monkeypatch.setattr(wk_1_reps_2_add_sub, "WAIT_STATE_CHECK_INTERVAL", 0.01)

    wait_task = asyncio.create_task(
        wk_1_reps_2_add_sub._wait_for_runs(
            run_ids=[run.id_ for run in runs],
            event=RunWaitEvent.END,
            quiet=True,
        )
    )

    states = [wk_1_reps_2_add_sub._get_run_wait_state(run) for run in runs]

    for run, state in zip(runs, states):
        await wait_for_run_waiter(state, run.id_, RunWaitEvent.END, wait_task=wait_task)

    # completing only the first run must not satisfy the wait.
    states[0].complete(runs[0].id_, RunWaitEvent.END)

    await asyncio.sleep(0.05)
    assert not wait_task.done()

    # once the second run completes, the wait can finish.
    states[1].complete(runs[1].id_, RunWaitEvent.END)

    async with asyncio_timeout(1):
        await wait_task


@pytest.mark.asyncio
async def test_wait_for_run_start(workflow_1_add_sub, monkeypatch):
    """Test wait_for_runs can wait for a run to start."""

    sub = workflow_1_add_sub.submissions[0]
    js = sub.jobscripts[0]
    fake_jobscript_submission(js)

    run = workflow_1_add_sub.get_EARs_from_IDs([0])[0]

    # query the filesystem frequently so the fallback path is quick to test.
    monkeypatch.setattr(workflow_1_add_sub, "WAIT_STATE_CHECK_INTERVAL", 0.01)

    wait_task = asyncio.create_task(
        workflow_1_add_sub._wait_for_runs(
            run_ids=[run.id_],
            event=RunWaitEvent.START,
            quiet=True,
        )
    )

    run_wait_state = workflow_1_add_sub._get_run_wait_state(run)

    await wait_for_run_waiter(
        run_wait_state,
        run.id_,
        RunWaitEvent.START,
        wait_task=wait_task,
    )

    # simulate the executor marking the run as started; do not send a ZMQ notification;
    # the fallback filesystem check must discover it.
    run_wait_state.complete(run.id_, RunWaitEvent.START)

    async with asyncio_timeout(1):
        await wait_task

    assert run_wait_state.get_waiter_ids(run.id_, RunWaitEvent.START) == ()

    assert_no_wait_endpoints(workflow_1_add_sub, sub.index, js.index)


@pytest.mark.asyncio
async def test_wait_for_runs_cancellation_cleanup(workflow_1_add_sub):
    """Test cancelling wait_for_runs removes its wait registrations."""

    sub = workflow_1_add_sub.submissions[0]
    js = sub.jobscripts[0]
    fake_jobscript_submission(js)

    run = workflow_1_add_sub.get_EARs_from_IDs([0])[0]

    wait_task = asyncio.create_task(
        workflow_1_add_sub._wait_for_runs(
            run_ids=[run.id_],
            event=RunWaitEvent.END,
            quiet=True,
        )
    )

    run_wait_state = workflow_1_add_sub._get_run_wait_state(run)

    # wait until both registrations have been made before cancelling.
    await wait_for_endpoint(
        workflow_1_add_sub,
        sub.index,
        js.index,
        wait_task=wait_task,
    )

    await wait_for_run_waiter(
        run_wait_state,
        run.id_,
        RunWaitEvent.END,
        wait_task=wait_task,
    )

    wait_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await wait_task

    assert run_wait_state.get_waiter_ids(run.id_, RunWaitEvent.END) == ()

    assert_no_wait_endpoints(workflow_1_add_sub, sub.index, js.index)


@pytest.mark.asyncio
async def test_wait_for_runs_cancellation_does_not_affect_other_waiter(
    workflow_1_add_sub,
    monkeypatch,
):
    """Test cancelling one run waiter does not affect another waiter."""

    sub = workflow_1_add_sub.submissions[0]
    js = sub.jobscripts[0]
    fake_jobscript_submission(js)

    run = workflow_1_add_sub.get_EARs_from_IDs([0])[0]

    monkeypatch.setattr(workflow_1_add_sub, "WAIT_STATE_CHECK_INTERVAL", 0.01)

    wait_task_1 = asyncio.create_task(
        workflow_1_add_sub._wait_for_runs(
            run_ids=[run.id_],
            event=RunWaitEvent.END,
            quiet=True,
        )
    )
    wait_task_2 = asyncio.create_task(
        workflow_1_add_sub._wait_for_runs(
            run_ids=[run.id_],
            event=RunWaitEvent.END,
            quiet=True,
        )
    )

    run_wait_state = workflow_1_add_sub._get_run_wait_state(run)

    # wait until both waiters have registered for the run:
    async with asyncio_timeout(1):
        while len(run_wait_state.get_waiter_ids(run.id_, RunWaitEvent.END)) < 2:
            if wait_task_1.done():
                await wait_task_1
                raise AssertionError(
                    "First wait completed before both waiters were registered."
                )
            if wait_task_2.done():
                await wait_task_2
                raise AssertionError(
                    "Second wait completed before both waiters were registered."
                )

            await asyncio.sleep(0.001)

    waiter_ids = set(run_wait_state.get_waiter_ids(run.id_, RunWaitEvent.END))
    assert len(waiter_ids) == 2

    # cancelling one wait must remove only its own registration
    wait_task_1.cancel()

    with pytest.raises(asyncio.CancelledError):
        await wait_task_1

    remaining_waiter_ids = set(run_wait_state.get_waiter_ids(run.id_, RunWaitEvent.END))

    assert len(remaining_waiter_ids) == 1
    assert remaining_waiter_ids < waiter_ids
    assert not wait_task_2.done()

    # complete the run without sending a ZMQ notification; the remaining waiter should
    # discover the completion through fallback polling:
    completed_waiter_ids = set(run_wait_state.complete(run.id_, RunWaitEvent.END))

    assert completed_waiter_ids == remaining_waiter_ids

    async with asyncio_timeout(1):
        await wait_task_2

    assert run_wait_state.get_waiter_ids(run.id_, RunWaitEvent.END) == ()
    assert_no_wait_endpoints(workflow_1_add_sub, sub.index, js.index)


@pytest.mark.integration
def test_wait_for_run_start_while_running(wk_sleep):
    """Test waiting for START returns while the run is still executing."""

    wk_sleep.submit(wait=False, add_to_known=False, status=False)

    try:
        wk_sleep.wait_for_runs([0], event=RunWaitEvent.START)
        run = wk_sleep.get_EARs_from_IDs([0])[0]
        assert run.status is EARStatus.running

    finally:
        # don't leave the workflow running when the test returns.
        wk_sleep.wait(quiet=True)

    run = wk_sleep.get_EARs_from_IDs([0])[0]
    assert run.status is EARStatus.success


@pytest.mark.integration
@pytest.mark.asyncio
async def test_wait_for_runs_with_running_event_loop(workflow_1):
    """Test synchronous wait_for_runs works with a running event loop."""

    workflow_1.submit(wait=False, status=False, add_to_known=False)

    # deliberately call the synchronous public API from a thread that already has a
    # running asyncio event loop, as in Jupyter:
    workflow_1.wait_for_runs([0], event=RunWaitEvent.END, quiet=True)

    run = workflow_1.get_EARs_from_IDs([0])[0]
    assert run.status in EARStatus.get_terminal_states()
