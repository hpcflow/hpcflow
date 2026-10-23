import multiprocessing
import os
import random

import pytest

from hpcflow.sdk.wait.completion import JobscriptCompletion

# use spawn for all platforms; fork on linux complains about possible deadlocks from
# multiple threads:
MP = multiprocessing.get_context("spawn")


def _mark_array_item(
    submissions_path,
    array_indices,
    array_idx,
    barrier,
):
    completion = JobscriptCompletion(
        submissions_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=array_indices,
    )

    barrier.wait(timeout=60)

    completion.mark_array_item_complete(array_idx)


def _mark_array_items(
    submissions_path,
    array_indices,
    items,
    barrier,
):
    completion = JobscriptCompletion(
        submissions_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=array_indices,
    )

    barrier.wait(timeout=60)

    for array_idx in items:
        completion.mark_array_item_complete(array_idx)


class SynchronisedJobscriptCompletion(JobscriptCompletion):
    def set_consolidation_sync(self, barrier, events):
        object.__setattr__(self, "_consolidation_barrier", barrier)
        object.__setattr__(self, "_consolidation_events", events)

    def _children_complete(self, node, children_path):
        self._consolidation_barrier.wait(timeout=5)

        complete = super()._children_complete(node, children_path)

        self._consolidation_events.put((os.getpid(), node.name, complete))

        return complete


def _mark_array_item_with_consolidation_barrier(
    submissions_path,
    array_indices,
    array_idx,
    start_barrier,
    consolidation_barrier,
    events,
):
    completion = SynchronisedJobscriptCompletion(
        submissions_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=array_indices,
    )
    completion.set_consolidation_sync(consolidation_barrier, events)

    start_barrier.wait(timeout=5)

    completion.mark_array_item_complete(array_idx)


@pytest.mark.integration
def test_concurrent_array_items_complete_jobscript(tmp_path):
    """Test that distinct concurrent array items can complete a jobscript.

    Two processes mark different array items complete concurrently, where
    together those items complete the jobscript. The resulting completion
    state should indicate that the whole jobscript is complete.
    """
    array_indices = tuple(range(10))

    completion = JobscriptCompletion(
        tmp_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=array_indices,
    )

    # everything except the final two items is already complete.
    for idx in range(8):
        completion.mark_array_item_complete(idx)

    assert not completion.is_complete()

    # two workers + this process.
    barrier = MP.Barrier(3)

    processes = [
        MP.Process(
            target=_mark_array_item,
            args=(tmp_path, array_indices, idx, barrier),
        )
        for idx in (8, 9)
    ]

    for process in processes:
        process.start()

    # neither child can enter mark_array_item_complete() until both children
    # and the parent have reached the barrier.
    barrier.wait(timeout=5)

    for process in processes:
        process.join(timeout=5)

    for process in processes:
        assert not process.is_alive()
        assert process.exitcode == 0

    assert completion.is_complete()
    assert not completion.array_path.exists()


@pytest.mark.integration
@pytest.mark.parametrize(
    ("num_items", "num_processes"),
    [
        (100, 20),
        (1_000, 30),
    ],
)
def test_array_completion_concurrent_stress(tmp_path, num_items, num_processes):
    """Stress-test completion-tree updates from concurrent array items.

    Multiple processes mark distinct array items complete concurrently,
    exercising creation and consolidation of completion state across the
    tree. All processes should finish successfully and the resulting state
    should indicate that the whole jobscript and each of its array items are
    complete.
    """
    array_indices = tuple(range(num_items))

    chunks = [list(array_indices[i::num_processes]) for i in range(num_processes)]
    for i, chunk in enumerate(chunks):
        random.Random(i).shuffle(chunk)

    barrier = MP.Barrier(num_processes + 1)

    processes = [
        MP.Process(
            target=_mark_array_items,
            args=(tmp_path, array_indices, chunk, barrier),
        )
        for chunk in chunks
    ]

    for process in processes:
        process.start()

    barrier.wait(timeout=30)

    for process in processes:
        process.join(timeout=30)

    for process in processes:
        assert not process.is_alive()
        assert process.exitcode == 0

    completion = JobscriptCompletion(
        tmp_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=array_indices,
    )

    assert completion.is_complete()

    for array_idx in array_indices:
        assert completion.is_array_item_complete(array_idx)


@pytest.mark.integration
def test_concurrent_array_item_consolidation_race(tmp_path):
    """Test concurrent consolidation of the same completion-tree node.

    Two processes complete distinct array items and are synchronised so that
    both observe their shared parent node as complete before either
    consolidates it. Both processes should be able to attempt consolidation
    concurrently without corrupting the completion state or failing.
    """
    array_indices = tuple(range(10))

    completion = JobscriptCompletion(
        tmp_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=array_indices,
    )

    # Leave two distinct array items incomplete.
    for idx in range(8):
        completion.mark_array_item_complete(idx)

    assert not completion.is_complete()

    # Parent participates so we know both processes have started.
    start_barrier = MP.Barrier(3)

    # Only the two workers participate here.
    consolidation_barrier = MP.Barrier(2)

    events = MP.Queue()
    processes = [
        MP.Process(
            target=_mark_array_item_with_consolidation_barrier,
            args=(
                tmp_path,
                array_indices,
                idx,
                start_barrier,
                consolidation_barrier,
                events,
            ),
        )
        for idx in (8, 9)
    ]

    for process in processes:
        process.start()

    start_barrier.wait(timeout=5)

    for process in processes:
        process.join(timeout=10)

    results = [events.get(timeout=1) for _ in range(2)]
    assert all(complete for _, _, complete in results)
    assert len({pid for pid, _, _ in results}) == 2

    for process in processes:
        assert not process.is_alive()
        assert process.exitcode == 0

    assert completion.is_complete()
