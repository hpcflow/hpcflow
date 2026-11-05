import errno
import logging
import os
from pathlib import Path

import pytest

from hpcflow.sdk.wait.completion import ArrayCompletionTree, JobscriptCompletion
from hpcflow.sdk.wait.utils import remove_superseded_directory


def node_ranges(nodes):
    return [(node.start, node.stop) for node in nodes]


def test_array_completion_tree_single_item():
    tree = ArrayCompletionTree.from_indices([0])

    assert node_ranges(tree.path_for(0)) == [(0, 0)]


def test_array_completion_tree_ten_items():
    tree = ArrayCompletionTree.from_indices(range(10))

    assert node_ranges(tree.path_for(6)) == [(0, 9), (6, 6)]


def test_array_completion_tree_500_items():
    tree = ArrayCompletionTree.from_indices(range(500))
    assert node_ranges(tree.path_for(62)) == [
        (0, 499),
        (50, 99),
        (60, 64),
        (62, 62),
    ]


@pytest.mark.parametrize(
    ("array_idx", "expected"),
    [
        (0, [(0, 499), (0, 49), (0, 4), (0, 0)]),
        (49, [(0, 499), (0, 49), (45, 49), (49, 49)]),
        (50, [(0, 499), (50, 99), (50, 54), (50, 50)]),
        (62, [(0, 499), (50, 99), (60, 64), (62, 62)]),
        (499, [(0, 499), (450, 499), (495, 499), (499, 499)]),
    ],
)
def test_array_completion_tree_paths(array_idx, expected):
    tree = ArrayCompletionTree.from_indices(range(500))
    assert node_ranges(tree.path_for(array_idx)) == expected


def test_array_completion_tree_non_power_of_ten():
    tree = ArrayCompletionTree.from_indices(range(23))
    assert len(tree.root.children) == 10
    child_sizes = [child.stop - child.start + 1 for child in tree.root.children]
    assert child_sizes == [3, 3, 3, 2, 2, 2, 2, 2, 2, 2]


def test_array_completion_tree_sparse_indices():
    tree = ArrayCompletionTree.from_indices([0, 10, 20, 30, 40])
    assert node_ranges(tree.path_for(20)) == [(0, 40), (20, 20)]
    with pytest.raises(ValueError, match="15"):
        tree.path_for(15)


def test_array_completion_tree_rejects_empty_indices():
    with pytest.raises(ValueError, match="at least one"):
        ArrayCompletionTree.from_indices([])


def test_array_completion_tree_rejects_unsorted_indices():
    with pytest.raises(ValueError, match="sorted"):
        ArrayCompletionTree.from_indices([0, 2, 1])


def test_array_completion_tree_rejects_duplicate_indices():
    with pytest.raises(ValueError, match="unique"):
        ArrayCompletionTree.from_indices([0, 1, 1, 2])


def test_array_completion_tree_rejects_invalid_fanout():
    with pytest.raises(ValueError, match="fanout"):
        ArrayCompletionTree.from_indices(range(10), fanout=1)


def test_array_item_completion_paths(tmp_path):
    completion = JobscriptCompletion(
        tmp_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=tuple(range(500)),
    )

    paths = completion.get_array_item_completion_paths(62)
    state_path = tmp_path / "0" / "js_state" / "0"
    assert paths == (
        state_path / "complete",
        state_path / "array" / "50-99_complete",
        state_path / "array" / "50-99" / "60-64_complete",
        state_path / "array" / "50-99" / "60-64" / "62_complete",
    )


@pytest.mark.parametrize("marker_idx", range(4))
def test_array_item_complete_from_ancestor_marker(tmp_path, marker_idx):
    completion = JobscriptCompletion(
        tmp_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=tuple(range(500)),
    )
    paths = completion.get_array_item_completion_paths(62)
    assert not completion.is_array_item_complete(62)

    paths[marker_idx].parent.mkdir(parents=True, exist_ok=True)
    paths[marker_idx].touch()
    assert completion.is_array_item_complete(62)


def test_unrelated_array_item_not_complete(tmp_path):
    completion = JobscriptCompletion(
        tmp_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=tuple(range(500)),
    )

    path = completion.get_array_item_completion_paths(62)[-1]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    assert completion.is_array_item_complete(62)
    assert not completion.is_array_item_complete(63)


def test_mark_array_item_complete(tmp_path):
    completion = JobscriptCompletion(
        tmp_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=tuple(range(500)),
    )

    completion.mark_array_item_complete(62)
    paths = completion.get_array_item_completion_paths(62)

    assert not paths[0].exists()
    assert not paths[1].exists()
    assert not paths[2].exists()
    assert paths[3].exists()

    assert completion.is_array_item_complete(62)
    assert not completion.is_array_item_complete(63)


def test_mark_array_items_consolidates_to_complete(tmp_path):
    completion = JobscriptCompletion(
        tmp_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=tuple(range(10)),
    )
    for idx in range(9):
        completion.mark_array_item_complete(idx)

    assert not completion.is_complete()

    completion.mark_array_item_complete(9)
    assert completion.is_complete()
    assert not completion.array_path.exists()


def test_jobscript_completion_marker_precedes_array_cleanup(tmp_path, monkeypatch):
    """A complete jobscript may still have superseded array state."""
    completion = JobscriptCompletion(
        tmp_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=(0, 1),
    )
    completion.mark_array_item_complete(1)
    cleanup_observations = []

    def decline_removal_claim(path):
        cleanup_observations.append((path, completion.is_complete(), path.exists()))
        return None

    monkeypatch.setattr(
        "hpcflow.sdk.wait.utils.claim_path_for_removal",
        decline_removal_claim,
    )
    completion.mark_array_item_complete(0)

    assert cleanup_observations == [(completion.array_path, True, True)]
    assert completion.is_complete()
    assert completion.array_path.is_dir()
    assert all(completion.is_array_item_complete(idx) for idx in (0, 1))


@pytest.mark.parametrize("fail_root", [False, True], ids=["intermediate", "root"])
def test_array_completion_continues_after_einval_claim(
    tmp_path, monkeypatch, caplog, fail_root
):
    """A failed cleanup claim must not prevent higher-level consolidation."""
    array_indices = tuple(range(20))
    completion = JobscriptCompletion(
        tmp_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=array_indices,
    )
    for idx in array_indices[:-1]:
        completion.mark_array_item_complete(idx)

    marker_paths = completion.get_array_item_completion_paths(array_indices[-1])
    children_path = completion.array_path if fail_root else marker_paths[-1].parent
    parent_marker = marker_paths[0] if fail_root else marker_paths[-2]
    original_replace = os.replace
    failed_claims = []

    def replace(src, dst):
        if Path(src) == children_path:
            assert parent_marker.exists()
            failed_claims.append((src, dst))
            raise OSError(errno.EINVAL, "Invalid argument")
        return original_replace(src, dst)

    monkeypatch.setattr("hpcflow.sdk.wait.utils.os.replace", replace)
    with caplog.at_level(logging.WARNING, logger="hpcflow.sdk.wait.utils"):
        completion.mark_array_item_complete(array_indices[-1])

    assert len(failed_claims) == 1
    assert completion.is_complete()
    assert completion.is_submission_complete()
    assert all(completion.is_array_item_complete(idx) for idx in array_indices)
    if fail_root:
        assert children_path.is_dir()
    records = [
        record for record in caplog.records if record.name == "hpcflow.sdk.wait.utils"
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    assert str(children_path) in records[0].getMessage()
    assert "Invalid argument" in records[0].getMessage()


def test_cleanup_claim_propagates_unexpected_os_error(tmp_path, monkeypatch):
    def fail_claim(path):
        raise OSError(errno.EIO, "I/O error")

    monkeypatch.setattr("hpcflow.sdk.wait.utils.claim_path_for_removal", fail_claim)
    with pytest.raises(OSError) as exc_info:
        remove_superseded_directory(tmp_path / "superseded")

    assert exc_info.value.errno == errno.EIO


def test_mark_array_items_consolidates_intermediate_node(tmp_path):
    completion = JobscriptCompletion(
        tmp_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=tuple(range(500)),
    )

    for idx in range(60, 65):
        completion.mark_array_item_complete(idx)

    paths = completion.get_array_item_completion_paths(62)

    assert not completion.is_complete()
    assert not paths[1].exists()
    assert paths[2].exists()

    # the individual leaf directory has been consolidated away:
    leaf_dir = paths[3].parent
    assert not leaf_dir.exists()

    # every item represented by the consolidated node is complete.
    for idx in range(60, 65):
        assert completion.is_array_item_complete(idx)

    assert not completion.is_array_item_complete(59)
    assert not completion.is_array_item_complete(65)


def test_mark_array_item_complete_is_idempotent(tmp_path):
    completion = JobscriptCompletion(
        tmp_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=tuple(range(10)),
    )
    completion.mark_array_item_complete(3)
    completion.mark_array_item_complete(3)
    assert completion.is_array_item_complete(3)


def test_is_array_item_complete_during_consolidation(tmp_path, monkeypatch):
    """Test that completion remains detectable during tree consolidation.

    Simulate a parent completion marker appearing after the reader's initial
    ancestor checks but before it finishes checking the leaf marker. The
    reader should recheck the ancestors and recognise the array item as
    complete, even though its leaf marker has been removed.
    """
    completion = JobscriptCompletion(
        tmp_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=tuple(range(500)),
    )

    paths = completion.get_array_item_completion_paths(62)
    consolidated_marker = paths[-2]
    exists_calls: dict[Path, int] = {}

    def fake_exists(path):
        exists_calls[path] = exists_calls.get(path, 0) + 1
        if path == consolidated_marker:
            # missing on the first traversal, present when ancestors
            # are rechecked.
            return exists_calls[path] > 1

        return False

    monkeypatch.setattr(Path, "exists", fake_exists)

    assert completion.is_array_item_complete(62)
