"""Each demo-template context owns its temporary file, including overlapping calls."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from hpcflow.app import app as hf


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    directory = tmp_path / "runtime"
    monkeypatch.setattr(hf, "_user_runtime_dir", directory)
    return directory


@pytest.mark.parametrize("doc", [True, False])
def test_overlapping_demo_template_contexts_are_isolated(runtime, doc):
    with hf.get_demo_workflow_template_file("workflow_1", doc=doc) as first:
        contents = first.read_text()
        with hf.get_demo_workflow_template_file("workflow_1", doc=doc) as second:
            assert first != second
            assert first.name == second.name == "workflow_1.yaml"
            assert second.read_text() == contents
        assert not second.exists()
        assert first.read_text() == contents
        assert ("doc:" in contents) == doc
    assert not first.exists()
    assert list(runtime.iterdir()) == []


def test_parallel_demo_templates_do_not_remove_each_others_files(runtime):
    barrier = Barrier(4)

    def read():
        with hf.get_demo_workflow_template_file("workflow_1") as path:
            barrier.wait(timeout=10)
            contents = path.read_text()
            barrier.wait(timeout=10)
            return path, contents

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _: read(), range(4)))
    assert len({path for path, _ in results}) == 4
    assert len({contents for _, contents in results}) == 1
    assert list(runtime.iterdir()) == []


def test_demo_template_cleanup_when_caller_raises(runtime):
    with pytest.raises(RuntimeError, match="caller failed"):
        with hf.get_demo_workflow_template_file("workflow_1") as path:
            assert path.is_file()
            raise RuntimeError("caller failed")
    assert not path.exists()
    assert list(runtime.iterdir()) == []


@pytest.mark.parametrize("doc", [True, False])
def test_demo_template_delete_false_preserves_private_copy(runtime, doc):
    with hf.get_demo_workflow_template_file("workflow_1", doc=doc, delete=False) as path:
        contents = path.read_text()
    assert path.name == "workflow_1.yaml"
    assert path.parent.parent == runtime
    assert path.read_text() == contents
    assert ("doc:" in contents) == doc
