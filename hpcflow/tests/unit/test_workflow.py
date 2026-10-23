from __future__ import annotations
import copy
from io import BytesIO
from dataclasses import FrozenInstanceError, dataclass, fields, is_dataclass, replace
from pathlib import Path
from textwrap import dedent
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import Mock

import numpy as np
import zstandard as zstd
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.actions import ElementActionRun
from hpcflow.sdk.core.errors import (
    MissingInputs,
    WorkflowNotFoundError,
)
from hpcflow.sdk.core.test_utils import (
    make_workflow,
    P1_parameter_cls as P1,
    make_test_data_YAML_workflow,
)
from hpcflow.sdk.core.workflow import ArrayOutput, _ArrayOutputArchive

if TYPE_CHECKING:
    from collections.abc import Iterator
    from hpcflow.sdk.core.actions import Action, ActionEnvironment
    from hpcflow.sdk.core.command_files import FileSpec
    from hpcflow.sdk.core.parameters import Parameter
    from hpcflow.sdk.core.task_schema import TaskSchema
    from hpcflow.sdk.core.workflow import Workflow


@pytest.fixture
def persistent_workflow() -> Iterator[Workflow]:
    tmp_dir = hf._ensure_user_runtime_dir().joinpath("test_data")
    tmp_dir.mkdir(exist_ok=True)
    wk = make_test_data_YAML_workflow("workflow_1.yaml", path=tmp_dir, overwrite=True)
    yield wk
    wk._delete_no_confirm()


@pytest.mark.skip(reason="Cannot delete the zip file! Something still has a handle.")
def test_workflow_zip(persistent_workflow):
    zip_path = persistent_workflow.zip()

    # check zipped is loadable:
    wk_zipped = hf.Workflow(zip_path)
    assert wk_zipped.tasks[0].elements[0].get("inputs.p1") == 101

    del wk_zipped._store.fs
    del wk_zipped._store
    del wk_zipped

    # delete the zipped workflow:
    Path(zip_path).unlink()


def test_save_array_output_appends(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    empty_workflow = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="array_outputs"), path=tmp_path, store="zarr"
    )
    task = SimpleNamespace(
        insert_ID=20,
        index=2,
    )
    ear_values = {
        "run_file_ID": 6,
        "id_": 100,
        "index": 0,
        "task": task,
        "element": SimpleNamespace(index=3, id_=30),
        "element_iteration": SimpleNamespace(index=4, id_=40),
        "element_action": SimpleNamespace(action_idx=5),
        "data_idx": {"outputs.p1": 12},
    }
    ear = Mock(spec=ElementActionRun, **ear_values)
    ears_by_ID = {100: ear}

    def get_test_EARs(run_IDs):
        if isinstance(run_IDs, int):
            return ears_by_ID[run_IDs]
        return [ears_by_ID[run_ID] for run_ID in run_IDs]

    monkeypatch.setattr(empty_workflow, "get_EARs_from_IDs", get_test_EARs)
    parameter_values: list[tuple[int, object]] = []
    monkeypatch.setattr(
        empty_workflow,
        "set_parameter_value",
        lambda param_id, value: parameter_values.append((param_id, value)),
    )

    first_value = {"a": [[0, 1, 2], [3, 4, 5]], "b": np.arange(4)}
    for key, value in first_value.items():
        empty_workflow._save_array_output(ear, f"p1.{key}", value)

    ear_values.update(
        id_=101,
        index=1,
        element=SimpleNamespace(index=7, id_=70),
        element_iteration=SimpleNamespace(index=8, id_=80),
        element_action=SimpleNamespace(action_idx=9),
    )
    ear = Mock(spec=ElementActionRun, **ear_values)
    ears_by_ID[101] = ear
    second_value = {"a": np.arange(12).reshape(3, 4), "b": np.arange(4, 8)}
    for key, value in second_value.items():
        empty_workflow._save_array_output(ear, f"p1.{key}", value)

    array_archive_path = (
        Path(empty_workflow._store.path)
        / "parameters"
        / "array_outputs"
        / "p1.a"
        / "0"
        / "6.npz.zst"
    )
    with np.load(
        BytesIO(zstd.ZstdDecompressor().decompress(array_archive_path.read_bytes())),
        allow_pickle=False,
    ) as archive:
        np.testing.assert_array_equal(
            archive["data"],
            np.array(
                [
                    [[0, 1, 2, 0], [3, 4, 5, 0], [0, 0, 0, 0]],
                    [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]],
                ]
            ),
        )
        np.testing.assert_array_equal(archive["shape"], [[2, 3], [3, 4]])
        np.testing.assert_array_equal(archive["element_idx"], [3, 7])
        np.testing.assert_array_equal(archive["element_ID"], [30, 70])
        np.testing.assert_array_equal(archive["iteration_idx"], [4, 8])
        np.testing.assert_array_equal(archive["iteration_ID"], [40, 80])
        np.testing.assert_array_equal(archive["action_idx"], [5, 9])
        np.testing.assert_array_equal(archive["run_idx"], [0, 1])
        np.testing.assert_array_equal(archive["run_ID"], [100, 101])
        np.testing.assert_array_equal(archive["task_insert_ID"], [20, 20])
        np.testing.assert_array_equal(archive["task_idx"], [2, 2])

    task_array_archive_path = (
        Path(empty_workflow._store.path)
        / "parameters"
        / "array_outputs"
        / "p1.b"
        / "0"
        / "6.npz.zst"
    )
    with np.load(
        BytesIO(zstd.ZstdDecompressor().decompress(task_array_archive_path.read_bytes())),
        allow_pickle=False,
    ) as archive:
        np.testing.assert_array_equal(archive["data"], [[0, 1, 2, 3], [4, 5, 6, 7]])
        assert "shape" not in archive.files

    assert not (
        Path(empty_workflow._store.path)
        / "parameters"
        / "array_outputs"
        / "p1.missing"
        / "0"
        / "6.npz.zst"
    ).exists()
    output = empty_workflow.get_array_output("p1.a", 100)
    assert isinstance(output, ArrayOutput)
    np.testing.assert_array_equal(output.data, first_value["a"])
    np.testing.assert_array_equal(output.run_ID, [100])
    np.testing.assert_array_equal(output.element_ID, [30])
    np.testing.assert_array_equal(output.shape, [[2, 3]])
    destination = tmp_path / "scalar-run.npy"
    empty_workflow.export_array_output("p1.a", 100, destination)
    np.testing.assert_array_equal(np.load(destination), first_value["a"])
    output = empty_workflow.get_array_output("outputs.p1.a", [101, 100], sort=False)
    assert output.name == "p1.a"
    arrays = output.data
    assert isinstance(arrays, list)
    np.testing.assert_array_equal(arrays[0], second_value["a"])
    np.testing.assert_array_equal(arrays[1], first_value["a"])
    for key, expected in {
        "task_insert_ID": [20, 20],
        "task_idx": [2, 2],
        "element_idx": [7, 3],
        "element_ID": [70, 30],
        "iteration_idx": [8, 4],
        "iteration_ID": [80, 40],
        "action_idx": [9, 5],
        "run_idx": [1, 0],
        "run_ID": [101, 100],
        "shape": [[3, 4], [2, 3]],
    }.items():
        np.testing.assert_array_equal(getattr(output, key), expected)
    output = empty_workflow.get_array_output("p1.b", [101, 100], sort=False)
    arrays = output.data
    assert isinstance(arrays, np.ndarray)
    np.testing.assert_array_equal(arrays, [second_value["b"], first_value["b"]])
    assert output.shape is None
    with pytest.raises(FrozenInstanceError):
        setattr(output, "data", np.array([]))
    assert parameter_values == []


@pytest.mark.parametrize("second_size", [2, 3])
@pytest.mark.parametrize("with_shape", [False, True])
def test_get_array_output_across_files(
    empty_workflow: Workflow,
    monkeypatch: pytest.MonkeyPatch,
    second_size: int,
    with_shape: bool,
) -> None:
    ears_by_ID = {
        100: SimpleNamespace(run_file_ID=6, id_=100),
        101: SimpleNamespace(run_file_ID=500, id_=101),
    }
    monkeypatch.setattr(
        empty_workflow,
        "get_EARs_from_IDs",
        lambda run_IDs: [ears_by_ID[run_ID] for run_ID in run_IDs],
    )
    values = [np.array([1, 2]), np.arange(3, 3 + second_size)]
    for idx, ((run_ID, ear), value) in enumerate(zip(ears_by_ID.items(), values)):
        shape_metadata = {"shape": np.array([value.shape])} if with_shape and idx else {}
        buffer = BytesIO()
        np.savez(
            buffer,
            allow_pickle=False,
            data=value[np.newaxis, ...],
            task_insert_ID=[60 + idx],
            task_idx=[70 + idx],
            element_idx=[idx],
            element_ID=[10 + idx],
            iteration_idx=[20 + idx],
            iteration_ID=[30 + idx],
            action_idx=[40 + idx],
            run_idx=[50 + idx],
            run_ID=[run_ID],
            **shape_metadata,
        )
        path = empty_workflow._get_array_output_archive_path("p1.a", ear.run_file_ID)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(zstd.ZstdCompressor(level=1).compress(buffer.getvalue()))

    output = empty_workflow.get_array_output("p1.a", [101, 100, 101], sort=False)
    arrays = output.data

    assert isinstance(arrays, np.ndarray if second_size == 2 and not with_shape else list)
    for actual, expected_array in zip(arrays, [values[1], values[0], values[1]]):
        np.testing.assert_array_equal(actual, expected_array)
    for key, expected in {
        "task_insert_ID": [61, 60, 61],
        "task_idx": [71, 70, 71],
        "element_idx": [1, 0, 1],
        "element_ID": [11, 10, 11],
        "iteration_idx": [21, 20, 21],
        "iteration_ID": [31, 30, 31],
        "action_idx": [41, 40, 41],
        "run_idx": [51, 50, 51],
        "run_ID": [101, 100, 101],
    }.items():
        np.testing.assert_array_equal(getattr(output, key), expected)
    if with_shape:
        np.testing.assert_array_equal(output.shape, [[second_size], [2], [second_size]])
    else:
        assert output.shape is None

    read_archive = Mock(wraps=empty_workflow._read_array_output_archive)
    monkeypatch.setattr(empty_workflow, "_read_array_output_archive", read_archive)
    output_all = empty_workflow.get_array_output("p1.a")
    np.testing.assert_array_equal(output_all.run_ID, [100, 101])
    for actual, expected_array in zip(output_all.data, values):
        np.testing.assert_array_equal(actual, expected_array)
    assert read_archive.call_count == 2

    destination = Path(empty_workflow.path) / "export.npy"
    if second_size != 2:
        destination.write_bytes(b"existing export")
        with pytest.raises(ValueError, match="shapes are inconsistent"):
            empty_workflow.export_array_output(
                "p1.a", [101, 100, 101], destination, sort=False, overwrite=True
            )
        assert destination.read_bytes() == b"existing export"
    else:
        assert (
            empty_workflow.export_array_output(
                "p1.a", [101, 100, 101], destination, sort=False
            )
            == destination
        )
        np.testing.assert_array_equal(
            np.load(destination, allow_pickle=False),
            np.stack([values[1], values[0], values[1]]),
        )
        original = destination.read_bytes()
        with pytest.raises(FileExistsError):
            empty_workflow.export_array_output("p1.a", path=destination)
        assert destination.read_bytes() == original
        empty_workflow.export_array_output("p1.a", path=destination, overwrite=True)
        np.testing.assert_array_equal(np.load(destination), np.stack(values))


def test_get_array_output_without_saved_runs(empty_workflow: Workflow) -> None:
    with pytest.raises(ValueError, match="No saved runs selected"):
        empty_workflow.get_array_output("missing")
    with pytest.raises(ValueError, match="No saved runs selected"):
        empty_workflow.get_array_output("missing", [])


@pytest.mark.parametrize("name", ["p1.a", "outputs.p1.a"])
@pytest.mark.parametrize("run_file_ID", [0, 6, 499, 500, 999, 1000, 250000])
def test_get_array_output_archive_path(
    empty_workflow: Workflow, name: str, run_file_ID: int
) -> None:
    path = empty_workflow._get_array_output_archive_path(name, run_file_ID)

    assert path == (
        Path(empty_workflow._store.path)
        / "parameters"
        / "array_outputs"
        / "p1.a"
        / str(run_file_ID // 500)
        / f"{run_file_ID % 500}.npz.zst"
    )
    assert not path.parent.exists()


@pytest.mark.parametrize("sort", [None, False, True])
@pytest.mark.parametrize("variable_shapes", [False, True])
def test_get_array_output_sort(
    empty_workflow: Workflow,
    monkeypatch: pytest.MonkeyPatch,
    sort: bool | None,
    variable_shapes: bool,
) -> None:
    keys = np.array(
        [
            [1, 0, 0, 0],
            [0, 1, 0, 0],
            [0, 0, 1, 0],
            [0, 0, 0, 1],
            [0, 0, 0, 0],
            [0, 0, 0, 0],
        ]
    )
    metadata = {
        field.name: np.arange(6)
        for field in fields(_ArrayOutputArchive)
        if field.name not in {"data", "shape"}
    }
    for column, key in enumerate(
        ("element_idx", "iteration_idx", "action_idx", "run_idx")
    ):
        metadata[key] = keys[:, column]
    metadata["task_insert_ID"] = np.array([20, 10, 20, 10, 20, 10])
    data = np.repeat(np.arange(6)[:, None], 6, axis=1)
    shapes = np.arange(1, 7)[:, None] if variable_shapes else None
    archive = _ArrayOutputArchive(data=data, shape=shapes, **metadata)
    monkeypatch.setattr(
        empty_workflow,
        "get_EARs_from_IDs",
        Mock(return_value=[SimpleNamespace(id_=idx, run_file_ID=0) for idx in range(6)]),
    )
    read_archive = Mock(return_value=archive)
    monkeypatch.setattr(empty_workflow, "_read_array_output_archive", read_archive)
    requested = [0, 1, 2, 3, 5, 4, 2]
    if sort is None:
        output = empty_workflow.get_array_output("p1.a", requested)
    else:
        output = empty_workflow.get_array_output("p1.a", requested, sort=sort)

    expected = requested if sort is False else [5, 3, 1, 4, 2, 2, 0]
    assert isinstance(output.data, list if variable_shapes else np.ndarray)
    for actual, idx in zip(output.data, expected):
        np.testing.assert_array_equal(
            actual, data[idx, : idx + 1] if variable_shapes else data[idx]
        )
    for key, values in metadata.items():
        np.testing.assert_array_equal(getattr(output, key), values[expected])
    if variable_shapes:
        assert shapes is not None
        np.testing.assert_array_equal(output.shape, shapes[expected])
    else:
        assert output.shape is None
    read_archive.assert_called_once()
    if not variable_shapes:
        destination = Path(empty_workflow.path) / "sorted.npy"
        export = empty_workflow.export_array_output(
            "p1.a", requested, destination, sort=sort is not False
        )
        np.testing.assert_array_equal(np.load(export), data[expected])


def test_array_output_shards_limit_files(empty_workflow: Workflow) -> None:
    shards: dict[Path, set[str]] = {}
    for run_file_ID in range(1501):
        path = empty_workflow._get_array_output_archive_path("p1.a", run_file_ID)
        shards.setdefault(path.parent, set()).add(path.name)

    assert [len(names) for names in shards.values()] == [500, 500, 500, 1]
    path.parent.mkdir(parents=True)
    path.touch()
    assert empty_workflow.list_array_outputs() == ["p1.a"]


def test_array_output_archive_path_rejects_negative_ID(empty_workflow: Workflow) -> None:
    with pytest.raises(ValueError, match="Invalid array output run-file ID"):
        empty_workflow._get_array_output_archive_path("p1.a", -1)


@pytest.mark.parametrize(
    "name", ["", "outputs.", ".p1", "p1.", "p1..a", "p1/a", "p1\\a", ".."]
)
def test_get_array_output_archive_path_invalid_name(
    empty_workflow: Workflow, name: str
) -> None:
    with pytest.raises(ValueError, match="Invalid dot-delimited array output path"):
        empty_workflow._get_array_output_archive_path(name, 6)


@pytest.mark.parametrize(
    ("shape", "valid"),
    [
        (None, True),
        ([[1], [2]], True),
        ([[-1], [2]], False),
        ([[3], [2]], False),
        ([[1, 2], [1, 2]], False),
        ([[1]], False),
    ],
)
def test_read_array_output_archive(
    empty_workflow: Workflow,
    tmp_path: Path,
    shape: list[list[int]] | None,
    valid: bool,
) -> None:
    contents = {
        "data": np.array([[1, 2], [3, 4]]),
        "task_insert_ID": np.array([20, 21]),
        "task_idx": np.array([0, 1]),
        "element_idx": np.array([0, 1]),
        "element_ID": np.array([10, 11]),
        "iteration_idx": np.array([2, 3]),
        "iteration_ID": np.array([12, 13]),
        "action_idx": np.array([4, 5]),
        "run_idx": np.array([6, 7]),
        "run_ID": np.array([100, 101]),
    }
    if shape is not None:
        contents["shape"] = np.array(shape)
    path = tmp_path / "arrays.npz.zst"
    buffer = BytesIO()
    np.savez(buffer, allow_pickle=False, **contents)
    path.write_bytes(zstd.ZstdCompressor(level=1).compress(buffer.getvalue()))

    if not valid:
        with pytest.raises(ValueError, match="invalid shape metadata"):
            empty_workflow._read_array_output_archive(path)
        return

    archive = empty_workflow._read_array_output_archive(path)

    assert is_dataclass(archive)
    for field in fields(archive):
        if field.name == "shape" and shape is None:
            assert archive.shape is None
        else:
            np.testing.assert_array_equal(
                getattr(archive, field.name), contents[field.name]
            )
    with pytest.raises(FrozenInstanceError):
        setattr(archive, "data", np.array([]))


def test_task_disable_array_outputs() -> None:
    task = hf.Task(
        schema=hf.TaskSchema("array_output_task"),
        disable_array_outputs=["p1.a"],
    )

    assert task.disable_array_outputs == ["p1.a"]
    assert task.to_dict()["disable_array_outputs"] == ["p1.a"]


@pytest.mark.parametrize("use_element_sets", [False, True])
def test_task_disable_array_outputs_from_YAML(
    use_element_sets: bool, reload_template_components
) -> None:
    task_params = (
        "    element_sets:\n      - inputs:\n          p1: 1\n"
        if use_element_sets
        else "    inputs:\n      p1: 1\n"
    )
    wkt_yaml = dedent("""\
            name: disable_array_outputs_YAML
            template_components:
              task_schemas:
                - objective: array_output_task
                  inputs:
                    - parameter: p1
                  outputs:
                    - parameter: p2
                  actions:
                    - script: test.py
            tasks:
              - schema: array_output_task
                disable_array_outputs: [p2.a]
            """) + task_params
    wkt = hf.WorkflowTemplate.from_YAML_string(wkt_yaml)

    task = wkt.tasks[0]
    assert task.disable_array_outputs == ["p2.a"]
    assert task.element_sets[0].inputs[0].value == 1
    assert task.to_dict()["disable_array_outputs"] == ["p2.a"]


def test_list_array_outputs(empty_workflow: Workflow) -> None:
    assert empty_workflow.list_array_outputs() == []
    root = Path(empty_workflow._store.path) / "parameters" / "array_outputs"
    (root / "empty").mkdir(parents=True)
    tasks = [
        hf.Task(
            schema=hf.TaskSchema(
                objective="array_task_one",
                actions=[hf.Action(script="test.py")],
                outputs=[hf.Parameter("z"), hf.Parameter("shared")],
            )
        ),
        hf.Task(
            schema=hf.TaskSchema(
                objective="array_task_two",
                actions=[hf.Action(script="test.py")],
                outputs=[hf.Parameter("a"), hf.Parameter("shared")],
            )
        ),
    ]
    empty_workflow.add_task(tasks[0])
    empty_workflow.add_task(tasks[1])
    first, second = empty_workflow.tasks
    assert first.list_array_outputs() == []
    for name, task_IDs in {
        "z.output": [first.insert_ID],
        "a.output": [second.insert_ID],
        "shared": [first.insert_ID, second.insert_ID],
    }.items():
        path = empty_workflow._get_array_output_archive_path(name, 0)
        path.parent.mkdir(parents=True)
        buffer = BytesIO()
        np.savez(
            buffer,
            allow_pickle=False,
            data=np.zeros((len(task_IDs), 2)),
            task_insert_ID=task_IDs,
            **{
                key: np.arange(len(task_IDs))
                for key in (
                    "task_idx",
                    "element_idx",
                    "element_ID",
                    "iteration_idx",
                    "iteration_ID",
                    "action_idx",
                    "run_idx",
                    "run_ID",
                )
            },
        )
        path.write_bytes(zstd.ZstdCompressor(level=1).compress(buffer.getvalue()))
    assert empty_workflow.list_array_outputs() == ["a.output", "shared", "z.output"]
    assert first.list_array_outputs() == ["shared", "z.output"]
    assert second.list_array_outputs() == ["a.output", "shared"]


def _write_array_output_archive(
    workflow: Workflow,
    name: str,
    run_file_ID: int,
    run_IDs: list[int],
    data: np.ndarray,
    shape: np.ndarray | None = None,
) -> Path:
    """Write a shard archive with simple, run-ID-derived metadata."""
    path = workflow._get_array_output_archive_path(name, run_file_ID)
    path.parent.mkdir(parents=True, exist_ok=True)
    ids = np.asarray(run_IDs)
    buffer = BytesIO()
    np.savez(
        buffer,
        allow_pickle=False,
        data=data,
        task_insert_ID=np.zeros_like(ids),
        task_idx=np.zeros_like(ids),
        element_idx=ids - 100,
        element_ID=ids - 90,
        iteration_idx=np.zeros_like(ids),
        iteration_ID=ids - 80,
        action_idx=np.zeros_like(ids),
        run_idx=ids - 70,
        run_ID=ids,
        **({"shape": shape} if shape is not None else {}),
    )
    path.write_bytes(zstd.ZstdCompressor(level=1).compress(buffer.getvalue()))
    return path


def _archive_files(workflow: Workflow, name: str) -> list[str]:
    root = workflow._get_array_output_root(name)
    return sorted(
        str(path.relative_to(root)).replace("\\", "/")
        for path in root.rglob("*")
        if path.is_file()
    )


@pytest.mark.parametrize("ragged", [False, True])
def test_consolidate_array_outputs(
    empty_workflow: Workflow, monkeypatch: pytest.MonkeyPatch, ragged: bool
) -> None:
    ears_by_ID = {
        100: SimpleNamespace(run_file_ID=6, id_=100),
        101: SimpleNamespace(run_file_ID=500, id_=101),
        102: SimpleNamespace(run_file_ID=501, id_=102),
    }
    monkeypatch.setattr(
        empty_workflow,
        "get_EARs_from_IDs",
        lambda run_IDs: (
            ears_by_ID[run_IDs]
            if isinstance(run_IDs, int)
            else [ears_by_ID[run_ID] for run_ID in run_IDs]
        ),
    )
    second = np.array([3, 4, 5]) if ragged else np.array([3, 4])
    # two shards in different shard directories (0/6 and 1/0), newest run first:
    _write_array_output_archive(empty_workflow, "p1.a", 500, [101], second[None])
    _write_array_output_archive(empty_workflow, "p1.a", 6, [100], np.array([[1, 2]]))
    assert _archive_files(empty_workflow, "p1.a") == ["0/6.npz.zst", "1/0.npz.zst"]

    results = empty_workflow.consolidate_array_outputs(status=False)

    consolidated = empty_workflow._get_array_output_consolidated_path("p1.a")
    assert results == {"p1.a": consolidated}
    assert _archive_files(empty_workflow, "p1.a") == ["consolidated.npz.zst"]
    assert not (consolidated.parent / "0").exists()
    assert empty_workflow.list_array_outputs() == ["p1.a"]
    archive = empty_workflow._read_array_output_archive(consolidated)
    np.testing.assert_array_equal(archive.run_ID, [100, 101])  # sorted by run ID
    if ragged:
        np.testing.assert_array_equal(archive.data, [[1, 2, 0], [3, 4, 5]])
        np.testing.assert_array_equal(archive.shape, [[2], [3]])
    else:
        np.testing.assert_array_equal(archive.data, [[1, 2], [3, 4]])
        assert archive.shape is None

    # all three reader routes resolve to the consolidated archive:
    output_all = empty_workflow.get_array_output("p1.a")
    np.testing.assert_array_equal(output_all.run_ID, [100, 101])
    np.testing.assert_array_equal(output_all.data[1], second)
    np.testing.assert_array_equal(output_all.element_ID, [10, 11])
    output_sel = empty_workflow.get_array_output("p1.a", [101, 100], sort=False)
    np.testing.assert_array_equal(output_sel.run_ID, [101, 100])
    np.testing.assert_array_equal(output_sel.data[0], second)
    np.testing.assert_array_equal(
        empty_workflow.get_array_output("p1.a", 100).data, [1, 2]
    )
    assert empty_workflow._resolve_array_output_archive_path("p1.a", 6) == consolidated

    # a run saved after consolidation lands in a new shard and is folded in on repeat:
    _write_array_output_archive(empty_workflow, "p1.a", 501, [102], np.array([[6, 7]]))
    assert _archive_files(empty_workflow, "p1.a") == [
        "1/1.npz.zst",
        "consolidated.npz.zst",
    ]
    assert empty_workflow._resolve_array_output_archive_path("p1.a", 501) != consolidated
    np.testing.assert_array_equal(
        empty_workflow.get_array_output("p1.a").run_ID, [100, 101, 102]
    )
    assert empty_workflow.consolidate_array_outputs("outputs.p1.a") == {
        "p1.a": consolidated
    }
    assert _archive_files(empty_workflow, "p1.a") == ["consolidated.npz.zst"]
    archive = empty_workflow._read_array_output_archive(consolidated)
    np.testing.assert_array_equal(archive.run_ID, [100, 101, 102])
    np.testing.assert_array_equal(archive.data[2][:2], [6, 7])

    # nothing left to merge: no-op that still reports the existing archive
    read_archive = Mock(wraps=empty_workflow._read_array_output_archive)
    monkeypatch.setattr(empty_workflow, "_read_array_output_archive", read_archive)
    assert empty_workflow.consolidate_array_outputs(status=False) == {
        "p1.a": consolidated
    }
    read_archive.assert_not_called()


@pytest.mark.parametrize("consolidate", [None, False, True])
def test_zip_consolidates_array_outputs(
    empty_workflow: Workflow, tmp_path: Path, consolidate: bool | None
) -> None:
    import zipfile

    _write_array_output_archive(empty_workflow, "p1.a", 6, [100], np.array([[1, 2]]))
    _write_array_output_archive(empty_workflow, "p1.a", 500, [101], np.array([[3, 4]]))
    if consolidate is None:
        zip_path = empty_workflow.zip(path=str(tmp_path), status=False)
    else:
        zip_path = empty_workflow.zip(
            path=str(tmp_path), status=False, consolidate=consolidate
        )

    expected = (
        ["0/6.npz.zst", "1/0.npz.zst"]
        if consolidate is False
        else ["consolidated.npz.zst"]
    )
    assert _archive_files(empty_workflow, "p1.a") == expected
    with zipfile.ZipFile(zip_path) as archive:
        members = sorted(
            name.split("parameters/array_outputs/p1.a/", 1)[1]
            for name in archive.namelist()
            if "parameters/array_outputs/p1.a/" in name and name.endswith(".npz.zst")
        )
    assert members == expected


def test_consolidate_array_outputs_selection(empty_workflow: Workflow) -> None:
    assert empty_workflow.consolidate_array_outputs(status=False) == {}
    with pytest.raises(ValueError, match="No array-output archives found for 'nope'"):
        empty_workflow.consolidate_array_outputs("nope", status=False)

    for name in ("p1.m", "p1.m.__mask__", "p1.other"):
        _write_array_output_archive(empty_workflow, name, 6, [100], np.array([[1.0]]))

    # naming a masked output consolidates its mask archive too; others untouched:
    results = empty_workflow.consolidate_array_outputs(["p1.m"], status=False)
    assert sorted(results) == ["p1.m", "p1.m.__mask__"]
    assert _archive_files(empty_workflow, "p1.m") == ["consolidated.npz.zst"]
    assert _archive_files(empty_workflow, "p1.m.__mask__") == ["consolidated.npz.zst"]
    assert _archive_files(empty_workflow, "p1.other") == ["0/6.npz.zst"]
    assert empty_workflow.list_array_outputs() == ["p1.m", "p1.other"]

    # consolidating everything includes internal mask archives:
    results = empty_workflow.consolidate_array_outputs(status=False)
    assert sorted(results) == ["p1.m", "p1.m.__mask__", "p1.other"]
    assert _archive_files(empty_workflow, "p1.other") == ["consolidated.npz.zst"]


def test_consolidate_array_outputs_verification_failure_keeps_shards(
    empty_workflow: Workflow, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_array_output_archive(empty_workflow, "p1.a", 6, [100], np.array([[1, 2]]))
    _write_array_output_archive(empty_workflow, "p1.a", 7, [101], np.array([[3, 4]]))
    consolidated = empty_workflow._get_array_output_consolidated_path("p1.a")
    real_read = empty_workflow._read_array_output_archive

    def corrupt_read(path: Path):
        archive = real_read(path)
        if path == consolidated:
            # simulate a truncated re-read of the freshly written archive
            return replace(archive, run_ID=archive.run_ID[:1])
        return archive

    monkeypatch.setattr(empty_workflow, "_read_array_output_archive", corrupt_read)
    with pytest.raises(RuntimeError, match="holds 1 entries; expected 2"):
        empty_workflow.consolidate_array_outputs(status=False)
    assert _archive_files(empty_workflow, "p1.a") == [
        "0/6.npz.zst",
        "0/7.npz.zst",
        "consolidated.npz.zst",
    ]


def _make_archive(
    data: np.ndarray, run_IDs: list[int], shape: np.ndarray | None = None
) -> _ArrayOutputArchive:
    zeros = np.zeros(len(run_IDs), dtype=np.int64)
    return _ArrayOutputArchive(
        data=data,
        task_insert_ID=zeros,
        task_idx=zeros,
        element_idx=zeros,
        element_ID=zeros,
        iteration_idx=zeros,
        iteration_ID=zeros,
        action_idx=zeros,
        run_idx=zeros,
        run_ID=np.asarray(run_IDs),
        shape=shape,
    )


def test_merge_array_output_archives() -> None:
    merge = hf.Workflow._merge_array_output_archives
    with pytest.raises(TypeError, match="incompatible with existing dtype"):
        merge(
            "x",
            [
                _make_archive(np.zeros((1, 2)), [0]),
                _make_archive(np.zeros((1, 2), dtype=int), [1]),
            ],
        )
    # fixed-width strings of different widths are widened, not rejected:
    merged = merge(
        "x",
        [
            _make_archive(np.array([["Al", "Cu"]], dtype="U2"), [1]),
            _make_archive(np.array([["Al-Cu", "Steel1"]], dtype="U6"), [0]),
        ],
    )
    assert merged["data"].dtype == np.dtype("U6")
    np.testing.assert_array_equal(merged["data"], [["Al-Cu", "Steel1"], ["Al", "Cu"]])
    with pytest.raises(ValueError, match="different dimensionality"):
        merge(
            "x",
            [
                _make_archive(np.zeros((1, 2)), [0]),
                _make_archive(np.zeros((1, 2, 2)), [1]),
            ],
        )
    with pytest.raises(ValueError, match=r"more than once: \[0\]"):
        merge(
            "x",
            [_make_archive(np.zeros((1, 2)), [0]), _make_archive(np.zeros((1, 2)), [0])],
        )

    # 0-d entries: no shape metadata, sorted by run ID
    merged = merge(
        "x",
        [
            _make_archive(np.array([5.0, 6.0]), [3, 1]),
            _make_archive(np.array([7.0]), [2]),
        ],
    )
    np.testing.assert_array_equal(merged["data"], [6.0, 7.0, 5.0])
    np.testing.assert_array_equal(merged["run_ID"], [1, 2, 3])
    assert "shape" not in merged

    # existing shape metadata is preserved and combined with implied shapes
    merged = merge(
        "x",
        [
            _make_archive(np.array([[1, 0], [2, 3]]), [1, 0], shape=np.array([[1], [2]])),
            _make_archive(np.array([[4, 5, 6]]), [2]),
        ],
    )
    np.testing.assert_array_equal(merged["run_ID"], [0, 1, 2])
    np.testing.assert_array_equal(merged["data"], [[2, 3, 0], [1, 0, 0], [4, 5, 6]])
    np.testing.assert_array_equal(merged["shape"], [[2], [1], [3]])
    assert merged["shape"].dtype == np.int32


@pytest.mark.parametrize("as_list", [False, True])
@pytest.mark.parametrize("with_shape", [False, True])
@pytest.mark.parametrize("dtype", ["float64", "int32"])
def test_array_output_repr(as_list: bool, with_shape: bool, dtype: str) -> None:
    metadata = {
        field.name: np.arange(20)
        for field in fields(ArrayOutput)
        if field.name not in {"name", "data", "shape"}
    }
    data = (
        [np.zeros((i + 1, 2), dtype=dtype) for i in range(20)]
        if as_list
        else np.zeros((20, 2), dtype=dtype)
    )
    output = ArrayOutput(
        name="p1.a",
        data=data,
        **metadata,
        shape=np.full((20, 1), 2) if with_shape else None,
    )

    representation = repr(output)

    lines = representation.splitlines()
    assert lines[0] == "ArrayOutput("
    assert lines[-1] == ")"
    assert len(lines) == len(fields(ArrayOutput)) + 3
    assert lines[1] == "    name='p1.a',"
    assert output.dtype == np.dtype(dtype)
    assert lines[2] == f"    dtype={dtype},"
    assert lines[3] == (
        "    data=[(1, 2), (2, 2), (3, 2), (4, 2), (5, 2), "
        "(6, 2), (7, 2), (8, 2), (9, 2), ..., (20, 2)],"
        if as_list
        else "    data=(20, 2),"
    )
    for name in metadata:
        assert (f"    {name}=[0, 1, 2, 3, 4, 5, 6, 7, 8, ..., 19],") in lines
    assert lines[-2] == (
        "    shape=[[2], [2], [2], [2], [2], [2], [2], [2], [2], ..., [2]],"
        if with_shape
        else "    shape=None,"
    )


@pytest.mark.parametrize("dtype", ["float32", "float64", "int32"])
def test_array_output_append_rejects_dtype_mismatch(
    empty_workflow: Workflow, dtype: str
) -> None:
    ear = Mock(
        spec=ElementActionRun,
        run_file_ID=6,
        id_=100,
        index=0,
        task=SimpleNamespace(insert_ID=20, index=2),
        element=SimpleNamespace(index=0, id_=0),
        element_iteration=SimpleNamespace(index=0, id_=0),
        element_action=SimpleNamespace(action_idx=0),
    )
    empty_workflow._save_array_output(ear, "p1.a", np.array([1, 2], dtype="int64"))
    path = empty_workflow._get_array_output_archive_path("p1.a", 6)
    original_bytes = path.read_bytes()

    with pytest.raises(TypeError, match=f"dtype {dtype}.*dtype int64"):
        empty_workflow._save_array_output(ear, "p1.a", np.array([3, 4], dtype=dtype))

    assert path.read_bytes() == original_bytes


@pytest.mark.parametrize("kind", ["U", "S"])
@pytest.mark.parametrize("widen", [True, False])
def test_array_output_append_widens_fixed_width_strings(
    empty_workflow: Workflow, kind: str, widen: bool
) -> None:
    """Fixed-width string entries of different widths share one archive."""

    def make_ear(idx: int) -> Mock:
        return Mock(
            spec=ElementActionRun,
            run_file_ID=6,
            id_=100 + idx,
            index=idx,
            task=SimpleNamespace(insert_ID=20, index=2),
            element=SimpleNamespace(index=idx, id_=idx),
            element_iteration=SimpleNamespace(index=0, id_=idx),
            element_action=SimpleNamespace(action_idx=0),
        )

    short = np.array(["Al", "Cu"], dtype=f"{kind}2")
    long = np.array(["Al-Cu", "Steel1"], dtype=f"{kind}6")
    first, second = (short, long) if widen else (long, short)
    empty_workflow._save_array_output(make_ear(0), "p1.label", first)
    empty_workflow._save_array_output(make_ear(1), "p1.label", second)

    archive = empty_workflow._read_array_output_archive(
        empty_workflow._get_array_output_archive_path("p1.label", 6)
    )
    assert archive.data.dtype == np.dtype(f"{kind}6")
    np.testing.assert_array_equal(archive.data, np.stack([first, second]))
    np.testing.assert_array_equal(archive.run_ID, [100, 101])

    # same width in a different string kind is still rejected:
    other_kind = "S" if kind == "U" else "U"
    with pytest.raises(TypeError, match="incompatible with existing dtype"):
        empty_workflow._save_array_output(
            make_ear(2), "p1.label", np.array(["x", "y"], dtype=f"{other_kind}6")
        )


def make_workflow_w1_with_config_kwargs(
    config_kwargs, path, param_p1: Parameter, param_p2: Parameter
) -> Workflow:
    hf.load_config(**config_kwargs)
    s1 = hf.TaskSchema("ts1", actions=[], inputs=[param_p1], outputs=[param_p2])
    t1 = hf.Task(schema=s1, inputs=[hf.InputValue(param_p1, 101)])
    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1])
    return hf.Workflow.from_template(wkt, path=path)


@pytest.fixture
def empty_workflow(tmp_path) -> Workflow:
    return hf.Workflow.from_template(hf.WorkflowTemplate(name="w1"), path=tmp_path)


@pytest.fixture
def param_p1() -> Parameter:
    return hf.Parameter("p1")


@pytest.fixture
def param_p1c() -> Parameter:
    return hf.Parameter("p1c")


@pytest.fixture
def param_p2() -> Parameter:
    return hf.Parameter("p2")


@pytest.fixture
def param_p3() -> Parameter:
    return hf.Parameter("p3")


@pytest.fixture
def act_env_1() -> ActionEnvironment:
    return hf.ActionEnvironment("env_1")


@pytest.fixture
def act_1(act_env_1: ActionEnvironment) -> Action:
    return hf.Action(
        commands=[hf.Command("<<parameter:p1>>")],
        environments=[act_env_1],
    )


@pytest.fixture
def act_2(act_env_1: ActionEnvironment) -> Action:
    return hf.Action(
        commands=[hf.Command("<<parameter:p2>> <<parameter:p3>>")],
        environments=[act_env_1],
    )


@pytest.fixture
def file_spec_fs1() -> FileSpec:
    return hf.FileSpec(label="file1", name="file1.txt")


@pytest.fixture
def act_3(
    act_env_1: ActionEnvironment,
    param_p2: Parameter,
    file_spec_fs1: FileSpec,
) -> Action:
    return hf.Action(
        commands=[hf.Command("<<parameter:p1>>")],
        output_file_parsers=[
            hf.OutputFileParser(output=param_p2, output_files=[file_spec_fs1]),
        ],
        environments=[act_env_1],
    )


@pytest.fixture
def schema_s1(param_p1: Parameter, act_1: Action) -> TaskSchema:
    return hf.TaskSchema("ts1", actions=[act_1], inputs=[param_p1])


@pytest.fixture
def schema_s2(param_p2: Parameter, param_p3: Parameter, act_2: Action) -> TaskSchema:
    return hf.TaskSchema("ts2", actions=[act_2], inputs=[param_p2, param_p3])


@pytest.fixture
def schema_s3(param_p1: Parameter, param_p2: Parameter, act_3: Action) -> TaskSchema:
    return hf.TaskSchema("ts1", actions=[act_3], inputs=[param_p1], outputs=[param_p2])


@pytest.fixture
def schema_s4(
    param_p1: Parameter,
) -> TaskSchema:
    return hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=param_p1)],
        actions=[
            hf.Action(
                environments=[hf.ActionEnvironment("null_env")],
                commands=[hf.Command("Write-Output '<<parameter:p1>>'")],
            )
        ],
    )


@pytest.fixture
def schema_s4c(
    param_p1c: Parameter,
) -> TaskSchema:
    return hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=param_p1c)],
        actions=[
            hf.Action(
                environments=[hf.ActionEnvironment("null_env")],
                commands=[hf.Command("Write-Output '<<parameter:p1c>>'")],
            )
        ],
    )


@pytest.fixture
def workflow_w1(tmp_path: Path, schema_s3: TaskSchema, param_p1: Parameter) -> Workflow:
    t1 = hf.Task(schema=schema_s3, inputs=[hf.InputValue(param_p1, 101)])
    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1])
    return hf.Workflow.from_template(wkt, path=tmp_path)


def test_make_empty_workflow(empty_workflow: Workflow):
    assert empty_workflow.path is not None


def test_raise_on_missing_workflow(tmp_path: Path):
    with pytest.raises(WorkflowNotFoundError):
        hf.Workflow(tmp_path)


def test_add_empty_task(empty_workflow: Workflow, schema_s1: TaskSchema):
    t1 = hf.Task(schema=schema_s1)
    wk_t1 = empty_workflow._add_empty_task(t1)
    assert len(empty_workflow.tasks) == 1 and wk_t1.index == 0 and wk_t1.name == "ts1"


def test_raise_on_missing_inputs_add_first_task(
    empty_workflow: Workflow, schema_s1: TaskSchema, param_p1: Parameter
):
    t1 = hf.Task(schema=schema_s1)
    with pytest.raises(MissingInputs) as exc_info:
        empty_workflow.add_task(t1)

    assert exc_info.value.missing_inputs == (param_p1.typ,)


def test_raise_on_missing_inputs_add_second_task(
    workflow_w1: Workflow, schema_s2: TaskSchema, param_p3: Parameter
):
    t2 = hf.Task(schema=schema_s2)
    with pytest.raises(MissingInputs) as exc_info:
        workflow_w1.add_task(t2)

    assert exc_info.value.missing_inputs == (param_p3.typ,)  # p2 comes from existing task


@pytest.mark.skip(reason="TODO: Not implemented.")
def test_new_workflow_deleted_on_creation_failure():
    pass


def test_WorkflowTemplate_from_YAML_string():
    wkt_yml = dedent("""
        name: simple_workflow

        tasks:
        - schema: dummy_task_1
          element_sets:
          - inputs:
              p2: 201
              p5: 501
            sequences:
              - path: inputs.p1
                nesting_order: 0
                values: [101, 102]
    """)
    hf.WorkflowTemplate.from_YAML_string(wkt_yml)


def test_WorkflowTemplate_from_YAML_string_without_element_sets():
    wkt_yml = dedent("""
        name: simple_workflow

        tasks:
        - schema: dummy_task_1
          inputs:
            p2: 201
            p5: 501
          sequences:
            - path: inputs.p1
              nesting_order: 0
              values: [101, 102]
    """)
    hf.WorkflowTemplate.from_YAML_string(wkt_yml)


def test_WorkflowTemplate_from_YAML_string_with_and_without_element_sets_equivalence():
    wkt_yml_1 = dedent("""
        name: simple_workflow
        resources:
          any:
            random_seed: 0
        tasks:
        - schema: dummy_task_1
          element_sets:
            - inputs:
                p2: 201
                p5: 501
              sequences:
                - path: inputs.p1
                  nesting_order: 0
                  values: [101, 102]
    """)
    wkt_yml_2 = dedent("""
        name: simple_workflow
        resources:
          any:
            random_seed: 0
        tasks:
        - schema: dummy_task_1
          inputs:
            p2: 201
            p5: 501
          sequences:
            - path: inputs.p1
              nesting_order: 0
              values: [101, 102]
    """)
    wkt_1 = hf.WorkflowTemplate.from_YAML_string(wkt_yml_1)
    wkt_2 = hf.WorkflowTemplate.from_YAML_string(wkt_yml_2)
    assert wkt_1 == wkt_2


def test_store_has_pending_during_add_task(
    workflow_w1: Workflow, schema_s2: TaskSchema, param_p3: Parameter
):
    t2 = hf.Task(schema=schema_s2, inputs=[hf.InputValue(param_p3, 301)])
    with workflow_w1.batch_update():
        workflow_w1.add_task(t2)
        assert workflow_w1._store.has_pending


def test_empty_batch_update_does_nothing(workflow_w1: Workflow):
    with workflow_w1.batch_update():
        assert not workflow_w1._store.has_pending


def test_closest_task_input_source_chosen(tmp_path: Path):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p1",), "t1"),
            ({"p1": None}, ("p1",), "t2"),
            ({"p1": None}, ("p1",), "t3"),
        ],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    assert wk.tasks.t3.get_task_dependencies(as_objects=True) == [wk.tasks.t2]


def test_WorkflowTemplate_from_JSON_string_without_element_sets():
    wkt_json = dedent("""
        {
            "name": "test_wk",
            "tasks": [
                {
                    "schema": "test_t1_bash",
                    "inputs": {
                        "p1": 101
                    }
                }
            ]
        }
    """)
    hf.WorkflowTemplate.from_JSON_string(wkt_json)


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_equivalent_element_input_parameter_value_class_and_kwargs(
    tmp_path: Path,
    store: str,
    schema_s4c: TaskSchema,
    param_p1c: Parameter,
):
    a_value = 101
    t1_1 = hf.Task(
        schema=[schema_s4c],
        inputs=[hf.InputValue(parameter=param_p1c, value=P1(a=a_value))],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        inputs=[hf.InputValue(parameter=param_p1c, value={"a": a_value})],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    assert (
        wk.tasks.t1_1.elements[0].inputs.p1c.value
        == wk.tasks.t1_2.elements[0].inputs.p1c.value
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_equivalent_element_input_parameter_value_class_method_and_kwargs(
    tmp_path: Path,
    store: str,
    schema_s4c: TaskSchema,
    param_p1c: Parameter,
):
    b_val = 50
    c_val = 51
    expected_a_val = b_val + c_val
    t1_1 = hf.Task(
        schema=[schema_s4c],
        inputs=[hf.InputValue(parameter=param_p1c, value=P1.from_data(b=b_val, c=c_val))],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        inputs=[
            hf.InputValue(
                parameter=param_p1c,
                value={"b": b_val, "c": c_val},
                value_class_method="from_data",
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    assert wk.tasks.t1_1.elements[0].inputs.p1c.value.a == expected_a_val
    assert (
        wk.tasks.t1_1.elements[0].inputs.p1c.value
        == wk.tasks.t1_2.elements[0].inputs.p1c.value
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_input_value_class_expected_value(
    tmp_path: Path, store: str, schema_s4c: TaskSchema, param_p1c: Parameter
):
    a_value = 101
    t1_value_exp = P1(a=a_value)
    t2_value_exp = {"a": a_value}
    t1_1 = hf.Task(
        schema=[schema_s4c],
        inputs=[hf.InputValue(parameter=param_p1c, value=t1_value_exp)],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        inputs=[hf.InputValue(parameter=param_p1c, value=t2_value_exp)],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    value_1 = wk.tasks.t1_1.template.element_sets[0].inputs[0].value
    value_2 = wk.tasks.t1_2.template.element_sets[0].inputs[0].value
    assert value_1 == t1_value_exp
    assert value_2 == t2_value_exp


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_input_value_class_method_expected_value(
    tmp_path: Path, store: str, schema_s4c: TaskSchema, param_p1c: Parameter
):
    b_val = 50
    c_val = 51
    t1_value_exp = P1.from_data(b=b_val, c=c_val)
    t2_value_exp = {"b": b_val, "c": c_val}
    t1_1 = hf.Task(
        schema=[schema_s4c],
        inputs=[hf.InputValue(parameter=param_p1c, value=t1_value_exp)],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        inputs=[
            hf.InputValue(
                parameter=param_p1c,
                value=t2_value_exp,
                value_class_method="from_data",
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    value_1 = wk.tasks.t1_1.template.element_sets[0].inputs[0].value
    value_2 = wk.tasks.t1_2.template.element_sets[0].inputs[0].value
    assert value_1 == t1_value_exp
    assert value_2 == t2_value_exp


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_equivalent_element_input_sequence_parameter_value_class_and_kwargs(
    tmp_path: Path, store: str, schema_s4c: TaskSchema
):
    data = {"a": 101}
    obj = P1(**data)  # type: ignore[arg-type]  # python/mypy#15317
    t1_1 = hf.Task(
        schema=[schema_s4c],
        sequences=[hf.ValueSequence(path="inputs.p1c", values=[obj], nesting_order=0)],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        sequences=[hf.ValueSequence(path="inputs.p1c", values=[data], nesting_order=0)],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    val_1 = wk.tasks.t1_1.elements[0].inputs.p1c.value
    val_2 = wk.tasks.t1_2.elements[0].inputs.p1c.value
    assert val_1 == val_2 == obj


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_equivalent_element_input_sequence_parameter_value_class_method_and_kwargs(
    tmp_path: Path, store: str, schema_s4c: TaskSchema
):
    data = {"b": 50, "c": 51}
    obj = P1.from_data(**data)
    t1_1 = hf.Task(
        schema=[schema_s4c],
        sequences=[hf.ValueSequence(path="inputs.p1c", values=[obj], nesting_order=0)],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        sequences=[
            hf.ValueSequence(
                path="inputs.p1c",
                values=[data],
                value_class_method="from_data",
                nesting_order=0,
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    val_1 = wk.tasks.t1_1.elements[0].inputs.p1c.value
    val_2 = wk.tasks.t1_2.elements[0].inputs.p1c.value
    assert val_1 == val_2 == obj


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_sequence_value_class_expected_value(
    tmp_path: Path, store: str, schema_s4c: TaskSchema
):
    data = {"a": 101}
    obj = P1(**data)  # type: ignore[arg-type]  # python/mypy#15317
    t1_1 = hf.Task(
        schema=[schema_s4c],
        sequences=[hf.ValueSequence(path="inputs.p1c", values=[obj], nesting_order=0)],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        sequences=[hf.ValueSequence(path="inputs.p1c", values=[data], nesting_order=0)],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    value_1 = wk.tasks.t1_1.template.element_sets[0].sequences[0].values[0]
    value_2 = wk.tasks.t1_2.template.element_sets[0].sequences[0].values[0]
    assert value_1 == obj
    assert value_2 == data


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_sequence_value_class_method_expected_value(
    tmp_path: Path, store: str, schema_s4c: TaskSchema
):
    data = {"b": 50, "c": 51}
    obj = P1.from_data(**data)
    t1_1 = hf.Task(
        schema=[schema_s4c],
        sequences=[hf.ValueSequence(path="inputs.p1c", values=[obj], nesting_order=0)],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        sequences=[
            hf.ValueSequence(
                path="inputs.p1c",
                values=[data],
                nesting_order=0,
                value_class_method="from_data",
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    value_1 = wk.tasks.t1_1.template.element_sets[0].sequences[0].values[0]
    value_2 = wk.tasks.t1_2.template.element_sets[0].sequences[0].values[0]
    assert value_1 == obj
    assert value_2 == data


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_expected_element_input_parameter_value_class_merge_sequence(
    tmp_path: Path, store: str, schema_s4c: TaskSchema, param_p1c: Parameter
):
    a_val = 101
    d_val = 201
    obj_exp = P1(a=a_val, d=d_val)

    t1 = hf.Task(
        schema=[schema_s4c],
        inputs=[hf.InputValue(parameter=param_p1c, value={"a": a_val})],
        sequences=[
            hf.ValueSequence(path="inputs.p1c.d", values=[d_val], nesting_order=0)
        ],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    wk.tasks.t1.template.element_sets[0].sequences[0].values[0]
    assert wk.tasks.t1.elements[0].inputs.p1c.value == obj_exp


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_expected_element_input_parameter_value_class_method_merge_sequence(
    tmp_path: Path, store: str, schema_s4c: TaskSchema, param_p1c: Parameter
):
    b_val = 50
    c_val = 51
    obj_exp = P1.from_data(b=b_val, c=c_val)

    t1 = hf.Task(
        schema=[schema_s4c],
        inputs=[
            hf.InputValue(
                parameter=param_p1c, value={"b": b_val}, value_class_method="from_data"
            )
        ],
        sequences=[
            hf.ValueSequence(path="inputs.p1c.c", values=[c_val], nesting_order=0)
        ],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    wk.tasks.t1.template.element_sets[0].sequences[0].values[0]
    assert wk.tasks.t1.elements[0].inputs.p1c.value == obj_exp


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_upstream_input_source_merge_with_current_input_modification(
    tmp_path: Path, store: str, param_p2: Parameter
):
    s1 = hf.TaskSchema(objective="t1", inputs=[hf.SchemaInput(parameter=param_p2)])
    s2 = hf.TaskSchema(objective="t2", inputs=[hf.SchemaInput(parameter=param_p2)])
    tasks = [
        hf.Task(schema=s1, inputs=[hf.InputValue("p2", {"a": 101})]),
        hf.Task(schema=s2, inputs=[hf.InputValue("p2", value=102, path="b")]),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    p2 = wk.tasks[1].elements[0].inputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == {"a": 101, "b": 102}


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_upstream_input_source_with_sub_parameter(
    tmp_path: Path, store: str, param_p2: Parameter
):
    s1 = hf.TaskSchema(objective="t1", inputs=[hf.SchemaInput(parameter=param_p2)])
    s2 = hf.TaskSchema(objective="t2", inputs=[hf.SchemaInput(parameter=param_p2)])
    tasks = [
        hf.Task(
            schema=s1,
            inputs=[
                hf.InputValue("p2", {"a": 101}),
                hf.InputValue("p2", value=102, path="b"),
            ],
        ),
        hf.Task(schema=s2),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    p2 = wk.tasks[1].elements[0].inputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == {"a": 101, "b": 102}


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_from_template_data_workflow_reload(tmp_path: Path, store: str):
    wk_name = "temp"
    t1 = hf.Task(schema=hf.task_schemas.test_t1_ps, inputs=[hf.InputValue("p1", 101)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    wk_ld = hf.Workflow(wk.path)
    assert (
        wk.tasks[0].elements[0].get_data_idx()
        == wk_ld.tasks[0].elements[0].get_data_idx()
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_from_template_workflow_reload(tmp_path: Path, store: str):
    wk_name = "temp"
    t1 = hf.Task(schema=hf.task_schemas.test_t1_ps, inputs=[hf.InputValue("p1", 101)])
    wkt = hf.WorkflowTemplate(name=wk_name, tasks=[t1])
    wk = hf.Workflow.from_template(
        template=wkt,
        path=tmp_path,
        store=store,
    )
    wk_ld = hf.Workflow(wk.path)
    assert (
        wk.tasks[0].elements[0].get_data_idx()
        == wk_ld.tasks[0].elements[0].get_data_idx()
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_from_YAML_str_template_workflow_reload(tmp_path: Path, store: str):
    yaml_str = dedent("""
    name: temp
    tasks: 
      - schema: test_t1_ps
        inputs:
          p1: 101
    """)
    wk = hf.Workflow.from_YAML_string(
        YAML_str=yaml_str,
        path=tmp_path,
        store=store,
    )
    wk_ld = hf.Workflow(wk.path)
    assert (
        wk.tasks[0].elements[0].get_data_idx()
        == wk_ld.tasks[0].elements[0].get_data_idx()
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_from_template_workflow_add_task_reload(tmp_path: Path, store: str):
    wk_name = "temp"
    t1 = hf.Task(schema=hf.task_schemas.test_t1_ps, inputs=[hf.InputValue("p1", 101)])
    wkt = hf.WorkflowTemplate(name=wk_name)
    wk = hf.Workflow.from_template(
        template=wkt,
        path=tmp_path,
        store=store,
    )
    wk.add_task(t1)
    wk_ld = hf.Workflow(wk.path)
    assert (
        wk.tasks[0].elements[0].get_data_idx()
        == wk_ld.tasks[0].elements[0].get_data_idx()
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_batch_update_mode_false_after_empty_workflow_init(tmp_path: Path, store: str):
    wk_name = "temp"
    wk = hf.Workflow.from_template_data(
        tasks=[],
        path=tmp_path,
        template_name=wk_name,
        store=store,
    )
    assert wk._in_batch_mode == False
