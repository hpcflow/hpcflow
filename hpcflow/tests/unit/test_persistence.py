from __future__ import annotations
from pathlib import Path
import sys
from unittest.mock import Mock

from typing import Any, cast, TYPE_CHECKING
import numpy as np
import zstandard as zstd
import zarr  # type: ignore
import pytest
from hpcflow.sdk.core.test_utils import (
    make_schemas,
    make_test_data_YAML_workflow,
    make_workflow,
)
from hpcflow.sdk.persistence.json import (
    JSONPersistentStore,
    JsonStoreElement,
    JsonStoreElementIter,
    JsonStoreEAR,
)
from hpcflow.sdk.persistence.zarr import ZarrPersistentStore
from hpcflow.sdk.core.parameters import NullDefault

from hpcflow.app import app as hf

if TYPE_CHECKING:
    from hpcflow.sdk.persistence.zarr import ZarrPersistentStore


@pytest.fixture
def zarr_array_workflow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple:
    workflow = hf.Workflow.from_template_data(
        template_name="array_test",
        tasks=[
            hf.Task(
                schema=hf.TaskSchema(
                    "array_test",
                    inputs=[hf.Parameter("p2")],
                    outputs=[hf.Parameter("p1")],
                    actions=[hf.Action(script="test.py")],
                ),
                inputs=[hf.InputValue("p2", np.arange(3))],
            )
        ],
        path=tmp_path,
        store="zarr",
    )
    task = workflow.tasks[0]
    assert workflow.list_array_outputs() == []
    runs = list(workflow.get_EARs_of_tasks([task.insert_ID]))
    assert len(runs) == 1
    run = runs[0]
    monkeypatch.setattr(run, "_run_file_ID", 6)
    original_get_runs = workflow.get_EARs_from_IDs
    monkeypatch.setattr(
        workflow,
        "get_EARs_from_IDs",
        lambda IDs: run if isinstance(IDs, int) else original_get_runs(IDs),
    )
    monkeypatch.setattr(
        workflow._store,
        "_get_run_file_lookup",
        lambda IDs, file_offset=0: {0: {6 + file_offset: {run.id_: 0}}},
    )
    return workflow, run


@pytest.mark.parametrize("disabled", [[], ["p1"]])
@pytest.mark.parametrize("numpy_array", [False, True])
def test_json_output_arrays_do_not_use_npz(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    disabled: list[str],
    numpy_array: bool,
) -> None:
    workflow = hf.Workflow.from_template_data(
        template_name="json_array_test",
        tasks=[
            hf.Task(
                schema=hf.TaskSchema(
                    "json_array_test",
                    outputs=[hf.Parameter("p1")],
                    actions=[hf.Action(script="test.py")],
                ),
                disable_array_outputs=disabled,
            )
        ],
        path=tmp_path,
        store="json",
    )
    run = next(iter(workflow.get_EARs_of_tasks([workflow.tasks[0].insert_ID])))
    write_archive = Mock(side_effect=AssertionError("JSON must not write NPZ files"))
    read_archive = Mock(side_effect=AssertionError("JSON must not read NPZ files"))
    monkeypatch.setattr(workflow, "_save_array_output", write_archive)
    monkeypatch.setattr(workflow, "_read_array_output_archive", read_archive)
    array = np.arange(6).reshape(2, 3)
    values = {"nested": {"data": array if numpy_array else array.tolist()}}

    if numpy_array:
        with pytest.raises(ValueError, match="cannot be serialised"):
            workflow.save_parameter("outputs.p1", values, EAR_ID=run.id_)
    else:
        workflow.save_parameter("outputs.p1", values, EAR_ID=run.id_)
        reloaded = hf.Workflow(workflow.path)
        param_ID = run.data_idx["outputs.p1"]
        assert isinstance(param_ID, int)
        stored = reloaded.get_parameter_data(param_ID)
        assert stored == values
    assert workflow.list_array_outputs() == []
    assert not list(Path(workflow.path).rglob("*.npz*"))
    write_archive.assert_not_called()
    read_archive.assert_not_called()


def test_zarr_output_arrays_written_only_to_npz(zarr_array_workflow: tuple) -> None:
    workflow, run = zarr_array_workflow
    task = run.task
    nested_data = np.arange(6).reshape(2, 3)
    sequence_array = np.array(7, dtype="int32")
    unconfigured_array = np.array([1.5, 2.5], dtype="float32")
    values = {
        "nested": {"data": nested_data},
        "unconfigured": unconfigured_array,
        "sequence": [sequence_array],
        "plain_list": [1, 2],
    }

    workflow.save_parameter("outputs.p1", values, EAR_ID=run.id_)

    for name, expected in {
        "p1.nested.data": nested_data,
        "p1.unconfigured": unconfigured_array,
        "p1.sequence.0": sequence_array,
    }.items():
        archive = workflow._read_array_output_archive(
            workflow._get_array_output_archive_path(name, 6)
        )
        assert len(archive.run_ID) == 1
        np.testing.assert_array_equal(archive.data[0], expected)
        np.testing.assert_array_equal(archive.run_ID, [run.id_])
        np.testing.assert_array_equal(archive.task_insert_ID, [task.insert_ID])
        assert archive.data.dtype == expected.dtype
    assert workflow.list_array_outputs() == [
        "p1.nested.data",
        "p1.sequence.0",
        "p1.unconfigured",
    ]
    param_ID = run.data_idx["outputs.p1"]
    inner_group = workflow._store._get_or_create_parameter_data_array_inner_group(
        param_ID, mode="r+"
    )
    parameter_name = workflow._store._param_data_arr_grp_names(param_ID)[-1]
    assert parameter_name not in inner_group
    input_ID = run.data_idx["inputs.p2"]
    input_arrays = workflow._store.get_parameter_data_array_group(input_ID)
    np.testing.assert_array_equal(input_arrays["arr_0"][...], np.arange(3))
    root_array = np.arange(4, dtype="float64")
    workflow.set_parameter_value(param_ID, root_array)
    archive = workflow._read_array_output_archive(
        workflow._get_array_output_archive_path("p1", 6)
    )
    np.testing.assert_array_equal(archive.data, root_array[np.newaxis, ...])
    assert parameter_name not in inner_group
    original_bytes = workflow._get_array_output_archive_path("p1", 6).read_bytes()
    with pytest.raises(TypeError, match="dtype int32.*dtype float64"):
        workflow._store._set_parameter_values(
            {param_ID: (np.arange(4, dtype="int32"), False)}
        )
    assert workflow._get_array_output_archive_path("p1", 6).read_bytes() == original_bytes
    assert parameter_name not in inner_group


@pytest.mark.parametrize("run_file_ID", [499, 500, 1000])
def test_zarr_array_output_zstandard_compression(
    zarr_array_workflow: tuple,
    monkeypatch: pytest.MonkeyPatch,
    run_file_ID: int,
) -> None:
    workflow, run = zarr_array_workflow
    monkeypatch.setattr(run, "_run_file_ID", run_file_ID)
    value = np.zeros((100, 100), dtype="float64")
    workflow.save_parameter("outputs.p1", value, EAR_ID=run.id_)
    path = workflow._get_array_output_archive_path("p1", run_file_ID)
    compressed = path.read_bytes()
    uncompressed = zstd.ZstdDecompressor().decompress(compressed)
    assert uncompressed.startswith(b"PK")
    assert len(compressed) < len(uncompressed) // 10
    archive = workflow._read_array_output_archive(path)
    np.testing.assert_array_equal(archive.data[0], value)
    assert workflow.list_array_outputs() == ["p1"]


@pytest.mark.parametrize("dataset_copy", [False, True])
@pytest.mark.parametrize("storage", ["npz", "legacy", "dual"])
def test_zarr_output_array_decode_prefers_npz(
    zarr_array_workflow: tuple,
    monkeypatch: pytest.MonkeyPatch,
    dataset_copy: bool,
    storage: str,
) -> None:
    workflow, run = zarr_array_workflow
    nested_data = np.arange(6).reshape(2, 3)
    list_array = np.array([1.5, 2.5], dtype="float32")
    scalar_array = np.array(7, dtype="int32")
    values = {
        "nested": {"data": nested_data},
        "scalar": scalar_array,
        "list": [list_array],
    }
    if storage != "npz":
        run.task.template.disable_array_outputs = ["p1"]
    workflow.save_parameter("outputs.p1", values, EAR_ID=run.id_)
    run.task.template.disable_array_outputs = []
    param_ID = run.data_idx["outputs.p1"]
    inner_group = workflow._store._get_or_create_parameter_data_array_inner_group(
        param_ID, mode="r+"
    )
    parameter_name = workflow._store._param_data_arr_grp_names(param_ID)[-1]
    if storage == "npz":
        assert parameter_name not in inner_group
    elif storage == "dual":
        arrays = inner_group[parameter_name]
        for idx, (name, value) in enumerate(
            (
                ("p1.nested.data", nested_data),
                ("p1.scalar", scalar_array),
                ("p1.list.0", list_array),
            )
        ):
            workflow._save_array_output(run, name, value)
            arrays[f"arr_{idx}"][...] = -99
    monkeypatch.setattr(
        workflow._store,
        "_get_run_submission_metadata",
        lambda IDs: {ID: (0, 0, 6, 0) for ID in IDs},
    )
    read_archive = Mock(wraps=workflow._read_array_output_archive)
    monkeypatch.setattr(workflow, "_read_array_output_archive", read_archive)
    workflow._store.parameter_cache.clear()

    stored = workflow.get_parameter_data(param_ID, dataset_copy=dataset_copy)

    for actual, expected in [
        (stored["nested"]["data"], nested_data),
        (stored["scalar"], scalar_array),
        (stored["list"][0], list_array),
    ]:
        np.testing.assert_array_equal(actual, expected)
        assert actual.dtype == expected.dtype
    assert read_archive.call_count == (0 if storage == "legacy" else 3)


@pytest.mark.parametrize(
    ("task_disabled", "schema_disabled", "expected"),
    [
        ([], [], ["p1.a", "p1.b"]),
        (["p1.a"], [], ["p1.b"]),
        ([], ["outputs.p1.b"], ["p1.a"]),
        (["p1.a"], ["p1.b"], []),
        (["outputs.p1"], [], []),
        ([], ["p1"], []),
        (["p1.missing"], [], ["p1.a", "p1.b"]),
    ],
)
def test_zarr_disable_array_outputs(
    zarr_array_workflow: tuple,
    monkeypatch: pytest.MonkeyPatch,
    task_disabled: list[str],
    schema_disabled: list[str],
    expected: list[str],
) -> None:
    workflow, run = zarr_array_workflow
    run.task.template.disable_array_outputs = task_disabled
    run.task.template.schemas[0].disable_array_outputs = schema_disabled
    values = {"a": np.arange(3), "b": np.arange(4)}

    workflow.save_parameter("outputs.p1", values, EAR_ID=run.id_)

    assert workflow.list_array_outputs() == expected
    param_ID = run.data_idx["outputs.p1"]
    inner_group = workflow._store._get_or_create_parameter_data_array_inner_group(
        param_ID, mode="r+"
    )
    parameter_name = workflow._store._param_data_arr_grp_names(param_ID)[-1]
    legacy_names = [name for name in values if f"p1.{name}" not in expected]
    if legacy_names:
        arrays = inner_group[parameter_name]
        assert sorted(arrays.keys()) == [f"arr_{idx}" for idx in range(len(legacy_names))]
        for idx, name in enumerate(legacy_names):
            np.testing.assert_array_equal(arrays[f"arr_{idx}"][...], values[name])
    else:
        assert parameter_name not in inner_group
    monkeypatch.setattr(
        workflow._store,
        "_get_run_submission_metadata",
        lambda IDs: {ID: (0, 0, 6, 0) for ID in IDs},
    )
    workflow._store.parameter_cache.clear()
    stored = workflow.get_parameter_data(param_ID, dataset_copy=True)
    for name, value in values.items():
        np.testing.assert_array_equal(stored[name], value)


def test_zarr_legacy_output_decode_without_array_archives(
    zarr_array_workflow: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    workflow, run = zarr_array_workflow
    run.task.template.disable_array_outputs = ["p1"]
    values = {"data": np.arange(6).reshape(2, 3), "text": "legacy"}
    workflow.save_parameter("outputs.p1", values, EAR_ID=run.id_)
    assert not (Path(workflow._store.path) / "parameters" / "array_outputs").exists()
    monkeypatch.setattr(
        workflow._store,
        "_get_run_submission_metadata",
        lambda IDs: {ID: (0, 0, 6, 0) for ID in IDs},
    )
    monkeypatch.setattr(
        workflow,
        "get_EARs_from_IDs",
        Mock(side_effect=AssertionError("Legacy decoding must not load array context")),
    )
    workflow._store.parameter_cache.clear()

    stored = workflow.get_parameter_data(run.data_idx["outputs.p1"], dataset_copy=True)

    np.testing.assert_array_equal(stored["data"], values["data"])
    assert stored["text"] == "legacy"


def test_zarr_zip_reads_array_outputs(
    zarr_array_workflow: tuple, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    workflow, run = zarr_array_workflow
    value = np.arange(6).reshape(2, 3)
    workflow.save_parameter("outputs.p1", {"data": value}, EAR_ID=run.id_)
    zip_workflow = hf.Workflow(workflow.zip(path=tmp_path, status=False))
    zip_run = cast(Any, zip_workflow.get_EARs_from_IDs(run.id_))
    monkeypatch.setattr(zip_run, "_run_file_ID", 6)
    original_get_runs = zip_workflow.get_EARs_from_IDs
    monkeypatch.setattr(
        zip_workflow,
        "get_EARs_from_IDs",
        lambda IDs: zip_run if isinstance(IDs, int) else original_get_runs(IDs),
    )
    monkeypatch.setattr(
        zip_workflow._store,
        "_get_run_submission_metadata",
        lambda IDs: {ID: (0, 0, 6, 0) for ID in IDs},
    )

    assert zip_workflow.list_array_outputs() == ["p1.data"]
    np.testing.assert_array_equal(zip_workflow.get_array_output("p1.data").data, [value])
    np.testing.assert_array_equal(
        zip_workflow.get_parameter_data(run.data_idx["outputs.p1"])["data"], value
    )


@pytest.mark.parametrize("disabled", [False, True])
def test_zarr_masked_output_array_decode(
    zarr_array_workflow: tuple, monkeypatch: pytest.MonkeyPatch, disabled: bool
) -> None:
    workflow, run = zarr_array_workflow
    if disabled:
        run.task.template.disable_array_outputs = ["p1"]
    value = np.ma.array([1.0, 2.0], mask=[False, True], fill_value=-10.0)
    values = {"plain": np.arange(3), "masked": value, "other_masked": value + 1}
    workflow.save_parameter("outputs.p1", values, EAR_ID=run.id_)
    param_ID = run.data_idx["outputs.p1"]
    arrays = workflow._store.get_parameter_data_array_group(param_ID)
    assert len(list(arrays.keys())) == (5 if disabled else 2)
    monkeypatch.setattr(
        workflow._store,
        "_get_run_submission_metadata",
        lambda IDs: {ID: (0, 0, 6, 0) for ID in IDs},
    )
    workflow._store.parameter_cache.clear()

    stored = workflow.get_parameter_data(param_ID, dataset_copy=True)
    np.testing.assert_array_equal(stored["plain"], values["plain"])
    for name in ("masked", "other_masked"):
        expected = values[name]
        assert isinstance(expected, np.ma.MaskedArray)
        assert isinstance(stored[name], np.ma.MaskedArray)
        np.testing.assert_array_equal(stored[name].data, expected.data)
        np.testing.assert_array_equal(stored[name].mask, expected.mask)
        assert stored[name].fill_value == expected.fill_value


@pytest.mark.parametrize("remove_directory", [False, True])
def test_zarr_npz_only_output_missing_archive_raises(
    zarr_array_workflow: tuple,
    monkeypatch: pytest.MonkeyPatch,
    remove_directory: bool,
) -> None:
    workflow, run = zarr_array_workflow
    workflow.save_parameter("outputs.p1", np.arange(3), EAR_ID=run.id_)
    path = workflow._get_array_output_archive_path("p1", 6)
    path.unlink()
    if remove_directory:
        path.parent.rmdir()
        path.parent.parent.rmdir()
        path.parent.parent.parent.rmdir()
    monkeypatch.setattr(
        workflow._store,
        "_get_run_submission_metadata",
        lambda IDs: {ID: (0, 0, 6, 0) for ID in IDs},
    )
    workflow._store.parameter_cache.clear()
    with pytest.raises(FileNotFoundError, match="Missing NPZ archive"):
        workflow.get_parameter_data(run.data_idx["outputs.p1"])


@pytest.mark.parametrize("valid_zstd", [False, True])
def test_zarr_array_output_decode_rejects_corrupt_archive(
    zarr_array_workflow: tuple, monkeypatch: pytest.MonkeyPatch, valid_zstd: bool
) -> None:
    workflow, run = zarr_array_workflow
    workflow.save_parameter("outputs.p1", np.arange(3), EAR_ID=run.id_)
    path = workflow._get_array_output_archive_path("p1", 6)
    corrupt_data = b"not an npz archive"
    path.write_bytes(
        zstd.ZstdCompressor(level=1).compress(corrupt_data)
        if valid_zstd
        else corrupt_data
    )
    monkeypatch.setattr(
        workflow._store,
        "_get_run_submission_metadata",
        lambda IDs: {ID: (0, 0, 6, 0) for ID in IDs},
    )
    workflow._store.parameter_cache.clear()

    with pytest.raises(ValueError if valid_zstd else zstd.ZstdError):
        workflow.get_parameter_data(run.data_idx["outputs.p1"])


@pytest.mark.skip("need to refactor `make_test_store_from_spec`")
def test_store_pending_add_task(tmp_path: Path):
    """Check expected pending state after adding a task."""

    # make store: 0 tasks:
    store = JSONPersistentStore.make_test_store_from_spec(hf, [], dir=tmp_path)
    task_ID = store.add_task()
    assert store._pending.add_tasks == {task_ID: []}


@pytest.mark.skip("need to refactor `make_test_store_from_spec`")
def test_store_pending_add_element(tmp_path: Path):
    """Check expected pending state after adding an element."""

    # make store: 1 task with 0 elements:
    store = JSONPersistentStore.make_test_store_from_spec(app=hf, spec=[{}], dir=tmp_path)
    elem_ID = store.add_element(task_ID=0)
    assert store._pending.add_elements == {
        elem_ID: JsonStoreElement(
            id_=elem_ID,
            is_pending=True,
            es_idx=0,
            task_ID=0,
            iteration_IDs=[],
            index=0,
            seq_idx={},
            src_idx={},
        )
    } and store._pending.add_task_element_IDs == {0: [0]}


@pytest.mark.skip("need to refactor `make_test_store_from_spec`")
@pytest.mark.parametrize("elem_ID", [0, 1])
def test_store_pending_add_element_iter(tmp_path: Path, elem_ID: int):
    """Check expected pending state after adding an element iteration."""

    # make store: 1 task with 2 elements and 0 iterations:
    store = JSONPersistentStore.make_test_store_from_spec(
        hf,
        [{"elements": [{}, {}]}],
        dir=tmp_path,
    )
    iter_ID = store.add_element_iteration(
        element_ID=elem_ID,
        data_idx={},
        schema_parameters=[],
    )
    assert store._pending.add_elem_iters == {
        iter_ID: JsonStoreElementIter(
            id_=iter_ID,
            is_pending=True,
            element_ID=elem_ID,
            EAR_IDs={},
            data_idx={},
            schema_parameters=[],
            EARs_initialised=False,
        )
    } and store._pending.add_elem_iter_IDs == {elem_ID: [iter_ID]}


@pytest.mark.skip("need to refactor `make_test_store_from_spec`")
def test_store_pending_add_EAR(tmp_path: Path):
    """Check expected pending state after adding an EAR."""

    # make store: 1 task with 1 element and 1 iteration:
    store = JSONPersistentStore.make_test_store_from_spec(
        hf,
        [{"elements": [{"iterations": [{}]}]}],
        dir=tmp_path,
    )
    EAR_ID = store.add_EAR(
        elem_iter_ID=0,
        action_idx=0,
        commands_idx=[],
        data_idx={},
        metadata={},
    )
    assert store._pending.add_EARs == {
        EAR_ID: JsonStoreEAR(
            id_=EAR_ID,
            is_pending=True,
            elem_iter_ID=0,
            action_idx=0,
            commands_idx=[],
            data_idx={},
            metadata={},
        )
    }


@pytest.mark.skip("need to refactor `make_test_store_from_spec`")
def test_get_task_elements_task_is_pending(tmp_path: Path):
    """Check we get an empty list when getting all task elements of a pending task to
    which no elements have been added."""
    # make store: 0 tasks:
    store = JSONPersistentStore.make_test_store_from_spec(hf, [], dir=tmp_path)
    task_ID = store.add_task()
    assert store.get_task_elements(task_ID, slice(0, None)) == []


@pytest.mark.skip("need to refactor `make_test_store_from_spec`")
def test_get_task_elements_single_element_is_pending(tmp_path: Path):
    """Check expected return when getting all task elements of a persistent task that has
    a single pending element."""
    # make store: 1 task
    store = JSONPersistentStore.make_test_store_from_spec(hf, [{}], dir=tmp_path)
    store.add_element(task_ID=0)
    assert store.get_task_elements(0, slice(0, None)) == [
        {
            "id": 0,
            "is_pending": True,
            "element_idx": 0,
            "iteration_IDs": [],
            "task_ID": 0,
            "iterations": [],
        }
    ]


@pytest.mark.skip("need to refactor `make_test_store_from_spec`")
def test_get_task_elements_multi_element_one_pending(tmp_path: Path):
    """Check expected return when getting all task elements of a persistent task that has
    a persistent element and a pending element."""
    # make store: 1 task with 1 element:
    store = JSONPersistentStore.make_test_store_from_spec(
        hf, [{"elements": [{}]}], dir=tmp_path
    )
    store.add_element(task_ID=0)
    assert store.get_task_elements(0, slice(0, None)) == [
        {
            "id": 0,
            "is_pending": False,
            "element_idx": 0,
            "iteration_IDs": [],
            "task_ID": 0,
            "iterations": [],
        },
        {
            "id": 1,
            "is_pending": True,
            "element_idx": 1,
            "iteration_IDs": [],
            "task_ID": 0,
            "iterations": [],
        },
    ]


@pytest.mark.skip("need to refactor `make_test_store_from_spec`")
def test_get_task_elements_single_element_iter_pending(tmp_path: Path):
    """Check expected return when getting all task elements of a persistent task that has
    a persistent element with a pending iteration."""
    # make store: 1 task with 1 element:
    store = JSONPersistentStore.make_test_store_from_spec(
        hf, [{"elements": [{}]}], dir=tmp_path
    )
    store.add_element_iteration(element_ID=0, data_idx={}, schema_parameters=[])
    assert store.get_task_elements(0, slice(0, None)) == [
        {
            "id": 0,
            "is_pending": False,
            "element_idx": 0,
            "iteration_IDs": [0],
            "task_ID": 0,
            "iterations": [
                {
                    "id": 0,
                    "is_pending": True,
                    "element_ID": 0,
                    "EAR_IDs": {},
                    "data_idx": {},
                    "schema_parameters": [],
                    "EARs": {},
                }
            ],
        },
    ]


@pytest.mark.skip("need to refactor `make_test_store_from_spec`")
def test_get_task_elements_single_element_iter_EAR_pending(tmp_path: Path):
    """Check expected return when getting all task elements of a persistent task that has
    a persistent element with a persistent iteration and a pending EAR"""
    # make store: 1 task with 1 element with 1 iteration:
    store = JSONPersistentStore.make_test_store_from_spec(
        hf, [{"elements": [{"iterations": [{}]}]}], dir=tmp_path
    )
    store.add_EAR(elem_iter_ID=0, action_idx=0, commands_idx=[], data_idx={}, metadata={})
    assert store.get_task_elements(0, slice(0, None)) == [
        {
            "id": 0,
            "is_pending": False,
            "element_idx": 0,
            "iteration_IDs": [0],
            "task_ID": 0,
            "iterations": [
                {
                    "id": 0,
                    "is_pending": False,
                    "element_ID": 0,
                    "EAR_IDs": {0: [0]},
                    "data_idx": {},
                    "schema_parameters": [],
                    "EARs": {
                        0: [
                            {
                                "id_": 0,
                                "is_pending": True,
                                "elem_iter_ID": 0,
                                "action_idx": 0,
                                "commands_idx": [],
                                "data_idx": {},
                                "metadata": {},
                            }
                        ]
                    },
                },
            ],
        },
    ]


def test_make_zarr_store_zstd_compressor(tmp_path: Path):
    wk = make_test_data_YAML_workflow(
        workflow_name="workflow_1.yaml",
        path=tmp_path,
        store="zarr",
        store_kwargs={"compressor": "zstd"},
    )


def test_make_zarr_store_no_compressor(tmp_path: Path):
    wk = make_test_data_YAML_workflow(
        workflow_name="workflow_1.yaml",
        path=tmp_path,
        store="zarr",
        store_kwargs={"compressor": None},
    )


@pytest.mark.integration
@pytest.mark.skip(reason="Rechunking needs to be re-implemented.")
def test_zarr_rechunk_data_equivalent(tmp_path: Path):
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 101},
        repeats=3,
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="test_run_rechunk",
        workflow_name="test_run_rechunk",
        path=tmp_path,
    )
    wk.submit(wait=True, status=False, add_to_known=False)
    wk.rechunk_runs(backup=True, status=False, chunk_size=None)  # None -> one chunk

    arr = cast("ZarrPersistentStore", wk._store)._get_EARs_task_array(0)  # type: ignore[attr-defined]
    assert arr.chunks == arr.shape

    bak_path = (Path(arr.store.path) / arr.path).with_suffix(".bak")
    arr_bak = zarr.open(bak_path)

    assert arr_bak.chunks == (1, 1, 10, 2)  # (elem, elem, iter, act)

    # check backup and new runs data are equal:
    assert np.all(arr[:] == arr_bak[:])

    # check attributes are equal:
    assert arr.attrs.asdict() == arr_bak.attrs.asdict()


@pytest.mark.integration
@pytest.mark.skip(reason="Rechunking needs to be re-implemented.")
def test_zarr_rechunk_data_equivalent_custom_chunk_size(tmp_path: Path):
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 101},
        repeats=3,
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="test_run_rechunk",
        workflow_name="test_run_rechunk",
        path=tmp_path,
    )
    wk.submit(wait=True, status=False, add_to_known=False)
    wk.rechunk_runs(backup=True, status=False, chunk_size=(2, 2, 1, 2))

    arr = cast("ZarrPersistentStore", wk._store)._get_EARs_task_array(0)  # type: ignore[attr-defined]
    assert arr.chunks == (2, 2, 1, 2)  # (elem, elem, iter, act)

    bak_path = (Path(arr.store.path) / arr.path).with_suffix(".bak")
    arr_bak = zarr.open(bak_path)

    assert arr_bak.chunks == (1, 1, 10, 2)  # (elem, elem, iter, act)

    # check backup and new runs data are equal:
    assert np.all(arr[:] == arr_bak[:])


@pytest.mark.integration
@pytest.mark.skip(reason="Rechunking needs to be re-implemented.")
def test_zarr_rechunk_data_no_backup_load_runs(tmp_path: Path):
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 101},
        repeats=3,
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="test_run_rechunk",
        workflow_name="test_run_rechunk",
        path=tmp_path,
    )
    wk.submit(wait=True, status=False, add_to_known=False)
    wk.rechunk_runs(backup=False, status=False)

    arr = cast("ZarrPersistentStore", wk._store)._get_EARs_task_array(0)  # type: ignore[attr-defined]

    bak_path = (Path(arr.store.path) / arr.path).with_suffix(".bak")
    assert not bak_path.is_file()

    # check we can load runs:
    runs = wk._store._get_persistent_EARs(id_lst=list(range(wk.num_EARs)))
    run_ID = []
    for i in runs.values():
        run_ID.append(i.id_)


@pytest.mark.integration
@pytest.mark.skip(reason="Rechunking needs to be re-implemented.")
def test_zarr_rechunk_data_no_backup_load_parameter_base(tmp_path: Path):
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 101},
        repeats=3,
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="test_run_rechunk",
        workflow_name="test_run_rechunk",
        path=tmp_path,
    )
    wk.submit(wait=True, status=False, add_to_known=False)

    params_old = wk.get_all_parameter_data()
    wk.rechunk_parameter_base(backup=False, status=False)

    wk = wk.reload()
    params_new = wk.get_all_parameter_data()
    assert params_new == params_old

    arr = cast("ZarrPersistentStore", wk._store)._get_parameter_base_array()  # type: ignore[attr-defined]

    bak_path = (Path(arr.store.path) / arr.path).with_suffix(".bak")
    assert not bak_path.is_file()

    # check we can load parameters:
    param_IDs = []
    for i in wk.get_all_parameters():
        param_IDs.append(i.id_)


def test_get_parameter_sources_duplicate_ids(tmp_path):
    wk = make_workflow(
        schemas_spec=[[{"p1": None}, ("p1",), "t1"]],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    id_lst = [0, 1, 1, 2, 0]
    src = wk._store.get_parameter_sources(id_lst)
    assert len(src) == len(id_lst)
    assert src[0] == src[4]
    assert src[1] == src[2]


def _transform_jobscript_dependencies_to_encodable(
    deps: dict[tuple[int, int], dict[tuple[int, int], dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    """Transform a dict of jobscript dependencies written in a more testing-friendly/
    convenient format into the format expected by the method
    `ZarrPersistentStore._encode_jobscript_block_dependencies`.

    """
    max_js_idx = max(i[0] for i in deps)
    sub_js: dict[str, list[dict[str, Any]]] = {
        "jobscripts": [
            {"blocks": [], "index": js_idx} for js_idx in range(max_js_idx + 1)
        ]
    }
    for (js_idx, blk_idx), deps_i in deps.items():
        sub_js["jobscripts"][js_idx]["blocks"].append(
            {
                "dependencies": [[[k[0], k[1]], v] for k, v in deps_i.items()],
                "index": blk_idx,
            }
        )
    return sub_js


def test_zarr_encode_jobscript_block_dependencies_element_mapping_array_non_array_equivalence():
    deps_1 = {
        (0, 0): {},
        (1, 0): {(0, 0): {"js_element_mapping": {0: [0]}, "is_array": True}},
    }
    deps_2 = {
        (0, 0): {},
        (1, 0): {(0, 0): {"js_element_mapping": {0: np.array([0])}, "is_array": True}},
    }
    deps_1 = _transform_jobscript_dependencies_to_encodable(deps_1)
    deps_2 = _transform_jobscript_dependencies_to_encodable(deps_2)
    arr_1 = ZarrPersistentStore._encode_jobscript_block_dependencies(deps_1)
    arr_2 = ZarrPersistentStore._encode_jobscript_block_dependencies(deps_2)
    assert np.array_equal(arr_1, arr_2)


def test_zarr_encode_decode_jobscript_block_dependencies():

    deps = {
        (0, 0): {},
        (1, 0): {
            (0, 0): {
                "js_element_mapping": {0: [0], 1: [1]},
                "is_array": True,
            }
        },
        (2, 0): {
            (1, 0): {
                "js_element_mapping": {0: [0, 1], 1: [0, 1]},
                "is_array": False,
            }
        },
        (2, 1): {
            (0, 0): {"js_element_mapping": {0: [0, 1]}, "is_array": False},
            (2, 0): {"js_element_mapping": {0: [0, 1]}, "is_array": False},
        },
    }
    deps_t = _transform_jobscript_dependencies_to_encodable(deps)
    arr = ZarrPersistentStore._encode_jobscript_block_dependencies(deps_t)
    assert np.array_equal(
        arr,
        np.array(
            [
                2,
                0,
                0,
                12,
                1,
                0,
                9,
                0,
                0,
                1,
                2,
                0,
                0,
                2,
                1,
                1,
                14,
                2,
                0,
                11,
                1,
                0,
                0,
                3,
                0,
                0,
                1,
                3,
                1,
                0,
                1,
                18,
                2,
                1,
                7,
                0,
                0,
                0,
                3,
                0,
                0,
                1,
                7,
                2,
                0,
                0,
                3,
                0,
                0,
                1,
            ]
        ),
    )
    deps_rt = ZarrPersistentStore._decode_jobscript_block_dependencies(arr)
    assert deps_rt == deps


def test_zarr_encode_decode_jobscript_block_dependencies_large_many_to_one():
    deps = {
        (0, 0): {},
        (1, 0): {
            (0, 0): {"js_element_mapping": {0: list(range(1_000_000))}, "is_array": False}
        },
    }
    deps_t = _transform_jobscript_dependencies_to_encodable(deps)
    arr = ZarrPersistentStore._encode_jobscript_block_dependencies(deps_t)
    deps_rt = ZarrPersistentStore._decode_jobscript_block_dependencies(arr)
    assert deps_rt == deps


def test_zarr_encode_decode_jobscript_block_dependencies_large_one_to_one():
    deps = {
        (0, 0): {},
        (1, 0): {
            (0, 0): {
                "js_element_mapping": {i: [i] for i in range(1_000_000)},
                "is_array": False,
            }
        },
    }
    deps_t = _transform_jobscript_dependencies_to_encodable(deps)
    arr = ZarrPersistentStore._encode_jobscript_block_dependencies(deps_t)
    deps_rt = ZarrPersistentStore._decode_jobscript_block_dependencies(arr)
    assert deps_rt == deps


@pytest.mark.parametrize(
    "array",
    (
        np.array([]),
        np.empty(0),
        np.empty((0, 1, 2)),
        np.array([1, 2, 3]),
        np.array([[1, 2, 3], [4, 5, 6]]),
    ),
)
def test_zarr_save_persistent_array_shape(tmp_path, array):
    s1 = make_schemas(({"p1": None}, ()))
    t1 = hf.Task(schema=s1, inputs={"p1": array})
    wk = hf.Workflow.from_template_data(
        template_name="test_save_empty_array",
        tasks=[t1],
        path=tmp_path,
    )
    assert array.shape == wk.tasks[0].elements[0].get("inputs.p1")[:].shape


def test_zarr_single_chunk_threshold(tmp_path):
    # test very large arrays (> ~1 GB) are saved using multiple chunks
    array = np.zeros(
        268_435_456
    )  # ~ 2.147483647 GB; greater than blosc's max buffer size
    s1 = make_schemas(({"p1": None}, ()))
    t1 = hf.Task(schema=s1, inputs={"p1": array})
    wk = hf.Workflow.from_template_data(
        template_name="test_large_array",
        tasks=[t1],
        path=tmp_path,
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_store_parameter_encode_decode_types(tmp_path, store):

    (s1,) = make_schemas(
        (
            {
                "p1": NullDefault.NULL,
                "p2": NullDefault.NULL,
                "p3": NullDefault.NULL,
                "p4": NullDefault.NULL,
                "p5": NullDefault.NULL,
                "p6": NullDefault.NULL,
                "p7": NullDefault.NULL,
                "p8": NullDefault.NULL,
                "p9": NullDefault.NULL,
            },
            tuple(),
        ),
    )

    p1 = [1, 2, 3]
    p2 = (1, 2, 3)
    p3 = {1, 2, 3}
    p4 = None
    p5 = np.arange(10)
    p6 = np.ma.array(np.arange(10), mask=np.random.randint(0, 2, 10))
    p7 = [[1, 2], (3, 4), {5, 6}]
    p8 = np.ma.array(np.arange(10), mask=np.ones(10))  # fully masked
    p9 = {
        "a2": np.ma.array(np.arange(10), mask=np.ones(10), fill_value=999)
    }  # custom fill value

    t1 = hf.Task(
        schema=s1,
        inputs={
            "p1": p1,
            "p2": p2,
            "p3": p3,
            "p4": p4,
            "p5": p5 if store == "zarr" else None,
            "p6": p6 if store == "zarr" else None,
            "p7": p7,
            "p8": p8 if store == "zarr" else None,
            "p9": p9 if store == "zarr" else None,
        },
    )

    wk = hf.Workflow.from_template_data(
        template_name="test_store_encoders",
        tasks=[t1],
        store=store,
        path=tmp_path,
    )

    assert wk.tasks[0].elements[0].get("inputs.p1") == p1
    assert wk.tasks[0].elements[0].get("inputs.p2") == p2
    assert wk.tasks[0].elements[0].get("inputs.p3") == p3
    assert wk.tasks[0].elements[0].get("inputs.p4") == p4
    assert wk.tasks[0].elements[0].get("inputs.p7") == p7

    if store == "zarr":
        assert np.allclose(wk.tasks[0].elements[0].get("inputs.p5"), p5)
        assert np.ma.allclose(wk.tasks[0].elements[0].get("inputs.p6"), p6)
        assert np.ma.allclose(wk.tasks[0].elements[0].get("inputs.p8"), p8)
        assert np.ma.allclose(wk.tasks[0].elements[0].get("inputs.p9.a2"), p9["a2"])
        assert (
            wk.tasks[0].elements[0].get("inputs.p9")["a2"].fill_value
            == p9["a2"].fill_value
        )
