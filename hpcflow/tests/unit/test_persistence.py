from __future__ import annotations
from pathlib import Path
import sys
from typing import Any, Dict, List, Tuple
from typing import cast, TYPE_CHECKING
import numpy as np
import zarr  # type: ignore
import pytest
from hpcflow.sdk.core.test_utils import make_test_data_YAML_workflow, make_workflow
from hpcflow.sdk.persistence.json import (
    JSONPersistentStore,
    JsonStoreElement,
    JsonStoreElementIter,
    JsonStoreEAR,
)
from hpcflow.sdk.persistence.zarr import ZarrPersistentStore

from hpcflow.app import app as hf

if TYPE_CHECKING:
    from hpcflow.sdk.persistence.zarr import ZarrPersistentStore


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


def test_make_zarr_store_zstd_compressor(null_config, tmp_path: Path):
    wk = make_test_data_YAML_workflow(
        workflow_name="workflow_1.yaml",
        path=tmp_path,
        store="zarr",
        store_kwargs={"compressor": "zstd"},
    )


def test_make_zarr_store_no_compressor(null_config, tmp_path: Path):
    wk = make_test_data_YAML_workflow(
        workflow_name="workflow_1.yaml",
        path=tmp_path,
        store="zarr",
        store_kwargs={"compressor": None},
    )


@pytest.mark.integration
@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Python 3.8 support is being removed anyway."
)
def test_zarr_rechunk_data_equivalent(null_config, tmp_path: Path):
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

    arr = cast("ZarrPersistentStore", wk._store)._get_EARs_arr()
    assert arr.chunks == arr.shape

    bak_path = (Path(arr.store.path) / arr.path).with_suffix(".bak")
    arr_bak = zarr.open(bak_path)

    assert arr_bak.chunks == (1, 1)  # runs array is 2D

    # check backup and new runs data are equal:
    assert np.all(arr[:] == arr_bak[:])

    # check attributes are equal:
    assert arr.attrs.asdict() == arr_bak.attrs.asdict()


@pytest.mark.integration
@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="Python 3.8 support is being removed anyway."
)
def test_zarr_rechunk_data_equivalent_custom_chunk_size(null_config, tmp_path: Path):
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
    wk.rechunk_runs(backup=True, status=False, chunk_size=2)

    arr = cast("ZarrPersistentStore", wk._store)._get_EARs_arr()
    assert arr.chunks == (2, 2)  # runs array is 2D

    bak_path = (Path(arr.store.path) / arr.path).with_suffix(".bak")
    arr_bak = zarr.open(bak_path)

    assert arr_bak.chunks == (1, 1)  # runs array is 2D

    # check backup and new runs data are equal:
    assert np.all(arr[:] == arr_bak[:])


@pytest.mark.integration
def test_zarr_rechunk_data_no_backup_load_runs(null_config, tmp_path: Path):
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

    arr = cast("ZarrPersistentStore", wk._store)._get_EARs_arr()

    bak_path = (Path(arr.store.path) / arr.path).with_suffix(".bak")
    assert not bak_path.is_file()

    # check we can load runs:
    runs = wk._store._get_persistent_EARs(id_lst=list(range(wk.num_EARs)))
    run_ID = []
    for i in runs.values():
        run_ID.append(i.id_)


@pytest.mark.integration
def test_zarr_rechunk_data_no_backup_load_parameter_base(null_config, tmp_path: Path):
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

    arr = cast("ZarrPersistentStore", wk._store)._get_parameter_base_array()

    bak_path = (Path(arr.store.path) / arr.path).with_suffix(".bak")
    assert not bak_path.is_file()

    # check we can load parameters:
    param_IDs = []
    for i in wk.get_all_parameters():
        param_IDs.append(i.id_)


def test_get_parameter_sources_duplicate_ids(null_config, tmp_path):
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
