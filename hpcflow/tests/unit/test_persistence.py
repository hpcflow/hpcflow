from pathlib import Path
import numpy as np
import zarr
import pytest
from hpcflow.sdk.core.test_utils import make_test_data_YAML_workflow
from hpcflow.sdk.persistence.base import StoreEAR, StoreElement, StoreElementIter
from hpcflow.sdk.persistence.json import JSONPersistentStore

from hpcflow.app import app as hf


@pytest.mark.skip("need to refactor `make_test_store_from_spec`")
def test_store_pending_add_task(tmp_path):
    """Check expected pending state after adding a task."""

    # make store: 0 tasks:
    store = JSONPersistentStore.make_test_store_from_spec([], dir=tmp_path)
    task_ID = store.add_task()
    assert store._pending.add_tasks == {task_ID: []}


@pytest.mark.skip("need to refactor `make_test_store_from_spec`")
def test_store_pending_add_element(tmp_path):
    """Check expected pending state after adding an element."""

    # make store: 1 task with 0 elements:
    store = JSONPersistentStore.make_test_store_from_spec(app=hf, spec=[{}], dir=tmp_path)
    elem_ID = store.add_element(task_ID=0)
    assert store._pending.add_elements == {
        elem_ID: StoreElement(
            id_=elem_ID,
            is_pending=True,
            element_idx=0,
            task_ID=0,
            iteration_IDs=[],
        )
    } and store._pending.add_task_element_IDs == {0: [0]}


@pytest.mark.skip("need to refactor `make_test_store_from_spec`")
@pytest.mark.parametrize("elem_ID", [0, 1])
def test_store_pending_add_element_iter(tmp_path, elem_ID):
    """Check expected pending state after adding an element iteration."""

    # make store: 1 task with 2 elements and 0 iterations:
    store = JSONPersistentStore.make_test_store_from_spec(
        [{"elements": [{}, {}]}],
        dir=tmp_path,
    )
    iter_ID = store.add_element_iteration(
        element_ID=elem_ID,
        data_idx={},
        schema_parameters=[],
    )
    assert store._pending.add_elem_iters == {
        iter_ID: StoreElementIter(
            id_=iter_ID,
            is_pending=True,
            element_ID=elem_ID,
            EAR_IDs={},
            data_idx={},
            schema_parameters=[],
        )
    } and store._pending.add_elem_iter_IDs == {elem_ID: [iter_ID]}


@pytest.mark.skip("need to refactor `make_test_store_from_spec`")
def test_store_pending_add_EAR(tmp_path):
    """Check expected pending state after adding an EAR."""

    # make store: 1 task with 1 element and 1 iteration:
    store = JSONPersistentStore.make_test_store_from_spec(
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
        EAR_ID: StoreEAR(
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
def test_get_task_elements_task_is_pending(tmp_path):
    """Check we get an empty list when getting all task elements of a pending task to
    which no elements have been added."""
    # make store: 0 tasks:
    store = JSONPersistentStore.make_test_store_from_spec([], dir=tmp_path)
    task_ID = store.add_task()
    assert store.get_task_elements(task_ID, slice(0, None)) == []


@pytest.mark.skip("need to refactor `make_test_store_from_spec`")
def test_get_task_elements_single_element_is_pending(tmp_path):
    """Check expected return when getting all task elements of a persistent task that has
    a single pending element."""
    # make store: 1 task
    store = JSONPersistentStore.make_test_store_from_spec([{}], dir=tmp_path)
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
def test_get_task_elements_multi_element_one_pending(tmp_path):
    """Check expected return when getting all task elements of a persistent task that has
    a persistent element and a pending element."""
    # make store: 1 task with 1 element:
    store = JSONPersistentStore.make_test_store_from_spec(
        [{"elements": [{}]}], dir=tmp_path
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
def test_get_task_elements_single_element_iter_pending(tmp_path):
    """Check expected return when getting all task elements of a persistent task that has
    a persistent element with a pending iteration."""
    # make store: 1 task with 1 element:
    store = JSONPersistentStore.make_test_store_from_spec(
        [{"elements": [{}]}], dir=tmp_path
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
def test_get_task_elements_single_element_iter_EAR_pending(tmp_path):
    """Check expected return when getting all task elements of a persistent task that has
    a persistent element with a persistent iteration and a pending EAR"""
    # make store: 1 task with 1 element with 1 iteration:
    store = JSONPersistentStore.make_test_store_from_spec(
        [{"elements": [{"iterations": [{}]}]}], dir=tmp_path
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


def test_make_zarr_store_zstd_compressor(null_config, tmp_path):
    wk = make_test_data_YAML_workflow(
        workflow_name="workflow_1.yaml",
        path=tmp_path,
        store="zarr",
        store_kwargs={"compressor": "zstd"},
    )


def test_make_zarr_store_no_compressor(null_config, tmp_path):
    wk = make_test_data_YAML_workflow(
        workflow_name="workflow_1.yaml",
        path=tmp_path,
        store="zarr",
        store_kwargs={"compressor": None},
    )


@pytest.mark.integration
def test_zarr_rechunk_data_equivalent(null_config, tmp_path):
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

    arr = wk._store._get_EARs_arr()
    assert arr.chunks == arr.shape

    bak_path = (Path(arr.store.path) / arr.path).with_suffix(".bak")
    arr_bak = zarr.open(bak_path)

    assert arr_bak.chunks == (1, 1)  # runs array is 2D

    # check backup and new runs data are equal:
    assert np.all(arr[:] == arr_bak[:])

    # check attributes are equal:
    assert arr.attrs.asdict() == arr_bak.attrs.asdict()


@pytest.mark.integration
def test_zarr_rechunk_data_equivalent_custom_chunk_size(null_config, tmp_path):
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

    arr = wk._store._get_EARs_arr()
    assert arr.chunks == (2, 2)  # runs array is 2D

    bak_path = (Path(arr.store.path) / arr.path).with_suffix(".bak")
    arr_bak = zarr.open(bak_path)

    assert arr_bak.chunks == (1, 1)  # runs array is 2D

    # check backup and new runs data are equal:
    assert np.all(arr[:] == arr_bak[:])


@pytest.mark.integration
def test_zarr_rechunk_data_no_backup_load_runs(null_config, tmp_path):
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

    arr = wk._store._get_EARs_arr()

    bak_path = (Path(arr.store.path) / arr.path).with_suffix(".bak")
    assert not bak_path.is_file()

    # check we can load runs:
    runs = wk._store._get_persistent_EARs(id_lst=list(range(wk.num_EARs)))
    run_ID = []
    for i in runs.values():
        run_ID.append(i.id_)


@pytest.mark.integration
def test_zarr_rechunk_data_no_backup_load_parameter_base(null_config, tmp_path):
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

    arr = wk._store._get_parameter_base_array()

    bak_path = (Path(arr.store.path) / arr.path).with_suffix(".bak")
    assert not bak_path.is_file()

    # check we can load parameters:
    params = wk.get_all_parameters()
    param_IDs = []
    for i in params:
        param_IDs.append(i.id_)
