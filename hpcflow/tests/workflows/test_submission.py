import os
from pathlib import Path
import pytest
from hpcflow.app import app as hf
from hpcflow.tests.workflows.conftest import (
    assert_wk_1_repeats_success,
)
from hpcflow.sdk.core.test_utils import (
    almost_submit,
    launch_forced_array_item,
    wait_for_array_item_completion,
    wait_for_jobscript_completion,
)


@pytest.mark.integration
def test_zarr_metadata_file_modification_times_many_jobscripts(tmp_path):
    """Test that root group attributes are modified first, then individual jobscript
    at-submit-metadata chunk files, then the submission at-submit-metadata group
    attributes."""

    num_js = 30
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 100},
        sequences=[
            hf.ValueSequence(
                path="resources.any.resources_id", values=list(range(num_js))
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_zarr_metadata_attrs_modified_times",
        path=tmp_path,
        tasks=[t1],
        store="zarr",
    )
    wk.submit(add_to_known=False, status=False, cancel=True)

    mtime_meta_group = Path(wk.path).joinpath(".zattrs").stat().st_mtime
    mtime_mid_jobscript_chunk = (
        wk._store._get_jobscripts_at_submit_metadata_arr_path(0)
        .joinpath(str(int(num_js / 2)))
        .stat()
        .st_mtime
    )
    mtime_submission_group = (
        wk._store._get_submission_metadata_group_path(0)
        .joinpath(".zattrs")
        .stat()
        .st_mtime
    )
    assert mtime_meta_group < mtime_mid_jobscript_chunk < mtime_submission_group


@pytest.mark.integration
def test_json_metadata_file_modification_times_many_jobscripts(tmp_path):
    """Test that the metadata.json file is modified first, then the submissions.json
    file."""

    num_js = 30
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 100},
        sequences=[
            hf.ValueSequence(
                path="resources.any.resources_id", values=list(range(num_js))
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_zarr_metadata_attrs_modified_times",
        path=tmp_path,
        tasks=[t1],
        store="json",
    )
    wk.submit(add_to_known=False, status=False, cancel=True)

    mtime_meta = Path(wk.path).joinpath("metadata.json").stat().st_mtime
    mtime_subs = Path(wk.path).joinpath("submissions.json").stat().st_mtime
    assert mtime_meta < mtime_subs


@pytest.mark.integration
def test_subission_start_end_times_equal_to_first_and_last_jobscript_start_end_times(
    tmp_path,
):
    num_js = 2
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 100},
        sequences=[
            hf.ValueSequence(
                path="resources.any.resources_id", values=list(range(num_js))
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_subission_start_end_times",
        path=tmp_path,
        tasks=[t1],
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    sub = wk.submissions[0]
    jobscripts = sub.jobscripts

    assert len(jobscripts) == num_js

    # submission has two jobscripts, so start time should be start time of first jobscript:
    assert sub.start_time == jobscripts[0].start_time

    # ...and end time should be end time of second jobscript:
    assert sub.end_time == jobscripts[1].end_time


@pytest.mark.integration
def test_multiple_jobscript_functions_files(tmp_path):
    if os.name == "nt":
        shell_exes = ["powershell.exe", "pwsh.exe", "pwsh.exe"]
    else:
        shell_exes = ["/bin/bash", "bash", "bash"]
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 100},
        sequences=[
            hf.ValueSequence(
                path="resources.any.shell_args.executable",
                values=shell_exes,
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_multi_js_funcs_files",
        path=tmp_path,
        tasks=[t1],
        store="json",
    )
    wk.submit(add_to_known=True, status=False, cancel=True)

    sub_js = wk.submissions[0].jobscripts
    assert len(sub_js) == 2

    funcs_0 = sub_js[0].jobscript_functions_path
    funcs_1 = sub_js[1].jobscript_functions_path

    assert funcs_0.is_file()
    assert funcs_1.is_file()
    assert funcs_0 != funcs_1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_forced_array_execution(wk_1_repeats_2):
    """Test completion tracking for a forced direct job array.

    Execute the array items out of order and verify that completing individual items does
    not prematurely mark the whole jobscript as complete. Once the final item completes,
    verify that the array completion state is consolidated to the jobscript-level
    completion marker and the intermediate array state is removed.

    """
    prepared = almost_submit(wk_1_repeats_2, force_array=True, status=False)
    sub = wk_1_repeats_2.submissions[-1]
    js = sub.jobscripts[0]

    assert js.array_indices is not None

    completion = js.completion_obj

    # deliberately don't execute in array-index order.
    array_indices = tuple(reversed(js.array_indices))

    for array_idx in array_indices[:-1]:
        launch_forced_array_item(js, prepared, array_idx)
        await wait_for_array_item_completion(completion, array_idx)
        assert not completion.is_complete()

    final_idx = array_indices[-1]
    launch_forced_array_item(js, prepared, final_idx)
    await wait_for_jobscript_completion(completion)

    assert completion.is_complete()
    assert not completion.array_path.exists()
    assert_wk_1_repeats_success(wk_1_repeats_2)
