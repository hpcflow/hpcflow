import os
from pathlib import Path
import pytest
from hpcflow.app import app as hf


@pytest.mark.integration
def test_zarr_metadata_file_modification_times_many_jobscripts(null_config, tmp_path):
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
def test_json_metadata_file_modification_times_many_jobscripts(null_config, tmp_path):
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
    null_config, tmp_path
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
def test_multiple_jobscript_functions_files(null_config, tmp_path):
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
