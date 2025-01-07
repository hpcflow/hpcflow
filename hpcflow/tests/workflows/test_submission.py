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
        store="zarr",  # do zarr and json
    )
    wk.submit(add_to_known=False, status=False, cancel=True)

    mtime_meta_group = Path(wk.path).joinpath(".zattrs").stat().st_mtime
    mtime_mid_jobscript_chunk = (
        Path(wk._store.path)
        .joinpath(wk._store._get_jobscripts_metadata_arr(0).path, str(int(num_js / 2)))
        .stat()
        .st_mtime
    )
    mtime_submission_group = (
        Path(wk._store.path)
        .joinpath(wk._store._get_submission_metadata_group(0).path, ".zattrs")
        .stat()
        .st_mtime
    )
    assert mtime_meta_group < mtime_mid_jobscript_chunk < mtime_submission_group
