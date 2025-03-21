import time
from pathlib import Path
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import make_test_data_YAML_workflow


@pytest.mark.wsl
def test_workflow_1(tmp_path: Path, null_config):
    wk = make_test_data_YAML_workflow("workflow_1_wsl.yaml", path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    time.sleep(20)  # TODO: bug! for some reason the new parameter isn't actually written
    # to disk when using WSL until several seconds after the workflow has finished!
    # this is probably because the NTFS filesystem is "sync'd" via polling in this case?
    # so changes made on the NTFS files by WSL are not immediate on the Windows side.
    # perhaps when we re-wire the wait command, we could add an option to wait on a
    # parameter being set, which could watch the relevant chunk file for changes?

    # ACTUALLY: I think wait is not working here at all for WSL... it's returning early!
    p2 = wk.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == "201"
