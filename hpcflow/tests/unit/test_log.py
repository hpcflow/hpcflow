import pytest

from hpcflow.app import app as hf


def test_temporary_file_logger(tmp_path):
    js_log = tmp_path / "js.log"
    run_log = tmp_path / "run.log"

    hf.log.remove_file_handler()
    hf.log.add_file_logger(js_log, level="DEBUG")

    hf.submission_logger.debug("before")

    with hf.log.temporary_file_logger(run_log):
        hf.submission_logger.debug("during")

    hf.submission_logger.debug("after")

    assert "before" in js_log.read_text()

    assert "during" not in js_log.read_text()
    assert "after" in js_log.read_text()

    assert "before" not in run_log.read_text()
    assert "during" in run_log.read_text()
    assert "after" not in run_log.read_text()

    with pytest.raises(RuntimeError):
        with hf.log.temporary_file_logger(run_log):
            raise RuntimeError

    hf.submission_logger.debug("after exception")

    assert "after exception" in js_log.read_text()
    assert "after exception" not in run_log.read_text()
