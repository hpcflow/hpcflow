from __future__ import annotations
import os
import time
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.config.errors import (
    ConfigFileValidationError,
    ConfigItemCallbackError,
    ConfigNonConfigurableError,
    ConfigReadOnlyError,
)


def test_reset_config(new_null_config) -> None:
    cfg_dir = hf.config.config_directory
    machine_name = hf.config.machine
    new_machine_name = machine_name + "123"
    hf.config.machine = new_machine_name
    assert hf.config.machine == new_machine_name
    hf.reset_config(config_dir=cfg_dir)
    assert hf.config.machine == machine_name


def test_raise_on_invalid_config_file(new_null_config) -> None:
    # make an invalid config file:
    cfg_path = hf.config.config_file_path
    with cfg_path.open("at+") as f:
        f.write("something_invalid: 1\n")

    # try to load the invalid file:
    cfg_dir = hf.config.config_directory
    with pytest.raises(ConfigFileValidationError):
        hf.reload_config(config_dir=cfg_dir, warn=False)
    hf.reset_config(config_dir=cfg_dir, warn=False)
    hf.unload_config()


def test_reset_invalid_config(new_null_config) -> None:
    # make an invalid config file:
    cfg_path = hf.config.config_file_path
    with cfg_path.open("at+") as f:
        f.write("something_invalid: 1\n")

    # check we can reset the invalid file:
    cfg_dir = hf.config.config_directory
    hf.reset_config(config_dir=cfg_dir, warn=False)
    hf.unload_config()


def test_raise_on_set_default_scheduler_not_in_schedulers_list_invalid_name(
    null_config,
) -> None:
    new_default = "invalid-scheduler"
    with pytest.raises(ConfigItemCallbackError):
        hf.config.default_scheduler = new_default


def test_raise_on_set_default_scheduler_not_in_schedulers_list_valid_name(
    null_config,
) -> None:
    new_default = "slurm"  # valid but unsupported (by default) scheduler
    with pytest.raises(ConfigItemCallbackError):
        hf.config.default_scheduler = new_default


def test_without_callbacks_ctx_manager(null_config) -> None:
    # set a new shell that would raise an error in the `callback_supported_shells`:
    new_default = "bash" if os.name == "nt" else "powershell"

    with hf.config._without_callbacks("callback_supported_shells"):
        hf.config.default_shell = new_default
        assert hf.config.default_shell == new_default

    # outside the context manager, the callback is reinstated, which should raise:
    with pytest.raises(ConfigItemCallbackError):
        hf.config.default_shell

    # unload the modified config so it's not reused by other tests
    hf.unload_config()


@pytest.mark.xfail(reason="Might occasionally fail.")
def test_cache_faster_than_no_cache(null_config):
    n = 10_000
    tic = time.perf_counter()
    for _ in range(n):
        _ = hf.config.machine
    toc = time.perf_counter()
    elapsed_no_cache = toc - tic

    with hf.config.cached_config():
        tic = time.perf_counter()
        for _ in range(n):
            _ = hf.config.machine
        toc = time.perf_counter()
    elapsed_cache = toc - tic

    assert elapsed_cache < elapsed_no_cache


def test_cache_read_only(new_null_config):
    """Check we cannot modify the config when using the cache"""

    # check we can set an item first:
    hf.machine = "abc"
    assert hf.machine == "abc"

    with pytest.raises(ConfigReadOnlyError):
        with hf.config.cached_config():
            hf.config.set("machine", "123")

    with pytest.raises(ConfigReadOnlyError):
        with hf.config.cached_config():
            hf.config.machine = "456"


def test_workflow_template_config_validation(new_null_config, tmp_path):
    wkt = hf.WorkflowTemplate(
        tasks=[],
        config={"log_file_level": "debug"},
        name="test_workflow_config_validation",
    )
    assert wkt.config == {"log_file_level": "debug"}


def test_workflow_template_config_validation_raises(unload_config, tmp_path):
    with pytest.raises(ConfigNonConfigurableError):
        hf.WorkflowTemplate(
            tasks=[],
            config={"bad_key": "debug"},
            name="test_workflow_config_validation_raises",
        )

    # workflow template config validation should not need to load the whole config:
    assert not hf.is_config_loaded


def test_config_with_updates(new_null_config):
    level_1 = hf.config.get("log_console_level")
    with hf.config._with_updates({"log_console_level": "debug"}):
        level_2 = hf.config.get("log_console_level")
    level_3 = hf.config.get("log_console_level")
    assert level_1 == level_3 != level_2
    hf.reload_config()


@pytest.mark.integration
def test_workflow_template_config_set(new_null_config, tmp_path):
    """Test we can set a workflow-level config item and that it is correctly applied
    during execution."""

    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 101},
    )
    log_path = tmp_path / "log.log"
    hf.config.set("log_file_level", "warning")
    hf.config.set("log_file_path", log_path)

    log_str_1 = "this should not appear in the log file"
    hf.submission_logger.debug(log_str_1)

    log_str_2 = "this should appear in the log file"
    hf.submission_logger.warning(log_str_2)

    assert log_path.is_file()
    log_file_contents = log_path.read_text()
    assert log_str_1 not in log_file_contents
    assert log_str_2 in log_file_contents

    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        config={"log_file_level": "debug"},
        resources={"any": {"write_app_logs": True}},
        workflow_name="test_workflow_config",
        template_name="test_workflow_config",
        path=tmp_path,
    )
    wk.submit(wait=True, status=False, add_to_known=False)

    # check some DEBUG messages present in the run logs
    debug_str = " DEBUG hpcflow.persistence:"

    run = wk.get_EARs_from_IDs([0])[0]
    run_log_path = run.get_app_log_path()
    assert run_log_path.is_file()

    run_log_contents = run_log_path.read_text()
    assert debug_str in run_log_contents

    # log file level should not have changed:
    assert hf.config.get("log_file_level") == "warning"

    hf.reload_config()
