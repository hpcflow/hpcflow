from __future__ import annotations
from contextlib import nullcontext
from copy import deepcopy
import os
import time
import pytest

import hpcflow.sdk.config.config as config_module
from hpcflow.app import app as hf
from hpcflow.sdk.config import ConfigOptions
from hpcflow.sdk.config.errors import (
    ConfigFileValidationError,
    ConfigItemCallbackError,
    ConfigNonConfigurableError,
    ConfigReadOnlyError,
    ConfigUnknownItemError,
)


def test_reset_config(modifiable_config) -> None:
    cfg_dir = hf.config.config_directory
    machine_name = hf.config.machine
    new_machine_name = machine_name + "123"
    hf.config.machine = new_machine_name
    assert hf.config.machine == new_machine_name
    hf.reset_config(config_dir=cfg_dir)
    assert hf.config.machine == machine_name


def test_raise_on_invalid_config_file(modifiable_config) -> None:
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


def test_reset_invalid_config(modifiable_config) -> None:
    # make an invalid config file:
    cfg_path = hf.config.config_file_path
    with cfg_path.open("at+") as f:
        f.write("something_invalid: 1\n")

    # check we can reset the invalid file:
    cfg_dir = hf.config.config_directory
    hf.reset_config(config_dir=cfg_dir, warn=False)


def test_raise_on_set_default_scheduler_not_in_schedulers_list_invalid_name(
    modifiable_config,
) -> None:
    new_default = "invalid-scheduler"
    with pytest.raises(ConfigItemCallbackError):
        hf.config.default_scheduler = new_default


def test_raise_on_set_default_scheduler_not_in_schedulers_list_valid_name(
    modifiable_config,
) -> None:
    new_default = "slurm"  # valid but unsupported (by default) scheduler
    with pytest.raises(ConfigItemCallbackError):
        hf.config.default_scheduler = new_default


def test_without_callbacks_ctx_manager(modifiable_config) -> None:
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
def test_cache_faster_than_no_cache(modifiable_config):
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


def test_cache_read_only(modifiable_config):
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


def test_workflow_template_config_validation(modifiable_config, tmp_path):
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


def test_config_with_updates(modifiable_config):
    level_1 = hf.config.get("log_console_level")
    with hf.config._with_updates({"log_console_level": "debug"}):
        level_2 = hf.config.get("log_console_level")
    level_3 = hf.config.get("log_console_level")
    assert level_1 == level_3 != level_2


@pytest.mark.parametrize("key", ["delete_execute_dir", "move_scratch_to_workflow"])
def test_directory_policy_default_and_override(modifiable_config, key: str):
    assert hf.config.get(key) is True
    hf.config.set(key, False)
    assert hf.config.get(key) is False


def test_config_options_preserves_positional_arguments(tmp_path):
    options = ConfigOptions(
        tmp_path, "TEST_CONFIG_DIR", hf.config_options.default_config, (), None
    )
    assert options.extra_schemas == ()
    assert options.default_known_configs_dir is None
    assert not hasattr(options, "defaulted_config_keys")


def test_config_options_allows_missing_builtin_default(tmp_path):
    key = "move_scratch_to_workflow"
    default_config = deepcopy(hf.config_options.default_config)
    del default_config["config"]["move_scratch_to_workflow"]
    options = ConfigOptions(tmp_path, "TEST_CONFIG_DIR", default_config)
    assert options.get_runtime_default(key) is True
    assert key not in options.default_config["config"]


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("persist_unset", [False, True])
def test_get_missing_item_default(modifiable_config, cached: bool, persist_unset: bool):
    key = "move_scratch_to_workflow"
    hf.config.unset(key)
    if persist_unset:
        hf.config.save()
        hf.reload_config(config_dir=hf.config.config_directory, warn=False)
    config_path = hf.config.config_file_path
    contents = config_path.read_bytes()
    with hf.config.cached_config() if cached else nullcontext():
        assert hf.config.get(key) is True
        defaults: tuple[object, ...] = (True, False, 0, [], {})
        for default in defaults:
            assert hf.config.get(key, default=default) is True
            assert hf.config.get(key) is True
        assert hf.config.get(key, default=True, ret_root_and_parts=True) == (
            True,
            True,
            [key],
        )
        assert hf.config.get_all()[key] is True
    assert config_path.read_bytes() == contents


@pytest.mark.parametrize("cached", [False, True])
def test_get_caller_default_for_unset_legacy_setting(modifiable_config, cached: bool):
    key = "delete_execute_dir"
    hf.config.unset(key)
    with hf.config.cached_config() if cached else nullcontext():
        defaults: tuple[object, ...] = (True, False, 0, [], {})
        for default in defaults:
            assert hf.config.get(key, default=default) == default
            assert hf.config.get(key) is None


def test_runtime_default_comes_from_app_config(modifiable_config, monkeypatch):
    key = "move_scratch_to_workflow"
    hf.config.unset(key)
    monkeypatch.setitem(hf.config_options.default_config["config"], key, False)
    assert hf.config.get(key, default=True) is False
    assert getattr(hf.config, key) is False
    assert hf.config.get_all()[key] is False


def test_missing_app_default_uses_builtin_default(modifiable_config, monkeypatch):
    key = "move_scratch_to_workflow"
    hf.config.unset(key)
    monkeypatch.delitem(hf.config_options.default_config["config"], key)
    assert hf.config.get(key) is True
    assert hf.config.get_all()[key] is True
    assert key not in hf.config_options.default_config["config"]


def test_mutable_runtime_default_is_not_modified(modifiable_config, monkeypatch):
    key = "user_affiliations"
    hf.config.unset(key)
    monkeypatch.setitem(hf.config_options.default_config["config"], key, [])
    monkeypatch.setattr(config_module, "DEFAULTED_CONFIG_KEYS", [key])
    hf.config.get_all()[key].append("example")
    assert hf.config_options.default_config["config"]["user_affiliations"] == []
    assert hf.config.get(key) == []


@pytest.mark.parametrize("value", [False, True])
def test_get_default_preserves_explicit_value(modifiable_config, value: bool):
    hf.config.set("move_scratch_to_workflow", value)
    with hf.config.cached_config():
        assert hf.config.get("move_scratch_to_workflow", default=not value) is value


def test_get_nested_default(modifiable_config):
    assert hf.config.get("shells.missing", default=[]) == []


def test_get_default_rejects_unknown_item(modifiable_config):
    with pytest.raises(ConfigUnknownItemError):
        hf.config.get("unknown_setting", default=True)


def test_unset_without_runtime_default_keeps_existing_semantics(modifiable_config):
    hf.config.unset("log_file_path")
    assert hf.config.get("log_file_path") is None
    assert "log_file_path" not in hf.config.get_all()
    hf.config.save()
    hf.reload_config(config_dir=hf.config.config_directory, warn=False)
    assert hf.config.get("log_file_path") is None
    assert "log_file_path" not in hf.config.get_all()


@pytest.mark.integration
def test_workflow_template_config_set(modifiable_config, tmp_path):
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


def test_dot_delimited_override(modifiable_config):
    key = (
        "shells.powershell.defaults.executable"
        if os.name == "nt"
        else "shells.bash.defaults.executable"
    )
    value = "MY_SHELL_EXE"
    hf.unload_config()
    hf.load_config(overrides={key: value})
    assert hf.config.get(key) == value
