from __future__ import annotations
import os
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.config.errors import ConfigFileValidationError, ConfigItemCallbackError


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
