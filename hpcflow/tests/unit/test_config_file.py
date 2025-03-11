from __future__ import annotations
from typing import Any
import pytest
from hpcflow.sdk.config.config_file import ConfigFile
from hpcflow.sdk.config.errors import ConfigFileInvocationIncompatibleError


def test_select_invocation_default_no_specified_matches() -> None:
    configs: dict[str, Any] = {
        "default": {
            "invocation": {
                "environment_setup": None,
                "match": {},
            }
        }
    }
    matched = ConfigFile.select_invocation(
        configs=configs,
        run_time_info={},
        path="/path/to/config.yaml",
    )
    assert matched == "default"


def test_select_invocation_default_no_specified_matches_with_run_time_info() -> None:
    configs: dict[str, Any] = {
        "default": {
            "invocation": {
                "environment_setup": None,
                "match": {},
            }
        }
    }
    run_time_info = {"hostname": "my-machine"}
    matched = ConfigFile.select_invocation(
        configs=configs,
        run_time_info=run_time_info,
        path="/path/to/config.yaml",
    )
    assert matched == "default"


def test_select_invocation_single_specified_match() -> None:
    configs: dict[str, Any] = {
        "default": {
            "invocation": {
                "environment_setup": None,
                "match": {"hostname": "my-machine"},
            }
        }
    }
    run_time_info = {"hostname": "my-machine"}
    matched = ConfigFile.select_invocation(
        configs=configs,
        run_time_info=run_time_info,
        path="/path/to/config.yaml",
    )
    assert matched == "default"


def test_select_invocation_single_specified_match_list_match_first() -> None:
    configs: dict[str, Any] = {
        "default": {
            "invocation": {
                "environment_setup": None,
                "match": {"hostname": ["my-machine", "my-other-machine"]},
            }
        }
    }
    run_time_info = {"hostname": "my-machine"}
    matched = ConfigFile.select_invocation(
        configs=configs,
        run_time_info=run_time_info,
        path="/path/to/config.yaml",
    )
    assert matched == "default"


def test_select_invocation_single_specified_match_list_match_second() -> None:
    configs: dict[str, Any] = {
        "default": {
            "invocation": {
                "environment_setup": None,
                "match": {"hostname": ["my-other-machine", "my-machine"]},
            }
        }
    }
    run_time_info = {"hostname": "my-machine"}
    matched = ConfigFile.select_invocation(
        configs=configs,
        run_time_info=run_time_info,
        path="/path/to/config.yaml",
    )
    assert matched == "default"


def test_select_invocation_raise_on_no_match() -> None:
    configs: dict[str, Any] = {
        "default": {
            "invocation": {
                "environment_setup": None,
                "match": {"hostname": "my-machine"},
            }
        }
    }
    run_time_info = {"hostname": "my-other-machine"}
    with pytest.raises(ConfigFileInvocationIncompatibleError):
        ConfigFile.select_invocation(
            configs=configs,
            run_time_info=run_time_info,
            path="/path/to/config.yaml",
        )


def test_select_invocation_multiple_specified_higher_specificity_selected_first() -> None:
    configs: dict[str, Any] = {
        "specific": {
            "invocation": {
                "environment_setup": None,
                "match": {"hostname": "my-machine"},
            }
        },
        "default": {
            "invocation": {
                "environment_setup": None,
                "match": {},
            }
        },
    }
    run_time_info = {"hostname": "my-machine"}
    matched = ConfigFile.select_invocation(
        configs=configs,
        run_time_info=run_time_info,
        path="/path/to/config.yaml",
    )
    assert matched == "specific"


def test_select_invocation_multiple_specified_higher_specificity_selected_second() -> (
    None
):
    configs: dict[str, Any] = {
        "default": {
            "invocation": {
                "environment_setup": None,
                "match": {},
            }
        },
        "specific": {
            "invocation": {
                "environment_setup": None,
                "match": {"hostname": "my-machine"},
            }
        },
    }
    run_time_info = {"hostname": "my-machine"}
    matched = ConfigFile.select_invocation(
        configs=configs,
        run_time_info=run_time_info,
        path="/path/to/config.yaml",
    )
    assert matched == "specific"
