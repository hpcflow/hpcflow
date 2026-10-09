from __future__ import annotations
import logging
import os
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.runtime import RunTimeInfo


def test_in_pytest_if_not_frozen() -> None:
    """This is to check we can get the correct invocation command when running non-frozen
    tests (when frozen the invocation command is just the executable file)."""
    if not hf.run_time_info.is_frozen:
        assert hf.run_time_info.in_pytest


@pytest.mark.parametrize("image", [None, "", "hpcflow:test"])
@pytest.mark.parametrize(
    "app_name,package_name", [("hpcflow", "hpcflow"), ("MatFlow", "matflow")]
)
def test_container_image_runtime_info(monkeypatch, image, app_name, package_name):
    monkeypatch.setattr(
        RunTimeInfo, "invocation_command", property(lambda self: (package_name,))
    )
    variable = f"{package_name.upper()}_CONTAINER"
    other_variable = (
        "HPCFLOW_CONTAINER" if package_name == "matflow" else "MATFLOW_CONTAINER"
    )
    monkeypatch.setenv(other_variable, "unrelated-app:test")
    if image is None:
        monkeypatch.delenv(variable, raising=False)
    else:
        monkeypatch.setenv(variable, image)
    info = RunTimeInfo(app_name, package_name, "test", logging.getLogger(__name__))
    assert info.container_image == (image or None)
    assert info.in_container is bool(image)
    assert info.to_dict()["container_image"] == (image or None)
    assert info.to_dict()["in_container"] is bool(image)


@pytest.mark.parametrize("package_name", ["hpcflow", "matflow"])
def test_container_host_execution_os(monkeypatch, package_name):
    monkeypatch.setenv(f"{package_name.upper()}_CONTAINER", "test:image")
    monkeypatch.setenv(f"{package_name.upper()}_CONTAINER_HOST_OS", "nt")
    info = RunTimeInfo(package_name, package_name, "test", logging.getLogger(__name__))
    assert info.execution_os == "nt"
    assert info.container_host_os == "nt"
    monkeypatch.delenv(f"{package_name.upper()}_CONTAINER_HOST_OS")
    info = RunTimeInfo(package_name, package_name, "test", logging.getLogger(__name__))
    assert info.execution_os == os.name


@pytest.mark.parametrize("host_os,image", [("invalid", "image"), ("nt", None)])
def test_invalid_container_host_execution_os(monkeypatch, host_os, image):
    monkeypatch.setenv("HPCFLOW_CONTAINER_HOST_OS", host_os)
    if image:
        monkeypatch.setenv("HPCFLOW_CONTAINER", image)
    else:
        monkeypatch.delenv("HPCFLOW_CONTAINER", raising=False)
    with pytest.raises(ValueError):
        RunTimeInfo("hpcflow", "hpcflow", "test", logging.getLogger(__name__))
