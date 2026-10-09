from __future__ import annotations
import logging
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
