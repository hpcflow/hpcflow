"""Display shared paths without altering environment source definitions."""

from pathlib import PurePosixPath
from types import SimpleNamespace

import pytest
from rich.console import Console

from hpcflow.sdk.containerisation.paths import display_host_path
from hpcflow.sdk.app import BaseApp


@pytest.mark.parametrize("package", ["hpcflow", "matflow"])
@pytest.mark.parametrize("host", ["/shared/config with spaces", r"D:\shared\config"])
def test_display_host_config_path(monkeypatch, package, host):
    monkeypatch.setenv(f"{package.upper()}_CONTAINER_HOST_CONFIG_DIR", host)
    expected = host + ("\\" if "\\" in host else "/") + "configured_envs.yaml"
    source = f"/config/.{package}-container/configured_envs.yaml"
    assert display_host_path(package, source) == expected
    assert display_host_path(package, f"/config/.{package}-container-other/file") == (
        f"/config/.{package}-container-other/file"
    )
    env = SimpleNamespace(
        specifiers={},
        setup_label=None,
        source_file=PurePosixPath(source),
        name="python_env",
    )
    app = SimpleNamespace(package_name=package, envs=[env])
    console = Console(width=200)
    with console.capture() as capture:
        console.print(BaseApp._get_envs_table(app))
    assert expected in capture.get()
    assert env.source_file == PurePosixPath(source)


def test_display_native_and_work_paths(monkeypatch):
    monkeypatch.delenv("HPCFLOW_CONTAINER_HOST_CONFIG_DIR", raising=False)
    assert display_host_path("hpcflow", "/config/.hpcflow-container/file") == (
        "/config/.hpcflow-container/file"
    )
    monkeypatch.setenv("HPCFLOW_CONTAINER_HOST_WORK_DIR", "/shared/work")
    monkeypatch.setenv("HPCFLOW_CONTAINER_WORK_MOUNT", "/host-workflows/abc")
    assert display_host_path("hpcflow", "/work/envs.yaml") == "/shared/work/envs.yaml"
    assert display_host_path("hpcflow", "/host-workflows/abc/envs.yaml") == (
        "/shared/work/envs.yaml"
    )
    assert display_host_path("hpcflow", "<builtin>") == "<builtin>"
