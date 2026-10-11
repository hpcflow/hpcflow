"""Translate shared container paths for display without changing operational paths."""

import os
from pathlib import PurePosixPath, PureWindowsPath


def display_host_path(app_name: str, path: object) -> str:
    """Display recognised configuration and current-work mounts as host paths."""
    value = str(path)
    prefix = app_name.upper()
    config_host = os.environ.get(f"{prefix}_CONTAINER_HOST_CONFIG_DIR")
    work_host = os.environ.get(f"{prefix}_CONTAINER_HOST_WORK_DIR")
    work_mount = os.environ.get(f"{prefix}_CONTAINER_WORK_MOUNT")
    mounts = [(f"/config/.{app_name}-container", config_host), ("/work", work_host)]
    if work_mount:
        mounts.append((work_mount, work_host))
    for mount, host in mounts:
        if host and (value == mount or value.startswith(mount + "/")):
            host_path = PureWindowsPath(host) if "\\" in host else PurePosixPath(host)
            return str(host_path.joinpath(*value[len(mount) :].strip("/").split("/")))
    return value
