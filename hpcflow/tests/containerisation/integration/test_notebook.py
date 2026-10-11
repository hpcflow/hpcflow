"""Live notebook-server checks using the container image and host wrapper."""

import json
import os
from importlib.resources import files
from pathlib import Path
import re
import shutil
import socket
import subprocess
import time

import pytest
import requests

from hpcflow.app import app as hf


@pytest.mark.integration
@pytest.mark.skipif(os.name != "nt", reason="Windows host with Linux container")
def test_container_notebook_postprocessing(tmp_path, monkeypatch):
    """Authenticate a live server, execute its kernel against a workflow, and save a notebook."""
    image = os.environ.get("HPCFLOW_TEST_CONTAINER_IMAGE")
    pwsh = shutil.which("pwsh")
    if not image or not pwsh:
        pytest.skip("Set HPCFLOW_TEST_CONTAINER_IMAGE and install PowerShell 7")

    workflow = hf.make_demo_workflow("workflow_1", path=tmp_path, status=False)
    wrapper = tmp_path / "hpcflow.ps1"
    wrapper.write_text(
        files("hpcflow.sdk.containerisation")
        .joinpath("container_wrapper.ps1")
        .read_text()
        .replace("__APP_NAME_", "__HPCFLOW_"),
        encoding="utf-8",
        newline="\n",
    )
    monkeypatch.setenv("HPCFLOW_CONFIG_DIR", str(tmp_path / "config"))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "app-data"))
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    log_path = tmp_path / "server.log"
    container_id = None

    def find_containers():
        """Identify only containers bound to this test's unique host directory."""
        ids = subprocess.check_output(["docker", "ps", "-q"], text=True).split()
        if not ids:
            return []
        records = json.loads(
            subprocess.check_output(["docker", "inspect", *ids], text=True)
        )
        root = str(tmp_path).replace("\\", "/").casefold()
        return [
            record["Id"]
            for record in records
            if any(
                mount["Source"].replace("\\", "/").casefold() == root
                for mount in record["Mounts"]
            )
        ]

    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [
                pwsh,
                "-NoProfile",
                "-File",
                str(wrapper),
                "-Image",
                image,
                "-NotebookPort",
                str(port),
                "notebook",
            ],
            cwd=tmp_path,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 90
            token = None
            while time.monotonic() < deadline:
                contents = log_path.read_text(encoding="utf-8")
                match = re.search(
                    rf"127\.0\.0\.1:{port}/\S*\?token=([a-zA-Z0-9]+)", contents
                )
                if match:
                    token = match[1]
                    break
                assert process.poll() is None, contents
                time.sleep(0.5)
            assert token, log_path.read_text(encoding="utf-8")

            assert requests.get(url + "/api/kernels", timeout=10).status_code in (
                401,
                403,
            )
            headers = {"Authorization": f"token {token}"}
            response = requests.post(
                url + "/api/kernels",
                headers=headers,
                json={"name": "python3"},
                timeout=30,
            )
            response.raise_for_status()
            kernel_id = response.json()["id"]
            ids = find_containers()
            assert len(ids) == 1, ids
            container_id = ids[0]
            inspection = json.loads(
                subprocess.check_output(["docker", "inspect", container_id], text=True)
            )[0]
            assert inspection["NetworkSettings"]["Ports"]["8888/tcp"] == [
                {"HostIp": "127.0.0.1", "HostPort": str(port)}
            ]
            code = (
                "import sys\n"
                "from pathlib import Path\n"
                "from hpcflow.app import app as hf\n"
                f"workflow = hf.Workflow({('/work/' + Path(workflow.path).name)!r})\n"
                f"assert workflow.id_ == {workflow.id_!r}\n"
                "assert sys.executable.startswith('/opt/venv/')\n"
                "Path('/work/postprocessed.txt').write_text(workflow.id_)\n"
                "print('postprocessing-ok')\n"
            )
            # Exercise the actual server kernel, not a separate Python process.
            client = r"""
import asyncio, json, sys, uuid
from tornado.websocket import websocket_connect

async def execute():
    kernel, token, code = sys.argv[1:]
    connection = await websocket_connect(
        f"ws://127.0.0.1:8888/api/kernels/{kernel}/channels?token={token}"
    )
    message_id = uuid.uuid4().hex
    await connection.write_message(json.dumps({
        "header": {"msg_id": message_id, "username": "test", "session": uuid.uuid4().hex,
                   "msg_type": "execute_request", "version": "5.3"},
        "parent_header": {}, "metadata": {}, "channel": "shell",
        "content": {"code": code, "silent": False, "store_history": True,
                    "user_expressions": {}, "allow_stdin": False, "stop_on_error": True},
    }))
    output = ""
    while True:
        raw = await asyncio.wait_for(connection.read_message(), timeout=60)
        assert raw is not None, "Kernel websocket closed"
        message = json.loads(raw)
        if message["parent_header"].get("msg_id") != message_id:
            continue
        kind = message["header"]["msg_type"]
        assert kind != "error", message["content"]
        if kind == "stream":
            output += message["content"]["text"]
        if kind == "status" and message["content"]["execution_state"] == "idle":
            break
    connection.close()
    assert "postprocessing-ok" in output, output

asyncio.run(execute())
"""
            subprocess.run(
                [
                    "docker",
                    "exec",
                    container_id,
                    "python",
                    "-c",
                    client,
                    kernel_id,
                    token,
                    code,
                ],
                check=True,
                capture_output=True,
                timeout=90,
            )
            assert (tmp_path / "postprocessed.txt").read_text() == workflow.id_
            notebook = {
                "cells": [
                    {
                        "cell_type": "code",
                        "metadata": {},
                        "source": code,
                        "outputs": [],
                        "execution_count": None,
                    }
                ],
                "metadata": {
                    "kernelspec": {
                        "name": "python3",
                        "display_name": "Python 3",
                        "language": "python",
                    }
                },
                "nbformat": 4,
                "nbformat_minor": 5,
            }
            saved = requests.put(
                url + "/api/contents/postprocessing.ipynb",
                headers=headers,
                json={"type": "notebook", "content": notebook},
                timeout=10,
            )
            saved.raise_for_status()
            source = json.loads((tmp_path / "postprocessing.ipynb").read_text())["cells"][
                0
            ]["source"]
            assert "".join(source) == code
        finally:
            if container_id is None:
                ids = find_containers()
            else:
                ids = [container_id]
            for identifier in ids:
                subprocess.run(
                    ["docker", "stop", "--time", "5", identifier],
                    check=True,
                    capture_output=True,
                    timeout=20,
                )
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=20)
