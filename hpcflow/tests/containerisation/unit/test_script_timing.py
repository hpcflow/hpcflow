"""Generated scripts only send timing when an executor server is available."""

import ast
import os
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from hpcflow.sdk.core.actions import Action


@pytest.mark.parametrize("package", ["hpcflow", "matflow"])
@pytest.mark.parametrize("port", [None, "12345", "invalid"])
def test_generated_script_timing(tmp_path, monkeypatch, package, port):
    snippet = tmp_path / "example.py"
    snippet.write_text(
        "def example():\n    return None\n", encoding="utf-8", newline="\n"
    )
    action = SimpleNamespace(
        script_is_python_snippet=True,
        is_OFP=False,
        script_data_out_grouped={},
        _app=SimpleNamespace(
            package_name=package,
            module=package,
            config=SimpleNamespace(config_directory=tmp_path, config_key="default"),
        ),
    )
    tree = ast.parse(Action.compose_source(action, snippet))
    main = tree.body[-1]
    guard = next(
        node
        for node in main.body
        if isinstance(node, ast.If)
        and any(
            isinstance(child, ast.Attribute) and child.attr == "send_timeit"
            for child in ast.walk(node)
        )
    )
    key = f"{package.upper()}_RUN_PORT"
    if port is None:
        monkeypatch.delenv(key, raising=False)
    else:
        monkeypatch.setenv(key, port)
    send = Mock()
    namespace = {
        "os": os,
        "app": SimpleNamespace(Executor=SimpleNamespace(send_timeit=send)),
        "_script_orchestration_time": 1.5,
        "_user_work_time": 2.5,
    }
    code = compile(ast.Module(body=[guard], type_ignores=[]), "<timing>", "exec")
    if port == "invalid":
        with pytest.raises(ValueError):
            exec(code, namespace)
        send.assert_not_called()
    else:
        exec(code, namespace)
        if port is None:
            send.assert_not_called()
        else:
            send.assert_called_once_with(
                hostname="localhost",
                port_number=12345,
                orchestration_time=1.5,
                work_time=2.5,
            )
