from __future__ import annotations
from pathlib import Path
import sys

import pytest

import hpcflow.app as hf
from hpcflow.sdk.submission.shells import ALL_SHELLS


def test_process_JS_header_args_app_invoc_windows_powershell() -> None:
    """
    Three types of invocation commands exist:
      1. the frozen app executable
      2. a python executable calling the hpcflow package CLI module
      3. a python executable calling the hpcflow entry point command

    For the purposes of this test, 2. and 3. are equivalent. We test the expected output
    of `WindowsPowerShell.process_JS_header_args` on the `app_invoc` key for all
    variations. If there is a space in the executable, we expect the call operator (`&`)
    to be used, followed by a single-quoted executable path. We expect executable
    arguments (e.g. the hpcflow package CLI module path) to be double-quoted, regardless
    of whether they include spaces.

    """

    app_invocs = [
        ("C:\\path\to\frozen\app.exe",),
        ("C:\\path\\to\\frozen\\app with spaces.exe",),
        ("C:\\path\\to\\python.exe", "C:\\path\\to\\hpcflow\\cli.py"),
        ("C:\\path\\to\\python with spaces.exe", "C:\\path\\to\\hpcflow\\cli.py"),
        (
            "C:\\path\\to\\python with spaces.exe",
            "C:\\path\\to\\hpcflow\\cli with spaces.py",
        ),
    ]
    expected = [
        "C:\\path\to\frozen\app.exe",
        "& 'C:\\path\\to\\frozen\\app with spaces.exe'",
        'C:\\path\\to\\python.exe "C:\\path\\to\\hpcflow\\cli.py"',
        "& 'C:\\path\\to\\python with spaces.exe' \"C:\\path\\to\\hpcflow\\cli.py\"",
        "& 'C:\\path\\to\\python with spaces.exe' \"C:\\path\\to\\hpcflow\\cli with spaces.py\"",
    ]
    shell = ALL_SHELLS["powershell"]["nt"]()
    for i, j in zip(app_invocs, expected):
        processed = shell.process_JS_header_args({"app_invoc": i})
        assert processed["app_invoc"] == j


def test_process_JS_header_args_app_invoc_bash() -> None:
    """
    Three types of invocation commands exist:
      1. the frozen app executable
      2. a python executable calling the hpcflow package CLI module
      3. a python executable calling the hpcflow entry point command

    For the purposes of this test, 2. and 3. are equivalent. We test the expected output
    of `Bash.process_JS_header_args` on the `app_invoc` key for all
    variations. If there is a space in the executable, we expect spaces to be escaped
    using the backslash. We expect executable arguments (e.g. the hpcflow package CLI
    module path) to be double-quoted, regardless of whether they include spaces.

    """

    app_invocs = [
        ("/mnt/path/to/frozen/app.exe",),
        ("/mnt/path/to/frozen/app with spaces.exe",),
        ("/mnt/path/to/python.exe", "/mnt/path/to/hpcflow/cli.py"),
        ("/mnt/path/to/python with spaces.exe", "/mnt/path/to/hpcflow/cli.py"),
        (
            "/mnt/path/to/python with spaces.exe",
            "/mnt/path/to/hpcflow/cli with spaces.py",
        ),
    ]
    expected = [
        "/mnt/path/to/frozen/app.exe",
        "/mnt/path/to/frozen/app\\ with\\ spaces.exe",
        '/mnt/path/to/python.exe "/mnt/path/to/hpcflow/cli.py"',
        '/mnt/path/to/python\\ with\\ spaces.exe "/mnt/path/to/hpcflow/cli.py"',
        '/mnt/path/to/python\\ with\\ spaces.exe "/mnt/path/to/hpcflow/cli with spaces.py"',
    ]
    shell = ALL_SHELLS["bash"]["posix"]()
    for i, j in zip(app_invocs, expected):
        processed = shell.process_JS_header_args({"app_invoc": i})
        assert processed["app_invoc"] == j


def test_format_array_powershell():
    shell = ALL_SHELLS["powershell"]["nt"]()
    assert shell.format_array([1, 2, 3]) == "@(1, 2, 3)"


def test_format_array_get_item_powershell():
    shell = ALL_SHELLS["powershell"]["nt"]()
    assert shell.format_array_get_item("my_arr", 3) == "$my_arr[3]"


def test_format_array_bash():
    shell = ALL_SHELLS["bash"]["posix"]()
    assert shell.format_array([1, 2, 3]) == "(1 2 3)"


def test_format_array_get_item_bash():
    shell = ALL_SHELLS["bash"]["posix"]()
    assert shell.format_array_get_item("my_arr", 3) == r"${my_arr[3]}"


@pytest.mark.integration
@pytest.mark.skipif(condition=sys.platform == "win32", reason="This is a bash-only test.")
def test_executable_args_bash_login(null_config, tmp_path: Path):
    """Check if we provide a `--login` argument to the shell `executable_args`, we end up
    in a login shell on bash."""

    cmd = "shopt -q login_shell && echo 'Login shell' || echo 'Not login shell'"
    s1 = hf.TaskSchema(
        objective="t1",
        actions=[hf.Action(commands=[hf.Command(command=cmd)])],
    )
    t1 = hf.Task(
        schema=s1,
        resources={"any": {"shell_args": {"executable_args": ["--login"]}}},
    )
    wkt = hf.WorkflowTemplate(name="test_bash_login", tasks=[t1])
    wk = hf.Workflow.from_template(
        template=wkt,
        path=tmp_path,
    )
    wk.submit(wait=True, status=False, add_to_known=False)
    assert wk.submissions[0].jobscripts[0].get_stdout().strip() == "Login shell"
