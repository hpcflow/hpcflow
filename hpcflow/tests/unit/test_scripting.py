from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import numpy as np
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.actions import ElementActionRun
from hpcflow.sdk.scripting import ScriptWriter
from hpcflow.sdk.scripting.writer import ScriptWorkerArgs
from hpcflow.sdk.submission.shells import get_shell
from hpcflow.sdk.submission.jobscript import Jobscript, JobscriptBlock
from hpcflow.sdk.submission.submission import Submission


def test_python_worker_source_compiles(tmp_path: Path) -> None:
    action = hf.Action(script="<<script:do_nothing.py>>", script_exe="python_script")
    hf.TaskSchema(objective="worker_source", actions=[action])
    args = ScriptWorkerArgs(0, hf.Environment("python"), "python", "python")
    writer = ScriptWriter.create(args, {0: action})

    writer.write_worker(tmp_path)

    path = args.get_script_path(tmp_path)
    source = path.read_text(encoding="utf-8")
    assert b"\r\n" not in path.read_bytes()
    compile(source, str(path), "exec")
    with pytest.raises(FileNotFoundError):
        exec(
            compile(source, str(path), "exec"),
            {"__name__": "__main__", "__file__": str(path)},
        )


def test_base_script_writer_rejects_unimplemented_generation() -> None:
    args = ScriptWorkerArgs(0, hf.Environment("python"), "python", "python")
    with pytest.raises(NotImplementedError):
        ScriptWriter(args, {}).compose_worker()


@pytest.mark.parametrize("setup", [None, ["$env:WORKER_ENV = 'one'", "Set-Location ."]])
def test_worker_commands_include_environment_setup(
    tmp_path: Path, setup: list[str] | None
) -> None:
    args = ScriptWorkerArgs(
        0, hf.Environment("python", setup=setup), "python worker.py", "python"
    )
    ScriptWriter(args, {}).write_worker_commands_file(tmp_path)

    path = args.get_commands_path(tmp_path)
    expected = "\n".join([*(setup or []), args.exec_cmd]) + "\n"
    assert path.read_bytes() == expected.encode("utf-8")


@pytest.mark.parametrize(
    ("shell_name", "os_name"),
    [("bash", "posix"), ("powershell", "nt")],
)
def test_worker_commands_quote_script_path_and_remove_args(
    tmp_path: Path, shell_name: str, os_name: str
) -> None:
    args = ScriptWorkerArgs(
        0,
        hf.Environment("python"),
        (
            'python "<<script_path>>" <<args>>'
            if shell_name == "powershell"
            else "python <<script_path>> <<args>>"
        ),
        "python",
        shell_name,
    )
    shell = get_shell(shell_name, os_name=os_name)
    script_path = tmp_path / "worker with spaces.py"
    args.get_commands_path(tmp_path).parent.mkdir(exist_ok=True, parents=True)

    ScriptWriter(args, {}).write_worker_commands_file(
        tmp_path,
        script_path=script_path,
        shell=shell,
    )

    expected = f"python {shell.quote_arg(script_path)}\n"
    assert args.get_commands_path(tmp_path).read_text(encoding="utf-8") == expected
    assert args.get_commands_path(tmp_path).suffix == (
        ".sh" if shell_name == "bash" else ".ps1"
    )


def test_worker_command_template_requires_script_path(tmp_path: Path) -> None:
    args = ScriptWorkerArgs(
        0,
        hf.Environment("python"),
        "python <<script_path>>",
        "python",
    )
    with pytest.raises(ValueError, match="script_path"):
        ScriptWriter(args, {}).write_worker_commands_file(tmp_path)


def test_script_worker_args_require_jobscript_index() -> None:
    run = hf.ElementActionRun.__new__(hf.ElementActionRun)
    run._jobscript_idx = None
    with pytest.raises(RuntimeError, match="jobscript index"):
        _ = run.script_worker_args


def test_script_worker_hash_matches_worker_args(monkeypatch: pytest.MonkeyPatch) -> None:
    run = hf.ElementActionRun.__new__(hf.ElementActionRun)
    run._jobscript_idx = 3
    args = ScriptWorkerArgs(3, hf.Environment("python"), "python", "python")
    get_args = Mock(return_value=args)
    monkeypatch.setattr(run, "get_script_worker_args", get_args)
    monkeypatch.setattr(
        hf.ElementActionRun, "use_script_worker", property(lambda _: True)
    )

    assert run.get_script_worker_hash() == args.stable_hash
    get_args.assert_called_once_with(3)


def test_worker_registry_uses_block_and_action_coordinates() -> None:
    action = hf.Action(
        script="<<script:do_nothing.py>>",
        script_exe="python_script",
    )
    first_args = ScriptWorkerArgs(0, hf.Environment("python"), "python", "python")
    second_args = ScriptWorkerArgs(0, hf.Environment("python_alt"), "python", "python")

    def make_run(worker_args: ScriptWorkerArgs) -> ElementActionRun:
        return cast(
            ElementActionRun,
            SimpleNamespace(
                use_script_worker=True,
                get_script_worker_args=Mock(return_value=worker_args),
                action=action,
            ),
        )

    runs = [
        make_run(first_args),
        make_run(first_args),
        make_run(second_args),
    ]
    jobscript = cast(
        Jobscript,
        SimpleNamespace(
            index=0,
            all_EAR_IDs=np.array([0, 1, 2], dtype=int),
            blocks=[
                cast(
                    JobscriptBlock,
                    SimpleNamespace(index=0, EAR_ID=np.array([[0]], dtype=int)),
                ),
                cast(
                    JobscriptBlock,
                    SimpleNamespace(index=1, EAR_ID=np.array([[1], [2]], dtype=int)),
                ),
            ],
        ),
    )

    registry = Submission._get_script_worker_actions(jobscript, runs)

    assert registry == {
        first_args: {(0, 0): action, (1, 0): action},
        second_args: {(1, 1): action},
    }
