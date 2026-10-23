from pathlib import Path
from unittest.mock import Mock

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.scripting import ScriptWriter
from hpcflow.sdk.scripting.writer import ScriptWorkerArgs


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
    with pytest.raises(NotImplementedError, match="protocol"):
        exec(compile(source, str(path), "exec"), {"__name__": "__main__"})


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
