from pathlib import Path
import subprocess
from unittest.mock import Mock

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.workflow import Workflow
from hpcflow.sdk.submission.debug import prepare_debug_run, select_debug_run
from hpcflow.sdk.submission.enums import JobscriptElementState
from hpcflow.sdk.submission.schedulers.sge import SGEPosix
from hpcflow.sdk.submission.schedulers.slurm import SlurmPosix
from hpcflow.sdk.submission.shells.bash import Bash


@pytest.mark.parametrize(
    ("state", "activity", "selected"),
    [
        (EARStatus.error, None, True),
        (EARStatus.running, JobscriptElementState.running, False),
        (EARStatus.running, JobscriptElementState.pending, False),
        (EARStatus.running, JobscriptElementState.finished, True),
        (EARStatus.running, None, True),
        (EARStatus.running, OSError("scheduler unavailable"), True),
        (EARStatus.success, None, False),
        (EARStatus.skipped, None, False),
    ],
)
def test_select_first_failed(state, activity, selected: bool) -> None:
    import numpy as np

    workflow = Mock(spec=Workflow)
    workflow._app = Mock()
    run = Mock(id_=4, status=state, submission_idx=0)
    block = Mock(EAR_ID=np.array([[4]]))
    job = Mock(blocks=[block])
    workflow.submissions = [Mock(jobscripts=[job])]
    workflow.get_all_EARs.return_value = [run]
    if isinstance(activity, Exception):
        job.get_active_states.side_effect = activity
    else:
        job.get_active_states.return_value = (
            {} if activity is None else {0: {0: activity}}
        )
    if selected:
        assert select_debug_run(workflow, None, True) is run
    else:
        with pytest.raises(ValueError, match="No errored or interrupted"):
            select_debug_run(workflow, None, True)


def _workflow(tmp_path: Path, combine: bool = False) -> Workflow:
    task = hf.Task(
        schema=[
            hf.TaskSchema(
                objective="debug",
                inputs=[hf.SchemaInput("p1")],
                outputs=[hf.SchemaOutput("p2")],
                actions=[
                    hf.Action(
                        script="<<script:main_script_test_direct_in_direct_out.py>>",
                        script_exe="python_script",
                        script_data_in="direct",
                        script_data_out="direct",
                        requires_dir=True,
                        environments=[hf.ActionEnvironment(environment="python_env")],
                    )
                ],
            )
        ],
        inputs={"p1": 3},
    )
    workflow = hf.Workflow.from_template_data(
        tasks=[task],
        template_name="debug",
        path=tmp_path,
        resources={"any": {"combine_scripts": combine}},
    )
    workflow.add_submission()
    return workflow


def _snapshot(workflow: Workflow) -> dict[Path, tuple[int, bytes]]:
    return {
        path: (path.stat().st_mtime_ns, path.read_bytes())
        for path in Path(workflow.path).rglob("*")
        if path.is_file()
        and not any(part.startswith("debug_run_") for part in path.parts)
    }


@pytest.mark.integration
@pytest.mark.parametrize(
    ("combine", "fail"), [(False, False), (True, False), (False, True)]
)
def test_debug_executes_edited_script_without_persistence(
    tmp_path: Path, combine: bool, fail: bool
) -> None:
    workflow = _workflow(tmp_path, combine)
    run = workflow.get_EARs_from_IDs([0])[0]
    original_dir = run.prepare_directory()
    assert original_dir is not None
    (original_dir / "sentinel.txt").write_text("original", newline="\n")
    before = _snapshot(workflow)
    debug = prepare_debug_run(workflow, run_id=0, direct=True)
    assert debug.script_path is not None
    source = debug.script_path.read_text()
    source = source.replace(
        "# process",
        'from pathlib import Path\n    Path("sentinel.txt").write_text("debug")\n'
        '    print(f"INPUT={p1}")',
    )
    if fail:
        source = source.replace(
            'print(f"INPUT={p1}")', 'raise ValueError("debug failure")'
        )
    debug.script_path.write_text(source, encoding="utf-8", newline="\n")
    assert debug.execute() == int(fail), (debug.directory / "stderr.txt").read_text()
    if fail:
        assert "debug failure" in (debug.directory / "stderr.txt").read_text()
    else:
        assert "INPUT=3" in (debug.directory / "stdout.txt").read_text()
    assert (debug.directory / "work" / "sentinel.txt").read_text() == "debug"
    assert _snapshot(workflow) == before
    assert workflow.get_EARs_from_IDs([0])[0].status is EARStatus.prepared
    output_id = run.data_idx["outputs.p2"]
    assert isinstance(output_id, int)
    assert not workflow.is_parameter_set(output_id)
    assert prepare_debug_run(workflow, run_id=0, direct=True).directory != debug.directory


@pytest.mark.integration
@pytest.mark.parametrize("scheduler_name", ["slurm", "sge"])
def test_debug_scheduler_submission_is_single_and_does_not_record_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scheduler_name: str
) -> None:
    workflow = _workflow(tmp_path)
    job = workflow.submissions[0].jobscripts[0]
    job._shell_obj = Bash()
    job._scheduler_obj = (
        SlurmPosix(
            directives={
                "--time": "10:00",
                "--array": "1-10",
                "--output": "original-output",
            }
        )
        if scheduler_name == "slurm"
        else SGEPosix(
            directives={"-l": "h_rt=00:10:00", "-t": "1-10", "-o": "original-output"}
        )
    )
    before = _snapshot(workflow)
    debug = prepare_debug_run(workflow, run_id=0)
    text = debug.jobscript_path.read_text()
    assert "1-10" not in text
    assert "original-output" not in text
    assert ("--time 10:00" if scheduler_name == "slurm" else "-l h_rt=00:10:00") in text
    assert str(debug.directory) in text
    launch = Mock(return_value=subprocess.CompletedProcess(["sbatch"], 0, "1234\n", ""))
    monkeypatch.setattr(subprocess, "run", launch)
    assert debug.execute() == "1234"
    command = launch.call_args.args[0]
    expected_submit = (
        ["sbatch", "--parsable"] if scheduler_name == "slurm" else ["qsub", "-terse"]
    )
    assert command == [*expected_submit, str(debug.jobscript_path)]
    assert launch.call_args.kwargs["cwd"] == debug.directory
    assert _snapshot(workflow) == before
