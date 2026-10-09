"""Host activity snapshots for containerised submission monitoring."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias

from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.submission.enums import JobscriptElementState
from hpcflow.sdk.submission.schedulers.sge import SGEPosix
from hpcflow.sdk.submission.schedulers.slurm import SlurmPosix

if TYPE_CHECKING:
    from hpcflow.sdk.app import BaseApp

HostStates: TypeAlias = dict[
    tuple[str, int], dict[int, dict[int, dict[int, JobscriptElementState]]]
]


def known_workflow_path(app: BaseApp, path: str | Path) -> Path:
    """Use a mount-specific persistent path rather than the ambiguous /work alias."""
    mount = os.getenv(f"{app.package_name.upper()}_CONTAINER_WORK_MOUNT")
    path = Path(path)
    if not mount:
        return path
    if (
        not mount.startswith("/host-workflows/")
        or "/" in mount[len("/host-workflows/") :]
    ):
        raise ValueError("Invalid container workflow mount.")
    return Path(mount) / path.relative_to("/work")


def prepare_host_show(app: BaseApp) -> dict:
    """Collect queries without inspecting processes or invoking schedulers."""
    jobs = []
    try:
        known = app.read_known_submissions_file()
    except FileNotFoundError:
        known = []
    seen = set()
    for entry in known:
        path = entry["path"]
        if path == "/work" or path.startswith("/work/"):
            raise ValueError(
                "Legacy /work known-submissions entries have no persistent mount identity. "
                "Replay their host submission acknowledgement with the updated wrapper "
                "before using show."
            )
        if not entry["is_active"] or not Path(path).exists():
            continue
        workflow = app.Workflow(path)
        if workflow.id_ != entry["workflow_id"]:
            continue
        sub = workflow.submissions[entry["sub_idx"]]
        for js_idx in sub.submitted_jobscripts:
            js = sub.jobscripts[js_idx]
            key = (path, sub.index, js.index)
            if key in seen:
                continue
            seen.add(key)
            if js.submit_machine != app.config.get("machine"):
                raise ValueError(
                    "Host monitoring requires the original submission machine."
                )
            scheduler = js.scheduler
            query = None
            if isinstance(scheduler, SlurmPosix):
                # Query all jobs: squeue then succeeds even when a requested job is gone.
                query = [*scheduler.show_cmd, "--noheader", "--format", "%200i %30T"]
            elif isinstance(scheduler, SGEPosix):
                query = [*scheduler.show_cmd, "-u", "$USER", "-g", "d"]
            elif js.is_scheduled:
                raise ValueError(
                    f"Unsupported host monitoring scheduler: {js.scheduler_name}"
                )
            jobs.append(
                {
                    "workflow_path": path,
                    "workflow_id": workflow.id_,
                    "submission_index": sub.index,
                    "jobscript_index": js.index,
                    "scheduler": js.scheduler_name,
                    "process_id": js.process_ID,
                    "submit_command": js.submit_cmdline,
                    "query": query,
                }
            )
    return {"schema_version": 1, "jobs": jobs}


def parse_host_show(app: BaseApp, snapshot: object) -> HostStates:
    """Validate a complete snapshot before allowing registry updates."""
    plan = prepare_host_show(app)
    if (
        not isinstance(snapshot, dict)
        or snapshot.get("schema_version") != 1
        or not isinstance(snapshot.get("jobs"), list)
    ):
        raise ValueError("Invalid host monitoring snapshot.")
    expected = {
        (job["workflow_path"], job["submission_index"], job["jobscript_index"]): job
        for job in plan["jobs"]
    }
    states: HostStates = {}
    seen = set()
    for result in snapshot["jobs"]:
        if not isinstance(result, dict):
            raise ValueError("Invalid host job result.")
        if (
            not isinstance(result.get("workflow_path"), str)
            or type(result.get("submission_index")) is not int
            or type(result.get("jobscript_index")) is not int
        ):
            raise ValueError("Invalid host job identity.")
        key = (
            result.get("workflow_path"),
            result.get("submission_index"),
            result.get("jobscript_index"),
        )
        if key not in expected or key in seen:
            raise ValueError("Unknown or duplicate host job result.")
        seen.add(key)
        job = expected[key]
        if result.get("workflow_id") != job["workflow_id"]:
            raise ValueError("Host monitoring workflow identity changed.")
        workflow = app.Workflow(job["workflow_path"])
        js = workflow.submissions[job["submission_index"]].jobscripts[
            job["jobscript_index"]
        ]
        state: JobscriptElementState | Mapping[int, JobscriptElementState] | None
        if js.is_scheduled:
            stdout = result.get("stdout")
            if not isinstance(stdout, str):
                raise ValueError("Missing host scheduler output.")
            scheduler = js.scheduler
            if isinstance(scheduler, (SlurmPosix, SGEPosix)):
                reference = js.scheduler_job_ID
                if reference is None:
                    raise ValueError("Missing scheduler job ID.")
                state = scheduler._parse_job_states(stdout).get(reference)
            else:
                raise ValueError("Unsupported host scheduler.")
        else:
            if type(result.get("active")) is not bool:
                raise ValueError("Missing host process activity.")
            state = JobscriptElementState.running if result["active"] else None
        sub_states = states.setdefault(
            (job["workflow_path"], job["submission_index"]), {}
        )
        if state is not None and not all(
            ear.status in EARStatus.get_non_running_submitted_states()
            for ear in js.all_EARs
        ):
            if isinstance(state, Mapping):
                if not js.is_array:
                    raise ValueError("Array states returned for a non-array job.")
                sub_states[js.index] = {0: dict(state)}
            else:
                sub_states[js.index] = {
                    idx: {i: state for i in range(block.num_elements)}
                    for idx, block in enumerate(js.blocks)
                }
    if seen != expected.keys():
        raise ValueError("Incomplete host monitoring snapshot; registry was not updated.")
    return states
