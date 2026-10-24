"""Load persisted workflow state and assemble a generated script worker."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from hpcflow.app import app as hf
from hpcflow.sdk.execution.script_worker.adapter import ScriptWorkerInvocation
from hpcflow.sdk.execution.script_worker.protocol import ExecuteScriptRequest
from hpcflow.sdk.execution.script_worker.service import (
    SCRIPT_WORKER_ENDPOINT_ENV,
    ScriptWorkerService,
)

if TYPE_CHECKING:
    from hpcflow.sdk.core.workflow import Workflow

WorkerActionKey = tuple[int, int]
ActionScripts = Mapping[WorkerActionKey, Callable[..., Any]]


@dataclass(frozen=True)
class ScriptWorkerManifest:
    """Persisted identities and callable coordinates assigned to one worker."""

    workflow_path: str
    submission_idx: int
    jobscript_idx: int
    worker_id: str
    action_keys: tuple[WorkerActionKey, ...]
    config_directory: str
    config_key: str
    version: int = 1

    @classmethod
    def from_dict(cls, data: object) -> ScriptWorkerManifest:
        """Parse a manifest mapping, rejecting unsupported and malformed fields."""
        if not isinstance(data, dict):
            raise ValueError("Script-worker manifest must be a JSON object.")
        expected = {
            "version",
            "workflow_path",
            "submission_idx",
            "jobscript_idx",
            "worker_id",
            "action_keys",
            "config_directory",
            "config_key",
        }
        if set(data) != expected:
            raise ValueError("Script-worker manifest has missing or unknown fields.")

        version = data["version"]
        submission_idx = data["submission_idx"]
        jobscript_idx = data["jobscript_idx"]
        if type(version) is not int or version != 1:
            raise ValueError(f"Unsupported script-worker manifest version: {version!r}.")
        for name, value in (
            ("submission_idx", submission_idx),
            ("jobscript_idx", jobscript_idx),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"Manifest {name} must be a nonnegative integer.")

        string_fields = ("workflow_path", "worker_id", "config_directory", "config_key")
        if any(
            not isinstance(data[name], str) or not data[name] for name in string_fields
        ):
            raise ValueError("Script-worker manifest string fields must be nonempty.")
        worker_id = data["worker_id"]
        if len(worker_id) != 10 or any(c not in "0123456789abcdef" for c in worker_id):
            raise ValueError("Manifest worker_id is not a valid worker identity.")

        raw_action_keys = data["action_keys"]
        if not isinstance(raw_action_keys, list) or not raw_action_keys:
            raise ValueError("Manifest action_keys must be a nonempty list.")
        action_keys: list[WorkerActionKey] = []
        for key in raw_action_keys:
            if (
                not isinstance(key, list)
                or len(key) != 2
                or any(type(value) is not int or value < 0 for value in key)
            ):
                raise ValueError(
                    "Manifest action keys must be nonnegative integer pairs."
                )
            action_keys.append((key[0], key[1]))
        if len(set(action_keys)) != len(action_keys):
            raise ValueError("Manifest action_keys must be unique.")

        return cls(
            workflow_path=data["workflow_path"],
            submission_idx=submission_idx,
            jobscript_idx=jobscript_idx,
            worker_id=worker_id,
            action_keys=tuple(action_keys),
            config_directory=data["config_directory"],
            config_key=data["config_key"],
        )

    def to_dict(self) -> dict[str, object]:
        """Return the JSON-compatible representation of this manifest."""
        return {
            "version": self.version,
            "workflow_path": self.workflow_path,
            "submission_idx": self.submission_idx,
            "jobscript_idx": self.jobscript_idx,
            "worker_id": self.worker_id,
            "action_keys": [list(key) for key in self.action_keys],
            "config_directory": self.config_directory,
            "config_key": self.config_key,
        }

    @classmethod
    def read(cls, path: Path) -> ScriptWorkerManifest:
        """Read and validate a manifest from a UTF-8 JSON file."""
        with path.open(encoding="utf-8") as fp:
            return cls.from_dict(json.load(fp))

    def write(self, path: Path) -> None:
        """Write this manifest as UTF-8 JSON using LF line endings."""
        with path.open("w", encoding="utf-8", newline="\n") as fp:
            json.dump(self.to_dict(), fp, indent=2)
            fp.write("\n")


def build_script_worker_invocations(
    manifest: ScriptWorkerManifest,
    action_scripts: ActionScripts,
    workflow: Workflow,
    *,
    environment: Mapping[str, str] | None = None,
) -> dict[ExecuteScriptRequest, ScriptWorkerInvocation]:
    """Bind generated callables to persisted runs at declared coordinates.

    Each requested action key must exist in both the manifest and generated registry;
    every matching persisted run must also match the worker's environment identity.
    """
    function_keys = set(action_scripts)
    if function_keys != set(manifest.action_keys):
        raise ValueError("Generated action registry does not match its manifest.")
    if any(not callable(function) for function in action_scripts.values()):
        raise TypeError("Generated action registry values must be callable.")

    try:
        submission = workflow.submissions[manifest.submission_idx]
        jobscript = submission.jobscripts[manifest.jobscript_idx]
    except IndexError as exc:
        raise ValueError(
            "Manifest references a missing submission or jobscript."
        ) from exc

    runs_by_id = {run.id_: run for run in jobscript.all_EARs}
    invocations: dict[ExecuteScriptRequest, ScriptWorkerInvocation] = {}
    run_environment = dict(os.environ if environment is None else environment)

    for block_idx, action_idx in manifest.action_keys:
        if block_idx >= len(jobscript.blocks):
            raise ValueError(f"Manifest references missing block {block_idx}.")
        block = jobscript.blocks[block_idx]
        if action_idx >= block.EAR_ID.shape[0]:
            raise ValueError(
                f"Manifest references missing action row {action_idx} in block "
                f"{block_idx}."
            )

        matched_runs = 0
        for element_idx, raw_run_id in enumerate(block.EAR_ID[action_idx]):
            run_id = int(raw_run_id)
            if run_id < 0:
                continue
            try:
                run = runs_by_id[run_id]
            except KeyError as exc:
                raise ValueError(f"Jobscript references missing run {run_id}.") from exc
            if (
                run.submission_idx != manifest.submission_idx
                or run.jobscript_idx != manifest.jobscript_idx
                or run.element_action.action_idx != action_idx
            ):
                raise ValueError(
                    f"Run {run_id} does not match manifest block/action identity."
                )
            if not run.is_snippet_script or not run.action.script_is_python_snippet:
                raise ValueError(f"Run {run_id} is not a Python snippet script.")
            worker_args = run.get_script_worker_args(manifest.jobscript_idx)
            if worker_args.stable_hash != manifest.worker_id:
                raise ValueError(f"Run {run_id} does not match manifest worker identity.")

            request = ExecuteScriptRequest(
                run_id=run_id,
                block_idx=block_idx,
                action_idx=action_idx,
                element_idx=element_idx,
            )
            invocations[request] = ScriptWorkerInvocation(
                request,
                run,
                action_scripts[(block_idx, action_idx)],
                environment=run_environment,
                std_path=run.get_app_std_path(),
                log_path=(
                    run.get_app_log_path() if run.resources.write_app_logs else None
                ),
            )
            matched_runs += 1

        if matched_runs == 0:
            raise ValueError(
                f"Manifest action key {(block_idx, action_idx)!r} has no assigned runs."
            )

    return invocations


def run_generated_script_worker(
    worker_script: str | Path, action_scripts: ActionScripts
) -> None:
    """Load persisted state, validate the registry, and serve the worker endpoint.

    The manifest path is derived from the generated script path by replacing its
    suffix with ``.json``. The endpoint is supplied by ``ScriptWorkerClient``.
    """
    manifest_path = Path(worker_script).with_suffix(".json")
    manifest = ScriptWorkerManifest.read(manifest_path)
    hf.load_config(
        config_dir=manifest.config_directory,
        config_key=manifest.config_key,
        warn=False,
    )
    workflow = hf.Workflow(manifest.workflow_path, load_config=True)
    invocations = build_script_worker_invocations(
        manifest,
        action_scripts,
        workflow,
    )
    try:
        endpoint = os.environ[SCRIPT_WORKER_ENDPOINT_ENV]
    except KeyError as exc:
        raise RuntimeError(
            f"Missing required {SCRIPT_WORKER_ENDPOINT_ENV} environment variable."
        ) from exc
    ScriptWorkerService(endpoint, invocations).run()
