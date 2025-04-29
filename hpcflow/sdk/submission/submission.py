"""
A collection of submissions to a scheduler, generated from a workflow.
"""

from __future__ import annotations
from collections import defaultdict
import shutil
from pathlib import Path
import socket
from textwrap import indent
from typing import Any, Literal, overload, TYPE_CHECKING
from typing_extensions import override
import warnings


from hpcflow.sdk.utils.strings import shorten_list_str
import numpy as np

from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.errors import (
    JobscriptSubmissionFailure,
    MissingEnvironmentError,
    MissingEnvironmentExecutableError,
    MissingEnvironmentExecutableInstanceError,
    MultipleEnvironmentsError,
    SubmissionFailure,
    OutputFileParserNoOutputError,
)
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.object_list import ObjectListMultipleMatchError
from hpcflow.sdk.core.utils import parse_timestamp, current_timestamp
from hpcflow.sdk.submission.enums import SubmissionStatus
from hpcflow.sdk.core import RUN_DIR_ARR_DTYPE
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.utils.strings import shorten_list_str

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from datetime import datetime
    from typing import ClassVar, Literal
    from rich.status import Status
    from numpy.typing import NDArray
    from .jobscript import Jobscript
    from .enums import JobscriptElementState
    from .schedulers import Scheduler
    from .shells import Shell
    from .types import SubmissionPart
    from ..core.element import ElementActionRun
    from ..core.environment import Environment
    from ..core.object_list import EnvironmentsList
    from ..core.workflow import Workflow
    from ..core.cache import ObjectCache


# jobscript attributes that are set persistently just after the jobscript has been
# submitted to the scheduler:
JOBSCRIPT_SUBMIT_TIME_KEYS = (
    "submit_cmdline",
    "scheduler_job_ID",
    "process_ID",
    "submit_time",
)
# submission attributes that are set persistently just after all of a submission's
# jobscripts have been submitted:
SUBMISSION_SUBMIT_TIME_KEYS = {
    "submission_parts": dict,
}


@hydrate
class Submission(JSONLike):
    """
    A collection of jobscripts to be submitted to a scheduler.

    Parameters
    ----------
    index: int
        The index of this submission.
    jobscripts: list[~hpcflow.app.Jobscript]
        The jobscripts in the submission.
    workflow: ~hpcflow.app.Workflow
        The workflow this is part of.
    submission_parts: dict
        Description of submission parts.
    JS_parallelism: bool
        Whether to exploit jobscript parallelism.
    environments: ~hpcflow.app.EnvironmentsList
        The execution environments to use.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="jobscripts",
            class_name="Jobscript",
            is_multiple=True,
            parent_ref="_submission",
        ),
        ChildObjectSpec(
            name="environments",
            class_name="EnvironmentsList",
        ),
    )

    TMP_DIR_NAME = "tmp"
    LOG_DIR_NAME = "app_logs"
    APP_STD_DIR_NAME = "app_std"
    JS_DIR_NAME = "jobscripts"
    JS_STD_DIR_NAME = "js_std"
    JS_RUN_IDS_DIR_NAME = "js_run_ids"
    JS_FUNCS_DIR_NAME = "js_funcs"
    JS_WIN_PIDS_DIR_NAME = "js_pids"
    JS_SCRIPT_INDICES_DIR_NAME = "js_script_indices"
    SCRIPTS_DIR_NAME = "scripts"
    COMMANDS_DIR_NAME = "commands"
    WORKFLOW_APP_ALIAS = "wkflow_app"

    def __init__(
        self,
        index: int,
        jobscripts: list[Jobscript],
        workflow: Workflow | None = None,
        at_submit_metadata: dict[str, Any] | None = None,
        JS_parallelism: bool | Literal["direct", "scheduled"] | None = None,
        environments: EnvironmentsList | None = None,
    ):
        self._index = index
        self._jobscripts = jobscripts
        self._at_submit_metadata = at_submit_metadata or {
            k: v() for k, v in SUBMISSION_SUBMIT_TIME_KEYS.items()
        }
        self._JS_parallelism = JS_parallelism
        self._environments = environments  # assigned by _set_environments

        self._submission_parts_lst: list[SubmissionPart] | None = (
            None  # assigned on first access
        )

        if workflow:
            #: The workflow this is part of.
            self.workflow = workflow

        self._set_parent_refs()

    def _ensure_JS_parallelism_set(self):
        """Ensure that the JS_parallelism attribute is one of `True`, `False`, `'direct'`
        or `'scheduled'`.

        Notes
        -----
        This method is called after the Submission object is first created in
        `Workflow._add_submission`.

        """
        # if JS_parallelism explicitly requested but store doesn't support, raise:
        supports_JS_para = self.workflow._store._features.jobscript_parallelism
        if self.JS_parallelism:
            # could be: True | "direct" | "scheduled"
            if not supports_JS_para:
                # if status:
                #     status.stop()
                raise ValueError(
                    f"Store type {self.workflow._store!r} does not support jobscript "
                    f"parallelism."
                )
        elif self.JS_parallelism is None:
            # by default only use JS parallelism for scheduled jobscripts:
            self._JS_parallelism = "scheduled" if supports_JS_para else False

    @TimeIt.decorator
    def _set_environments(self) -> None:
        filterable = self._app.ElementResources.get_env_instance_filterable_attributes()

        # map required environments and executable labels to job script indices:
        req_envs: dict[tuple[tuple[str, ...], tuple[Any, ...]], dict[str, set[int]]] = (
            defaultdict(lambda: defaultdict(set))
        )
        with self.workflow.cached_merged_parameters():
            # using the cache (for `run.env_spec_hashable` -> `run.resources`) should
            # significantly speed up this loop, unless a large resources sequence is used:
            for js_idx, all_EARs_i in enumerate(self.all_EARs_by_jobscript):
                for run in all_EARs_i:
                    env_spec_h = run.env_spec_hashable
                    for exec_label_j in run.action.get_required_executables():
                        req_envs[env_spec_h][exec_label_j].add(js_idx)
                    # add any environment for which an executable was not required:
                    if env_spec_h not in req_envs:
                        req_envs[env_spec_h]

        # check these envs/execs exist in app data:
        envs: list[Environment] = []
        for env_spec_h, exec_js in req_envs.items():
            env_spec = self._app.Action.env_spec_from_hashable(env_spec_h)
            try:
                env_i = self._app.envs.get(**env_spec)
            except ObjectListMultipleMatchError:
                raise MultipleEnvironmentsError(env_spec)
            except ValueError:
                raise MissingEnvironmentError(env_spec) from None
            else:
                if env_i not in envs:
                    envs.append(env_i)

            for exec_i_lab, js_idx_set in exec_js.items():
                try:
                    exec_i = env_i.executables.get(exec_i_lab)
                except ValueError:
                    raise MissingEnvironmentExecutableError(
                        env_spec, exec_i_lab
                    ) from None

                # check matching executable instances exist:
                for js_idx_j in js_idx_set:
                    js_res = self.jobscripts[js_idx_j].resources
                    filter_exec = {j: getattr(js_res, j) for j in filterable}
                    if not exec_i.filter_instances(**filter_exec):
                        raise MissingEnvironmentExecutableInstanceError(
                            env_spec, exec_i_lab, js_idx_j, filter_exec
                        )

        # save env definitions to the environments attribute:
        self._environments = self._app.EnvironmentsList(envs)

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        dct = super()._postprocess_to_dict(d)
        del dct["_workflow"]
        del dct["_index"]
        del dct["_submission_parts_lst"]
        return {k.lstrip("_"): v for k, v in dct.items()}

    @property
    def index(self) -> int:
        """
        The index of this submission.
        """
        return self._index

    @property
    def environments(self) -> EnvironmentsList:
        """
        The execution environments to use.
        """
        assert self._environments
        return self._environments

    @property
    def at_submit_metadata(self) -> dict[str, dict[str, Any]]:
        return self.workflow._store.get_submission_at_submit_metadata(
            sub_idx=self.index, metadata_attr=self._at_submit_metadata
        )

    @property
    def _submission_parts(self) -> dict[str, list[int]]:
        return self.at_submit_metadata["submission_parts"] or {}

    @property
    def submission_parts(self) -> list[SubmissionPart]:
        if self._submission_parts_lst is None:
            self._submission_parts_lst = [
                {
                    "submit_time": parse_timestamp(dt, self.workflow.ts_fmt),
                    "jobscripts": js_idx,
                }
                for dt, js_idx in self._submission_parts.items()
            ]
        return self._submission_parts_lst

    @TimeIt.decorator
    def get_start_time(self, submit_time: str) -> datetime | None:
        """Get the start time of a given submission part."""
        times = (
            self.jobscripts[i].start_time for i in self._submission_parts[submit_time]
        )
        return min((t for t in times if t is not None), default=None)

    @TimeIt.decorator
    def get_end_time(self, submit_time: str) -> datetime | None:
        """Get the end time of a given submission part."""
        times = (self.jobscripts[i].end_time for i in self._submission_parts[submit_time])
        return max((t for t in times if t is not None), default=None)

    @property
    @TimeIt.decorator
    def start_time(self) -> datetime | None:
        """Get the first non-None start time over all submission parts."""
        times = (
            self.get_start_time(submit_time) for submit_time in self._submission_parts
        )
        return min((t for t in times if t is not None), default=None)

    @property
    @TimeIt.decorator
    def end_time(self) -> datetime | None:
        """Get the final non-None end time over all submission parts."""
        times = (self.get_end_time(submit_time) for submit_time in self._submission_parts)
        return max((t for t in times if t is not None), default=None)

    @property
    def jobscripts(self) -> list[Jobscript]:
        """
        The jobscripts in this submission.
        """
        return self._jobscripts

    @property
    def JS_parallelism(self) -> bool | Literal["direct", "scheduled"] | None:
        """
        Whether to exploit jobscript parallelism.
        """
        return self._JS_parallelism

    @property
    def workflow(self) -> Workflow:
        """
        The workflow this is part of.
        """
        return self._workflow

    @workflow.setter
    def workflow(self, wk: Workflow):
        self._workflow = wk

    @property
    def jobscript_indices(self) -> tuple[int, ...]:
        """All associated jobscript indices."""
        return tuple(js.index for js in self.jobscripts)

    @property
    def submitted_jobscripts(self) -> tuple[int, ...]:
        """Jobscript indices that have been successfully submitted."""
        return tuple(j for sp in self.submission_parts for j in sp["jobscripts"])

    @property
    def outstanding_jobscripts(self) -> tuple[int, ...]:
        """Jobscript indices that have not yet been successfully submitted."""
        return tuple(set(self.jobscript_indices).difference(self.submitted_jobscripts))

    @property
    def status(self) -> SubmissionStatus:
        """
        The status of this submission.
        """
        if not self.submission_parts:
            return SubmissionStatus.PENDING
        elif set(self.submitted_jobscripts) == set(self.jobscript_indices):
            return SubmissionStatus.SUBMITTED
        else:
            return SubmissionStatus.PARTIALLY_SUBMITTED

    @property
    def needs_submit(self) -> bool:
        """
        Whether this submission needs a submit to be done.
        """
        return self.status in (
            SubmissionStatus.PENDING,
            SubmissionStatus.PARTIALLY_SUBMITTED,
        )

    @property
    def needs_app_log_dir(self) -> bool:
        """
        Whether this submision requires an app log directory.
        """
        for js in self.jobscripts:
            if js.resources.write_app_logs:
                return True
        return False

    @property
    def needs_win_pids_dir(self) -> bool:
        """
        Whether this submision requires a directory for process ID files (Windows only).
        """
        for js in self.jobscripts:
            if js.os_name == "nt":
                return True
        return False

    @property
    def needs_script_indices_dir(self) -> bool:
        """
        Whether this submision requires a directory for combined-script script ID files.
        """
        for js in self.jobscripts:
            if js.resources.combine_scripts:
                return True
        return False

    @classmethod
    def get_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        """
        The directory path to files associated with the specified submission.
        """
        return submissions_path / str(sub_idx)

    @classmethod
    def get_tmp_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        """
        The path to the temporary files directory, for the specified submission.
        """
        return cls.get_path(submissions_path, sub_idx) / cls.TMP_DIR_NAME

    @classmethod
    def get_app_log_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        """
        The path to the app log directory for this submission, for the specified
        submission.
        """
        return cls.get_path(submissions_path, sub_idx) / cls.LOG_DIR_NAME

    @staticmethod
    def get_app_log_file_name(run_ID: int | str) -> str:
        """
        The app log file name.
        """
        # TODO: consider combine_app_logs argument
        return f"r_{run_ID}.log"

    @classmethod
    def get_app_log_file_path(cls, submissions_path: Path, sub_idx: int, run_ID: int):
        """
        The file path to the app log, for the specified submission.
        """
        return (
            cls.get_path(submissions_path, sub_idx)
            / cls.LOG_DIR_NAME
            / cls.get_app_log_file_name(run_ID)
        )

    @classmethod
    def get_app_std_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        """
        The path to the app standard output and error stream files directory, for the
        specified submission.
        """
        return cls.get_path(submissions_path, sub_idx) / cls.APP_STD_DIR_NAME

    @classmethod
    def get_js_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        """
        The path to the jobscript files directory, for the specified submission.
        """
        return cls.get_path(submissions_path, sub_idx) / cls.JS_DIR_NAME

    @classmethod
    def get_js_std_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        """
        The path to the jobscript standard output and error files directory, for the
        specified submission.
        """
        return cls.get_path(submissions_path, sub_idx) / cls.JS_STD_DIR_NAME

    @classmethod
    def get_js_run_ids_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        """
        The path to the directory containing jobscript run IDs, for the specified
        submission.
        """
        return cls.get_path(submissions_path, sub_idx) / cls.JS_RUN_IDS_DIR_NAME

    @classmethod
    def get_js_funcs_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        """
        The path to the directory containing the shell functions that are invoked within
        jobscripts and commmand files, for the specified submission.
        """
        return cls.get_path(submissions_path, sub_idx) / cls.JS_FUNCS_DIR_NAME

    @classmethod
    def get_js_win_pids_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        """
        The path to the directory containing process ID files (Windows only), for the
        specified submission.
        """
        return cls.get_path(submissions_path, sub_idx) / cls.JS_WIN_PIDS_DIR_NAME

    @classmethod
    def get_js_script_indices_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        """
        The path to the directory containing script indices for combined-script jobscripts
        only, for the specified submission.
        """
        return cls.get_path(submissions_path, sub_idx) / cls.JS_SCRIPT_INDICES_DIR_NAME

    @classmethod
    def get_scripts_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        """
        The path to the directory containing action scripts, for the specified submission.
        """
        return cls.get_path(submissions_path, sub_idx) / cls.SCRIPTS_DIR_NAME

    @classmethod
    def get_commands_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        """
        The path to the directory containing command files, for the specified submission.
        """
        return cls.get_path(submissions_path, sub_idx) / cls.COMMANDS_DIR_NAME

    @property
    def path(self) -> Path:
        """
        The path to the directory containing action scripts.
        """
        return self.get_path(self.workflow.submissions_path, self.index)

    @property
    def tmp_path(self) -> Path:
        """
        The path to the temporary files directory for this submission.
        """
        return self.get_tmp_path(self.workflow.submissions_path, self.index)

    @property
    def app_log_path(self) -> Path:
        """
        The path to the app log directory for this submission for this submission.
        """
        return self.get_app_log_path(self.workflow.submissions_path, self.index)

    @property
    def app_std_path(self) -> Path:
        """
        The path to the app standard output and error stream files directory, for the
        this submission.
        """
        return self.get_app_std_path(self.workflow.submissions_path, self.index)

    @property
    def js_path(self) -> Path:
        """
        The path to the jobscript files directory, for this submission.
        """
        return self.get_js_path(self.workflow.submissions_path, self.index)

    @property
    def js_std_path(self) -> Path:
        """
        The path to the jobscript standard output and error files directory, for this
        submission.
        """
        return self.get_js_std_path(self.workflow.submissions_path, self.index)

    @property
    def js_run_ids_path(self) -> Path:
        """
        The path to the directory containing jobscript run IDs, for this submission.
        """
        return self.get_js_run_ids_path(self.workflow.submissions_path, self.index)

    @property
    def js_funcs_path(self) -> Path:
        """
        The path to the directory containing the shell functions that are invoked within
        jobscripts and commmand files, for this submission.
        """
        return self.get_js_funcs_path(self.workflow.submissions_path, self.index)

    @property
    def js_win_pids_path(self) -> Path:
        """
        The path to the directory containing process ID files (Windows only), for this
        submission.
        """
        return self.get_js_win_pids_path(self.workflow.submissions_path, self.index)

    @property
    def js_script_indices_path(self) -> Path:
        """
        The path to the directory containing script indices for combined-script jobscripts
        only, for this submission.
        """
        return self.get_js_script_indices_path(self.workflow.submissions_path, self.index)

    @property
    def scripts_path(self) -> Path:
        """
        The path to the directory containing action scripts, for this submission.
        """
        return self.get_scripts_path(self.workflow.submissions_path, self.index)

    @property
    def commands_path(self) -> Path:
        """
        The path to the directory containing command files, for this submission.
        """
        return self.get_commands_path(self.workflow.submissions_path, self.index)

    @property
    @TimeIt.decorator
    def all_EAR_IDs(self) -> Iterable[int]:
        """
        The IDs of all EARs in this submission.
        """
        return (i for js in self.jobscripts for i in js.all_EAR_IDs)

    @property
    @TimeIt.decorator
    def all_EARs(self) -> Iterable[ElementActionRun]:
        """
        All EARs in this submission.
        """
        return (ear for js in self.jobscripts for ear in js.all_EARs)

    @property
    @TimeIt.decorator
    def all_EARs_IDs_by_jobscript(self) -> list[np.ndarray]:
        return [i.all_EAR_IDs for i in self.jobscripts]

    @property
    @TimeIt.decorator
    def all_EARs_by_jobscript(self) -> list[list[ElementActionRun]]:
        ids = [i.all_EAR_IDs for i in self.jobscripts]
        all_EARs = {i.id_: i for i in self.workflow.get_EARs_from_IDs(self.all_EAR_IDs)}
        return [[all_EARs[i] for i in js_ids] for js_ids in ids]

    @property
    @TimeIt.decorator
    def EARs_by_elements(self) -> Mapping[int, Mapping[int, Sequence[ElementActionRun]]]:
        """
        All EARs in this submission, grouped by element.
        """
        task_elem_EARs: dict[int, dict[int, list[ElementActionRun]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for ear in self.all_EARs:
            task_elem_EARs[ear.task.index][ear.element.index].append(ear)
        return task_elem_EARs

    @property
    def is_scheduled(self) -> tuple[bool, ...]:
        """Return whether each jobscript of this submission uses a scheduler or not."""
        return tuple(i.is_scheduled for i in self.jobscripts)

    @overload
    def get_active_jobscripts(
        self, as_json: Literal[False] = False
    ) -> Mapping[int, Mapping[int, Mapping[int, JobscriptElementState]]]: ...

    @overload
    def get_active_jobscripts(
        self, as_json: Literal[True]
    ) -> Mapping[int, Mapping[int, Mapping[int, str]]]: ...

    @TimeIt.decorator
    def get_active_jobscripts(
        self,
        as_json: Literal[True] | Literal[False] = False,  # TODO: why can't we use bool?
    ) -> Mapping[int, Mapping[int, Mapping[int, JobscriptElementState | str]]]:
        """Get jobscripts that are active on this machine, and their active states."""
        # this returns: {JS_IDX: {BLOCK_IDX: {JS_ELEMENT_IDX: STATE}}}
        # TODO: query the scheduler once for all jobscripts?
        return {
            js.index: act_states
            for js in self.jobscripts
            if (act_states := js.get_active_states(as_json=as_json))
        }

    @TimeIt.decorator
    def _write_scripts(
        self, cache: ObjectCache, status: Status | None = None
    ) -> tuple[dict[int, int | None], NDArray, dict[int, list[Path]]]:
        """Write to disk all action scripts associated with this submission."""
        # TODO: rename this method

        # TODO: need to check is_snippet_script is exclusive? i.e. only `script` and no
        # `commands` in the action?
        # TODO: scripts must have the same exe and the same environment as well?
        # TODO: env_spec should be included in jobscript hash if combine_scripts=True ?

        actions_by_schema: dict[str, dict[int, set]] = defaultdict(
            lambda: defaultdict(set)
        )
        combined_env_specs = {}

        # task insert IDs and action indices for each combined_scripts jobscript:
        combined_actions = {}

        cmd_hashes = defaultdict(set)
        num_runs_tot = sum(len(js.all_EAR_IDs) for js in self.jobscripts)
        run_indices = np.ones((num_runs_tot, 9), dtype=int) * -1
        run_inp_files = defaultdict(
            list
        )  # keys are `run_idx`, values are Paths to copy to run dir
        run_cmd_file_names: dict[int, int | None] = {}  # None if no commands to write
        run_idx = 0

        if status:
            status.update(f"Adding new submission: processing run 1/{num_runs_tot}.")

        all_runs = cache.runs
        assert all_runs is not None
        runs_ids_by_js = self.all_EARs_IDs_by_jobscript

        with self.workflow.cached_merged_parameters():
            for js in self.jobscripts:
                js_idx = js.index
                js_run_0 = all_runs[runs_ids_by_js[js.index][0]]

                if js.resources.combine_scripts:
                    # this will be one or more snippet scripts that needs to be combined into
                    # one script for the whole jobscript

                    # need to write one script + one commands file for the whole jobscript

                    # env_spec will be the same for all runs of this jobscript:
                    combined_env_specs[js_idx] = js_run_0.env_spec
                    combined_actions[js_idx] = [
                        [j[0:2] for j in i.task_actions] for i in js.blocks
                    ]

                for idx, run_id in enumerate(js.all_EAR_IDs):
                    run = all_runs[run_id]

                    run_indices[run_idx] = [
                        run.task.insert_ID,
                        run.element.id_,
                        run.element_iteration.id_,
                        run.id_,
                        run.element.index,
                        run.element_iteration.index,
                        run.element_action.action_idx,
                        run.index,
                        int(run.action.requires_dir),
                    ]
                    run_idx += 1

                    if status and run_idx % 10 == 0:
                        status.update(
                            f"Adding new submission: processing run {run_idx}/{num_runs_tot}."
                        )

                    if js.resources.combine_scripts:
                        if idx == 0:
                            # the commands file for a combined jobscript won't have
                            # any parameter data in the command line, so should raise
                            # if something is found to be unset:
                            run.try_write_commands(
                                environments=self.environments,
                                jobscript=js,
                                raise_on_unset=True,
                            )
                        run_cmd_file_names[run.id_] = None

                    else:
                        if run.is_snippet_script:
                            actions_by_schema[run.action.task_schema.name][
                                run.element_action.action_idx
                            ].add(run.env_spec_hashable)

                        if run.action.commands:
                            hash_i = run.get_commands_file_hash()
                            # TODO: could further reduce number of files in the case the data
                            # indices hash is the same: if commands objects are the same and
                            # environment objects are the same, then the files will be the
                            # same, even if runs come from different task schemas/actions...
                            if hash_i not in cmd_hashes:
                                try:
                                    run.try_write_commands(
                                        environments=self.environments,
                                        jobscript=js,
                                    )
                                except OutputFileParserNoOutputError:
                                    # no commands to write, might be used just for saving
                                    # files
                                    run_cmd_file_names[run.id_] = None
                            cmd_hashes[hash_i].add(run.id_)
                        else:
                            run_cmd_file_names[run.id_] = None

                    if run.action.requires_dir:
                        # TODO: what is type of `path`?
                        for name, path in run.get("input_files", {}).items():
                            if path:
                                run_inp_files[run_idx].append(path)

        for run_ids in cmd_hashes.values():
            run_ids_srt = sorted(run_ids)
            root_id = run_ids_srt[0]  # used for command file name for this group
            # TODO: could store multiple IDs to reduce number of files created
            for run_id_i in run_ids_srt:
                if run_id_i not in run_cmd_file_names:
                    run_cmd_file_names[run_id_i] = root_id

        if status:
            status.update("Adding new submission: writing scripts...")

        seen: dict[int, Path] = {}
        combined_script_data: dict[int, dict[int, list[tuple[str, Path, bool]]]] = (
            defaultdict(lambda: defaultdict(list))
        )
        for task in self.workflow.tasks:
            for schema in task.template.schemas:
                if schema.name in actions_by_schema:
                    for idx, action in enumerate(schema.actions):

                        if not action.script:
                            continue

                        for env_spec_h in actions_by_schema[schema.name][idx]:

                            env_spec = action.env_spec_from_hashable(env_spec_h)
                            name, snip_path, specs = action.get_script_artifact_name(
                                env_spec=env_spec,
                                act_idx=idx,
                                ret_specifiers=True,
                            )
                            script_hash = action.get_script_determinant_hash(specs)
                            script_path = self.scripts_path / name
                            prev_path = seen.get(script_hash)
                            if script_path == prev_path:
                                continue

                            elif prev_path:
                                # try to make a symbolic link to the file previously
                                # created:
                                try:
                                    script_path.symlink_to(prev_path.name)
                                except OSError:
                                    # windows requires admin permission, copy instead:
                                    shutil.copy(prev_path, script_path)
                            else:
                                # write script to disk:
                                source_str = action.compose_source(snip_path)
                                if source_str:
                                    with script_path.open("wt", newline="\n") as fp:
                                        fp.write(source_str)
                                    seen[script_hash] = script_path

        # combined script stuff
        for js_idx, act_IDs in combined_actions.items():
            for block_idx, act_IDs_i in enumerate(act_IDs):
                for task_iID, act_idx in act_IDs_i:
                    task = self.workflow.tasks.get(insert_ID=task_iID)
                    schema = task.template.schemas[0]  # TODO: multiple schemas
                    action = schema.actions[act_idx]
                    func_name, snip_path = action.get_script_artifact_name(
                        env_spec=combined_env_specs[js_idx],
                        act_idx=act_idx,
                        ret_specifiers=False,
                        include_suffix=False,
                        specs_suffix_delim="_",  # can't use "." in function name
                    )
                    combined_script_data[js_idx][block_idx].append(
                        (func_name, snip_path, action.requires_dir)
                    )

        for js_idx, action_scripts in combined_script_data.items():
            js = self.jobscripts[js_idx]

            script_str, script_indices, num_elems, num_acts = js.compose_combined_script(
                [i for _, i in sorted(action_scripts.items())]
            )
            js.write_script_indices_file(script_indices, num_elems, num_acts)

            script_path = self.scripts_path / f"js_{js_idx}.py"  # TODO: refactor name
            with script_path.open("wt", newline="\n") as fp:
                fp.write(script_str)

        return run_cmd_file_names, run_indices, run_inp_files

    @TimeIt.decorator
    def _calculate_run_dir_indices(
        self,
        run_indices: np.ndarray,
        cache: ObjectCache,
    ) -> tuple[np.ndarray, np.ndarray]:

        assert cache.elements is not None
        assert cache.iterations is not None
        # get the multiplicities of all tasks, elements, iterations, and runs:
        wk_num_tasks = self.workflow.num_tasks
        task_num_elems = {}
        elem_num_iters = {}
        iter_num_acts = {}
        iter_acts_num_runs = {}
        for task in self.workflow.tasks:
            elem_IDs = task.element_IDs
            task_num_elems[task.insert_ID] = len(elem_IDs)
            for elem_ID in elem_IDs:
                iter_IDs = cache.elements[elem_ID].iteration_IDs
                elem_num_iters[elem_ID] = len(iter_IDs)
                for iter_ID in iter_IDs:
                    run_IDs = cache.iterations[iter_ID].EAR_IDs
                    if run_IDs:  # the schema might have no actions
                        iter_num_acts[iter_ID] = len(run_IDs)
                        for act_idx, act_run_IDs in run_IDs.items():
                            iter_acts_num_runs[(iter_ID, act_idx)] = len(act_run_IDs)
                    else:
                        iter_num_acts[iter_ID] = 0

        max_u8 = np.iinfo(np.uint8).max
        max_u32 = np.iinfo(np.uint32).max
        MAX_ELEMS_PER_DIR = 1000  # TODO: configurable (add `workflow_defaults` to Config)
        MAX_ITERS_PER_DIR = 1000
        requires_dir_idx = np.where(run_indices[:, -1] == 1)[0]
        run_dir_arr = np.empty(requires_dir_idx.size, dtype=RUN_DIR_ARR_DTYPE)
        run_ids = np.empty(requires_dir_idx.size, dtype=int)

        elem_depths: dict[int, int] = {}
        iter_depths: dict[int, int] = {}
        for idx in range(requires_dir_idx.size):
            row = run_indices[requires_dir_idx[idx]]
            t_iID, e_id, i_id, r_id, e_idx, i_idx, a_idx, r_idx = row[:-1]
            run_ids[idx] = r_id

            num_elems_i = task_num_elems[t_iID]
            num_iters_i = elem_num_iters[e_id]
            num_acts_i = iter_num_acts[i_id]  # see TODO below
            num_runs_i = iter_acts_num_runs[(i_id, a_idx)]

            e_depth = 1
            if num_elems_i == 1:
                e_idx = max_u32
            elif num_elems_i > MAX_ELEMS_PER_DIR:
                if (e_depth := elem_depths.get(t_iID, -1)) == -1:
                    e_depth = int(
                        np.ceil(np.log(num_elems_i) / np.log(MAX_ELEMS_PER_DIR))
                    )
                    elem_depths[t_iID] = e_depth

            # TODO: i_idx should be either MAX or the iteration ID, which will index into
            # a separate array to get the formatted loop indices e.g.
            # ("outer_loop_0_inner_loop_9")
            i_depth = 1
            if num_iters_i == 1:
                i_idx = max_u32
            elif num_iters_i > MAX_ITERS_PER_DIR:
                if (i_depth := iter_depths.get(e_id, -1)) == -1:
                    i_depth = int(
                        np.ceil(np.log(num_iters_i) / np.log(MAX_ITERS_PER_DIR))
                    )
                    iter_depths[e_id] = i_depth

            a_idx = max_u8  # TODO: for now, always exclude action index dir

            if num_runs_i == 1:
                r_idx = max_u8

            if wk_num_tasks == 1:
                t_iID = max_u8

            run_dir_arr[idx] = (t_iID, e_idx, i_idx, a_idx, r_idx, e_depth, i_depth)

        return run_dir_arr, run_ids

    @TimeIt.decorator
    def _write_execute_dirs(
        self,
        run_indices: NDArray,
        run_inp_files: dict[int, list[Path]],
        cache: ObjectCache,
        status: Status | None = None,
    ):

        if status:
            status.update("Adding new submission: resolving execution directories...")

        run_dir_arr, run_idx = self._calculate_run_dir_indices(run_indices, cache)

        # set run dirs in persistent array:
        if run_idx.size:
            self.workflow._store.set_run_dirs(run_dir_arr, run_idx)

        # retrieve run directories as paths. array is not yet commited, so pass in
        # directly:
        run_dirs = self.workflow.get_run_directories(dir_indices_arr=run_dir_arr)

        if status:
            status.update("Adding new submission: making execution directories...")

        # make directories
        for idx, run_dir in enumerate(run_dirs):
            assert run_dir
            run_dir.mkdir(parents=True, exist_ok=True)
            inp_files_i = run_inp_files.get(run_idx[idx])
            if inp_files_i:
                # copy (TODO: optionally symlink) any input files:
                for path_i in inp_files_i:
                    shutil.copy(path_i, run_dir)

    @staticmethod
    def get_unique_schedulers_of_jobscripts(
        jobscripts: Iterable[Jobscript],
    ) -> Iterable[tuple[tuple[tuple[int, int], ...], Scheduler]]:
        """Get unique schedulers and which of the passed jobscripts they correspond to.

        Uniqueness is determined only by the `QueuedScheduler.unique_properties` tuple.

        Parameters
        ----------
        jobscripts: list[~hpcflow.app.Jobscript]

        Returns
        -------
        scheduler_mapping
            Mapping where keys are a sequence of jobscript index descriptors and
            the values are the scheduler to use for that jobscript.
            A jobscript index descriptor is a pair of the submission index and the main
            jobscript index.
        """
        js_idx: list[list[tuple[int, int]]] = []
        schedulers: list[Scheduler] = []

        # list of tuples of scheduler properties we consider to determine "uniqueness",
        # with the first string being the scheduler type (class name):
        seen_schedulers: dict[tuple, int] = {}

        for js in jobscripts:
            if (
                sched_idx := seen_schedulers.get(key := js.scheduler.unique_properties)
            ) is None:
                seen_schedulers[key] = sched_idx = len(seen_schedulers) - 1
                schedulers.append(js.scheduler)
                js_idx.append([])
            js_idx[sched_idx].append((js.submission.index, js.index))

        return zip(map(tuple, js_idx), schedulers)

    @property
    @TimeIt.decorator
    def _unique_schedulers(
        self,
    ) -> Iterable[tuple[tuple[tuple[int, int], ...], Scheduler]]:
        return self.get_unique_schedulers_of_jobscripts(self.jobscripts)

    @TimeIt.decorator
    def get_unique_schedulers(self) -> Mapping[tuple[tuple[int, int], ...], Scheduler]:
        """Get unique schedulers and which of this submission's jobscripts they
        correspond to.

        Returns
        -------
        scheduler_mapping
            Mapping where keys are a sequence of jobscript index descriptors and
            the values are the scheduler to use for that jobscript.
            A jobscript index descriptor is a pair of the submission index and the main
            jobscript index.
        """
        # This is an absurd type; you never use the key as a key
        return dict(self._unique_schedulers)

    @TimeIt.decorator
    def get_unique_shells(self) -> Iterable[tuple[tuple[int, ...], Shell]]:
        """Get unique shells and which jobscripts they correspond to."""
        js_idx: list[list[int]] = []
        shells: list[Shell] = []

        for js in self.jobscripts:
            if js.shell not in shells:
                shells.append(js.shell)
                js_idx.append([])
            shell_idx = shells.index(js.shell)
            js_idx[shell_idx].append(js.index)

        return zip(map(tuple, js_idx), shells)

    def _update_at_submit_metadata(self, submission_parts: dict[str, list[int]]):
        """Update persistent store and in-memory record of at-submit metadata.

        Notes
        -----
        Currently there is only one type of at-submit metadata, which is the
        submission-parts: a mapping between a string submit-time, and the list of
        jobscript indices that were submitted at that submit-time. This method updates
        the recorded submission parts to include those passed here.

        """

        self.workflow._store.update_at_submit_metadata(
            sub_idx=self.index,
            submission_parts=submission_parts,
        )

        self._at_submit_metadata["submission_parts"].update(submission_parts)

        # cache is now invalid:
        self._submission_parts_lst = None

    def _append_submission_part(self, submit_time: str, submitted_js_idx: list[int]):
        self._update_at_submit_metadata(submission_parts={submit_time: submitted_js_idx})

    def get_jobscript_functions_name(self, shell: Shell, shell_idx: int) -> str:
        """Get the name of the jobscript functions file for the specified shell."""
        return f"js_funcs_{shell_idx}{shell.JS_EXT}"

    def get_jobscript_functions_path(self, shell: Shell, shell_idx: int) -> Path:
        """Get the path of the jobscript functions file for the specified shell."""
        return self.js_funcs_path / self.get_jobscript_functions_name(shell, shell_idx)

    def _compose_functions_file(self, shell: Shell) -> str:
        """Prepare the contents of the jobscript functions file for the specified
        shell.

        Notes
        -----
        The functions file includes, at a minimum, a shell function that invokes the app
        with provided arguments. This file will be sourced/invoked within all jobscripts
        and command files that share the specified shell.

        """

        cfg_invocation = self._app.config._file.get_invocation(
            self._app.config._config_key
        )
        env_setup = cfg_invocation["environment_setup"]
        if env_setup:
            env_setup = indent(env_setup.strip(), shell.JS_ENV_SETUP_INDENT)
            env_setup += "\n\n" + shell.JS_ENV_SETUP_INDENT
        else:
            env_setup = shell.JS_ENV_SETUP_INDENT
        app_invoc = list(self._app.run_time_info.invocation_command)

        app_caps = self._app.package_name.upper()
        func_file_args = shell.process_JS_header_args(  # TODO: rename?
            {
                "workflow_app_alias": self.WORKFLOW_APP_ALIAS,
                "env_setup": env_setup,
                "app_invoc": app_invoc,
                "app_caps": app_caps,
                "config_dir": str(self._app.config.config_directory),
                "config_invoc_key": self._app.config.config_key,
            }
        )
        out = shell.JS_FUNCS.format(**func_file_args)
        return out

    def _write_functions_file(self, shell: Shell, shell_idx: int) -> None:
        """Write the jobscript functions file for the specified shell.

        Notes
        -----
        The functions file includes, at a minimum, a shell function that invokes the app
        with provided arguments. This file will be sourced/invoked within all jobscripts
        and command files that share the specified shell.

        """
        js_funcs_str = self._compose_functions_file(shell)
        path = self.get_jobscript_functions_path(shell, shell_idx)
        with path.open("wt", newline="\n") as fp:
            fp.write(js_funcs_str)

    @TimeIt.decorator
    def submit(
        self,
        status: Status | None,
        ignore_errors: bool = False,
        print_stdout: bool = False,
        add_to_known: bool = True,
    ) -> list[int]:
        """Generate and submit the jobscripts of this submission."""

        # TODO: support passing list of jobscript indices to submit; this will allow us
        # to test a submision with multiple "submission parts". would also need to check
        # dependencies if this customised list is passed

        outstanding = self.outstanding_jobscripts

        # get scheduler, shell and OS version information (also an opportunity to fail
        # before trying to submit jobscripts):
        js_vers_info: dict[int, dict[str, str | list[str]]] = {}
        for js_indices, sched in self._unique_schedulers:
            try:
                vers_info = sched.get_version_info()
            except Exception:
                if not ignore_errors:
                    raise
                vers_info = {}
            for _, js_idx in js_indices:
                if js_idx in outstanding:
                    js_vers_info.setdefault(js_idx, {}).update(vers_info)

        js_shell_indices = {}
        for shell_idx, (js_indices_2, shell) in enumerate(self.get_unique_shells()):
            try:
                vers_info = shell.get_version_info()
            except Exception:
                if not ignore_errors:
                    raise
                vers_info = {}
            for js_idx in js_indices_2:
                if js_idx in outstanding:
                    js_vers_info.setdefault(js_idx, {}).update(vers_info)
                    js_shell_indices[js_idx] = shell_idx

            # write a file containing useful shell functions:
            self._write_functions_file(shell, shell_idx)

        hostname = socket.gethostname()
        machine = self._app.config.get("machine")
        for js_idx, vers_info_i in js_vers_info.items():
            js = self.jobscripts[js_idx]
            js._set_version_info(vers_info_i)
            js._set_submit_hostname(hostname)
            js._set_submit_machine(machine)
            js._set_shell_idx(js_shell_indices[js_idx])

        self.workflow._store._pending.commit_all()

        # map jobscript `index` to (scheduler job ID or process ID, is_array):
        scheduler_refs: dict[int, tuple[str, bool]] = {}
        submitted_js_idx: list[int] = []
        errs: list[JobscriptSubmissionFailure] = []
        for js in self.jobscripts:
            # check not previously submitted:
            if js.index not in outstanding:
                continue

            # check all dependencies were submitted now or previously:
            if not all(
                js_idx in submitted_js_idx or js_idx in self.submitted_jobscripts
                for js_idx, _ in js.dependencies
            ):
                warnings.warn(
                    f"Cannot submit jobscript index {js.index} since not all of its "
                    f"dependencies have been submitted: {js.dependencies!r}"
                )
                continue

            try:
                if status:
                    status.update(
                        f"Submitting jobscript {js.index + 1}/{len(self.jobscripts)}..."
                    )
                js_ref_i = js.submit(scheduler_refs, print_stdout=print_stdout)
                scheduler_refs[js.index] = (js_ref_i, js.is_array)
                submitted_js_idx.append(js.index)

            except JobscriptSubmissionFailure as err:
                errs.append(err)
                continue

            # TODO: some way to handle KeyboardInterrupt during submission?
            #   - stop, and cancel already submitted?

        if submitted_js_idx:
            dt_str = current_timestamp().strftime(self._app._submission_ts_fmt)
            self._append_submission_part(
                submit_time=dt_str,
                submitted_js_idx=submitted_js_idx,
            )
            # ensure `_submission_parts` is committed
            self.workflow._store._pending.commit_all()

            # add a record of the submission part to the known-submissions file
            if add_to_known:
                self._app._add_to_known_submissions(
                    wk_path=self.workflow.path,
                    wk_id=self.workflow.id_,
                    sub_idx=self.index,
                    sub_time=dt_str,
                )

        if errs and not ignore_errors:
            if status:
                status.stop()
            raise SubmissionFailure(self.index, submitted_js_idx, errs)

        len_js = len(submitted_js_idx)
        print(f"Submitted {len_js} jobscript{'s' if len_js > 1 else ''}.")

        return submitted_js_idx

    @TimeIt.decorator
    def cancel(self) -> None:
        """
        Cancel the active jobs for this submission's jobscripts.
        """
        if not (act_js := self.get_active_jobscripts()):
            print("No active jobscripts to cancel.")
            return
        for js_indices, sched in self._unique_schedulers:
            # filter by active jobscripts:
            if js_idx := [i[1] for i in js_indices if i[1] in act_js]:
                print(
                    f"Cancelling jobscripts {shorten_list_str(js_idx, items=5)} of "
                    f"submission {self.index} of workflow {self.workflow.name!r}."
                )
                jobscripts = [self.jobscripts[i] for i in js_idx]
                sched_refs = [js.scheduler_js_ref for js in jobscripts]
                sched.cancel_jobs(js_refs=sched_refs, jobscripts=jobscripts)
            else:
                print("No active jobscripts to cancel.")

    @TimeIt.decorator
    def get_scheduler_job_IDs(self) -> tuple[str, ...]:
        """Return jobscript scheduler job IDs."""
        return tuple(
            js_i.scheduler_job_ID
            for js_i in self.jobscripts
            if js_i.scheduler_job_ID is not None
        )

    @TimeIt.decorator
    def get_process_IDs(self) -> tuple[int, ...]:
        """Return jobscript process IDs."""
        return tuple(
            js_i.process_ID for js_i in self.jobscripts if js_i.process_ID is not None
        )

    @TimeIt.decorator
    def list_jobscripts(
        self,
        max_js: int | None = None,
        jobscripts: list[int] | None = None,
        width: int | None = None,
    ) -> None:
        """Print a table listing jobscripts and associated information.

        Parameters
        ----------
        max_js
            Maximum jobscript index to display. This cannot be specified with `jobscripts`.
        jobscripts
            A list of jobscripts to display. This cannot be specified with `max_js`.
        width
            Width in characters of the printed table.

        """
        self.workflow.list_jobscripts(
            sub_idx=self.index, max_js=max_js, jobscripts=jobscripts, width=width
        )

    @TimeIt.decorator
    def list_task_jobscripts(
        self,
        task_names: list[str] | None = None,
        max_js: int | None = None,
        width: int | None = None,
    ) -> None:
        """Print a table listing the jobscripts associated with the specified (or all)
        tasks for the specified submission.

        Parameters
        ----------
        task_names
            List of sub-strings to match to task names. Only matching task names will be
            included.
        max_js
            Maximum jobscript index to display.
        width
            Width in characters of the printed table.

        """
        self.workflow.list_task_jobscripts(
            sub_idx=self.index, max_js=max_js, task_names=task_names, width=width
        )
