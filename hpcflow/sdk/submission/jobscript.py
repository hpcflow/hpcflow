"""
Model of information submitted to a scheduler.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
from textwrap import indent
from typing import cast, overload, TYPE_CHECKING
from typing_extensions import override

import numpy as np
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.errors import (
    JobscriptSubmissionFailure,
    NotSubmitMachineError,
)

from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.utils import parse_timestamp, current_timestamp
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.submission.schedulers.direct import DirectScheduler
from hpcflow.sdk.submission.shells import get_shell, DEFAULT_SHELL_NAMES

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence
    from datetime import datetime
    from pathlib import Path
    from typing import Any, ClassVar, Literal
    from typing_extensions import TypeIs
    from numpy.typing import NDArray, ArrayLike
    from ..core.actions import ElementActionRun
    from ..core.element import ElementResources
    from ..core.loop_cache import LoopIndex
    from ..core.types import JobscriptSubmissionFailureArgs
    from ..core.workflow import WorkflowTask, Workflow
    from .submission import Submission
    from .shells.base import Shell
    from .schedulers import Scheduler, QueuedScheduler
    from .enums import JobscriptElementState
    from .types import (
        JobScriptCreationArguments,
        JobScriptDescriptor,
        ResolvedDependencies,
        SchedulerRef,
        VersionInfo,
    )


@TimeIt.decorator
def generate_EAR_resource_map(
    task: WorkflowTask,
    loop_idx: LoopIndex[str, int],
) -> tuple[Sequence[ElementResources], Sequence[int], NDArray, NDArray]:
    """
    Generate an integer array whose rows represent actions and columns represent task
    elements and whose values index unique resources.
    """
    # TODO: assume single iteration for now; later we will loop over Loop tasks for each
    # included task and call this func with specific loop indices
    none_val = -1
    resources: list[ElementResources] = []
    resource_hashes: list[int] = []

    arr_shape = (task.num_actions, task.num_elements)
    resource_map = np.empty(arr_shape, dtype=int)
    EAR_ID_map = np.empty(arr_shape, dtype=int)
    # EAR_idx_map = np.empty(
    #     shape=arr_shape,
    #     dtype=[("EAR_idx", np.int32), ("run_idx", np.int32), ("iteration_idx", np.int32)],
    # )
    resource_map[:] = none_val
    EAR_ID_map[:] = none_val
    # EAR_idx_map[:] = (none_val, none_val, none_val)  # TODO: add iteration_idx as well

    for element in task.elements[:]:
        for iter_i in element.iterations:
            if iter_i.loop_idx != loop_idx:
                continue
            if iter_i.EARs_initialised:  # not strictly needed (actions will be empty)
                for act_idx, action in iter_i.actions.items():
                    for run in action.runs:
                        if run.status == EARStatus.pending:
                            # TODO: consider `time_limit`s
                            res_hash = run.resources.get_jobscript_hash()
                            if res_hash not in resource_hashes:
                                resource_hashes.append(res_hash)
                                resources.append(run.resources)
                            resource_map[act_idx][element.index] = resource_hashes.index(
                                res_hash
                            )
                            EAR_ID_map[act_idx, element.index] = run.id_

    # set defaults for and validate unique resources:
    for res in resources:
        res.set_defaults()
        res.validate_against_machine()

    return (
        resources,
        resource_hashes,
        resource_map,
        EAR_ID_map,
    )


@TimeIt.decorator
def group_resource_map_into_jobscripts(
    resource_map: ArrayLike,
    none_val: Any = -1,
) -> tuple[list[JobScriptDescriptor], NDArray]:
    """
    Convert a resource map into a plan for what elements to group together into jobscripts.
    """
    resource_map_ = np.asanyarray(resource_map)
    resource_idx = np.unique(resource_map_)
    jobscripts: list[JobScriptDescriptor] = []
    allocated = np.zeros_like(resource_map_)
    js_map = np.ones_like(resource_map_, dtype=float) * np.nan
    nones_bool: NDArray = resource_map_ == none_val
    stop = False
    for act_idx in range(resource_map_.shape[0]):
        for res_i in resource_idx:
            if res_i == none_val:
                continue

            if res_i not in resource_map_[act_idx]:
                continue

            resource_map_[nones_bool] = res_i
            diff = np.cumsum(np.abs(np.diff(resource_map_[act_idx:], axis=0)), axis=0)

            elem_bool = np.logical_and(
                resource_map_[act_idx] == res_i, allocated[act_idx] == False
            )
            elem_idx = np.where(elem_bool)[0]
            act_elem_bool = np.logical_and(elem_bool, nones_bool[act_idx] == False)
            act_elem_idx: tuple[NDArray, ...] = np.where(act_elem_bool)

            # add elements from downstream actions:
            ds_bool = np.logical_and(
                diff[:, elem_idx] == 0,
                nones_bool[act_idx + 1 :, elem_idx] == False,
            )
            ds_act_idx: NDArray
            ds_elem_idx: NDArray
            ds_act_idx, ds_elem_idx = np.where(ds_bool)
            ds_act_idx += act_idx + 1
            ds_elem_idx = elem_idx[ds_elem_idx]

            EARs_by_elem: dict[int, list[int]] = {
                k.item(): [act_idx] for k in act_elem_idx[0]
            }
            for ds_a, ds_e in zip(ds_act_idx, ds_elem_idx):
                EARs_by_elem.setdefault(ds_e.item(), []).append(ds_a.item())

            EARs = np.vstack([np.ones_like(act_elem_idx) * act_idx, act_elem_idx])
            EARs = np.hstack([EARs, np.array([ds_act_idx, ds_elem_idx])])

            if not EARs.size:
                continue

            js: JobScriptDescriptor = {
                "resources": res_i,
                "elements": dict(sorted(EARs_by_elem.items(), key=lambda x: x[0])),
            }
            allocated[EARs[0], EARs[1]] = True
            js_map[EARs[0], EARs[1]] = len(jobscripts)
            jobscripts.append(js)

            if np.all(allocated[~nones_bool]):
                stop = True
                break

        if stop:
            break

    resource_map_[nones_bool] = none_val

    return jobscripts, js_map


@TimeIt.decorator
def resolve_jobscript_dependencies(
    jobscripts: Mapping[int, JobScriptCreationArguments],
    element_deps: Mapping[int, Mapping[int, Sequence[int]]],
) -> Mapping[int, dict[int, ResolvedDependencies]]:
    """
    Discover concrete dependencies between jobscripts.
    """
    # first pass is to find the mappings between jobscript elements:
    jobscript_deps: dict[int, dict[int, ResolvedDependencies]] = {}
    for js_idx, elem_deps in element_deps.items():
        # keys of new dict are other jobscript indices on which this jobscript (js_idx)
        # depends:
        jobscript_deps[js_idx] = {}

        for js_elem_idx_i, EAR_deps_i in elem_deps.items():
            # locate which jobscript elements this jobscript element depends on:
            for EAR_dep_j in EAR_deps_i:
                for js_k_idx, js_k in jobscripts.items():
                    if js_k_idx == js_idx:
                        break

                    if EAR_dep_j in js_k["EAR_ID"]:
                        if js_k_idx not in jobscript_deps[js_idx]:
                            jobscript_deps[js_idx][js_k_idx] = {"js_element_mapping": {}}

                        jobscript_deps[js_idx][js_k_idx]["js_element_mapping"].setdefault(
                            js_elem_idx_i, []
                        )

                        # retrieve column index, which is the JS-element index:
                        js_elem_idx_k: int = np.where(
                            np.any(js_k["EAR_ID"] == EAR_dep_j, axis=0)
                        )[0][0].item()

                        # add js dependency element-mapping:
                        if (
                            js_elem_idx_k
                            not in jobscript_deps[js_idx][js_k_idx]["js_element_mapping"][
                                js_elem_idx_i
                            ]
                        ):
                            jobscript_deps[js_idx][js_k_idx]["js_element_mapping"][
                                js_elem_idx_i
                            ].append(js_elem_idx_k)

    # next we can determine if two jobscripts have a one-to-one element mapping, which
    # means they can be submitted with a "job array" dependency relationship:
    for js_i_idx, deps_i in jobscript_deps.items():
        for js_k_idx, deps_j in deps_i.items():
            # is this an array dependency?

            js_i_num_js_elements = jobscripts[js_i_idx]["EAR_ID"].shape[1]
            js_k_num_js_elements = jobscripts[js_k_idx]["EAR_ID"].shape[1]

            is_all_i_elems = sorted(set(deps_j["js_element_mapping"])) == list(
                range(js_i_num_js_elements)
            )

            is_all_k_single = set(
                len(i) for i in deps_j["js_element_mapping"].values()
            ) == {1}

            is_all_k_elems = sorted(
                i[0] for i in deps_j["js_element_mapping"].values()
            ) == list(range(js_k_num_js_elements))

            is_arr = is_all_i_elems and is_all_k_single and is_all_k_elems
            jobscript_deps[js_i_idx][js_k_idx]["is_array"] = is_arr

    return jobscript_deps


def _reindex_dependencies(
    jobscripts: Mapping[int, JobScriptCreationArguments], from_idx: int, to_idx: int
):
    for ds_js_idx, ds_js in jobscripts.items():
        if ds_js_idx <= from_idx:
            continue
        deps = ds_js["dependencies"]
        if from_idx in deps:
            deps[to_idx] = deps.pop(from_idx)


@TimeIt.decorator
def merge_jobscripts_across_tasks(
    jobscripts: Mapping[int, JobScriptCreationArguments]
) -> Mapping[int, JobScriptCreationArguments]:
    """Try to merge jobscripts between tasks.

    This is possible if two jobscripts share the same resources and have an array
    dependency (i.e. one-to-one element dependency mapping).

    """

    # The set of IDs of dicts that we've merged, allowing us to not keep that info in
    # the dicts themselves.
    merged: set[int] = set()

    for js_idx, js in jobscripts.items():
        # for now only attempt to merge a jobscript with a single dependency:
        if len(js["dependencies"]) != 1:
            continue
        deps = js["dependencies"]
        js_j_idx, dep_info = next(iter(deps.items()))
        js_j = jobscripts[js_j_idx]  # the jobscript we are merging `js` into

        # can only merge if resources are the same and is array dependency:
        if js["resource_hash"] == js_j["resource_hash"] and dep_info["is_array"]:
            num_loop_idx = len(
                js_j["task_loop_idx"]
            )  # TODO: should this be: `js_j["task_loop_idx"][0]`?

            # append task_insert_IDs
            js_j["task_insert_IDs"].append(js["task_insert_IDs"][0])
            js_j["task_loop_idx"].append(js["task_loop_idx"][0])

            add_acts = [(a, b, num_loop_idx) for a, b, _ in js["task_actions"]]

            js_j["task_actions"].extend(add_acts)
            for k, v in js["task_elements"].items():
                js_j["task_elements"][k].extend(v)

            # append to elements and elements_idx list
            js_j["EAR_ID"] = np.vstack((js_j["EAR_ID"], js["EAR_ID"]))

            # mark this js as defunct
            merged.add(id(js))

            # update dependencies of any downstream jobscripts that refer to this js
            _reindex_dependencies(jobscripts, js_idx, js_j_idx)

    # remove is_merged jobscripts:
    return {k: v for k, v in jobscripts.items() if id(v) not in merged}


@TimeIt.decorator
def jobscripts_to_list(
    jobscripts: Mapping[int, JobScriptCreationArguments]
) -> Iterator[JobScriptCreationArguments]:
    """Convert the jobscripts dict to a list, normalising jobscript indices so they refer
    to list indices; also remove `resource_hash`."""
    lst: list[JobScriptCreationArguments] = []
    for new_idx, (js_idx, js) in enumerate(jobscripts.items()):
        if js_idx != new_idx:
            # need to reindex jobscripts that depend on this one
            _reindex_dependencies(jobscripts, js_idx, new_idx)
        del js["resource_hash"]
        yield js


@hydrate
class Jobscript(JSONLike):
    """
    A group of actions that are submitted together to be executed by the underlying job
    management system as a single unit.

    Parameters
    ----------
    task_insert_IDs: list[int]
        The task insertion IDs.
    task_actions: list[tuple]
        The actions of the tasks.
        ``task insert ID, action_idx, index into task_loop_idx`` for each ``JS_ACTION_IDX``
    task_elements: dict[int, list[int]]
        The elements of the tasks.
        Maps ``JS_ELEMENT_IDX`` to list of ``TASK_ELEMENT_IDX`` for each ``TASK_INSERT_ID``
    EAR_ID:
        Element action run information.
    resources: ~hpcflow.app.ElementResources
        Resources to use
    task_loop_idx: list[dict]
        Description of what loops are in play.
    dependencies: dict[int, dict]
        Description of dependencies.
    submit_time: datetime
        When the jobscript was submitted, if known.
    submit_hostname: str
        Where the jobscript was submitted, if known.
    submit_machine: str
        Description of what the jobscript was submitted to, if known.
    submit_cmdline: str
        The command line used to do the commit, if known.
    scheduler_job_ID: str
        The job ID from the scheduler, if known.
    process_ID: int
        The process ID of the subprocess, if known.
    version_info: dict[str, ...]
        Version info about the target system.
    os_name: str
        The name of the OS.
    shell_name: str
        The name of the shell.
    scheduler_name: str
        The scheduler used.
    running: bool
        Whether the jobscript is currently running.
    """

    _EAR_files_delimiter: ClassVar[str] = ":"
    _workflow_app_alias: ClassVar[str] = "wkflow_app"

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="resources",
            class_name="ElementResources",
        ),
    )

    @classmethod
    def __is_QueuedScheduler(cls, value) -> TypeIs[QueuedScheduler]:
        return isinstance(value, cls._app.QueuedScheduler)

    def __init__(
        self,
        task_insert_IDs: list[int],
        task_actions: list[tuple[int, int, int]],
        task_elements: dict[int, list[int]],
        EAR_ID: NDArray,
        resources: ElementResources,
        task_loop_idx: list[dict[str, int]],
        dependencies: dict[int, ResolvedDependencies],
        submit_time: datetime | None = None,
        submit_hostname: str | None = None,
        submit_machine: str | None = None,
        submit_cmdline: list[str] | None = None,
        scheduler_job_ID: str | None = None,
        process_ID: int | None = None,
        version_info: VersionInfo | None = None,
        os_name: str | None = None,
        shell_name: str | None = None,
        scheduler_name: str | None = None,
        running: bool | None = None,
        resource_hash: str | None = None,
        elements: dict[int, list[int]] | None = None,
    ):
        if resource_hash is not None:
            raise AttributeError("resource_hash must not be supplied")
        if elements is not None:
            raise AttributeError("elements must not be supplied")
        self._task_insert_IDs = task_insert_IDs
        self._task_loop_idx = task_loop_idx

        # [ (task insert ID, action_idx, index into task_loop_idx) for each JS_ACTION_IDX ]:
        self._task_actions = task_actions

        # {JS_ELEMENT_IDX: [TASK_ELEMENT_IDX for each TASK_INSERT_ID] }:
        self._task_elements = task_elements

        self._EAR_ID = EAR_ID
        self._resources = resources
        self._dependencies = dependencies

        # assigned on parent `Submission.submit` (or retrieved form persistent store):
        self._submit_time = submit_time
        self._submit_hostname = submit_hostname
        self._submit_machine = submit_machine
        self._submit_cmdline = submit_cmdline

        self._scheduler_job_ID = scheduler_job_ID
        self._process_ID = process_ID
        self._version_info = version_info

        # assigned as submit-time:
        # TODO: these should now always be set in `resources` so shouldn't need these:
        self._os_name = os_name
        self._shell_name = shell_name
        self._scheduler_name = scheduler_name

        # assigned by parent Submission
        self._submission: Submission | None = None
        # assigned by parent Submission
        self._index: int | None = None
        # assigned on first access to `scheduler` property
        self._scheduler_obj: Scheduler | None = None
        # assigned on first access to `shell` property
        self._shell_obj: Shell | None = None
        # assigned on first access to `submit_time` property
        self._submit_time_obj: datetime | None = None
        self._running = running
        # assigned on first access to `all_EARs` property
        self._all_EARs: list[ElementActionRun] | None = None

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"index={self.index!r}, "
            f"task_insert_IDs={self.task_insert_IDs!r}, "
            f"resources={self.resources!r}, "
            f"dependencies={self.dependencies!r}"
            f")"
        )

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        dct = super()._postprocess_to_dict(d)
        del dct["_index"]
        del dct["_scheduler_obj"]
        del dct["_shell_obj"]
        del dct["_submit_time_obj"]
        del dct["_all_EARs"]
        dct = {k.lstrip("_"): v for k, v in dct.items()}
        dct["EAR_ID"] = cast("NDArray", dct["EAR_ID"]).tolist()
        return dct

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        json_like["EAR_ID"] = np.array(json_like["EAR_ID"])
        return super().from_json_like(json_like, shared_data)

    @property
    def workflow_app_alias(self) -> str:
        """
        Alias for the workflow app in job scripts.
        """
        return self._workflow_app_alias

    def get_commands_file_name(
        self, js_action_idx: int | str, shell: Shell | None = None
    ) -> str:
        """
        Get the name of a file containing commands for a particular jobscript action.
        """
        return self._app.RunDirAppFiles.get_commands_file_name(
            js_idx=self.index,
            js_action_idx=js_action_idx,
            shell=shell or self.shell,
        )

    @property
    def task_insert_IDs(self) -> Sequence[int]:
        """
        The insertion IDs of tasks in this jobscript.
        """
        return self._task_insert_IDs

    @property
    def task_actions(self) -> Sequence[tuple[int, int, int]]:
        """
        The IDs of actions of each task in this jobscript.
        """
        return self._task_actions

    @property
    def task_elements(self) -> Mapping[int, Sequence[int]]:
        """
        The IDs of elements of each task in this jobscript.
        """
        return self._task_elements

    @property
    def EAR_ID(self) -> NDArray:
        """
        The array of EAR IDs.
        """
        return self._EAR_ID

    @property
    def all_EAR_IDs(self) -> Iterable[int]:
        """
        The IDs of all EARs in this jobscript.
        """
        return self.EAR_ID.flatten()

    @property
    @TimeIt.decorator
    def all_EARs(self) -> Sequence[ElementActionRun]:
        """
        Description of EAR information for this jobscript.
        """
        if not self._all_EARs:
            self._all_EARs = self.workflow.get_EARs_from_IDs(self.all_EAR_IDs)
        return self._all_EARs

    @property
    def resources(self) -> ElementResources:
        """
        The common resources that this jobscript requires.
        """
        return self._resources

    @property
    def task_loop_idx(self) -> Sequence[Mapping[str, int]]:
        """
        The description of where various task loops are.
        """
        return self._task_loop_idx

    @property
    def dependencies(self) -> Mapping[int, ResolvedDependencies]:
        """
        The dependency descriptor.
        """
        return self._dependencies

    @property
    @TimeIt.decorator
    def start_time(self) -> None | datetime:
        """The first known start time of any EAR in this jobscript."""
        if not self.is_submitted:
            return None
        return min(
            (ear.start_time for ear in self.all_EARs if ear.start_time), default=None
        )

    @property
    @TimeIt.decorator
    def end_time(self) -> None | datetime:
        """The last known end time of any EAR in this jobscript."""
        if not self.is_submitted:
            return None
        return max((ear.end_time for ear in self.all_EARs if ear.end_time), default=None)

    @property
    def submit_time(self) -> datetime | None:
        """
        When the jobscript was submitted, if known.
        """
        if self._submit_time_obj is None and self._submit_time is not None:
            self._submit_time_obj = parse_timestamp(
                self._submit_time, self.workflow.ts_fmt
            )
        return self._submit_time_obj

    @property
    def submit_hostname(self) -> str | None:
        """
        Where the jobscript was submitted, if known.
        """
        return self._submit_hostname

    @property
    def submit_machine(self) -> str | None:
        """
        Description of what the jobscript was submitted to, if known.
        """
        return self._submit_machine

    @property
    def submit_cmdline(self) -> list[str] | None:
        """
        The command line used to do the commit, if known.
        """
        return self._submit_cmdline

    @property
    def scheduler_job_ID(self) -> str | None:
        """
        The job ID from the scheduler, if known.
        """
        return self._scheduler_job_ID

    @property
    def process_ID(self) -> int | None:
        """
        The process ID from direct execution, if known.
        """
        return self._process_ID

    @property
    def version_info(self) -> VersionInfo | None:
        """
        Version information about the execution environment (OS, etc).
        """
        return self._version_info

    @property
    def index(self) -> int:
        """
        The index of this jobscript within its parent :py:class:`Submission`.
        """
        assert self._index is not None
        return self._index

    @property
    def submission(self) -> Submission:
        """
        The parent submission.
        """
        assert self._submission is not None
        return self._submission

    @property
    def workflow(self) -> Workflow:
        """
        The workflow this is all on behalf of.
        """
        return self.submission.workflow

    @property
    def num_actions(self) -> int:
        """
        The number of actions in this jobscript.
        """
        return self.EAR_ID.shape[0]

    @property
    def num_elements(self) -> int:
        """
        The number of elements in this jobscript.
        """
        return self.EAR_ID.shape[1]

    @property
    def is_array(self) -> bool:
        """
        Whether to generate an array job.
        """
        if self.scheduler_name == "direct":
            return False

        support_EAR_para = self.workflow._store._features.EAR_parallelism
        if self.resources.use_job_array is None:
            return self.num_elements > 1 and support_EAR_para
        if self.resources.use_job_array and not support_EAR_para:
            raise ValueError(
                f"Store type {self.workflow._store!r} does not support element "
                f"parallelism, so jobs cannot be submitted as scheduler arrays."
            )
        return self.resources.use_job_array

    @property
    def os_name(self) -> str:
        """
        The name of the OS to use.
        """
        name = self._os_name or self.resources.os_name
        assert name is not None
        return name

    @property
    def shell_name(self) -> str | None:
        """
        The name of the shell to use.
        """
        return self._shell_name or self.resources.shell

    @property
    def scheduler_name(self) -> str | None:
        """
        The name of the scheduler to use.
        """
        return self._scheduler_name or self.resources.scheduler

    def _get_submission_os_args(self) -> dict[str, str]:
        return {"linux_release_file": self._app.config.linux_release_file}

    def _get_submission_shell_args(self) -> dict[str, Any]:
        return self.resources.shell_args

    def _get_submission_scheduler_args(self) -> dict[str, Any]:
        return self.resources.scheduler_args

    def _get_shell(
        self,
        os_name: str,
        shell_name: str | None,
        os_args: dict[str, Any] | None = None,
        shell_args: dict[str, Any] | None = None,
    ) -> Shell:
        """Get an arbitrary shell, not necessarily associated with submission."""
        return get_shell(
            shell_name=shell_name,
            os_name=os_name,
            os_args=os_args or {},
            **(shell_args or {}),
        )

    @property
    def shell(self) -> Shell:
        """The shell for composing submission scripts."""
        if self._shell_obj is None:
            self._shell_obj = self._get_shell(
                os_name=self.os_name,
                shell_name=self.shell_name,
                os_args=self._get_submission_os_args(),
                shell_args=self._get_submission_shell_args(),
            )
        return self._shell_obj

    @property
    def scheduler(self) -> Scheduler:
        """The scheduler that submissions go to from this jobscript."""
        if self._scheduler_obj is None:
            assert self.scheduler_name
            self._scheduler_obj = self._app.get_scheduler(
                scheduler_name=self.scheduler_name,
                os_name=self.os_name,
                scheduler_args=self._get_submission_scheduler_args(),
            )
        return self._scheduler_obj

    @property
    def EAR_ID_file_name(self) -> str:
        """
        The name of a file containing EAR IDs.
        """
        return f"js_{self.index}_EAR_IDs.txt"

    @property
    def element_run_dir_file_name(self) -> str:
        """
        The name of a file containing run directory names.
        """
        return f"js_{self.index}_run_dirs.txt"

    @property
    def direct_stdout_file_name(self) -> str:
        """File for direct execution stdout."""
        return f"js_{self.index}_stdout.log"

    @property
    def direct_stderr_file_name(self) -> str:
        """File for direct execution stderr."""
        return f"js_{self.index}_stderr.log"

    @property
    def direct_win_pid_file_name(self) -> str:
        """File for holding the direct execution PID."""
        return f"js_{self.index}_pid.txt"

    @property
    def jobscript_name(self) -> str:
        """The name of the jobscript file."""
        return f"js_{self.index}{self.shell.JS_EXT}"

    @property
    def EAR_ID_file_path(self) -> Path:
        """
        The path to the file containing EAR IDs for this jobscript.
        """
        return self.submission.path / self.EAR_ID_file_name

    @property
    def element_run_dir_file_path(self) -> Path:
        """
        The path to the file containing run directory names for this jobscript.
        """
        return self.submission.path / self.element_run_dir_file_name

    @property
    def jobscript_path(self) -> Path:
        """
        The path to the file containing the jobscript file.
        """
        return self.submission.path / self.jobscript_name

    @property
    def direct_stdout_path(self) -> Path:
        """
        The path to the file containing the stdout from directly executed commands
        for this jobscript.
        """
        return self.submission.path / self.direct_stdout_file_name

    @property
    def direct_stderr_path(self) -> Path:
        """
        The path to the file containing the stderr from directly executed commands
        for this jobscript.
        """
        return self.submission.path / self.direct_stderr_file_name

    @property
    def direct_win_pid_file_path(self) -> Path:
        """
        The path to the file containing PIDs for directly executed commands for this
        jobscript. Windows only.
        """
        return self.submission.path / self.direct_win_pid_file_name

    def _set_submit_time(self, submit_time: datetime) -> None:
        self._submit_time = submit_time
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            submit_time=submit_time.strftime(self.workflow.ts_fmt),
        )

    def _set_submit_hostname(self, submit_hostname: str) -> None:
        self._submit_hostname = submit_hostname
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            submit_hostname=submit_hostname,
        )

    def _set_submit_machine(self, submit_machine: str) -> None:
        self._submit_machine = submit_machine
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            submit_machine=submit_machine,
        )

    def _set_submit_cmdline(self, submit_cmdline: list[str]) -> None:
        self._submit_cmdline = submit_cmdline
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            submit_cmdline=submit_cmdline,
        )

    def _set_scheduler_job_ID(self, job_ID: str) -> None:
        """For scheduled submission only."""
        self._scheduler_job_ID = job_ID
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            scheduler_job_ID=job_ID,
        )

    def _set_process_ID(self, process_ID: int) -> None:
        """For direct submission only."""
        self._process_ID = process_ID
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            process_ID=process_ID,
        )

    def _set_version_info(self, version_info: VersionInfo) -> None:
        self._version_info = version_info
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            version_info=version_info,
        )

    def _set_os_name(self) -> None:
        """Set the OS name for this jobscript. This is invoked at submit-time."""
        self._os_name = self.resources.os_name
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            os_name=self._os_name,
        )

    def _set_shell_name(self) -> None:
        """Set the shell name for this jobscript. This is invoked at submit-time."""
        self._shell_name = self.resources.shell
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            shell_name=self._shell_name,
        )

    def _set_scheduler_name(self) -> None:
        """Set the scheduler name for this jobscript. This is invoked at submit-time."""
        self._scheduler_name = self.resources.scheduler
        if self._scheduler_name:
            self.workflow._store.set_jobscript_metadata(
                sub_idx=self.submission.index,
                js_idx=self.index,
                scheduler_name=self._scheduler_name,
            )

    def get_task_loop_idx_array(self) -> NDArray:
        """
        Get an array of task loop indices.
        """
        loop_idx = np.empty_like(self.EAR_ID)
        loop_idx[:] = np.array([i[2] for i in self.task_actions]).reshape(
            (len(self.task_actions), 1)
        )
        return loop_idx

    @TimeIt.decorator
    def write_EAR_ID_file(self):
        """Write a text file with `num_elements` lines and `num_actions` delimited tokens
        per line, representing whether a given EAR must be executed."""

        with self.EAR_ID_file_path.open(mode="wt", newline="\n") as fp:
            # can't specify "open" newline if we pass the file name only, so pass handle:
            np.savetxt(
                fname=fp,
                X=(self.EAR_ID).T,
                fmt="%.0f",
                delimiter=self._EAR_files_delimiter,
            )

    @TimeIt.decorator
    def write_element_run_dir_file(self, run_dirs: list[list[Path]]):
        """Write a text file with `num_elements` lines and `num_actions` delimited tokens
        per line, representing the working directory for each EAR.

        We assume a given task element's actions all run in the same directory, but in
        general a jobscript "element" may cross task boundaries, so we need to provide
        the directory for each jobscript-element/jobscript-action combination.

        """
        run_dirs_paths = self.shell.prepare_element_run_dirs(run_dirs)
        with self.element_run_dir_file_path.open(mode="wt", newline="\n") as fp:
            # can't specify "open" newline if we pass the file name only, so pass handle:
            np.savetxt(
                fname=fp,
                X=np.array(run_dirs_paths),
                fmt="%s",
                delimiter=self._EAR_files_delimiter,
            )

    @TimeIt.decorator
    def compose_jobscript(
        self,
        deps: dict[int, tuple[str, bool]] | None = None,
        os_name: str | None = None,
        shell_name: str | None = None,
        os_args: dict[str, Any] | None = None,
        shell_args: dict[str, Any] | None = None,
        scheduler_name: str | None = None,
        scheduler_args: dict[str, Any] | None = None,
    ) -> str:
        """Prepare the jobscript file string."""

        os_name = os_name or self.os_name
        shell_name = shell_name or self.shell_name
        scheduler_name = scheduler_name or self.scheduler_name

        if not os_name:
            raise RuntimeError(
                f"Jobscript {self.index} `os_name` is not yet set. Pass the `os_name` as "
                f"a method argument to compose the jobscript for a given `os_name`."
            )
        if not shell_name:
            raise RuntimeError(
                f"Jobscript {self.index} `shell_name` is not yet set. Pass the "
                f"`shell_name` as a method argument to compose the jobscript for a given "
                f"`shell_name`."
            )
        if not scheduler_name:
            scheduler_name = self._app.config.default_scheduler

        shell = self._get_shell(
            os_name=os_name,
            shell_name=shell_name,
            os_args=os_args or self._get_submission_os_args(),
            shell_args=shell_args or self._get_submission_shell_args(),
        )
        scheduler = self._app.get_scheduler(
            scheduler_name=scheduler_name,
            os_name=os_name,
            scheduler_args=scheduler_args or self._get_submission_scheduler_args(),
        )

        cfg_invocation = self._app.config._file.get_invocation(
            self._app.config._config_key
        )
        if env_setup := cfg_invocation["environment_setup"]:
            env_setup = indent(env_setup.strip(), shell.JS_ENV_SETUP_INDENT)
            env_setup += "\n\n" + shell.JS_ENV_SETUP_INDENT
        else:
            env_setup = shell.JS_ENV_SETUP_INDENT

        header_args = shell.process_JS_header_args(
            {
                "workflow_app_alias": self.workflow_app_alias,
                "env_setup": env_setup,
                "app_invoc": list(self._app.run_time_info.invocation_command),
                "run_log_file": self._app.RunDirAppFiles.get_log_file_name(),
                "config_dir": str(self._app.config.config_directory),
                "config_invoc_key": self._app.config.config_key,
                "workflow_path": self.workflow.path,
                "sub_idx": self.submission.index,
                "js_idx": self.index,
                "EAR_file_name": self.EAR_ID_file_name,
                "element_run_dirs_file_path": self.element_run_dir_file_name,
            }
        )

        shebang = shell.JS_SHEBANG.format(
            shebang_executable=" ".join(shell.shebang_executable),
            shebang_args=scheduler.shebang_args,
        )
        header = shell.JS_HEADER.format(**header_args)

        if self.__is_QueuedScheduler(scheduler):
            header = shell.JS_SCHEDULER_HEADER.format(
                shebang=shebang,
                scheduler_options=scheduler.format_options(
                    resources=self.resources,
                    num_elements=self.num_elements,
                    is_array=self.is_array,
                    sub_idx=self.submission.index,
                ),
                header=header,
            )
        else:
            # the Scheduler (direct submission)
            assert isinstance(scheduler, DirectScheduler)
            wait_cmd = shell.get_wait_command(
                workflow_app_alias=self.workflow_app_alias,
                sub_idx=self.submission.index,
                deps=deps or {},
            )
            header = shell.JS_DIRECT_HEADER.format(
                shebang=shebang,
                header=header,
                workflow_app_alias=self.workflow_app_alias,
                wait_command=wait_cmd,
            )

        main = shell.JS_MAIN.format(
            num_actions=self.num_actions,
            EAR_files_delimiter=self._EAR_files_delimiter,
            workflow_app_alias=self.workflow_app_alias,
            commands_file_name=self.get_commands_file_name(r"${JS_act_idx}", shell=shell),
            run_stream_file=self._app.RunDirAppFiles.get_std_file_name(),
        )

        out = header

        if self.is_array:
            if not self.__is_QueuedScheduler(scheduler):
                raise Exception("can only schedule arrays of jobs to a queue")
            out += shell.JS_ELEMENT_ARRAY.format(
                scheduler_command=scheduler.js_cmd,
                scheduler_array_switch=scheduler.array_switch,
                scheduler_array_item_var=scheduler.array_item_var,
                num_elements=self.num_elements,
                main=main,
            )

        else:
            out += shell.JS_ELEMENT_LOOP.format(
                num_elements=self.num_elements,
                main=indent(main, shell.JS_INDENT),
            )

        return out

    @TimeIt.decorator
    def write_jobscript(
        self,
        os_name: str | None = None,
        shell_name: str | None = None,
        deps: dict[int, tuple[str, bool]] | None = None,
        os_args: dict[str, Any] | None = None,
        shell_args: dict[str, Any] | None = None,
        scheduler_name: str | None = None,
        scheduler_args: dict[str, Any] | None = None,
    ) -> Path:
        """
        Write the jobscript to its file.
        """
        js_str = self.compose_jobscript(
            deps=deps,
            os_name=os_name,
            shell_name=shell_name,
            os_args=os_args,
            shell_args=shell_args,
            scheduler_name=scheduler_name,
            scheduler_args=scheduler_args,
        )
        with self.jobscript_path.open("wt", newline="\n") as fp:
            fp.write(js_str)
        return self.jobscript_path

    @TimeIt.decorator
    def make_artifact_dirs(self) -> list[list[Path]]:
        """
        Create the directories that will hold artifacts associated with this jobscript.
        """
        EARs_arr = np.array(self.all_EARs).reshape(self.EAR_ID.shape)
        task_loop_idx_arr = self.get_task_loop_idx_array()

        return [
            [
                self.__make_action_dir(
                    EARs_arr[js_act_idx, js_elem_idx],
                    task_loop_idx_arr[js_act_idx, js_elem_idx].item(),
                    js_act_idx,
                    js_elem_idx,
                )
                for js_act_idx in range(self.num_actions)
            ]
            for js_elem_idx in range(self.num_elements)
        ]

    def __make_action_dir(
        self, EAR_i: ElementActionRun, l_idx: int, js_act_idx: int, js_elem_idx: int
    ) -> Path:
        t_iID = EAR_i.task.insert_ID
        r_idx = EAR_i.index
        loop_idx_i = self.task_loop_idx[l_idx]
        task_dir = self.workflow.tasks.get(insert_ID=t_iID).get_dir_name(loop_idx_i)
        elem_dir = EAR_i.element.dir_name

        EAR_dir = self.workflow.execution_path / task_dir / elem_dir / f"r_{r_idx}"
        EAR_dir.mkdir(exist_ok=True, parents=True)

        # copy (TODO: optionally symlink) any input files:
        for path in cast("dict[Any, str]", EAR_i.get("input_files", {})).values():
            if path:
                shutil.copy(path, EAR_dir)
        return EAR_dir.relative_to(self.workflow.path)

    @TimeIt.decorator
    def _launch_direct_js_win(self) -> int:
        # this is a "trick" to ensure we always get a fully detached new process (with no
        # parent); the `powershell.exe -Command` process exits after running the inner
        # `Start-Process`, which is where the jobscript is actually invoked. I could not
        # find a way using `subprocess.Popen()` to ensure the new process was fully
        # detached when submitting jobscripts via a Jupyter notebook in Windows.

        assert self.submit_cmdline is not None
        # Note we need powershell.exe for this "launcher process", but the shell used for
        # the jobscript itself need not be powershell.exe
        exe_path, arg_list = self.submit_cmdline[0], self.submit_cmdline[1:]

        # note powershell-escaped quotes, in case of spaces in arguments (this seems to
        # work okay even though we might have switch like arguments in this list, like
        # "-File"):
        arg_list_str = ",".join(f'"`"{i}`""' for i in arg_list)

        args = [
            "powershell.exe",
            "-Command",
            f"$JS_proc = Start-Process "
            f'-Passthru -NoNewWindow -FilePath "{exe_path}" '
            f'-RedirectStandardOutput "{self.direct_stdout_path}" '
            f'-RedirectStandardError "{self.direct_stderr_path}" '
            f'-WorkingDirectory "{self.workflow.path}" '
            f"-ArgumentList {arg_list_str}; "
            f'Set-Content -Path "{self.direct_win_pid_file_path}" -Value $JS_proc.Id',
        ]

        self._app.submission_logger.info(
            f"running direct Windows jobscript launcher process: {args!r}"
        )
        # for some reason we still need to create a "detached" process here as well:
        init_proc = subprocess.Popen(
            args=args,
            cwd=self.workflow.path,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        init_proc.wait()  # wait for the process ID file to be written
        return int(self.direct_win_pid_file_path.read_text())

    @TimeIt.decorator
    def _launch_direct_js_posix(self) -> int:
        # direct submission; submit jobscript asynchronously:
        # detached process, avoid interrupt signals propagating to the subprocess:
        assert self.submit_cmdline is not None
        with self.direct_stdout_path.open(
            "wt"
        ) as fp_stdout, self.direct_stderr_path.open("wt") as fp_stderr:
            # note: Popen copies the file objects, so this works!
            proc = subprocess.Popen(
                args=self.submit_cmdline,
                stdout=fp_stdout,
                stderr=fp_stderr,
                cwd=self.workflow.path,
                start_new_session=True,
            )
            return proc.pid

    @TimeIt.decorator
    def _launch_queued(
        self, submit_cmd: list[str], print_stdout: bool
    ) -> tuple[str, str]:
        # scheduled submission, wait for submission so we can parse the job ID:
        proc = subprocess.run(
            args=submit_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=self.workflow.path,
        )
        stdout = proc.stdout.decode().strip()
        stderr = proc.stderr.decode().strip()
        if print_stdout and stdout:
            print(stdout)
        if stderr:
            print(stderr)
        return stdout, stderr

    @TimeIt.decorator
    def submit(
        self,
        scheduler_refs: dict[int, tuple[str, bool]],
        print_stdout: bool = False,
    ) -> str:
        """
        Submit the jobscript to the scheduler.
        """
        # map each dependency jobscript index to the JS ref (job/process ID) and if the
        # dependency is an array dependency:
        deps: dict[int, tuple[str, bool]] = {}
        for js_idx, deps_i in self.dependencies.items():
            dep_js_ref, dep_js_is_arr = scheduler_refs[js_idx]
            # only submit an array dependency if both this jobscript and the dependency
            # are array jobs:
            dep_is_arr = deps_i["is_array"] and self.is_array and dep_js_is_arr
            deps[js_idx] = (dep_js_ref, dep_is_arr)

        if not self.submission.JS_parallelism and self.index > 0:
            # add fake dependencies to all previously submitted jobscripts to avoid
            # simultaneous execution:
            for js_idx, (js_ref, _) in scheduler_refs.items():
                if js_idx not in deps:
                    deps[js_idx] = (js_ref, False)

        run_dirs = self.make_artifact_dirs()
        self.write_EAR_ID_file()
        self.write_element_run_dir_file(run_dirs)
        js_path = self.shell.prepare_JS_path(self.write_jobscript(deps=deps))
        submit_cmd = self.scheduler.get_submit_command(self.shell, js_path, deps)
        self._app.submission_logger.info(
            f"submitting jobscript {self.index!r} with command: {submit_cmd!r}"
        )
        self._set_submit_cmdline(submit_cmd)
        self._set_submit_hostname(socket.gethostname())
        self._set_submit_machine(self._app.config.get("machine"))

        err_args: JobscriptSubmissionFailureArgs = {
            "submit_cmd": submit_cmd,
            "js_idx": self.index,
            "js_path": js_path,
        }
        job_ID: str | None = None
        process_ID: int | None = None
        try:
            if self.__is_QueuedScheduler(self.scheduler):
                # scheduled submission, wait for submission so we can parse the job ID:
                stdout, stderr = self._launch_queued(submit_cmd, print_stdout)
                err_args["stdout"] = stdout
                err_args["stderr"] = stderr
            else:
                if os.name == "nt":
                    process_ID = self._launch_direct_js_win()
                else:
                    process_ID = self._launch_direct_js_posix()
        except Exception as subprocess_exc:
            err_args["subprocess_exc"] = subprocess_exc
            raise JobscriptSubmissionFailure(
                "Failed to execute submit command.", **err_args
            )

        if self.__is_QueuedScheduler(self.scheduler):
            # scheduled submission
            if stderr:
                raise JobscriptSubmissionFailure(
                    "Non-empty stderr from submit command.", **err_args
                )

            try:
                job_ID = self.scheduler.parse_submission_output(stdout)
                assert job_ID is not None
            except Exception as job_ID_parse_exc:
                # TODO: maybe handle this differently. If there is no stderr, then the job
                # probably did submit fine, but the issue is just with parsing the job ID
                # (e.g. if the scheduler version was updated and it now outputs
                # differently).
                err_args["job_ID_parse_exc"] = job_ID_parse_exc
                raise JobscriptSubmissionFailure(
                    "Failed to parse job ID from stdout.", **err_args
                )

            self._set_scheduler_job_ID(job_ID)
            ref = job_ID

        else:
            # direct submission
            assert process_ID is not None
            self._set_process_ID(process_ID)
            # a downstream direct jobscript might need to wait for this jobscript, which
            # means this jobscript's process ID must be committed:
            self.workflow._store._pending.commit_all()
            ref = f"{process_ID}"

        self._set_submit_time(current_timestamp())

        return ref

    @property
    def is_submitted(self) -> bool:
        """Whether this jobscript has been submitted."""
        return self.index in self.submission.submitted_jobscripts

    @property
    def scheduler_js_ref(self):
        """
        The reference to the submitted job for the jobscript.
        """
        if self.__is_QueuedScheduler(self.scheduler):
            return self.scheduler_job_ID
        else:
            return (self.process_ID, self.submit_cmdline)

    @property
    def scheduler_ref(self) -> SchedulerRef:
        """
        The generalised scheduler reference descriptor.
        """
        return {"js_refs": [self.scheduler_js_ref], "num_js_elements": self.num_elements}

    @overload
    def get_active_states(
        self, as_json: Literal[False] = False
    ) -> Mapping[int, JobscriptElementState]:
        ...

    @overload
    def get_active_states(self, as_json: Literal[True]) -> dict[int, str]:
        ...

    @TimeIt.decorator
    def get_active_states(
        self, as_json: bool = False
    ) -> Mapping[int, JobscriptElementState] | dict[int, str]:
        """If this jobscript is active on this machine, return the state information from
        the scheduler."""

        out: dict[int, JobscriptElementState] = {}

        if self.is_submitted:
            self._app.submission_logger.debug(
                "checking if the jobscript is running according to EAR submission "
                "states."
            )

            not_run_states = EARStatus.get_non_running_submitted_states()
            all_EAR_states = set(ear.status for ear in self.all_EARs)
            self._app.submission_logger.debug(
                f"Unique EAR states are: {all_EAR_states!r}"
            )
            if all_EAR_states.issubset(not_run_states):
                self._app.submission_logger.debug(
                    "All jobscript EARs are in a non-running state"
                )

            elif self._app.config.get("machine") == self.submit_machine:
                self._app.submission_logger.debug(
                    "Checking if jobscript is running according to the scheduler/process "
                    "ID."
                )
                out_d = self.scheduler.get_job_state_info(**self.scheduler_ref)
                if out_d:
                    out_i = out_d[next(iter(out_d))]  # first item only
                    # if value is single-length dict with `None` key, then transform
                    # to one key for each jobscript element:
                    if tuple(out_i) == (None,):
                        out = {idx: out_i[None] for idx in range(self.num_elements)}
                    else:
                        out = cast("Any", out_i)

            else:
                raise NotSubmitMachineError()

        self._app.submission_logger.info(f"Jobscript is {'in' if not out else ''}active.")
        if as_json:
            return {idx: state.name for idx, state in out.items()}
        return out

    def cancel(self) -> None:
        """
        Cancel this jobscript.
        """
        self._app.submission_logger.info(
            f"Cancelling jobscript {self.index} of submission {self.submission.index}"
        )
        self.scheduler.cancel_jobs(**self.scheduler_ref, jobscripts=[self])
