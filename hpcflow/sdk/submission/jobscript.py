"""
Model of information submitted to a scheduler.
"""

from __future__ import annotations
from collections import defaultdict

import os
import shutil
import socket
import subprocess
from textwrap import dedent, indent
from typing import TextIO, cast, overload, TYPE_CHECKING
from typing_extensions import override

import numpy as np
from hpcflow.sdk.core import SKIPPED_EXIT_CODE
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.errors import (
    JobscriptSubmissionFailure,
    NotSubmitMachineError,
)

from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.utils import nth_value, parse_timestamp, current_timestamp
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.submission.schedulers import QueuedScheduler
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
    from ..core.types import JobscriptSubmissionFailureArgs, BlockActionKey
    from ..core.workflow import WorkflowTask, Workflow
    from ..persistence.base import PersistentStore
    from .submission import Submission
    from .shells.base import Shell
    from .schedulers import Scheduler
    from .enums import JobscriptElementState
    from .types import (
        JobScriptCreationArguments,
        JobScriptDescriptor,
        ResolvedJobscriptBlockDependencies,
        SchedulerRef,
        VersionInfo,
    )
    from ..core.cache import ObjectCache
from hpcflow.sdk.submission.submission import JOBSCRIPT_SUBMIT_TIME_KEYS


def is_jobscript_array(
    resources: ElementResources, num_elements: int, store: PersistentStore
) -> bool:
    """Return True if a job array should be used for the specified `ElementResources`."""
    if resources.scheduler in ("direct", "direct_posix"):
        if resources.use_job_array:
            raise ValueError(
                f"`use_job_array` not supported by scheduler: {resources.scheduler!r}"
            )
        return False

    if resources.combine_scripts:
        return False

    run_parallelism = store._features.EAR_parallelism
    if resources.use_job_array is None:
        if num_elements > 1 and run_parallelism:
            return True
        else:
            return False
    else:
        if resources.use_job_array and not run_parallelism:
            raise ValueError(
                f"Store type {store!r} does not support element parallelism, so jobs "
                f"cannot be submitted as scheduler arrays."
            )
        return resources.use_job_array


@TimeIt.decorator
def generate_EAR_resource_map(
    task: WorkflowTask,
    loop_idx: LoopIndex[str, int],
    cache: ObjectCache,
) -> tuple[Sequence[ElementResources], Sequence[int], NDArray, NDArray]:
    """
    Generate an integer array whose rows represent actions and columns represent task
    elements and whose values index unique resources.
    """
    none_val = -1
    resources: list[ElementResources] = []
    resource_hashes: list[int] = []

    arr_shape = (task.num_actions, task.num_elements)
    resource_map = np.empty(arr_shape, dtype=int)
    EAR_ID_map = np.empty(arr_shape, dtype=int)
    resource_map[:] = none_val
    EAR_ID_map[:] = none_val

    assert cache.elements is not None
    assert cache.iterations is not None

    for elem_id in task.element_IDs:
        element = cache.elements[elem_id]
        for iter_ID_i in element.iteration_IDs:
            iter_i = cache.iterations[iter_ID_i]
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
) -> Mapping[int, dict[int, ResolvedJobscriptBlockDependencies]]:
    """
    Discover concrete dependencies between jobscripts.
    """
    # first pass is to find the mappings between jobscript elements:
    jobscript_deps: dict[int, dict[int, ResolvedJobscriptBlockDependencies]] = {}
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
    jobscripts: Mapping[int, JobScriptCreationArguments],
    from_idx: int,
    to_idx: int,
):
    for ds_js_idx, ds_js in jobscripts.items():
        if ds_js_idx <= from_idx:
            continue
        deps = ds_js["dependencies"]
        if from_idx in deps:
            deps[to_idx] = deps.pop(from_idx)


@TimeIt.decorator
def merge_jobscripts_across_tasks(
    jobscripts: Mapping[int, JobScriptCreationArguments],
) -> Mapping[int, JobScriptCreationArguments]:
    """Try to merge jobscripts between tasks.

    This is possible if two jobscripts share the same resources and have an array
    dependency (i.e. one-to-one element dependency mapping).

    """

    # The set of IDs of dicts that we've merged, allowing us to not keep that info in
    # the dicts themselves.
    merged: set[int] = set()

    for js_idx, js in jobscripts.items():
        if not js["dependencies"]:
            continue

        closest_idx = cast("int", max(js["dependencies"]))
        closest_js = jobscripts[closest_idx]
        other_deps = {k: v for k, v in js["dependencies"].items() if k != closest_idx}

        # if all `other_deps` are also found within `closest_js`'s dependencies, then we
        # can merge `js` into `closest_js`:
        merge = True
        for dep_idx, dep_i in other_deps.items():
            try:
                if closest_js["dependencies"][dep_idx] != dep_i:
                    merge = False
            except KeyError:
                merge = False

        if merge:
            js_j = closest_js  # the jobscript we are merging `js` into
            js_j_idx = closest_idx
            dep_info = js["dependencies"][js_j_idx]

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
def resolve_jobscript_blocks(
    jobscripts: Mapping[int, JobScriptCreationArguments],
) -> list[dict[str, Any]]:
    """For contiguous, dependent, non-array jobscripts with identical resource
    requirements, combine into multi-block jobscripts.

    Parameters
    ----------
    jobscripts
        Dict whose values must be dicts with keys "is_array", "resource_hash" and
        "dependencies".
    run_parallelism
        True if the store supports run parallelism

    """
    js_new: list[list[JobScriptCreationArguments]] = (
        []
    )  # TODO: not the same type, e.g. dependencies have tuple keys,
    new_idx: dict[int, tuple[int, int]] = (
        {}
    )  # track new positions by new jobscript index and block index
    new_idx_inv: dict[int, list[int]] = defaultdict(list)
    prev_hash = None
    blocks: list[JobScriptCreationArguments] = []
    js_deps_rec: dict[int, set[int]] = {}  # recursive
    for js_idx, js_i in jobscripts.items():

        cur_js_idx = len(js_new)
        new_deps_js_j = {
            new_idx[i][0] for i in cast("Sequence[int]", js_i["dependencies"])
        }
        new_deps_js_j_rec = [
            k for i in new_deps_js_j for j in new_idx_inv[i] for k in js_deps_rec[j]
        ]

        js_deps_rec[js_idx] = new_deps_js_j.union(new_deps_js_j_rec)

        # recursive dependencies of js_i (which we're looking to merge), excluding the
        # dependency on the current jobscript:
        js_j_deps_rec_no_cur = js_deps_rec[js_idx] - set([cur_js_idx])

        # recursive dependencies of the current jobscript:
        cur_deps_rec = {
            j for i in new_idx_inv[cur_js_idx] for j in js_deps_rec[i] if j != cur_js_idx
        }

        # can we mege js_i into the current jobscript, as far as dependencies are
        # concerned?
        deps_mergable = cur_js_idx in new_deps_js_j
        if deps_mergable and js_j_deps_rec_no_cur:
            deps_mergable = js_j_deps_rec_no_cur == cur_deps_rec

        if js_i["is_array"]:
            # array jobs cannot be merged into the same jobscript

            # append existing block:
            if blocks:
                js_new.append(blocks)
                prev_hash = None
                blocks = []

            new_idx[js_idx] = (len(js_new), 0)
            new_idx_inv[len(js_new)].append(js_idx)
            js_new.append([js_i])
            continue

        if js_idx == 0 or prev_hash is None:
            # (note: zeroth index will always exist)

            # start a new block:
            blocks.append(js_i)
            new_idx[js_idx] = (len(js_new), len(blocks) - 1)
            new_idx_inv[len(js_new)].append(js_idx)

            # set resource hash to compare with the next jobscript
            prev_hash = js_i["resource_hash"]

        elif js_i["resource_hash"] == prev_hash and deps_mergable:
            # merge with previous jobscript by adding another block
            # only merge if this jobscript's dependencies include the current jobscript,
            # and any other dependencies are included in the current jobscript's
            # dependencies
            blocks.append(js_i)
            new_idx[js_idx] = (len(js_new), len(blocks) - 1)
            new_idx_inv[len(js_new)].append(js_idx)

        else:
            # cannot merge, append the new jobscript data:
            js_new.append(blocks)

            # start a new block:
            blocks = [js_i]
            new_idx[js_idx] = (len(js_new), len(blocks) - 1)
            new_idx_inv[len(js_new)].append(js_idx)

            # set resource hash to compare with the next jobscript
            prev_hash = js_i["resource_hash"]

    # append remaining blocks:
    if blocks:
        js_new.append(blocks)
        prev_hash = None
        blocks = []

    # re-index dependencies:
    js_new_: list[dict[str, Any]] = []
    for js_i_idx, js_new_i in enumerate(js_new):

        resources = None
        is_array = None
        for block_j in js_new_i:
            for k, v in new_idx.items():
                dep_data = block_j["dependencies"].pop(k, None)
                if dep_data:
                    block_j["dependencies"][v] = dep_data

            del block_j["resource_hash"]
            resources = block_j.pop("resources", None)
            is_array = block_j.pop("is_array")

        js_new_.append(
            {
                "resources": resources,
                "is_array": is_array,
                "blocks": js_new[js_i_idx],
            }
        )

    return js_new_


@hydrate
class JobscriptBlock(JSONLike):
    """A rectangular block of element-actions to run within a jobscript.

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
    task_loop_idx: list[dict]
        Description of what loops are in play.
    dependencies: dict[tuple[int, int], dict]
        Description of dependencies. Keys are tuples of (jobscript index,
        jobscript-block index) of the dependency.
    index: int
        The index of the block within the parent jobscript.
    jobscript: ~hpcflow.app.Jobscript
        The parent jobscript.

    """

    def __init__(
        self,
        index: int,
        task_insert_IDs: list[int],
        task_loop_idx: list[dict[str, int]],
        task_actions: list[tuple[int, int, int]] | None = None,
        task_elements: dict[int, list[int]] | None = None,
        EAR_ID: NDArray | None = None,
        dependencies: (
            dict[tuple[int, int], ResolvedJobscriptBlockDependencies] | None
        ) = None,
        jobscript: Jobscript | None = None,
    ):
        self.jobscript = jobscript
        self._index = index
        self._task_insert_IDs = task_insert_IDs
        self._task_actions = task_actions
        self._task_elements = task_elements
        self._task_loop_idx = task_loop_idx
        self._EAR_ID = EAR_ID
        self._dependencies = dependencies

        self._all_EARs = None  # assigned on first access to `all_EARs` property

    @property
    def index(self) -> int:
        return self._index

    @property
    def submission(self) -> Submission:
        assert self.jobscript is not None
        return self.jobscript.submission

    @property
    def task_insert_IDs(self) -> Sequence[int]:
        """
        The insertion IDs of tasks in this jobscript-block.
        """
        return self._task_insert_IDs

    @property
    @TimeIt.decorator
    def task_actions(self) -> NDArray:
        """
        The IDs of actions of each task in this jobscript-block.
        """
        assert self.jobscript is not None
        return self.workflow._store.get_jobscript_block_task_actions_array(
            sub_idx=self.submission.index,
            js_idx=self.jobscript.index,
            blk_idx=self.index,
            task_actions_arr=self._task_actions,
        )

    @property
    @TimeIt.decorator
    def task_elements(self) -> Mapping[int, Sequence[int]]:
        """
        The IDs of elements of each task in this jobscript-block.
        """
        assert self.jobscript is not None
        return self.workflow._store.get_jobscript_block_task_elements_map(
            sub_idx=self.submission.index,
            js_idx=self.jobscript.index,
            blk_idx=self.index,
            task_elems_map=self._task_elements,
        )

    @property
    @TimeIt.decorator
    def EAR_ID(self) -> NDArray:
        """
        The array of EAR IDs in this jobscript-block.
        """
        assert self.jobscript is not None
        return self.workflow._store.get_jobscript_block_run_ID_array(
            sub_idx=self.submission.index,
            js_idx=self.jobscript.index,
            blk_idx=self.index,
            run_ID_arr=self._EAR_ID,
        )

    @property
    @TimeIt.decorator
    def dependencies(
        self,
    ) -> Mapping[tuple[int, int], ResolvedJobscriptBlockDependencies]:
        """
        The dependency descriptor.
        """
        assert self.jobscript is not None
        return self.workflow._store.get_jobscript_block_dependencies(
            sub_idx=self.submission.index,
            js_idx=self.jobscript.index,
            blk_idx=self.index,
            js_dependencies=self._dependencies,
        )

    @property
    def task_loop_idx(self) -> Sequence[Mapping[str, int]]:
        """
        The description of where various task loops are.
        """
        return self._task_loop_idx

    @property
    @TimeIt.decorator
    def num_actions(self) -> int:
        """
        The maximal number of actions in the jobscript-block.
        """
        return self.EAR_ID.shape[0]

    @property
    @TimeIt.decorator
    def num_elements(self) -> int:
        """
        The maximal number of elements in the jobscript-block.
        """
        return self.EAR_ID.shape[1]

    @property
    def workflow(self) -> Workflow:
        """
        The associated workflow.
        """
        assert self.jobscript is not None
        return self.jobscript.workflow

    @property
    @TimeIt.decorator
    def all_EARs(self) -> Sequence[ElementActionRun]:
        """
        Description of EAR information for this jobscript-block.
        """
        assert self.jobscript is not None
        return [i for i in self.jobscript.all_EARs if i.id_ in self.EAR_ID]

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        dct = super()._postprocess_to_dict(d)
        del dct["_all_EARs"]
        dct["_dependencies"] = [[list(k), v] for k, v in self.dependencies.items()]
        dct = {k.lstrip("_"): v for k, v in dct.items()}
        dct["EAR_ID"] = cast("NDArray", dct["EAR_ID"]).tolist()
        return dct

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        json_like["EAR_ID"] = (
            np.array(json_like["EAR_ID"]) if json_like["EAR_ID"] is not None else None
        )
        if json_like["dependencies"] is not None:
            # transform list to dict with tuple keys, and transform string keys in
            # `js_element_mapping` to integers:
            deps_processed = {}
            for i in json_like["dependencies"]:
                deps_processed_i = {
                    "js_element_mapping": {
                        int(k): v for k, v in i[1]["js_element_mapping"].items()
                    },
                    "is_array": i[1]["is_array"],
                }
                deps_processed[tuple(i[0])] = deps_processed_i
            json_like["dependencies"] = deps_processed

        return super().from_json_like(json_like, shared_data)

    def _get_EARs_arr(self) -> NDArray:
        """
        Get all associated EAR objects as a 2D array.
        """
        return np.array(self.all_EARs).reshape(self.EAR_ID.shape)

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
    def write_EAR_ID_file(self, fp: TextIO):
        """Write a text file with `num_elements` lines and `num_actions` delimited tokens
        per line, representing whether a given EAR must be executed."""
        assert self.jobscript is not None
        # can't specify "open" newline if we pass the file name only, so pass handle:
        np.savetxt(
            fname=fp,
            X=(self.EAR_ID).T,
            fmt="%.0f",
            delimiter=self.jobscript._EAR_files_delimiter,
        )


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
        ChildObjectSpec(
            name="blocks",
            class_name="JobscriptBlock",
            is_multiple=True,
            parent_ref="jobscript",
        ),
    )

    def __init__(
        self,
        index: int,
        is_array: bool,
        resources: ElementResources,
        blocks: list[JobscriptBlock],
        at_submit_metadata: dict[str, Any] | None = None,
        submit_hostname: str | None = None,
        submit_machine: str | None = None,
        shell_idx: int | None = None,
        version_info: VersionInfo | None = None,
        resource_hash: str | None = None,
        elements: dict[int, list[int]] | None = None,
    ):
        if resource_hash is not None:
            raise AttributeError("resource_hash must not be supplied")
        if elements is not None:
            raise AttributeError("elements must not be supplied")

        if not isinstance(blocks[0], JobscriptBlock):
            blocks = [
                JobscriptBlock(**i, index=idx, jobscript=self)
                for idx, i in enumerate(blocks)
            ]

        self._index = index
        self._blocks = blocks
        self._at_submit_metadata = at_submit_metadata or {
            k: None for k in JOBSCRIPT_SUBMIT_TIME_KEYS
        }
        self._is_array = is_array
        self._resources = resources

        # assigned on parent `Submission.submit` (or retrieved form persistent store):
        self._submit_hostname = submit_hostname
        self._submit_machine = submit_machine
        self._shell_idx = shell_idx

        self._version_info = version_info

        # assigned by parent Submission
        self._submission: Submission | None = None
        # assigned on first access to `scheduler` property
        self._scheduler_obj: Scheduler | None = None
        # assigned on first access to `shell` property
        self._shell_obj: Shell | None = None
        # assigned on first access to `submit_time` property
        self._submit_time_obj: datetime | None = None
        # assigned on first access to `all_EARs` property
        self._all_EARs: list[ElementActionRun] | None = None

        self._set_parent_refs()

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"index={self.index!r}, "
            f"blocks={self.blocks!r}, "
            f"resources={self.resources!r}, "
            f")"
        )

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        dct = super()._postprocess_to_dict(d)
        del dct["_scheduler_obj"]
        del dct["_shell_obj"]
        del dct["_submit_time_obj"]
        del dct["_all_EARs"]
        dct = {k.lstrip("_"): v for k, v in dct.items()}
        return dct

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        return super().from_json_like(json_like, shared_data)

    @property
    def workflow_app_alias(self) -> str:
        """
        Alias for the workflow app in job scripts.
        """
        return self.submission.WORKFLOW_APP_ALIAS

    def get_commands_file_name(
        self, block_act_key: BlockActionKey, shell: Shell | None = None
    ) -> str:
        """
        Get the name of a file containing commands for a particular jobscript action.
        """
        return self._app.RunDirAppFiles.get_commands_file_name(
            block_act_key,
            shell=shell or self.shell,
        )

    @property
    def blocks(self) -> Sequence[JobscriptBlock]:
        return self._blocks

    @property
    def at_submit_metadata(self) -> dict[str, Any]:
        return self.workflow._store.get_jobscript_at_submit_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            metadata_attr=self._at_submit_metadata,
        )

    @property
    def all_EAR_IDs(self) -> NDArray:
        """Return all run IDs of this jobscripts (across all blocks), removing missing
        run IDs (i.e. -1 values)"""
        return np.concatenate([i.EAR_ID[i.EAR_ID >= 0] for i in self.blocks])

    @property
    @TimeIt.decorator
    def all_EARs(self) -> Sequence[ElementActionRun]:
        """
        Description of EAR information for this jobscript.
        """
        return self.workflow.get_EARs_from_IDs(self.all_EAR_IDs)

    @property
    @TimeIt.decorator
    def resources(self) -> ElementResources:
        """
        The common resources that this jobscript requires.
        """
        return self._resources

    @property
    @TimeIt.decorator
    def dependencies(self) -> Mapping[tuple[int, int], dict[str, bool]]:
        """
        The dependency descriptor, accounting for all blocks within this jobscript.
        """
        deps = {}
        for block in self.blocks:
            for (js_idx, blk_idx), v in block.dependencies.items():
                if js_idx == self.index:
                    # block dependency is internal to this jobscript
                    continue
                else:
                    deps[js_idx, blk_idx] = {"is_array": v["is_array"]}
        return deps

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
    def submit_time(self):
        """
        When the jobscript was submitted, if known.
        """
        if self._submit_time_obj is None:
            if _submit_time := self.at_submit_metadata["submit_time"]:
                self._submit_time_obj = parse_timestamp(
                    _submit_time, self.workflow.ts_fmt
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
    def shell_idx(self):
        return self._shell_idx

    @property
    def submit_cmdline(self) -> list[str] | None:
        """
        The command line used to submit the jobscript, if known.
        """
        return self.at_submit_metadata["submit_cmdline"]

    @property
    def scheduler_job_ID(self) -> str | None:
        """
        The job ID from the scheduler, if known.
        """
        return self.at_submit_metadata["scheduler_job_ID"]

    @property
    def process_ID(self) -> int | None:
        """
        The process ID from direct execution, if known.
        """
        return self.at_submit_metadata["process_ID"]

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
    def is_array(self) -> bool:
        """
        Whether to generate an array job.
        """
        return self._is_array

    @property
    def os_name(self) -> str:
        """
        The name of the OS to use.
        """
        assert self.resources.os_name
        return self.resources.os_name

    @property
    def shell_name(self) -> str:
        assert self.resources.shell
        return self.resources.shell

    @property
    def scheduler_name(self) -> str:
        """
        The name of the scheduler to use.
        """
        assert self.resources.scheduler
        return self.resources.scheduler

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
    def combined_script_indices_file_name(self) -> str:
        return f"js_{self.index}_script_indices.txt"

    @property
    def direct_win_pid_file_name(self) -> str:
        """File for holding the direct execution PID."""
        return f"js_{self.index}_pid.txt"

    @property
    def jobscript_name(self) -> str:
        """The name of the jobscript file."""
        return f"js_{self.index}{self.shell.JS_EXT}"

    @property
    def jobscript_functions_name(self):
        assert self.shell_idx is not None
        return self.submission.get_jobscript_functions_name(self.shell, self.shell_idx)

    @property
    def EAR_ID_file_path(self) -> Path:
        """
        The path to the file containing EAR IDs for this jobscript.
        """
        return self.submission.js_run_ids_path / self.EAR_ID_file_name

    @property
    def combined_script_indices_file_path(self) -> Path:
        """
        The path to the file containing script indices, in the case this is a
        ``combine_scripts=True`` jobscript.
        """
        return (
            self.submission.js_script_indices_path
            / self.combined_script_indices_file_name
        )

    @property
    def jobscript_path(self) -> Path:
        """
        The path to the file containing the jobscript file.
        """
        return self.submission.js_path / self.jobscript_name

    @property
    def jobscript_functions_path(self) -> Path:
        """
        The path to the file containing the supporting shell functions."""
        assert self.shell_idx is not None
        return self.submission.get_jobscript_functions_path(self.shell, self.shell_idx)

    @property
    def std_path(self) -> Path:
        """Directory in which to store jobscript standard out and error stream files."""
        return self.submission.js_std_path / str(self.index)

    @property
    def direct_std_out_err_path(self) -> Path:
        """File path of combined standard output and error streams.

        Notes
        -----
        This path will only exist if `resources.combine_jobscript_std` is True. Otherwise,
        see `direct_stdout_path` and `direct_stderr_path` for the separate stream paths.

        """
        return self.get_std_out_err_path()

    @property
    def direct_stdout_path(self) -> Path:
        """File path to which the jobscript's standard output is saved, for direct
        execution only.

        Notes
        -----
        This returned path be the same as that from `get_stderr_path` if
        `resources.combine_jobscript_std` is True.

        """
        assert not self.is_scheduled
        return self.get_stdout_path()

    @property
    def direct_stderr_path(self) -> Path:
        """File path to which the jobscript's standard error is saved, for direct
        execution only.

        Notes
        -----
        This returned path be the same as that from `get_stdout_path` if
        `resources.combine_jobscript_std` is True.

        """
        assert not self.is_scheduled
        return self.get_stderr_path()

    def __validate_get_std_path_array_idx(self, array_idx: int | None = None):
        if array_idx is None and self.is_array:
            raise ValueError(
                "`array_idx` must be specified, since this jobscript is an array job."
            )
        elif array_idx is not None and not self.is_array:
            raise ValueError(
                "`array_idx` should not be specified, since this jobscript is not an "
                "array job."
            )

    def _get_stdout_path(self, array_idx: int | None = None) -> Path:
        """File path to the separate standard output stream.

        Notes
        -----
        This path will only exist if `resources.combine_jobscript_std` is False.
        Otherwise, see `get_std_out_err_path` for the combined stream path.

        """
        self.__validate_get_std_path_array_idx(array_idx)
        return self.std_path / self.scheduler.get_stdout_filename(
            js_idx=self.index, job_ID=self.scheduler_job_ID, array_idx=array_idx
        )

    def _get_stderr_path(self, array_idx: int | None = None) -> Path:
        """File path to the separate standard error stream.

        Notes
        -----
        This path will only exist if `resources.combine_jobscript_std` is False.
        Otherwise, see `get_std_out_err_path` for the combined stream path.

        """
        self.__validate_get_std_path_array_idx(array_idx)
        return self.std_path / self.scheduler.get_stderr_filename(
            js_idx=self.index, job_ID=self.scheduler_job_ID, array_idx=array_idx
        )

    def get_std_out_err_path(self, array_idx: int | None = None) -> Path:
        """File path of combined standard output and error streams.

        Notes
        -----
        This path will only exist if `resources.combine_jobscript_std` is True. Otherwise,
        see `get_stdout_path` and `get_stderr_path` for the separate stream paths.

        """
        self.__validate_get_std_path_array_idx(array_idx)
        return self.std_path / self.scheduler.get_std_out_err_filename(
            js_idx=self.index, job_ID=self.scheduler_job_ID, array_idx=array_idx
        )

    def get_stdout_path(self, array_idx: int | None = None) -> Path:
        """File path to which the jobscript's standard output is saved.

        Notes
        -----
        This returned path be the same as that from `get_stderr_path` if
        `resources.combine_jobscript_std` is True.

        """
        if self.resources.combine_jobscript_std:
            return self.get_std_out_err_path(array_idx=array_idx)
        else:
            return self._get_stdout_path(array_idx=array_idx)

    def get_stderr_path(self, array_idx: int | None = None) -> Path:
        """File path to which the jobscript's standard error is saved.

        Notes
        -----
        This returned path be the same as that from `get_stdout_path` if
        `resources.combine_jobscript_std` is True.

        """
        if self.resources.combine_jobscript_std:
            return self.get_std_out_err_path(array_idx=array_idx)
        else:
            return self._get_stderr_path(array_idx=array_idx)

    def get_stdout(self, array_idx: int | None = None) -> str:
        """Retrieve the contents of the standard output stream file.

        Notes
        -----
        In the case of non-array jobscripts, this will return the whole standard output,
        even if that includes multiple elements/actions.

        """
        return self.workflow.get_text_file(self.get_stdout_path(array_idx))

    def get_stderr(self, array_idx: int | None = None) -> str:
        """Retrieve the contents of the standard error stream file.

        Notes
        -----
        In the case of non-array jobscripts, this will return the whole standard error,
        even if that includes multiple elements/actions.

        """
        return self.workflow.get_text_file(self.get_stderr_path(array_idx))

    def print_stdout(self, array_idx: int | None = None) -> None:
        """Print the contents of the standard output stream file.

        Notes
        -----
        In the case of non-array jobscripts, this will print the whole standard output,
        even if that includes multiple elements/actions.

        """
        print(self.get_stdout(array_idx))

    def print_stderr(self, array_idx: int | None = None) -> None:
        """Print the contents of the standard error stream file.

        Notes
        -----
        In the case of non-array jobscripts, this will print the whole standard error,
        even if that includes multiple elements/actions.

        """
        print(self.get_stderr(array_idx))

    @property
    def direct_win_pid_file_path(self) -> Path:
        """
        The path to the file containing PIDs for directly executed commands for this
        jobscript. Windows only.
        """
        return self.submission.js_win_pids_path / self.direct_win_pid_file_name

    @property
    def is_scheduled(self) -> bool:
        return self.scheduler_name not in ("direct", "direct_posix")

    def _update_at_submit_metadata(
        self,
        submit_cmdline: list[str] | None = None,
        scheduler_job_ID: str | None = None,
        process_ID: int | None = None,
        submit_time: str | None = None,
    ):
        """Update persistent store and in-memory record of at-submit metadata for this
        jobscript.

        """
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            submit_cmdline=submit_cmdline,
            scheduler_job_ID=scheduler_job_ID,
            process_ID=process_ID,
            submit_time=submit_time,
        )

        if submit_cmdline is not None:
            self._at_submit_metadata["submit_cmdline"] = submit_cmdline
        if scheduler_job_ID is not None:
            self._at_submit_metadata["scheduler_job_ID"] = scheduler_job_ID
        if process_ID is not None:
            self._at_submit_metadata["process_ID"] = process_ID
        if submit_time is not None:
            self._at_submit_metadata["submit_time"] = submit_time

    def _set_submit_time(self, submit_time: datetime) -> None:
        self._update_at_submit_metadata(
            submit_time=submit_time.strftime(self.workflow.ts_fmt)
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

    def _set_shell_idx(self, shell_idx: int) -> None:
        self._shell_idx = shell_idx
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            shell_idx=shell_idx,
        )

    def _set_submit_cmdline(self, submit_cmdline: list[str]) -> None:
        self._update_at_submit_metadata(submit_cmdline=submit_cmdline)

    def _set_scheduler_job_ID(self, job_ID: str) -> None:
        """For scheduled submission only."""
        assert self.is_scheduled
        self._update_at_submit_metadata(scheduler_job_ID=job_ID)

    def _set_process_ID(self, process_ID: int) -> None:
        """For direct submission only."""
        assert not self.is_scheduled
        self._update_at_submit_metadata(process_ID=process_ID)

    def _set_version_info(self, version_info: VersionInfo) -> None:
        self._version_info = version_info
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            version_info=version_info,
        )

    @TimeIt.decorator
    def compose_jobscript(
        self,
        shell,
        deps: dict[int, tuple[str, bool]] | None = None,
        os_name: str | None = None,
        scheduler_name: str | None = None,
        scheduler_args: dict[str, Any] | None = None,
    ) -> str:
        """Prepare the jobscript file contents as a string."""
        scheduler_name = scheduler_name or self.scheduler_name
        assert scheduler_name
        assert os_name
        scheduler = self._app.get_scheduler(
            scheduler_name=scheduler_name,
            os_name=os_name,
            scheduler_args=scheduler_args or self._get_submission_scheduler_args(),
        )
        app_caps = self._app.package_name.upper()
        header_args = {
            "app_caps": app_caps,
            "jobscript_functions_name": self.jobscript_functions_name,
            "jobscript_functions_dir": self.submission.JS_FUNCS_DIR_NAME,
            "sub_idx": self.submission.index,
            "js_idx": self.index,
            "run_IDs_file_name": self.EAR_ID_file_name,
            "run_IDs_file_dir": self.submission.JS_RUN_IDS_DIR_NAME,
            "tmp_dir_name": self.submission.TMP_DIR_NAME,
            "log_dir_name": self.submission.LOG_DIR_NAME,
            "app_std_dir_name": self.submission.APP_STD_DIR_NAME,
            "scripts_dir_name": self.submission.SCRIPTS_DIR_NAME,
        }

        shebang = shell.JS_SHEBANG.format(
            shebang=" ".join(scheduler.shebang_executable or shell.shebang_executable)
        )
        header = shell.JS_HEADER.format(**header_args)

        if isinstance(scheduler, QueuedScheduler):
            header = shell.JS_SCHEDULER_HEADER.format(
                shebang=shebang,
                scheduler_options=scheduler.format_directives(
                    resources=self.resources,
                    num_elements=self.blocks[0].num_elements,  # only used for array jobs
                    is_array=self.is_array,
                    sub_idx=self.submission.index,
                    js_idx=self.index,
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

        out = header

        if self.resources.combine_scripts:
            run_cmd = shell.JS_RUN_CMD_COMBINED.format(
                workflow_app_alias=self.workflow_app_alias
            )
            out += run_cmd + "\n"
        else:
            run_cmd = shell.JS_RUN_CMD.format(workflow_app_alias=self.workflow_app_alias)

            if self.resources.write_app_logs:
                run_log_enable_disable = shell.JS_RUN_LOG_PATH_ENABLE.format(
                    run_log_file_name=self.submission.get_app_log_file_name(
                        run_ID=shell.format_env_var_get(f"{app_caps}_RUN_ID")
                    )
                )
            else:
                run_log_enable_disable = shell.JS_RUN_LOG_PATH_DISABLE

            block_run = shell.JS_RUN.format(
                EAR_files_delimiter=self._EAR_files_delimiter,
                app_caps=app_caps,
                run_cmd=run_cmd,
                sub_tmp_dir=self.submission.tmp_path,
                run_log_enable_disable=run_log_enable_disable,
            )
            if len(self.blocks) == 1:
                # forgo element and action loops if not necessary:
                block = self.blocks[0]
                if block.num_actions > 1:
                    block_act = shell.JS_ACT_MULTI.format(
                        num_actions=block.num_actions,
                        run_block=indent(block_run, shell.JS_INDENT),
                    )
                else:
                    block_act = shell.JS_ACT_SINGLE.format(run_block=block_run)

                main = shell.JS_MAIN.format(
                    action=block_act,
                    app_caps=app_caps,
                    block_start_elem_idx=0,
                )

                out += shell.JS_BLOCK_HEADER.format(app_caps=app_caps)
                if self.is_array:
                    if not isinstance(scheduler, QueuedScheduler):
                        raise Exception("can only schedule arrays of jobs to a queue")
                    out += shell.JS_ELEMENT_MULTI_ARRAY.format(
                        scheduler_command=scheduler.js_cmd,
                        scheduler_array_switch=scheduler.array_switch,
                        scheduler_array_item_var=scheduler.array_item_var,
                        num_elements=block.num_elements,
                        main=main,
                    )
                elif block.num_elements == 1:
                    out += shell.JS_ELEMENT_SINGLE.format(
                        block_start_elem_idx=0,
                        main=main,
                    )
                else:
                    out += shell.JS_ELEMENT_MULTI_LOOP.format(
                        block_start_elem_idx=0,
                        num_elements=block.num_elements,
                        main=indent(main, shell.JS_INDENT),
                    )

            else:
                # use a shell loop for blocks, so always write the inner element and action
                # loops:
                block_act = shell.JS_ACT_MULTI.format(
                    num_actions=shell.format_array_get_item("num_actions", "$block_idx"),
                    run_block=indent(block_run, shell.JS_INDENT),
                )
                main = shell.JS_MAIN.format(
                    action=block_act,
                    app_caps=app_caps,
                    block_start_elem_idx="$block_start_elem_idx",
                )

                # only non-array jobscripts will have multiple blocks:
                element_loop = shell.JS_ELEMENT_MULTI_LOOP.format(
                    block_start_elem_idx="$block_start_elem_idx",
                    num_elements=shell.format_array_get_item(
                        "num_elements", "$block_idx"
                    ),
                    main=indent(main, shell.JS_INDENT),
                )
                out += shell.JS_BLOCK_LOOP.format(
                    num_elements=shell.format_array(
                        [i.num_elements for i in self.blocks]
                    ),
                    num_actions=shell.format_array([i.num_actions for i in self.blocks]),
                    num_blocks=len(self.blocks),
                    app_caps=app_caps,
                    element_loop=indent(element_loop, shell.JS_INDENT),
                )

        out += shell.JS_FOOTER

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
        os_name = os_name or self.os_name
        shell_name = shell_name or self.shell_name
        assert os_name
        assert shell_name
        shell = self._get_shell(
            os_name=os_name,
            shell_name=shell_name,
            os_args=os_args or self._get_submission_os_args(),
            shell_args=shell_args or self._get_submission_shell_args(),
        )

        js_str = self.compose_jobscript(
            deps=deps,
            shell=shell,
            os_name=os_name,
            scheduler_name=scheduler_name,
            scheduler_args=scheduler_args,
        )
        with self.jobscript_path.open("wt", newline="\n") as fp:
            fp.write(js_str)

        return self.jobscript_path

    @TimeIt.decorator
    def _launch_direct_js_win(self, submit_cmd: list[str]) -> int:
        # this is a "trick" to ensure we always get a fully detached new process (with no
        # parent); the `powershell.exe -Command` process exits after running the inner
        # `Start-Process`, which is where the jobscript is actually invoked. I could not
        # find a way using `subprocess.Popen()` to ensure the new process was fully
        # detached when submitting jobscripts via a Jupyter notebook in Windows.

        # Note we need powershell.exe for this "launcher process", but the shell used for
        # the jobscript itself need not be powershell.exe
        exe_path, arg_list = submit_cmd[0], submit_cmd[1:]

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
    def _launch_direct_js_posix(self, submit_cmd: list[str]) -> int:
        # direct submission; submit jobscript asynchronously:
        # detached process, avoid interrupt signals propagating to the subprocess:

        def _launch(fp_stdout: TextIO, fp_stderr: TextIO) -> int:
            # note: Popen copies the file objects, so this works!
            proc = subprocess.Popen(
                args=submit_cmd,
                stdout=fp_stdout,
                stderr=fp_stderr,
                cwd=str(self.workflow.path),
                start_new_session=True,
            )
            return proc.pid

        if self.resources.combine_jobscript_std:
            with self.direct_std_out_err_path.open("wt") as fp_std:
                return _launch(fp_std, fp_std)
        else:
            with self.direct_stdout_path.open(
                "wt"
            ) as fp_stdout, self.direct_stderr_path.open("wt") as fp_stderr:
                return _launch(fp_stdout, fp_stderr)

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
        for (js_idx, _), deps_i in self.dependencies.items():
            dep_js_ref, dep_js_is_arr = scheduler_refs[js_idx]
            # only submit an array dependency if both this jobscript and the dependency
            # are array jobs:
            dep_is_arr = deps_i["is_array"] and self.is_array and dep_js_is_arr
            deps[js_idx] = (dep_js_ref, dep_is_arr)

        if self.index > 0:
            # prevent this jobscript executing if jobscript parallelism is not available:
            use_parallelism = (
                self.submission.JS_parallelism is True
                or {0: "direct", 1: "scheduled"}[self.is_scheduled]
                == self.submission.JS_parallelism
            )
            if not use_parallelism:
                # add fake dependencies to all previously submitted jobscripts to avoid
                # simultaneous execution:
                for js_idx, (js_ref, _) in scheduler_refs.items():
                    if js_idx not in deps:
                        deps[js_idx] = (js_ref, False)

        # make directory for jobscripts stdout/err stream files:
        self.std_path.mkdir(exist_ok=True)

        with self.EAR_ID_file_path.open(mode="wt", newline="\n") as ID_fp:
            for block in self.blocks:
                block.write_EAR_ID_file(ID_fp)

        js_path = self.shell.prepare_JS_path(self.write_jobscript(deps=deps))
        submit_cmd = self.scheduler.get_submit_command(self.shell, js_path, deps)
        self._app.submission_logger.info(
            f"submitting jobscript {self.index!r} with command: {submit_cmd!r}"
        )

        err_args: JobscriptSubmissionFailureArgs = {
            "submit_cmd": submit_cmd,
            "js_idx": self.index,
            "js_path": js_path,
        }
        job_ID: str | None = None
        process_ID: int | None = None
        try:
            if isinstance(self.scheduler, QueuedScheduler):
                # scheduled submission, wait for submission so we can parse the job ID:
                stdout, stderr = self._launch_queued(submit_cmd, print_stdout)
                err_args["stdout"] = stdout
                err_args["stderr"] = stderr
            else:
                if os.name == "nt":
                    process_ID = self._launch_direct_js_win(submit_cmd)
                else:
                    process_ID = self._launch_direct_js_posix(submit_cmd)
        except Exception as subprocess_exc:
            err_args["subprocess_exc"] = subprocess_exc
            raise JobscriptSubmissionFailure(
                "Failed to execute submit command.", **err_args
            )

        if isinstance(self.scheduler, QueuedScheduler):
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
            ref = str(process_ID)

        self._set_submit_cmdline(submit_cmd)
        self._set_submit_time(current_timestamp())

        # a downstream direct jobscript might need to wait for this jobscript, which
        # means this jobscript's process ID must be committed:
        self.workflow._store._pending.commit_all()

        return ref

    @property
    def is_submitted(self) -> bool:
        """Whether this jobscript has been submitted."""
        return self.index in self.submission.submitted_jobscripts

    @property
    def scheduler_js_ref(self) -> str | None | tuple[int | None, list[str] | None]:
        """
        The reference to the submitted job for the jobscript.
        """
        if isinstance(self.scheduler, QueuedScheduler):
            return self.scheduler_job_ID
        else:
            return (self.process_ID, self.submit_cmdline)

    @overload
    def get_active_states(
        self, as_json: Literal[False] = False
    ) -> Mapping[int, Mapping[int, JobscriptElementState]]: ...

    @overload
    def get_active_states(
        self, as_json: Literal[True]
    ) -> Mapping[int, Mapping[int, str]]: ...

    @TimeIt.decorator
    def get_active_states(
        self, as_json: bool = False
    ) -> Mapping[int, Mapping[int, JobscriptElementState | str]]:
        """If this jobscript is active on this machine, return the state information from
        the scheduler."""
        # this returns: {BLOCK_IDX: {JS_ELEMENT_IDX: STATE}}
        out: Mapping[int, Mapping[int, JobscriptElementState]] = {}
        if self.is_submitted:
            self._app.submission_logger.debug(
                "checking if the jobscript is running according to EAR submission "
                "states."
            )

            not_run_states = EARStatus.get_non_running_submitted_states()
            all_EAR_states = set(ear.status for ear in self.all_EARs)
            self._app.submission_logger.debug(
                f"Unique EAR states are: {tuple(i.name for i in all_EAR_states)!r}"
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
                out_d = self.scheduler.get_job_state_info(js_refs=[self.scheduler_js_ref])
                if out_d:
                    # remove scheduler ref (should be only one):
                    assert len(out_d) == 1
                    out_i = nth_value(cast("dict", out_d), 0)

                    if self.is_array:
                        # out_i is a dict keyed by array index; there will be exactly one
                        # block:
                        out = {0: out_i}
                    else:
                        # out_i is a single state:
                        out = {
                            idx: {i: out_i for i in range(block.num_elements)}
                            for idx, block in enumerate(self.blocks)
                        }

            else:
                raise NotSubmitMachineError()

        self._app.submission_logger.info(f"Jobscript is {'in' if not out else ''}active.")
        if as_json:
            return {
                block_idx: {k: v.name for k, v in block_data.items()}
                for block_idx, block_data in out.items()
            }
        return out

    def compose_combined_script(
        self, action_scripts: list[list[tuple[str, Path, bool]]]
    ) -> tuple[str, list[list[int]], list[int], list[int]]:
        """
        Prepare the combined-script file string, if applicable.
        """

        # use an index array for action scripts:
        script_names: list[str] = []
        requires_dir: list[bool] = []
        script_data: dict[str, tuple[int, Path]] = {}
        script_indices: list[list[int]] = []
        for i in action_scripts:
            indices_i: list[int] = []
            for name_j, path_j, req_dir_i in i:
                if name_j in script_data:
                    idx = script_data[name_j][0]
                else:
                    idx = len(script_names)
                    script_names.append(name_j)
                    requires_dir.append(req_dir_i)
                    script_data[name_j] = (idx, path_j)
                indices_i.append(idx)
            script_indices.append(indices_i)

        if not self.resources.combine_scripts:
            raise TypeError(
                f"Jobscript {self.index} is not a `combine_scripts` jobscript."
            )

        tab_indent = "    "

        script_funcs_lst: list[str] = []
        for act_name, (_, snip_path) in script_data.items():
            main_func_name = snip_path.stem
            with snip_path.open("rt") as fp:
                script_str = fp.read()
            script_funcs_lst.append(
                dedent(
                    """\
                def {act_name}(*args, **kwargs):
                {script_str}
                    return {main_func_name}(*args, **kwargs)
                """
                ).format(
                    act_name=act_name,
                    script_str=indent(script_str, tab_indent),
                    main_func_name=main_func_name,
                )
            )

        app_caps = self._app.package_name.upper()
        if self.resources.write_app_logs:
            sub_log_path = f'os.environ["{app_caps}_LOG_PATH"]'
        else:
            sub_log_path = '""'

        py_imports = dedent(
            """\
            import os
            from collections import defaultdict
            from pathlib import Path
            import traceback
            import time
            from typing import Dict

            import {app_module} as app

            from hpcflow.sdk.core.errors import UnsetParameterDataErrorBase

            log_path = {log_path}
            wk_path = os.getenv("{app_caps}_WK_PATH")
            """
        ).format(
            app_module=self._app.module,
            app_caps=app_caps,
            log_path=sub_log_path,
        )

        py_main_block_workflow_load = dedent(
            """\
                app.load_config(
                    log_file_path=log_path,
                    config_dir=r"{cfg_dir}",
                    config_key=r"{cfg_invoc_key}",
                )
                wk = app.Workflow(wk_path)
            """
        ).format(
            cfg_dir=self._app.config.config_directory,
            cfg_invoc_key=self._app.config.config_key,
            app_caps=app_caps,
        )

        func_invoc_lines = dedent(
            """\
                import pprint
                if not run.action.is_OFP and run.action.script_data_out_has_direct:
                    outputs = func(**func_kwargs)
                elif run.action.is_OFP:
                    out_name = run.action.output_file_parsers[0].output.typ
                    outputs = {out_name: func(**func_kwargs)}
                else:
                    outputs = {}
                    func(**func_kwargs)
            """
        )

        script_funcs = "\n".join(script_funcs_lst)
        script_names_str = "[" + ", ".join(f"{i}" for i in script_names) + "]"
        main = dedent(
            """\
            {py_imports}
            
            sub_std_path = Path(os.environ["{app_caps}_SUB_STD_DIR"], f"js_{js_idx}.txt")            
            with app.redirect_std_to_file(sub_std_path):
            {py_main_block_workflow_load}

                with open(os.environ["{app_caps}_RUN_ID_FILE"], mode="r") as fp:
                    lns = fp.read().strip().split("\\n")
                    run_IDs = [[int(i) for i in ln.split("{run_ID_delim}")] for ln in lns]

                get_all_runs_tic = time.perf_counter()
                run_IDs_flat = [j for i in run_IDs for j in i]
                runs = wk.get_EARs_from_IDs(run_IDs_flat, as_dict=True)
                run_skips : Dict[int, bool] = {{k: v.skip for k, v in runs.items()}}
                get_all_runs_toc = time.perf_counter()

                with open(os.environ["{app_caps}_SCRIPT_INDICES_FILE"], mode="r") as fp:
                    lns = fp.read().strip().split("\\n")
                    section_idx = -1
                    script_indices = []
                    for ln in lns:
                        if ln.startswith("#"):
                            section_idx += 1
                            continue
                        ln_parsed = [int(i) for i in ln.split("{script_idx_delim}")]
                        if section_idx == 0:
                            num_elements = ln_parsed
                        elif section_idx == 1:
                            num_actions = ln_parsed
                        else:
                            script_indices.append(ln_parsed)

                port = int(os.environ["{app_caps}_RUN_PORT"])
                action_scripts = {script_names}
                requires_dir = {requires_dir!r}
                run_dirs = wk.get_run_directories()

                get_ins_time_fp = open(f"js_{js_idx}_get_inputs_times.txt", "wt")
                func_time_fp = open(f"js_{js_idx}_func_times.txt", "wt")
                run_time_fp = open(f"js_{js_idx}_run_times.txt", "wt")
                set_start_multi_times_fp = open(f"js_{js_idx}_set_start_multi_times.txt", "wt")
                set_end_multi_times_fp = open(f"js_{js_idx}_set_end_multi_times.txt", "wt")
                save_multi_times_fp = open(f"js_{js_idx}_save_multi_times.txt", "wt")
                loop_term_times_fp = open(f"js_{js_idx}_loop_term_times.txt", "wt")

                get_all_runs_time = get_all_runs_toc - get_all_runs_tic
                print(f"get_all_runs_time: {{get_all_runs_time:.4f}}")
 
            app.logger.info(
                f"running {num_blocks} jobscript block(s) in combined jobscript index "
                f"{js_idx}."
            )

            block_start_elem_idx = 0
            for block_idx in range({num_blocks}):

                app.logger.info(f"running block index {{block_idx}}.")

                os.environ["{app_caps}_BLOCK_IDX"] = str(block_idx)

                block_run_IDs = [
                    run_IDs[block_start_elem_idx + i]
                    for i in range(num_elements[block_idx])
                ]

                for block_act_idx in range(num_actions[block_idx]):
                
                    app.logger.info(
                        f"running block action index {{block_act_idx}} "
                        f"(in block {{block_idx}})."
                    )

                    os.environ["{app_caps}_BLOCK_ACT_IDX"] = str(block_act_idx)

                    block_act_run_IDs = [i[block_act_idx] for i in block_run_IDs]

                    block_act_std_path = Path(
                        os.environ["{app_caps}_SUB_STD_DIR"],
                        f"js_{js_idx}_blk_{{block_idx}}_blk_act_{{block_act_idx}}.txt",
                    )
                    with app.redirect_std_to_file(block_act_std_path):
                        # set run starts for all runs of the block/action:
                        block_act_run_dirs = [run_dirs[i] for i in block_act_run_IDs]
                        block_act_runs = [runs[i] for i in block_act_run_IDs]

                        block_act_run_IDs_non_skipped = []
                        block_act_run_dirs_non_skipped = []
                        for i, j in zip(block_act_run_IDs, block_act_run_dirs):
                            if not run_skips[i]:
                                block_act_run_IDs_non_skipped.append(i)
                                block_act_run_dirs_non_skipped.append(j)
                                                     
                        if block_act_run_IDs_non_skipped:
                            set_start_multi_tic = time.perf_counter()
                            app.logger.info("setting run starts.")
                            wk.set_multi_run_starts(block_act_run_IDs_non_skipped, block_act_run_dirs_non_skipped, port)
                            app.logger.info("finished setting run starts.")
                            set_start_multi_toc = time.perf_counter()
                            set_start_multi_time = set_start_multi_toc - set_start_multi_tic
                            print(f"{{set_start_multi_time:.4f}}", file=set_start_multi_times_fp, flush=True)

                    all_act_outputs = {{}}
                    run_end_dat = defaultdict(list)
                    block_act_key=({js_idx}, block_idx, block_act_idx)

                    for block_elem_idx in range(num_elements[block_idx]):

                        js_elem_idx = block_start_elem_idx + block_elem_idx
                        run_ID = block_act_run_IDs[block_elem_idx]

                        app.logger.info(
                            f"run_ID is {{run_ID}}; block element index: {{block_elem_idx}}; "
                            f"block action index: {{block_act_idx}}; in block {{block_idx}}."
                        )

                        if run_ID == -1:                            
                            continue

                        run = runs[run_ID]

                        skip = run_skips[run_ID]
                        if skip:
                            app.logger.info(f"run_ID: {{run_ID}}; run is set to skip; skipping.")
                            # set run end
                            run_end_dat[block_act_key].append((run, {skipped_exit_code}, None))
                            continue
                            
                        run_tic = time.perf_counter()

                        os.environ["{app_caps}_BLOCK_ELEM_IDX"] = str(block_elem_idx)
                        os.environ["{app_caps}_JS_ELEM_IDX"] = str(js_elem_idx)                        
                        os.environ["{app_caps}_RUN_ID"] = str(run_ID)

                        std_path = Path(os.environ["{app_caps}_SUB_STD_DIR"], f"{{run_ID}}.txt")
                        with app.redirect_std_to_file(std_path):

                            if {write_app_logs!r}:
                                new_log_path = Path(
                                    os.environ["{app_caps}_SUB_LOG_DIR"],
                                    f"{run_log_name}",
                                )
                                # TODO: this doesn't work!
                                app.logger.info(
                                    f"run_ID: {{run_ID}}; moving log path to {{new_log_path}}"
                                )
                                app.config.log_path = new_log_path

                            run_dir = run_dirs[run_ID]

                            script_idx = script_indices[block_idx][block_act_idx]
                            req_dir = requires_dir[script_idx]
                            if req_dir:
                                app.logger.info(f"run_ID: {{run_ID}}; changing to run directory: {{run_dir}}")
                                os.chdir(run_dir)
                            
                            # retrieve script inputs:
                            app.logger.info(f"run_ID: {{run_ID}}; retrieving script inputs.")
                            get_ins_tic = time.perf_counter()
                            try:
                                with run.raise_on_failure_threshold() as unset_params:
                                    app.logger.info(f"run_ID: {{run_ID}}; writing script input files.")
                                    run.write_script_input_files(block_act_key)

                                    app.logger.info(f"run_ID: {{run_ID}}; retrieving funcion kwargs.")
                                    func_kwargs = run.get_py_script_func_kwargs(
                                        raise_on_unset=False,
                                        add_script_files=True,
                                        blk_act_key=block_act_key,
                                    )
                                    app.logger.info(
                                        f"run_ID: {{run_ID}}; script inputs have keys: "
                                        f"{{tuple(func_kwargs.keys())!r}}."
                                    )                                
                            except UnsetParameterDataErrorBase:
                                # not all required parameter data is set, so fail this run:
                                exit_code = 1
                                run_end_dat[block_act_key].append((run, exit_code, None))
                                app.logger.info(
                                    f"run_ID: {{run_ID}}; some parameter data is unset, "
                                    f"so cannot run; setting exit code to 1."
                                )
                                continue # don't run the function

                            get_ins_toc = time.perf_counter()

                            func = action_scripts[script_idx]
                            app.logger.info(f"run_ID: {{run_ID}}; function to run is: {{func.__name__}}")


                        try:
                            func_tic = time.perf_counter()
                            app.logger.info(f"run_ID: {{run_ID}}; invoking function.")
            {func_invoc_lines}
                            
                        except Exception:
                            print(f"Exception caught during execution of script function {{func.__name__}}.")
                            traceback.print_exc()
                            exit_code = 1
                            outputs = {{}}
                        else:
                            app.logger.info(f"run_ID: {{run_ID}}; finished function invocation.")
                            exit_code = 0
                        finally:
                            func_toc = time.perf_counter()
                            
                        with app.redirect_std_to_file(std_path):
                            # set run end
                            block_act_key=({js_idx}, block_idx, block_act_idx)
                            run_end_dat[block_act_key].append((run, exit_code, run_dir))

                            # store outputs to save at end:
                            app.logger.info(f"run_ID: {{run_ID}}; setting outputs to save.")
                            for name_i, out_i in outputs.items():
                                p_id = run.data_idx[f"outputs.{{name_i}}"]
                                all_act_outputs[p_id] = out_i
                            app.logger.info(f"run_ID: {{run_ID}}; finished setting outputs to save.")
                                
                            if req_dir:
                                app.logger.info(f"run_ID: {{run_ID}}; changing directory back")
                                os.chdir(os.environ["{app_caps}_SUB_TMP_DIR"])

                            if {write_app_logs!r}:
                                app.logger.info(f"run_ID: {{run_ID}}; moving log path back to " + {sub_log_path!r})
                                app.config.log_path = {sub_log_path}

                        run_toc = time.perf_counter()

                        get_ins_time = get_ins_toc - get_ins_tic
                        func_time = func_toc - func_tic
                        run_time = run_toc - run_tic

                        print(f"{{get_ins_time:.4f}}", file=get_ins_time_fp)
                        print(f"{{func_time:.4f}}", file=func_time_fp)
                        print(f"{{run_time:.4f}}", file=run_time_fp)

                    with app.redirect_std_to_file(block_act_std_path):
                    
                        if all_act_outputs:
                            # save outputs of all elements of this action
                            save_all_tic = time.perf_counter()
                            app.logger.info(
                                f"saving outputs of block action index {{block_act_idx}} "
                                f"in block {{block_idx}}."
                            )                            
                            wk.set_parameter_values(all_act_outputs)
                            app.logger.info(
                                f"finished saving outputs of block action index {{block_act_idx}} "
                                f"in block {{block_idx}}."
                            )
                            save_all_toc = time.perf_counter()
                            save_all_time_i = save_all_toc - save_all_tic
                            print(f"{{save_all_time_i:.4f}}", file=save_multi_times_fp, flush=True)

                        all_loop_term_tic = time.perf_counter()
                        app.logger.info(f"run_ID: {{run_ID}}; checking for loop terminations")
                        for run_i in block_act_runs:
                            if not run_skips[run_i.id_]:
                                skipped_IDs_i = wk._check_loop_termination(run_i)
                                for skip_ID in skipped_IDs_i:
                                    run_skips[skip_ID] =  2 # SkipReason.LOOP_TERMINATION
                                    if skip_ID in runs:
                                        runs[skip_ID]._skip = 2 # mutates runs within `run_end_dat`
                        app.logger.info(f"run_ID: {{run_ID}}; finished checking for loop terminations.")
                                    
                        all_loop_term_toc = time.perf_counter()
                        all_loop_term_time_i = all_loop_term_toc - all_loop_term_tic
                        print(f"{{all_loop_term_time_i:.4f}}", file=loop_term_times_fp, flush=True)

                        # set run end for all elements of this action
                        app.logger.info(f"run_ID: {{run_ID}}; setting run ends.")
                        set_multi_end_tic = time.perf_counter()
                        wk.set_multi_run_ends(run_end_dat)
                        set_multi_end_toc = time.perf_counter()
                        set_multi_end_time = set_multi_end_toc - set_multi_end_tic
                        app.logger.info(f"run_ID: {{run_ID}}; finished setting run ends.")
                        print(f"{{set_multi_end_time:.4f}}", file=set_end_multi_times_fp, flush=True)

                block_start_elem_idx += num_elements[block_idx]

            get_ins_time_fp.close()
            func_time_fp.close()
            run_time_fp.close()
            set_start_multi_times_fp.close()
            set_end_multi_times_fp.close()
            save_multi_times_fp.close()
            loop_term_times_fp.close()
        """
        ).format(
            py_imports=py_imports,
            py_main_block_workflow_load=indent(py_main_block_workflow_load, tab_indent),
            app_caps=self._app.package_name.upper(),
            script_idx_delim=",",  # TODO
            script_names=script_names_str,
            requires_dir=requires_dir,
            num_blocks=len(self.blocks),
            run_ID_delim=self._EAR_files_delimiter,
            run_log_name=self.submission.get_app_log_file_name(run_ID="{run_ID}"),
            js_idx=self.index,
            write_app_logs=self.resources.write_app_logs,
            sub_log_path=sub_log_path,
            skipped_exit_code=SKIPPED_EXIT_CODE,
            func_invoc_lines=indent(func_invoc_lines, tab_indent * 4),
        )

        script = dedent(
            """\
            {script_funcs}
            if __name__ == "__main__":
            {main}
        """
        ).format(script_funcs=script_funcs, main=indent(main, tab_indent))

        num_elems = [i.num_elements for i in self.blocks]
        num_acts = [len(i) for i in action_scripts]

        return script, script_indices, num_elems, num_acts

    def write_script_indices_file(
        self, indices: list[list[int]], num_elems: list[int], num_acts: list[int]
    ) -> None:
        """
        Write a text file containing the action script index for each block and action
        in a `combined_scripts` script.
        """
        delim = ","  # TODO: refactor?
        with self.combined_script_indices_file_path.open("wt") as fp:
            fp.write("# number of elements per block:\n")
            fp.write(delim.join(str(i) for i in num_elems) + "\n")
            fp.write("# number of actions per block:\n")
            fp.write(delim.join(str(i) for i in num_acts) + "\n")
            fp.write("# script indices:\n")
            for block in indices:
                fp.write(delim.join(str(i) for i in block) + "\n")

    def get_app_std_path(self) -> Path:
        std_dir = self.submission.get_app_std_path(
            self.workflow.submissions_path,
            self.submission.index,
        )
        return std_dir / f"js_{self.index}.txt"  # TODO: refactor
