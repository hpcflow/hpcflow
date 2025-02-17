from __future__ import annotations
from collections import defaultdict

from datetime import datetime, timedelta, timezone
import enum
import shutil
from pathlib import Path
import socket
from textwrap import indent
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
import warnings

import numpy as np

from hpcflow.sdk import app
from hpcflow.sdk.core.element import ElementResources
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
from hpcflow.sdk.core import RUN_DIR_ARR_DTYPE
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.utils.strings import shorten_list_str


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


def timedelta_format(td: timedelta) -> str:
    days, seconds = td.days, td.seconds
    hours = seconds // (60 * 60)
    seconds -= hours * (60 * 60)
    minutes = seconds // 60
    seconds -= minutes * 60
    return f"{days}-{hours:02}:{minutes:02}:{seconds:02}"


def timedelta_parse(td_str: str) -> timedelta:
    days, other = td_str.split("-")
    days = int(days)
    hours, mins, secs = [int(i) for i in other.split(":")]
    return timedelta(days=days, hours=hours, minutes=mins, seconds=secs)


class SubmissionStatus(enum.Enum):
    PENDING = 0  # not yet submitted
    SUBMITTED = 1  # all jobscripts submitted successfully
    PARTIALLY_SUBMITTED = 2  # some jobscripts submitted successfully


class Submission(JSONLike):

    TMP_DIR_NAME = "tmp"
    LOG_DIR_NAME = "app_logs"
    APP_STD_DIR_NAME = "app_std"
    JS_STD_DIR_NAME = "js_std"
    SCRIPTS_DIR_NAME = "scripts"
    COMMANDS_DIR_NAME = "commands"

    _child_objects = (
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

    def __init__(
        self,
        index: int,
        jobscripts: List[app.Jobscript],
        at_submit_metadata: Union[Dict, None] = None,
        workflow: Optional[app.Workflow] = None,
        JS_parallelism: Optional[Union[bool, Literal["direct", "scheduled"]]] = None,
        environments: Optional[app.EnvironmentsList] = None,
    ):
        self._index = index
        self._jobscripts = jobscripts
        self._at_submit_metadata = at_submit_metadata or {
            k: v() for k, v in SUBMISSION_SUBMIT_TIME_KEYS.items()
        }
        self._JS_parallelism = JS_parallelism
        self._environments = environments  # assigned by _set_environments

        self._submission_parts_lst = None  # assigned on first access; datetime objects

        if workflow:
            self.workflow = workflow

        self._set_parent_refs()

    def _ensure_JS_parallelism_set(self):
        """Ensure that the JS_parallelism attribute is one of `True`, `False`, `'direct'`
        or `'scheduled'`.

        Notes
        -----
        This method is called when the Submission object is first created in
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
    def _set_environments(self):
        filterable = ElementResources.get_env_instance_filterable_attributes()

        # map required environments and executable labels to job script indices:
        req_envs = defaultdict(lambda: defaultdict(set))
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
                        req_envs[env_spec_h] = defaultdict(set)

        # check these envs/execs exist in app data:
        envs = []
        for env_spec_h, exec_js in req_envs.items():
            env_spec = self.app.Action.env_spec_from_hashable(env_spec_h)
            non_name_spec = {k: v for k, v in env_spec.items() if k != "name"}
            spec_str = f" with specifiers {non_name_spec!r}" if non_name_spec else ""
            env_ref = f"{env_spec['name']!r}{spec_str}"
            try:
                env_i = self.app.envs.get(**env_spec)
            except ObjectListMultipleMatchError:
                raise MultipleEnvironmentsError(
                    f"Multiple environments {env_ref} are defined on this machine."
                )
            except ValueError:
                raise MissingEnvironmentError(
                    f"The environment {env_ref} is not defined on this machine, so the "
                    f"submission cannot be created."
                ) from None
            else:
                if env_i not in envs:
                    envs.append(env_i)

            for exec_i_lab, js_idx_set in exec_js.items():
                try:
                    exec_i = env_i.executables.get(exec_i_lab)
                except ValueError:
                    raise MissingEnvironmentExecutableError(
                        f"The environment {env_ref} as defined on this machine has no "
                        f"executable labelled {exec_i_lab!r}, which is required for this "
                        f"submission, so the submission cannot be created."
                    ) from None

                # check matching executable instances exist:
                for js_idx_j in js_idx_set:
                    js_j = self.jobscripts[js_idx_j]
                    filter_exec = {j: getattr(js_j.resources, j) for j in filterable}
                    exec_instances = exec_i.filter_instances(**filter_exec)
                    if not exec_instances:
                        raise MissingEnvironmentExecutableInstanceError(
                            f"No matching executable instances found for executable "
                            f"{exec_i_lab!r} of environment {env_ref} for jobscript "
                            f"index {js_idx_j!r} with requested resources "
                            f"{filter_exec!r}."
                        )

        # save env definitions to the environments attribute:
        self._environments = self.app.EnvironmentsList(envs)

    def to_dict(self):
        dct = super().to_dict()
        del dct["_workflow"]
        del dct["_index"]
        del dct["_submission_parts_lst"]
        dct = {k.lstrip("_"): v for k, v in dct.items()}
        return dct

    @property
    def index(self) -> int:
        return self._index

    @property
    def environments(self) -> app.EnvironmentsList:
        return self._environments

    @property
    def at_submit_metadata(self) -> Dict[Dict[str, Any]]:
        return self.workflow._store.get_submission_at_submit_metadata(
            sub_idx=self.index, metadata_attr=self._at_submit_metadata
        )

    @property
    def _submission_parts(self) -> Dict[str, List[int]]:
        return self.at_submit_metadata["submission_parts"] or {}

    @property
    def submission_parts(self) -> List[Dict[str, Any]]:
        if self._submission_parts_lst is None:
            self._submission_parts_lst = [
                {
                    "submit_time": datetime.strptime(dt, self.workflow.ts_fmt)
                    .replace(tzinfo=timezone.utc)
                    .astimezone(),
                    "jobscripts": js_idx,
                }
                for dt, js_idx in self._submission_parts.items()
            ]
        return self._submission_parts_lst

    @TimeIt.decorator
    def get_start_time(self, submit_time: str) -> Union[datetime, None]:
        """Get the start time of a given submission part."""
        js_idx = self._submission_parts[submit_time]
        all_part_starts = []
        for i in js_idx:
            start_time = self.jobscripts[i].start_time
            if start_time:
                all_part_starts.append(start_time)
        if all_part_starts:
            return min(all_part_starts)
        else:
            return None

    @TimeIt.decorator
    def get_end_time(self, submit_time: str) -> Union[datetime, None]:
        """Get the end time of a given submission part."""
        js_idx = self._submission_parts[submit_time]
        all_part_ends = []
        for i in js_idx:
            end_time = self.jobscripts[i].end_time
            if end_time:
                all_part_ends.append(end_time)
        if all_part_ends:
            return max(all_part_ends)
        else:
            return None

    @property
    @TimeIt.decorator
    def start_time(self):
        """Get the first non-None start time over all submission parts."""
        all_start_times = []
        for submit_time in self._submission_parts:
            start_i = self.get_start_time(submit_time)
            if start_i:
                all_start_times.append(start_i)
        if all_start_times:
            return min(all_start_times)
        else:
            return None

    @property
    @TimeIt.decorator
    def end_time(self):
        """Get the final non-None end time over all submission parts."""
        all_end_times = []
        for submit_time in self._submission_parts:
            end_i = self.get_end_time(submit_time)
            if end_i:
                all_end_times.append(end_i)
        if all_end_times:
            return max(all_end_times)
        else:
            return None

    @property
    def jobscripts(self) -> List:
        return self._jobscripts

    @property
    def JS_parallelism(self):
        return self._JS_parallelism

    @property
    def workflow(self) -> List:
        return self._workflow

    @workflow.setter
    def workflow(self, wk):
        self._workflow = wk

    @property
    def jobscript_indices(self) -> Tuple[int]:
        """All associated jobscript indices."""
        return tuple(i.index for i in self.jobscripts)

    @property
    def submitted_jobscripts(self) -> Tuple[int]:
        """Jobscript indices that have been successfully submitted."""
        return tuple(j for i in self.submission_parts for j in i["jobscripts"])

    @property
    def outstanding_jobscripts(self) -> Tuple[int]:
        """Jobscript indices that have not yet been successfully submitted."""
        return tuple(set(self.jobscript_indices) - set(self.submitted_jobscripts))

    @property
    def status(self):
        if not self.submission_parts:
            return SubmissionStatus.PENDING
        else:
            if set(self.submitted_jobscripts) == set(self.jobscript_indices):
                return SubmissionStatus.SUBMITTED
            else:
                return SubmissionStatus.PARTIALLY_SUBMITTED

    @property
    def needs_submit(self):
        return self.status in (
            SubmissionStatus.PENDING,
            SubmissionStatus.PARTIALLY_SUBMITTED,
        )

    @property
    def needs_app_log_dir(self) -> bool:
        for js in self.jobscripts:
            if js.resources.write_app_logs:
                return True
        return False

    @classmethod
    def get_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        return submissions_path / str(sub_idx)

    @classmethod
    def get_tmp_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        return cls.get_path(submissions_path, sub_idx) / cls.TMP_DIR_NAME

    @classmethod
    def get_app_log_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        return cls.get_path(submissions_path, sub_idx) / cls.LOG_DIR_NAME

    @staticmethod
    def get_app_log_file_name(run_ID: int):
        # TODO: consider combine_app_logs argument
        return f"r_{run_ID}.log"

    @classmethod
    def get_app_log_file_path(cls, submissions_path: Path, sub_idx: int, run_ID: int):
        return (
            cls.get_path(submissions_path, sub_idx)
            / cls.LOG_DIR_NAME
            / cls.get_app_log_file_name(run_ID)
        )

    @classmethod
    def get_app_std_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        return cls.get_path(submissions_path, sub_idx) / cls.APP_STD_DIR_NAME

    @classmethod
    def get_js_std_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        return cls.get_path(submissions_path, sub_idx) / cls.JS_STD_DIR_NAME

    @classmethod
    def get_scripts_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        return cls.get_path(submissions_path, sub_idx) / cls.SCRIPTS_DIR_NAME

    @classmethod
    def get_commands_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        return cls.get_path(submissions_path, sub_idx) / cls.COMMANDS_DIR_NAME

    @property
    def path(self) -> Path:
        return self.get_path(self.workflow.submissions_path, self.index)

    @property
    def tmp_path(self) -> Path:
        return self.get_tmp_path(self.workflow.submissions_path, self.index)

    @property
    def app_log_path(self) -> Path:
        return self.get_app_log_path(self.workflow.submissions_path, self.index)

    @property
    def app_std_path(self):
        return self.get_app_std_path(self.workflow.submissions_path, self.index)

    @property
    def js_std_path(self):
        return self.get_js_std_path(self.workflow.submissions_path, self.index)

    @property
    def scripts_path(self):
        return self.get_scripts_path(self.workflow.submissions_path, self.index)

    @property
    def commands_path(self):
        return self.get_commands_path(self.workflow.submissions_path, self.index)

    @property
    @TimeIt.decorator
    def all_EAR_IDs(self):
        return [i for js in self.jobscripts for i in js.all_EAR_IDs]

    @property
    @TimeIt.decorator
    def all_EARs(self):
        return [i for js in self.jobscripts for i in js.all_EARs]

    @property
    @TimeIt.decorator
    def all_EARs_IDs_by_jobscript(self) -> List[int]:
        return [i.all_EAR_IDs for i in self.jobscripts]

    @property
    @TimeIt.decorator
    def all_EARs_by_jobscript(self) -> List[List[int]]:
        ids = [i.all_EAR_IDs for i in self.jobscripts]
        all_EARs = {i.id_: i for i in self.workflow.get_EARs_from_IDs(self.all_EAR_IDs)}
        return [[all_EARs[i] for i in js_ids] for js_ids in ids]

    @property
    @TimeIt.decorator
    def EARs_by_elements(self):
        task_elem_EARs = defaultdict(lambda: defaultdict(list))
        for i in self.all_EARs:
            task_elem_EARs[i.task.index][i.element.index].append(i)
        return task_elem_EARs

    @property
    def is_scheduled(self) -> Tuple[bool]:
        """Return whether each jobscript of this submission uses a scheduler or not."""
        return tuple(i.is_scheduled for i in self.jobscripts)

    @TimeIt.decorator
    def get_active_jobscripts(
        self, as_json: bool = False
    ) -> Dict[int, Dict[int, Dict[int, JobscriptElementState]]]:
        """Get jobscripts that are active on this machine, and their active states."""
        # this returns: {JS_IDX: {BLOCK_IDX: {JS_ELEMENT_IDX: STATE}}}
        # TODO: query the scheduler once for all jobscripts?
        out = {}
        for js in self.jobscripts:
            active_states = js.get_active_states(as_json=as_json)
            if active_states:
                out[js.index] = active_states
        return out

    @TimeIt.decorator
    def _write_scripts(
        self, cache, status: Optional[Any] = None
    ) -> Tuple[Dict[int, int], np.NDarray]:
        """Write to disk all action scripts associated with this submission."""
        # TODO: rename this method

        # TODO: need to check is_snippet_script is exclusive? i.e. only `script` and no
        # `commands` in the action?
        # TODO: scripts must have the same exe and the same environment as well?
        # TODO: env_spec should be included in jobscript hash if combine_scripts=True ?

        actions_by_schema = defaultdict(lambda: defaultdict(set))
        combined_env_specs = {}

        # task insert IDs and action indices for each combined_scripts jobscript:
        combined_actions = {}

        cmd_hashes = defaultdict(set)
        num_runs_tot = sum(len(js.all_EAR_IDs) for js in self.jobscripts)
        run_indices = np.ones((num_runs_tot, 9), dtype=int) * -1
        run_inp_files = defaultdict(
            list
        )  # keys are `run_idx`, values are Paths to copy to run dir
        run_cmd_file_names = {}
        run_idx = 0

        if status:
            status.update(f"Adding new submission: processing run 1/{num_runs_tot}.")

        all_runs = cache.runs
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

        seen = {}
        combined_script_data = defaultdict(lambda: defaultdict(list))
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
        cache,
    ) -> Tuple[np.ndarray, np.ndarray]:

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

        elem_depths = {}
        iter_depths = {}
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
                e_depth = elem_depths.get(t_iID)
                if e_depth is None:
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
                i_depth = iter_depths.get(e_id)
                if i_depth is None:
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
        run_indices: np.NDArray,
        run_inp_files: Dict[int, List[Path]],
        cache: Any,
        status: Optional[Any] = None,
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

        # make directories
        for idx, run_dir in enumerate(run_dirs):
            run_dir.mkdir(parents=True, exist_ok=True)
            inp_files_i = run_inp_files.get(run_idx[idx])
            if inp_files_i:
                # copy (TODO: optionally symlink) any input files:
                for path_i in inp_files_i:
                    shutil.copy(path_i, run_dir)

        if status:
            status.update("Adding new submission: making execution directories...")

    @staticmethod
    def get_unique_schedulers_of_jobscripts(
        jobscripts: List[Jobscript],
    ) -> Dict[Tuple[Tuple[int, int]], Scheduler]:
        """Get unique schedulers and which of the passed jobscripts they correspond to.

        Uniqueness is determined only by the `Scheduler.unique_properties` tuple.

        """
        js_idx = []
        schedulers = []

        # list of tuples of scheduler properties we consider to determine "uniqueness",
        # with the first string being the scheduler type (class name):
        seen_schedulers = []

        for js in jobscripts:
            if js.scheduler.unique_properties not in seen_schedulers:
                seen_schedulers.append(js.scheduler.unique_properties)
                schedulers.append(js.scheduler)
                js_idx.append([])
            sched_idx = seen_schedulers.index(js.scheduler.unique_properties)
            js_idx[sched_idx].append((js.submission.index, js.index))

        sched_js_idx = dict(zip((tuple(i) for i in js_idx), schedulers))

        return sched_js_idx

    @TimeIt.decorator
    def get_unique_schedulers(self) -> Dict[Tuple[int], Scheduler]:
        """Get unique schedulers and which of this submission's jobscripts they
        correspond to."""
        return self.get_unique_schedulers_of_jobscripts(self.jobscripts)

    @TimeIt.decorator
    def get_unique_shells(self) -> Dict[Tuple[int], Shell]:
        """Get unique shells and which jobscripts they correspond to."""
        js_idx = []
        shells = []

        for js in self.jobscripts:
            if js.shell not in shells:
                shells.append(js.shell)
                js_idx.append([])
            shell_idx = shells.index(js.shell)
            js_idx[shell_idx].append(js.index)

        shell_js_idx = dict(zip((tuple(i) for i in js_idx), shells))

        return shell_js_idx

    def _raise_failure(self, submitted_js_idx, exceptions):
        msg = f"Some jobscripts in submission index {self.index} could not be submitted"
        if submitted_js_idx:
            msg += f" (but jobscripts {submitted_js_idx} were submitted successfully):"
        else:
            msg += ":"

        msg += "\n"
        for sub_err in exceptions:
            msg += (
                f"Jobscript {sub_err.js_idx} at path: {str(sub_err.js_path)!r}\n"
                f"Submit command: {sub_err.submit_cmd!r}.\n"
                f"Reason: {sub_err.message!r}\n"
            )
            if sub_err.subprocess_exc is not None:
                msg += f"Subprocess exception: {sub_err.subprocess_exc}\n"
            if sub_err.job_ID_parse_exc is not None:
                msg += f"Subprocess job ID parse exception: {sub_err.job_ID_parse_exc}\n"
            if sub_err.job_ID_parse_exc is not None:
                msg += f"Job ID parse exception: {sub_err.job_ID_parse_exc}\n"
            if sub_err.stdout:
                msg += f"Submission stdout:\n{indent(sub_err.stdout, '  ')}\n"
            if sub_err.stderr:
                msg += f"Submission stderr:\n{indent(sub_err.stderr, '  ')}\n"

        raise SubmissionFailure(message=msg)

    def _update_at_submit_metadata(self, submission_parts: Dict[str, List[int]]):
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

    def _append_submission_part(self, submit_time: str, submitted_js_idx: List[int]):
        self._update_at_submit_metadata(submission_parts={submit_time: submitted_js_idx})

    @TimeIt.decorator
    def submit(
        self,
        status,
        ignore_errors: Optional[bool] = False,
        print_stdout: Optional[bool] = False,
        add_to_known: Optional[bool] = True,
    ) -> List[int]:
        """Generate and submit the jobscripts of this submission."""

        # TODO: support passing list of jobscript indices to submit; this will allow us
        # to test a submision with multiple "submission parts". would also need to check
        # dependencies if this customised list is passed

        outstanding = self.outstanding_jobscripts

        # get scheduler, shell and OS version information (also an opportunity to fail
        # before trying to submit jobscripts):
        js_vers_info = {}
        for js_indices, sched in self.get_unique_schedulers().items():
            try:
                vers_info = sched.get_version_info()
            except Exception as err:
                if ignore_errors:
                    vers_info = {}
                else:
                    raise err
            for _, js_idx in js_indices:
                if js_idx in outstanding:
                    if js_idx not in js_vers_info:
                        js_vers_info[js_idx] = {}
                    js_vers_info[js_idx].update(vers_info)

        for js_indices, shell in self.get_unique_shells().items():
            try:
                vers_info = shell.get_version_info()
            except Exception as err:
                if ignore_errors:
                    vers_info = {}
                else:
                    raise err
            for js_idx in js_indices:
                if js_idx in outstanding:
                    if js_idx not in js_vers_info:
                        js_vers_info[js_idx] = {}
                    js_vers_info[js_idx].update(vers_info)

        hostname = socket.gethostname()
        machine = self.app.config.get("machine")
        for js_idx, vers_info_i in js_vers_info.items():
            js = self.jobscripts[js_idx]
            js._set_version_info(vers_info_i)
            js._set_submit_hostname(hostname)
            js._set_submit_machine(machine)

        self.workflow._store._pending.commit_all()

        # map jobscript `index` to (scheduler job ID or process ID, is_array):
        scheduler_refs = {}
        submitted_js_idx = []
        errs = []
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
            dt_str = datetime.utcnow().strftime(self.app._submission_ts_fmt)
            self._append_submission_part(
                submit_time=dt_str,
                submitted_js_idx=submitted_js_idx,
            )
            # ensure `_submission_parts` is committed
            self.workflow._store._pending.commit_all()

            # add a record of the submission part to the known-submissions file
            if add_to_known:
                self.app._add_to_known_submissions(
                    wk_path=self.workflow.path,
                    wk_id=self.workflow.id_,
                    sub_idx=self.index,
                    sub_time=dt_str,
                )

        if errs and not ignore_errors:
            if status:
                status.stop()
            self._raise_failure(submitted_js_idx, errs)

        len_js = len(submitted_js_idx)
        print(f"Submitted {len_js} jobscript{'s' if len_js > 1 else ''}.")

        return submitted_js_idx

    @TimeIt.decorator
    def cancel(self):
        act_js = list(self.get_active_jobscripts())
        if not act_js:
            print("No active jobscripts to cancel.")
            return
        for js_indices, sched in self.get_unique_schedulers().items():
            # filter by active jobscripts:
            js_idx = [i[1] for i in js_indices if i[1] in act_js]
            if js_idx:
                print(
                    f"Cancelling jobscripts {shorten_list_str(js_idx, items=5)} of "
                    f"submission {self.index} of workflow {self.workflow.name!r}."
                )
                jobscripts = [self.jobscripts[i] for i in js_idx]
                sched_refs = [i.scheduler_js_ref for i in jobscripts]
                sched.cancel_jobs(js_refs=sched_refs, jobscripts=jobscripts)
            else:
                print("No active jobscripts to cancel.")

    @TimeIt.decorator
    def get_scheduler_job_IDs(self) -> Tuple[str]:
        """Return jobscript scheduler job IDs."""
        return tuple(
            js_i.scheduler_job_ID
            for js_i in self.jobscripts
            if js_i.scheduler_job_ID is not None
        )

    @TimeIt.decorator
    def get_process_IDs(self) -> Tuple[int]:
        """Return jobscript process IDs."""
        return tuple(
            js_i.process_ID for js_i in self.jobscripts if js_i.process_ID is not None
        )

    @TimeIt.decorator
    def list_jobscripts(self, max_js: int = None, jobscripts: List[int] = None) -> None:
        """Print a table listing jobscripts and associated information.

        Parameters
        ----------
        max_js
            Maximum jobscript index to display. This cannot be specified with `jobscripts`.
        jobscripts
            A list of jobscripts to display. This cannot be specified with `max_js`.

        """
        self.workflow.list_jobscripts(
            sub_idx=self.index, max_js=max_js, jobscripts=jobscripts
        )
