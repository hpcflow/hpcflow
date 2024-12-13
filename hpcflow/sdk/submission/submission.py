"""
A collection of submissions to a scheduler, generated from a workflow.
"""

from __future__ import annotations
from collections import defaultdict
import os
from pathlib import Path
from typing import Any, overload, TYPE_CHECKING
from typing_extensions import override

from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.errors import (
    JobscriptSubmissionFailure,
    MissingEnvironmentError,
    MissingEnvironmentExecutableError,
    MissingEnvironmentExecutableInstanceError,
    MultipleEnvironmentsError,
    SubmissionFailure,
)
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.object_list import ObjectListMultipleMatchError
from hpcflow.sdk.core.utils import parse_timestamp, current_timestamp
from hpcflow.sdk.submission.enums import SubmissionStatus
from hpcflow.sdk.log import TimeIt

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from datetime import datetime
    from typing import ClassVar, Literal
    from rich.status import Status
    from .jobscript import Jobscript
    from .enums import JobscriptElementState
    from .schedulers import Scheduler
    from .shells import Shell
    from .types import SubmissionPart
    from ..core.element import ElementActionRun
    from ..core.environment import Environment
    from ..core.object_list import EnvironmentsList
    from ..core.workflow import Workflow


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

    def __init__(
        self,
        index: int,
        jobscripts: list[Jobscript],
        workflow: Workflow | None = None,
        submission_parts: dict[str, list[int]] | None = None,
        JS_parallelism: bool | None = None,
        environments: EnvironmentsList | None = None,
    ):
        self._index = index
        self._jobscripts = jobscripts
        self._submission_parts = submission_parts or {}
        self._JS_parallelism = JS_parallelism
        self._environments = environments

        self._submission_parts_lst: list[
            SubmissionPart
        ] | None = None  # assigned on first access

        if workflow:
            #: The workflow this is part of.
            self.workflow = workflow

        self._set_parent_refs()

        for js_idx, js in enumerate(self.jobscripts):
            js._index = js_idx

    @TimeIt.decorator
    def _set_environments(self) -> None:
        filterable = self._app.ElementResources.get_env_instance_filterable_attributes()

        # map required environments and executable labels to job script indices:
        req_envs: dict[
            tuple[tuple[str, ...], tuple[Any, ...]], dict[str, set[int]]
        ] = defaultdict(lambda: defaultdict(set))
        for js_idx, js_i in enumerate(self.jobscripts):
            for run in js_i.all_EARs:
                # Alas, mypy can't typecheck the next line if the type is right!
                # So we use Any to get it to shut up...
                env_spec_h: Any = tuple(zip(*run.env_spec.items()))  # hashable
                for exec_label_j in run.action.get_required_executables():
                    req_envs[env_spec_h][exec_label_j].add(js_idx)
                # Ensure overall element is present
                req_envs[env_spec_h]

        # check these envs/execs exist in app data:
        envs: list[Environment] = []
        for env_spec_h, exec_js in req_envs.items():
            env_spec = dict(zip(*env_spec_h))
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
    def submission_parts(self) -> list[SubmissionPart]:
        """
        Description of the parts of this submission.
        """
        if not self._submission_parts:
            return []

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
    def JS_parallelism(self) -> bool | None:
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
    def path(self) -> Path:
        """
        The path to files associated with this submission.
        """
        return self.workflow.submissions_path / str(self.index)

    @property
    def all_EAR_IDs(self) -> Iterable[int]:
        """
        The IDs of all EARs in this submission.
        """
        return (i for js in self.jobscripts for i in js.all_EAR_IDs)

    @property
    def all_EARs(self) -> Iterable[ElementActionRun]:
        """
        All EARs in this this submission.
        """
        return (ear for js in self.jobscripts for ear in js.all_EARs)

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
    def abort_EARs_file_name(self) -> str:
        """
        The name of a file describing what EARs have aborted.
        """
        return "abort_EARs.txt"

    @property
    def abort_EARs_file_path(self) -> Path:
        """
        The path to the file describing what EARs have aborted in this submission.
        """
        return self.path / self.abort_EARs_file_name

    @overload
    def get_active_jobscripts(
        self, as_json: Literal[False] = False
    ) -> Mapping[int, Mapping[int, JobscriptElementState]]:
        ...

    @overload
    def get_active_jobscripts(self, as_json: Literal[True]) -> dict[int, dict[int, str]]:
        ...

    @TimeIt.decorator
    def get_active_jobscripts(
        self, as_json: bool = False
    ) -> Mapping[int, Mapping[int, JobscriptElementState]] | dict[int, dict[int, str]]:
        """Get jobscripts that are active on this machine, and their active states."""
        # this returns: {JS_IDX: {JS_ELEMENT_IDX: STATE}}
        # TODO: query the scheduler once for all jobscripts?
        if as_json:
            details = (
                (js.index, js.get_active_states(as_json=True)) for js in self.jobscripts
            )
            return {idx: state for idx, state in details if state}
        else:
            dets2 = (
                (js.index, js.get_active_states(as_json=False)) for js in self.jobscripts
            )
            return {idx: state for idx, state in dets2 if state}

    def _write_abort_EARs_file(self) -> None:
        with self.abort_EARs_file_path.open(mode="wt", newline="\n") as fp:
            # write a single line for each EAR currently in the workflow:
            fp.write("\n".join("0" for _ in range(self.workflow.num_EARs)) + "\n")

    def _set_run_abort(self, run_ID: int) -> None:
        """Modify the abort runs file to indicate a specified run should be aborted."""
        with self.abort_EARs_file_path.open(mode="rt", newline="\n") as fp:
            lines = fp.read().splitlines()
        lines[run_ID] = "1"

        # write a new temporary run-abort file:
        tmp_suffix = self.abort_EARs_file_path.suffix + ".tmp"
        tmp = self.abort_EARs_file_path.with_suffix(tmp_suffix)
        self._app.submission_logger.debug(f"Creating temporary run abort file: {tmp!r}.")
        with tmp.open(mode="wt", newline="\n") as fp:
            fp.write("\n".join(lines) + "\n")

        # atomic rename, overwriting original:
        self._app.submission_logger.debug(
            "Replacing original run abort file with new temporary file."
        )
        os.replace(src=tmp, dst=self.abort_EARs_file_path)

    @staticmethod
    def get_unique_schedulers_of_jobscripts(
        jobscripts: Iterable[Jobscript],
    ) -> Iterable[tuple[tuple[tuple[int, int], ...], Scheduler]]:
        """Get unique schedulers and which of the passed jobscripts they correspond to.

        Uniqueness is determines only by the `QueuedScheduler.unique_properties` tuple.

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

    def _append_submission_part(self, submit_time: str, submitted_js_idx: list[int]):
        self._submission_parts[submit_time] = submitted_js_idx
        self.workflow._store.add_submission_part(
            sub_idx=self.index,
            dt_str=submit_time,
            submitted_js_idx=submitted_js_idx,
        )

    @TimeIt.decorator
    def submit(
        self,
        status: Status | None,
        ignore_errors: bool = False,
        print_stdout: bool = False,
        add_to_known: bool = True,
    ) -> list[int]:
        """Generate and submit the jobscripts of this submission."""

        # if JS_parallelism explicitly requested but store doesn't support, raise:
        supports_JS_para = self.workflow._store._features.jobscript_parallelism
        if self.JS_parallelism:
            if not supports_JS_para:
                if status:
                    status.stop()
                raise ValueError(
                    f"Store type {self.workflow._store!r} does not support jobscript "
                    f"parallelism."
                )
        elif self.JS_parallelism is None:
            self._JS_parallelism = supports_JS_para

        # set os_name and shell_name for each jobscript:
        for js in self.jobscripts:
            js._set_os_name()
            js._set_shell_name()
            js._set_scheduler_name()

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

        for js_indices_2, shell in self.get_unique_shells():
            try:
                vers_info = shell.get_version_info()
            except Exception:
                if not ignore_errors:
                    raise
                vers_info = {}
            for js_idx in js_indices_2:
                if js_idx in outstanding:
                    js_vers_info.setdefault(js_idx, {}).update(vers_info)

        for js_idx, vers_info_i in js_vers_info.items():
            self.jobscripts[js_idx]._set_version_info(vers_info_i)

        # for direct submission, it's important that os_name/shell_name/scheduler_name
        # are made persistent now, because `Workflow.write_commands`, which might be
        # invoked in a new process before submission has completed, needs to know these:
        self.workflow._store._pending.commit_all()

        # TODO: a submission should only be "submitted" once shouldn't it?
        # no; there could be an IO error (e.g. internet connectivity), so might
        # need to be able to reattempt submission of outstanding jobscripts.
        self.path.mkdir(exist_ok=True)
        if not self.abort_EARs_file_path.is_file():
            self._write_abort_EARs_file()

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
                i in submitted_js_idx or i in self.submitted_jobscripts
                for i in js.dependencies
            ):
                continue

            try:
                if status:
                    status.update(f"Submitting jobscript {js.index}...")
                js_ref_i = js.submit(scheduler_refs, print_stdout=print_stdout)
                scheduler_refs[js.index] = (js_ref_i, js.is_array)
                submitted_js_idx.append(js.index)

            except JobscriptSubmissionFailure as err:
                errs.append(err)
                continue

        if submitted_js_idx:
            dt_str = current_timestamp().strftime(self._app._submission_ts_fmt)
            self._append_submission_part(
                submit_time=dt_str,
                submitted_js_idx=submitted_js_idx,
            )
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
                    f"Cancelling jobscripts {js_idx!r} of submission {self.index} of "
                    f"workflow {self.workflow.name!r}."
                )
                jobscripts = [self.jobscripts[i] for i in js_idx]
                sched_refs = [js.scheduler_js_ref for js in jobscripts]
                sched.cancel_jobs(js_refs=sched_refs, jobscripts=jobscripts)
            else:
                print("No active jobscripts to cancel.")
