"""
Actions are base components of elements.
Element action runs (EARs) are the basic components of any enactment;
they may be grouped together within a jobscript for efficiency.
"""

from __future__ import annotations
from collections.abc import Mapping
import copy
from dataclasses import dataclass
import json
import contextlib
from collections import defaultdict
from pathlib import Path
import re
from textwrap import indent, dedent
from typing import cast, final, overload, TYPE_CHECKING
from typing_extensions import override

from watchdog.utils.dirsnapshot import DirectorySnapshotDiff

from hpcflow.sdk.core import ABORT_EXIT_CODE
from hpcflow.sdk.core.app_aware import AppAware
from hpcflow.sdk.core.enums import ActionScopeType, EARStatus
from hpcflow.sdk.core.skip_reason import SkipReason
from hpcflow.sdk.core.task import WorkflowTask
from hpcflow.sdk.core.errors import (
    ActionEnvironmentMissingNameError,
    MissingCompatibleActionEnvironment,
    OutputFileParserNoOutputError,
    UnknownScriptDataKey,
    UnknownScriptDataParameter,
    UnsupportedScriptDataFormat,
    UnsetParameterDataError,
    UnsetParameterFractionLimitExceededError,
    UnsetParameterNumberLimitExceededError,
)
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.parameters import ParameterValue
from hpcflow.sdk.typing import ParamSource, hydrate
from hpcflow.sdk.core.utils import (
    JSONLikeDirSnapShot,
    split_param_label,
    swap_nested_dict_keys,
    get_relative_path,
)
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.core.run_dir_files import RunDirAppFiles
from hpcflow.sdk.submission.enums import SubmissionStatus
from hpcflow.sdk.submission.submission import Submission
from hpcflow.sdk.utils.hashing import get_hash

if TYPE_CHECKING:
    from collections.abc import Callable, Container, Iterable, Iterator, Sequence
    from datetime import datetime
    from re import Pattern
    from typing import Any, ClassVar, Literal
    from typing_extensions import Self
    from valida.conditions import ConditionLike  # type: ignore

    from ..typing import DataIndex, ParamSource
    from ..submission.shells import Shell
    from ..submission.jobscript import Jobscript
    from .commands import Command
    from .command_files import InputFileGenerator, OutputFileParser, FileSpec
    from .element import (
        Element,
        ElementIteration,
        ElementInputs,
        ElementOutputs,
        ElementResources,
        ElementInputFiles,
        ElementOutputFiles,
    )
    from .environment import Environment
    from .parameters import SchemaParameter, Parameter
    from .rule import Rule
    from .task import WorkflowTask
    from .task_schema import TaskSchema
    from .types import ParameterDependence, ScriptData, BlockActionKey
    from .workflow import Workflow
    from .object_list import EnvironmentsList

ACTION_SCOPE_REGEX = r"(\w*)(?:\[(.*)\])?"


@dataclass
class UnsetParamTracker:
    """Class to track run IDs that are the sources of unset parameter data for some input
    parameter type.

    Attributes
    ----------
    run_ids
        Set of integer run IDs that have been tracked.
    group_size
        The size of the group, if the associated SchemaInput in question is a group.

    Notes
    -----
    Objects of this class are instantiated within
    `WorkflowTask._get_merged_parameter_data` when we are tracking unset parameters.

    """

    run_ids: set[int]
    group_size: int


#: Keyword arguments permitted for particular scopes.
ACTION_SCOPE_ALLOWED_KWARGS: Mapping[str, frozenset[str]] = {
    ActionScopeType.ANY.name: frozenset(),
    ActionScopeType.MAIN.name: frozenset(),
    ActionScopeType.PROCESSING.name: frozenset(),
    ActionScopeType.INPUT_FILE_GENERATOR.name: frozenset({"file"}),
    ActionScopeType.OUTPUT_FILE_PARSER.name: frozenset({"output"}),
}


class ElementActionRun(AppAware):
    """
    The Element Action Run (EAR) is an atomic unit of an enacted workflow, representing
    one unit of work (e.g., particular submitted job to run a program) within that
    overall workflow. With looping over, say, parameter spaces, there may be many EARs
    per element.

    Parameters
    ----------
    id_: int
        The ID of the EAR.
    is_pending: bool
        Whether this EAR is pending.
    element_action:
        The particular element action that this is a run of.
    index: int:
        The index of the run within the collection of runs.
    data_idx: dict
        Used for looking up input data to the EAR.
    commands_idx: list[int]
        Indices of commands to apply.
    start_time: datetime
        Time of start of run, if the run has ever been started.
    end_time: datetime
        Time of end of run, if the run has ever ended.
    snapshot_start: dict
        Parameters for taking a snapshot of the data directory before the run.
        If unspecified, no snapshot will be taken.
    snapshot_end: dict
        Parameters for taking a snapshot of the data directory after the run.
        If unspecified, no snapshot will be taken.
    submission_idx: int
        What submission was this (if it has been submitted)?
    success: bool
        Whether this EAR succeeded (if it has run).
    skip: bool
        Whether this EAR was skipped.
    exit_code: int
        The exit code, if known.
    metadata: dict
        Metadata about the EAR.
    run_hostname: str
        Where to run the EAR (if not locally).
    """

    def __init__(
        self,
        id_: int,
        is_pending: bool,
        element_action: ElementAction,
        index: int,
        data_idx: DataIndex,
        commands_idx: list[int],
        start_time: datetime | None,
        end_time: datetime | None,
        snapshot_start: dict[str, Any] | None,
        snapshot_end: dict[str, Any] | None,
        submission_idx: int | None,
        commands_file_ID: int | None,
        success: bool | None,
        skip: int,
        exit_code: int | None,
        metadata: dict[str, Any],
        run_hostname: str | None,
        port_number: int | None,
    ) -> None:
        self._id = id_
        self._is_pending = is_pending
        self._element_action = element_action
        self._index = index  # local index of this run with the action
        self._data_idx = data_idx
        self._commands_idx = commands_idx
        self._start_time = start_time
        self._end_time = end_time
        self._submission_idx = submission_idx
        self._commands_file_ID = commands_file_ID
        self._success = success
        self._skip = skip
        self._snapshot_start = snapshot_start
        self._snapshot_end = snapshot_end
        self._exit_code = exit_code
        self._metadata = metadata
        self._run_hostname = run_hostname
        self._port_number = port_number

        # assigned on first access of corresponding properties:
        self._inputs: ElementInputs | None = None
        self._outputs: ElementOutputs | None = None
        self._resources: ElementResources | None = None
        self._input_files: ElementInputFiles | None = None
        self._output_files: ElementOutputFiles | None = None
        self._ss_start_obj: JSONLikeDirSnapShot | None = None
        self._ss_end_obj: JSONLikeDirSnapShot | None = None
        self._ss_diff_obj: DirectorySnapshotDiff | None = None

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"id={self.id_!r}, index={self.index!r}, "
            f"element_action={self.element_action!r})"
        )

    @property
    def id_(self) -> int:
        """
        The ID of the EAR.
        """
        return self._id

    @property
    def is_pending(self) -> bool:
        """
        Whether this EAR is pending.
        """
        return self._is_pending

    @property
    def element_action(self) -> ElementAction:
        """
        The particular element action that this is a run of.
        """
        return self._element_action

    @property
    def index(self) -> int:
        """Run index."""
        return self._index

    @property
    def action(self) -> Action:
        """
        The action this is a run of.
        """
        return self.element_action.action

    @property
    def element_iteration(self) -> ElementIteration:
        """
        The iteration information of this run.
        """
        return self.element_action.element_iteration

    @property
    def element(self) -> Element:
        """
        The element this is a run of.
        """
        return self.element_iteration.element

    @property
    def workflow(self) -> Workflow:
        """
        The workflow this is a run of.
        """
        return self.element_iteration.workflow

    @property
    def data_idx(self) -> DataIndex:
        """
        Used for looking up input data to the EAR.
        """
        return self._data_idx

    @property
    def commands_idx(self) -> Sequence[int]:
        """
        Indices of commands to apply.
        """
        return self._commands_idx

    @property
    def metadata(self) -> Mapping[str, Any]:
        """
        Metadata about the EAR.
        """
        return self._metadata

    @property
    def run_hostname(self) -> str | None:
        """
        Where to run the EAR, if known/specified.
        """
        return self._run_hostname

    @property
    def port_number(self):
        return self._port_number

    @property
    def start_time(self) -> datetime | None:
        """
        When the EAR started.
        """
        return self._start_time

    @property
    def end_time(self) -> datetime | None:
        """
        When the EAR finished.
        """
        return self._end_time

    @property
    def submission_idx(self) -> int | None:
        """
        What actual submission index was this?
        """
        return self._submission_idx

    @property
    def commands_file_ID(self):
        return self._commands_file_ID

    @property
    def success(self) -> bool | None:
        """
        Did the EAR succeed?
        """
        return self._success

    @property
    def skip(self) -> int:
        """
        Was the EAR skipped?
        """
        return self._skip

    @property
    def skip_reason(self):
        return SkipReason(self.skip)

    @property
    def snapshot_start(self) -> JSONLikeDirSnapShot | None:
        """
        The snapshot of the data directory at the start of the run.
        """
        if self._ss_start_obj is None and self._snapshot_start:
            self._ss_start_obj = JSONLikeDirSnapShot(
                root_path=".",
                **self._snapshot_start,
            )
        return self._ss_start_obj

    @property
    def snapshot_end(self) -> JSONLikeDirSnapShot | None:
        """
        The snapshot of the data directory at the end of the run.
        """
        if self._ss_end_obj is None and self._snapshot_end:
            self._ss_end_obj = JSONLikeDirSnapShot(root_path=".", **self._snapshot_end)
        return self._ss_end_obj

    @property
    def dir_diff(self) -> DirectorySnapshotDiff | None:
        """
        The changes to the EAR working directory due to the execution of this EAR.
        """
        if (
            not self._ss_diff_obj
            and (ss := self.snapshot_start)
            and (se := self.snapshot_end)
        ):
            self._ss_diff_obj = DirectorySnapshotDiff(ss, se)
        return self._ss_diff_obj

    @property
    def exit_code(self) -> int | None:
        """
        The exit code of the underlying program run by the EAR, if known.
        """
        return self._exit_code

    @property
    def task(self) -> WorkflowTask:
        """
        The task that this EAR is part of the implementation of.
        """
        return self.element_action.task

    @property
    def status(self) -> EARStatus:
        """
        The state of this EAR.
        """

        if self.skip:
            return EARStatus.skipped

        elif self.end_time is not None:
            if self.exit_code == 0:
                return EARStatus.success
            elif self.action.abortable and self.exit_code == ABORT_EXIT_CODE:
                return EARStatus.aborted
            else:
                return EARStatus.error

        elif self.start_time is not None:
            return EARStatus.running

        elif self.submission_idx is not None:
            wk_sub_stat = self.workflow.submissions[self.submission_idx].status

            if wk_sub_stat == SubmissionStatus.PENDING:
                return EARStatus.prepared
            elif wk_sub_stat == SubmissionStatus.SUBMITTED:
                return EARStatus.submitted
            else:
                RuntimeError(f"Workflow submission status not understood: {wk_sub_stat}.")

        return EARStatus.pending

    def get_parameter_names(self, prefix: str) -> Sequence[str]:
        """Get parameter types associated with a given prefix.

        For inputs, labels are ignored. See `Action.get_parameter_names` for more
        information.

        Parameters
        ----------
        prefix
            One of "inputs", "outputs", "input_files", "output_files".
        """
        return self.action.get_parameter_names(prefix)

    def get_data_idx(self, path: str | None = None) -> DataIndex:
        """
        Get the data index of a value in the most recent iteration.

        Parameters
        ----------
        path:
            Path to the parameter.
        """
        return self.element_iteration.get_data_idx(
            path,
            action_idx=self.element_action.action_idx,
            run_idx=self.index,
        )

    @overload
    def get_parameter_sources(
        self,
        *,
        path: str | None = None,
        typ: str | None = None,
        as_strings: Literal[False] = False,
        use_task_index: bool = False,
    ) -> Mapping[str, ParamSource | list[ParamSource]]: ...

    @overload
    def get_parameter_sources(
        self,
        *,
        path: str | None = None,
        typ: str | None = None,
        as_strings: Literal[True],
        use_task_index: bool = False,
    ) -> Mapping[str, str]: ...

    @TimeIt.decorator
    def get_parameter_sources(
        self,
        *,
        path: str | None = None,
        typ: str | None = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ) -> Mapping[str, str] | Mapping[str, ParamSource | list[ParamSource]]:
        """
        Get the source or sources of a parameter in the most recent iteration.

        Parameters
        ----------
        path:
            Path to the parameter.
        typ:
            The parameter type.
        as_strings:
            Whether to return the result as human-readable strings.
        use_task_index:
            Whether to use the task index.
        """
        if as_strings:
            return self.element_iteration.get_parameter_sources(
                path,
                action_idx=self.element_action.action_idx,
                run_idx=self.index,
                typ=typ,
                as_strings=True,
                use_task_index=use_task_index,
            )
        return self.element_iteration.get_parameter_sources(
            path,
            action_idx=self.element_action.action_idx,
            run_idx=self.index,
            typ=typ,
            as_strings=False,
            use_task_index=use_task_index,
        )

    def get(
        self,
        path: str | None = None,
        default: Any | None = None,
        raise_on_missing: bool = False,
        raise_on_unset: bool = False,
    ) -> Any:
        """
        Get a value (parameter, input, output, etc.) from the most recent iteration.

        Parameters
        ----------
        path:
            Path to the value.
        default:
            Default value to provide if value absent.
        raise_on_missing:
            Whether to raise an exception on an absent value.
            If not, the default is returned.
        raise_on_unset:
            Whether to raise an exception on an explicitly unset value.
            If not, the default is returned.
        """
        return self.element_iteration.get(
            path=path,
            action_idx=self.element_action.action_idx,
            run_idx=self.index,
            default=default,
            raise_on_missing=raise_on_missing,
            raise_on_unset=raise_on_unset,
        )

    @overload
    def get_EAR_dependencies(self, as_objects: Literal[False] = False) -> set[int]: ...

    @overload
    def get_EAR_dependencies(
        self, as_objects: Literal[True]
    ) -> list[ElementActionRun]: ...

    @TimeIt.decorator
    def get_EAR_dependencies(self, as_objects=False) -> list[ElementActionRun] | set[int]:
        """Get EARs that this EAR depends on, or just their IDs."""
        out: set[int] = set()
        for src in self.get_parameter_sources(typ="EAR_output").values():
            for src_i in src if isinstance(src, list) else [src]:
                EAR_ID_i: int = src_i["EAR_ID"]
                if EAR_ID_i != self.id_:
                    # don't record a self dependency!
                    out.add(EAR_ID_i)

        if as_objects:
            return self.workflow.get_EARs_from_IDs(sorted(out))
        return out

    def get_input_dependencies(self) -> Mapping[str, ParamSource]:
        """Get information about locally defined input, sequence, and schema-default
        values that this EAR depends on. Note this does not get values from this EAR's
        task/schema, because the aim of this method is to help determine which upstream
        tasks this EAR depends on."""

        wanted_types = ("local_input", "default_input")
        return {
            k: v_i
            for k, v in self.get_parameter_sources().items()
            for v_i in (v if isinstance(v, list) else [v])
            if (
                v_i["type"] in wanted_types
                and v_i["task_insert_ID"] != self.task.insert_ID
            )
        }

    @overload
    def get_dependent_EARs(self, as_objects: Literal[False] = False) -> set[int]: ...

    @overload
    def get_dependent_EARs(self, as_objects: Literal[True]) -> list[ElementActionRun]: ...

    def get_dependent_EARs(
        self, as_objects: bool = False
    ) -> list[ElementActionRun] | set[int]:
        """Get downstream EARs that depend on this EAR."""
        deps = {
            run.id_
            for task in self.workflow.tasks[self.task.index :]
            for elem in task.elements[:]
            for iter_ in elem.iterations
            for run in iter_.action_runs
            # does EAR dependency belong to self?
            if self._id in run.get_EAR_dependencies()
        }
        if as_objects:
            return self.workflow.get_EARs_from_IDs(sorted(deps))
        return deps

    @property
    def inputs(self) -> ElementInputs:
        """
        The inputs to this EAR.
        """
        if not self._inputs:
            self._inputs = self._app.ElementInputs(element_action_run=self)
        return self._inputs

    @property
    def outputs(self) -> ElementOutputs:
        """
        The outputs from this EAR.
        """
        if not self._outputs:
            self._outputs = self._app.ElementOutputs(element_action_run=self)
        return self._outputs

    @property
    @TimeIt.decorator
    def resources(self) -> ElementResources:
        """
        The resources to use with (or used by) this EAR.
        """
        if not self._resources:
            self._resources = self.__get_resources_obj()
        return self._resources

    @property
    def input_files(self) -> ElementInputFiles:
        """
        The input files to the controlled program.
        """
        if not self._input_files:
            self._input_files = self._app.ElementInputFiles(element_action_run=self)
        return self._input_files

    @property
    def output_files(self) -> ElementOutputFiles:
        """
        The output files from the controlled program.
        """
        if not self._output_files:
            self._output_files = self._app.ElementOutputFiles(element_action_run=self)
        return self._output_files

    @property
    @TimeIt.decorator
    def env_spec(self) -> Mapping[str, Any]:
        """
        Environment details.
        """
        if (envs := self.resources.environments) is None:
            return {}
        return envs[self.action.get_environment_name()]

    @property
    @TimeIt.decorator
    def env_spec_hashable(self) -> tuple:
        return self.action.env_spec_to_hashable(self.env_spec)

    def get_directory(self) -> Path | None:
        """
        Get the working directory, if one is required.
        """
        return self.workflow.get_run_directories(run_ids=[self.id_])[0]

    def get_app_log_path(self) -> Path:
        assert self.submission_idx is not None
        return Submission.get_app_log_file_path(
            self.workflow.submissions_path,
            self.submission_idx,
            self.id_,
        )

    def get_app_std_path(self) -> Path:
        assert self.submission_idx is not None
        std_dir = Submission.get_app_std_path(
            self.workflow.submissions_path,
            self.submission_idx,
        )
        return std_dir / f"{self.id_}.txt"  # TODO: refactor

    @TimeIt.decorator
    def get_resources(self) -> Mapping[str, Any]:
        """Resolve specific resources for this EAR, considering all applicable scopes and
        template-level resources."""
        return self.element_iteration.get_resources(self.action)

    @TimeIt.decorator
    def __get_resources_obj(self) -> ElementResources:
        """Resolve specific resources for this EAR, considering all applicable scopes and
        template-level resources."""
        return self.element_iteration.get_resources_obj(self.action)

    def get_environment_spec(self) -> Mapping[str, Any]:
        """
        What environment to run in?
        """
        return self.action.get_environment_spec()

    def get_environment(self) -> Environment:
        """
        What environment to run in?
        """
        return self.action.get_environment()

    def get_all_previous_iteration_runs(
        self, include_self: bool = True
    ) -> list[ElementActionRun]:
        """Get a list of run over all iterations that correspond to this run, optionally
        including this run."""
        self_iter = self.element_iteration
        self_elem = self_iter.element
        self_act_idx = self.element_action.action_idx
        max_idx = self_iter.index + (1 if include_self else 0)
        return [
            iter_i.actions[self_act_idx].runs[-1]
            for iter_i in self_elem.iterations[:max_idx]
        ]

    def get_input_values(
        self,
        inputs: Sequence[str] | Mapping[str, Mapping[str, Any]] | None = None,
        label_dict: bool = True,
        raise_on_unset: bool = False,
    ) -> Mapping[str, Mapping[str, Any]]:
        """Get a dict of (optionally a subset of) inputs values for this run.

        Parameters
        ----------
        inputs:
            If specified, a list of input parameter types to include, or a dict whose keys
            are input parameter types to include. For schema inputs that have
            `multiple=True`, the input type should be labelled. If a dict is passed, and
            the key "all_iterations` is present and `True`, the return for that input
            will be structured to include values for all previous iterations.
        label_dict:
            If True, arrange the values of schema inputs with multiple=True as a dict
            whose keys are the labels. If False, labels will be included in the top level
            keys.

        """
        if not inputs:
            inputs = self.get_parameter_names("inputs")

        out: dict[str, dict[str, Any]] = {}
        for inp_name in inputs:
            if self.__all_iters(inputs, inp_name):
                val_i = {
                    f"iteration_{run_i.element_iteration.index}": {
                        "loop_idx": run_i.element_iteration.loop_idx,
                        "value": run_i.get(
                            f"inputs.{inp_name}", raise_on_unset=raise_on_unset
                        ),
                    }
                    for run_i in self.get_all_previous_iteration_runs(include_self=True)
                }
            else:
                val_i = self.get(f"inputs.{inp_name}", raise_on_unset=raise_on_unset)

            key, label_i = self.__split_input_name(inp_name, label_dict)
            if label_i:
                out.setdefault(key, {})[label_i] = val_i
            else:
                out[key] = val_i

        if self.action.script_pass_env_spec:
            out["env_spec"] = cast("Any", self.env_spec)

        return out

    @staticmethod
    def __all_iters(
        inputs: Sequence[str] | Mapping[str, Mapping[str, Any]], inp_name: str
    ) -> bool:
        try:
            return isinstance(inputs, Mapping) and bool(
                inputs[inp_name]["all_iterations"]
            )
        except (TypeError, KeyError):
            return False

    @staticmethod
    def __split_input_name(inp_name: str, label_dict: bool) -> tuple[str, str | None]:
        key = inp_name
        path, label = split_param_label(key)
        if label_dict and path:
            key = path  # exclude label from key
        # for sub-parameters, take only the final part as the dict key:
        return key.split(".")[-1], (label if label_dict else None)

    def get_input_values_direct(
        self, label_dict: bool = True, raise_on_unset: bool = False
    ) -> Mapping[str, Mapping[str, Any]]:
        """Get a dict of input values that are to be passed directly to a Python script
        function."""
        inputs = self.action.script_data_in_grouped.get("direct", {})
        return self.get_input_values(
            inputs=inputs, label_dict=label_dict, raise_on_unset=raise_on_unset
        )

    def get_IFG_input_values(self, raise_on_unset: bool = False) -> Mapping[str, Any]:
        """
        Get a dict of input values that are to be passed via an input file generator.
        """
        if not self.action._from_expand:
            raise RuntimeError(
                "Cannot get input file generator inputs from this EAR because the "
                "associated action is not expanded, meaning multiple IFGs might exists."
            )
        input_types = [i.typ for i in self.action.input_file_generators[0].inputs]
        inputs = {
            typ_i: self.get(f"inputs.{typ_i}", raise_on_unset=raise_on_unset)
            for typ_i in input_types
        }

        if self.action.script_pass_env_spec:
            inputs["env_spec"] = self.env_spec

        return inputs

    def get_OFP_output_files(self) -> Mapping[str, Path]:
        """
        Get a dict of output files that are going to be parsed to generate one or more
        outputs.
        """
        # TODO: can this return multiple files for a given FileSpec?
        if not self.action._from_expand:
            raise RuntimeError(
                "Cannot get output file parser files from this from EAR because the "
                "associated action is not expanded, meaning multiple OFPs might exist."
            )
        return {
            file_spec.label: Path(cast("str", file_spec.name.value()))
            for file_spec in self.action.output_file_parsers[0].output_files
        }

    def get_OFP_inputs(
        self, raise_on_unset: bool = False
    ) -> Mapping[str, str | list[str] | Mapping[str, Any]]:
        """
        Get a dict of input values that are to be passed to output file parsers.
        """
        if not self.action._from_expand:
            raise RuntimeError(
                "Cannot get output file parser inputs from this from EAR because the "
                "associated action is not expanded, meaning multiple OFPs might exist."
            )
        inputs: dict[str, str | list[str] | Mapping[str, Any]] = (
            {}
        )  # not sure this type is correct
        for inp_typ in self.action.output_file_parsers[0].inputs or []:
            inputs[inp_typ] = self.get(f"inputs.{inp_typ}", raise_on_unset=raise_on_unset)

        if self.action.script_pass_env_spec:
            inputs["env_spec"] = self.env_spec

        return inputs

    def get_OFP_outputs(
        self, raise_on_unset: bool = False
    ) -> Mapping[str, str | list[str]]:
        """
        Get the outputs that are required to execute an output file parser.
        """
        if not self.action._from_expand:
            raise RuntimeError(
                "Cannot get output file parser outputs from this from EAR because the "
                "associated action is not expanded, meaning multiple OFPs might exist."
            )
        outputs: dict[str, str | list[str]] = {}  # not sure this type is correct
        for out_typ in self.action.output_file_parsers[0].outputs or []:
            outputs[out_typ] = self.get(
                f"outputs.{out_typ}", raise_on_unset=raise_on_unset
            )
        return outputs

    def get_py_script_func_kwargs(
        self,
        raise_on_unset: bool = False,
        add_script_files: bool = False,
        blk_act_key: BlockActionKey | None = None,
    ) -> Mapping[str, Any]:
        """Get function arguments to run the Python script associated with this action.

        Parameters
        ----------
        raise_on_unset
            If True, raise if unset parameter data is found when trying to retrieve input
            data.
        add_script_files
            If True, include additional keys "_input_files" and "_output_files" that will
            be dicts mapping file formats to file names for script input and output files.
            If True, `js_blk_act_key` must be provided.
        js_blk_act_key
            A three-tuple of integers corresponding to the jobscript index, block index,
            and block-action index.
        """
        kwargs: dict[str, Any] = {}
        if self.action.is_IFG:
            ifg = self.action.input_file_generators[0]
            path = ifg.input_file.name.value()
            assert isinstance(path, str)
            kwargs["path"] = Path(path)
            kwargs.update(self.get_IFG_input_values(raise_on_unset=raise_on_unset))

        elif self.action.is_OFP:
            kwargs.update(self.get_OFP_output_files())
            kwargs.update(self.get_OFP_inputs(raise_on_unset=raise_on_unset))
            kwargs.update(self.get_OFP_outputs(raise_on_unset=raise_on_unset))

        if (
            not any((self.action.is_IFG, self.action.is_OFP))
            and self.action.script_data_in_has_direct
        ):
            kwargs.update(self.get_input_values_direct(raise_on_unset=raise_on_unset))

        if add_script_files:
            assert blk_act_key
            in_out_names = self.action.get_script_input_output_file_paths(blk_act_key)
            in_names, out_names = in_out_names["inputs"], in_out_names["outputs"]
            if in_names:
                kwargs["_input_files"] = in_names
            if out_names:
                kwargs["_output_files"] = out_names

        return kwargs

    def write_script_input_files(self, block_act_key: BlockActionKey) -> None:
        """
        Write values to files in standard formats.
        """
        for fmt, ins in self.action.script_data_in_grouped.items():
            in_vals = self.get_input_values(
                inputs=ins, label_dict=False, raise_on_unset=False
            )
            if writer := self.__source_writer_map.get(fmt):
                writer(self, in_vals, block_act_key)

    def __write_json_inputs(
        self,
        in_vals: Mapping[str, ParameterValue | list[ParameterValue]],
        block_act_key: BlockActionKey,
    ):
        in_vals_processed: dict[str, Any] = {}
        for k, v in in_vals.items():
            try:
                in_vals_processed[k] = (
                    v.prepare_JSON_dump() if isinstance(v, ParameterValue) else v
                )
            except (AttributeError, NotImplementedError):
                in_vals_processed[k] = v

        with self.action.get_param_dump_file_path_JSON(block_act_key).open("wt") as fp:
            json.dump(in_vals_processed, fp)

    def __write_hdf5_inputs(
        self,
        in_vals: Mapping[str, ParameterValue | list[ParameterValue]],
        block_act_key: BlockActionKey,
    ):
        import h5py  # type: ignore

        with h5py.File(
            self.action.get_param_dump_file_path_HDF5(block_act_key), mode="w"
        ) as h5file:
            for k, v in in_vals.items():
                grp_k = h5file.create_group(k)
                try:
                    assert isinstance(v, ParameterValue)
                    v.dump_to_HDF5_group(grp_k)
                except AssertionError:
                    # probably an element group (i.e. v is a list of `ParameterValue`
                    # objects):
                    assert isinstance(v, list)
                    v[0].dump_element_group_to_HDF5_group(v, grp_k)

    __source_writer_map: ClassVar[dict[str, Callable[..., None]]] = {
        "json": __write_json_inputs,
        "hdf5": __write_hdf5_inputs,
    }

    def __output_index(self, param_name: str) -> int:
        return cast("int", self.data_idx[f"outputs.{param_name}"])

    def _param_save(self, block_act_key: BlockActionKey, run_dir: Path | None = None):
        """Save script-generated parameters that are stored within the supported script
        data output formats (HDF5, JSON, etc)."""
        in_out_names = self.action.get_script_input_output_file_paths(
            block_act_key, directory=run_dir
        )

        import h5py  # type: ignore

        parameters = self._app.parameters
        for fmt, load_path in in_out_names["outputs"].items():
            if fmt == "json":
                with load_path.open(mode="rt") as f:
                    file_data: dict[str, Any] = json.load(f)
                    for param_name, param_dat in file_data.items():
                        param_id = self.__output_index(param_name)
                        if param_cls := parameters.get(param_name)._force_value_class():
                            try:
                                param_cls.save_from_JSON(
                                    param_dat, param_id, self.workflow
                                )
                                continue
                            except NotImplementedError:
                                pass
                        # try to save as a primitive:
                        self.workflow.set_parameter_value(
                            param_id=param_id, value=param_dat
                        )

            elif fmt == "hdf5":
                with h5py.File(load_path, mode="r") as h5file:
                    for param_name, h5_grp in h5file.items():
                        param_id = self.__output_index(param_name)
                        if param_cls := parameters.get(param_name)._force_value_class():
                            try:
                                param_cls.save_from_HDF5_group(
                                    h5_grp, param_id, self.workflow
                                )
                                continue
                            except NotImplementedError:
                                pass
                        # Unlike with JSON, we've no fallback so we warn
                        self._app.logger.warning(
                            "parameter %s could not be saved; serializer not found",
                            param_name,
                        )

    @property
    def is_snippet_script(self) -> bool:
        """Returns True if the action script string represents a script snippets that is
        to be modified before execution (e.g. to receive and provide parameter data)."""
        try:
            return self.action.is_snippet_script(self.action.script)
        except AttributeError:
            return False

    def get_script_artifact_name(self) -> str:
        """Return the script name that is used when writing the script to the artifacts
        directory within the workflow.

        Like `Action.get_script_name`, this is only applicable for snippet scripts.

        """
        art_name, snip_path = self.action.get_script_artifact_name(
            env_spec=self.env_spec,
            act_idx=self.element_action.action_idx,
            include_suffix=True,
            specs_suffix_delim=".",
        )
        return art_name

    def compose_commands(
        self, environments: EnvironmentsList, shell: Shell
    ) -> tuple[str, Mapping[int, Sequence[tuple[str, ...]]]]:
        """
        Write the EAR's enactment to disk in preparation for submission.

        Returns
        -------
        commands:
            List of argument words for the command that enacts the EAR.
            Converted to a string.
        shell_vars:
            Dict whose keys are command indices, and whose values are lists of tuples,
            where each tuple contains: (parameter name, shell variable name,
            "stdout"/"stderr").
        """
        self._app.persistence_logger.debug("EAR.compose_commands")
        env_spec = self.env_spec

        for ofp in self.action.output_file_parsers:
            # TODO: there should only be one at this stage if expanded?
            if ofp.output is None:
                raise OutputFileParserNoOutputError()

        command_lns: list[str] = []
        if (env := environments.get(**env_spec)).setup:
            command_lns.extend(env.setup)

        shell_vars: dict[int, list[tuple[str, ...]]] = {}
        for cmd_idx, command in enumerate(self.action.commands):
            if cmd_idx in self.commands_idx:
                # only execute commands that have no rules, or all valid rules:
                cmd_str, shell_vars[cmd_idx] = command.get_command_line(
                    EAR=self, shell=shell, env=env
                )
                command_lns.append(cmd_str)

        return ("\n".join(command_lns) + "\n"), shell_vars

    def get_commands_file_hash(self) -> int:
        """Get a hash that can be used to group together runs that will have the same
        commands file.

        This hash is not stable across sessions or machines.

        """
        return self.action.get_commands_file_hash(
            data_idx=self.get_data_idx(),
            action_idx=self.element_action.action_idx,
        )

    @overload
    def try_write_commands(
        self,
        jobscript: Jobscript,
        environments: EnvironmentsList,
        raise_on_unset: Literal[True],
    ) -> Path: ...

    @overload
    def try_write_commands(
        self,
        jobscript: Jobscript,
        environments: EnvironmentsList,
        raise_on_unset: Literal[False] = False,
    ) -> Path | None: ...

    def try_write_commands(
        self,
        jobscript: Jobscript,
        environments: EnvironmentsList,
        raise_on_unset: bool = False,
    ) -> Path | None:
        """Attempt to write the commands file for this run."""
        app_name = self._app.package_name
        try:
            commands, shell_vars = self.compose_commands(
                environments=environments,
                shell=jobscript.shell,
            )
        except UnsetParameterDataError:
            if raise_on_unset:
                raise
            self._app.submission_logger.debug(
                f"cannot yet write commands file for run ID {self.id_}; unset parameters"
            )
            return None

        for cmd_idx, var_dat in shell_vars.items():
            for param_name, shell_var_name, st_typ in var_dat:
                commands += jobscript.shell.format_save_parameter(
                    workflow_app_alias=jobscript.workflow_app_alias,
                    param_name=param_name,
                    shell_var_name=shell_var_name,
                    cmd_idx=cmd_idx,
                    stderr=(st_typ == "stderr"),
                    app_name=app_name,
                )

        commands_fmt = jobscript.shell.format_commands_file(app_name, commands)

        if jobscript.resources.combine_scripts:
            stem = f"js_{jobscript.index}"  # TODO: refactor
        else:
            stem = str(self.id_)

        cmd_file_name = f"{stem}{jobscript.shell.JS_EXT}"
        cmd_file_path: Path = jobscript.submission.commands_path / cmd_file_name
        with cmd_file_path.open("wt", newline="\n") as fp:
            fp.write(commands_fmt)

        return cmd_file_path

    @contextlib.contextmanager
    def raise_on_failure_threshold(self) -> Iterator[dict[str, UnsetParamTracker]]:
        """Context manager to track parameter types and associated run IDs for which those
        parameters were found to be unset when accessed via
        `WorkflowTask._get_merged_parameter_data`.

        """
        self.workflow._is_tracking_unset = True
        self.workflow._tracked_unset = defaultdict(
            lambda: UnsetParamTracker(run_ids=set(), group_size=-1)
        )
        try:
            yield dict(self.workflow._tracked_unset)
        except:
            raise
        else:
            try:
                for schema_inp in self.task.template.schema.inputs:
                    inp_path = f"inputs.{schema_inp.typ}"
                    if inp_path in self.workflow._tracked_unset:
                        unset_tracker = self.workflow._tracked_unset[inp_path]
                        unset_num = len(unset_tracker.run_ids)
                        unset_fraction = unset_num / unset_tracker.group_size
                        if isinstance(schema_inp.allow_failed_dependencies, float):
                            # `True` is converted to 1.0 on SchemaInput init
                            if unset_fraction > schema_inp.allow_failed_dependencies:
                                raise UnsetParameterFractionLimitExceededError(
                                    schema_inp,
                                    self.task,
                                    unset_fraction,
                                    log=self._app.submission_logger,
                                )
                        elif isinstance(schema_inp.allow_failed_dependencies, int):
                            if unset_num > schema_inp.allow_failed_dependencies:
                                raise UnsetParameterNumberLimitExceededError(
                                    schema_inp,
                                    self.task,
                                    unset_num,
                                    log=self._app.submission_logger,
                                )
            finally:
                self.workflow._is_tracking_unset = False
                self.workflow._tracked_unset = None
        finally:
            self.workflow._is_tracking_unset = False
            self.workflow._tracked_unset = None


class ElementAction(AppAware):
    """
    An abstract representation of an element's action at a particular iteration and
    the runs that enact that element iteration.

    Parameters
    ----------
    element_iteration:
        The iteration
    action_idx:
        The action index.
    runs:
        The list of run indices.
    """

    def __init__(
        self,
        element_iteration: ElementIteration,
        action_idx: int,
        runs: dict[Mapping[str, Any], Any],
    ):
        self._element_iteration = element_iteration
        self._action_idx = action_idx
        self._runs = runs

        # assigned on first access of corresponding properties:
        self._run_objs: list[ElementActionRun] | None = None
        self._inputs: ElementInputs | None = None
        self._outputs: ElementOutputs | None = None
        self._resources: ElementResources | None = None
        self._input_files: ElementInputFiles | None = None
        self._output_files: ElementOutputFiles | None = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"iter_ID={self.element_iteration.id_}, "
            f"scope={self.action.get_precise_scope().to_string()!r}, "
            f"action_idx={self.action_idx}, num_runs={self.num_runs}"
            f")"
        )

    @property
    def element_iteration(self) -> ElementIteration:
        """
        The iteration for this action.
        """
        return self._element_iteration

    @property
    def element(self) -> Element:
        """
        The element for this action.
        """
        return self.element_iteration.element

    @property
    def num_runs(self) -> int:
        """
        The number of runs associated with this action.
        """
        return len(self._runs)

    @property
    def runs(self) -> list[ElementActionRun]:
        """
        The EARs that this action is enacted by.
        """
        if self._run_objs is None:
            self._run_objs = [
                self._app.ElementActionRun(
                    element_action=self,
                    index=idx,
                    **{
                        k: v
                        for k, v in run_info.items()
                        if k not in ("elem_iter_ID", "action_idx")
                    },
                )
                for idx, run_info in enumerate(self._runs)
            ]
        return self._run_objs

    @property
    def task(self) -> WorkflowTask:
        """
        The task that this action is an instance of.
        """
        return self.element_iteration.task

    @property
    def action_idx(self) -> int:
        """
        The index of the action.
        """
        return self._action_idx

    @property
    def action(self) -> Action:
        """
        The abstract task that this is a concrete model of.
        """
        return self.task.template.get_schema_action(self.action_idx)

    @property
    def inputs(self) -> ElementInputs:
        """
        The inputs to this action.
        """
        if not self._inputs:
            self._inputs = self._app.ElementInputs(element_action=self)
        return self._inputs

    @property
    def outputs(self) -> ElementOutputs:
        """
        The outputs from this action.
        """
        if not self._outputs:
            self._outputs = self._app.ElementOutputs(element_action=self)
        return self._outputs

    @property
    def input_files(self) -> ElementInputFiles:
        """
        The input files to this action.
        """
        if not self._input_files:
            self._input_files = self._app.ElementInputFiles(element_action=self)
        return self._input_files

    @property
    def output_files(self) -> ElementOutputFiles:
        """
        The output files from this action.
        """
        if not self._output_files:
            self._output_files = self._app.ElementOutputFiles(element_action=self)
        return self._output_files

    def get_data_idx(self, path: str | None = None, run_idx: int = -1) -> DataIndex:
        """
        Get the data index for some path/run.
        """
        return self.element_iteration.get_data_idx(
            path,
            action_idx=self.action_idx,
            run_idx=run_idx,
        )

    @overload
    def get_parameter_sources(
        self,
        path: str | None = None,
        *,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: Literal[False] = False,
        use_task_index: bool = False,
    ) -> Mapping[str, ParamSource | list[ParamSource]]: ...

    @overload
    def get_parameter_sources(
        self,
        path: str | None = None,
        *,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: Literal[True],
        use_task_index: bool = False,
    ) -> Mapping[str, str]: ...

    def get_parameter_sources(
        self,
        path: str | None = None,
        *,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ) -> Mapping[str, str] | Mapping[str, ParamSource | list[ParamSource]]:
        """
        Get information about where parameters originated.
        """
        if as_strings:
            return self.element_iteration.get_parameter_sources(
                path,
                action_idx=self.action_idx,
                run_idx=run_idx,
                typ=typ,
                as_strings=True,
                use_task_index=use_task_index,
            )
        return self.element_iteration.get_parameter_sources(
            path,
            action_idx=self.action_idx,
            run_idx=run_idx,
            typ=typ,
            as_strings=False,
            use_task_index=use_task_index,
        )

    def get(
        self,
        path: str | None = None,
        run_idx: int = -1,
        default: Any | None = None,
        raise_on_missing: bool = False,
        raise_on_unset: bool = False,
    ) -> Any:
        """
        Get the value of a parameter.
        """
        return self.element_iteration.get(
            path=path,
            action_idx=self.action_idx,
            run_idx=run_idx,
            default=default,
            raise_on_missing=raise_on_missing,
            raise_on_unset=raise_on_unset,
        )

    def get_parameter_names(self, prefix: str) -> list[str]:
        """Get parameter types associated with a given prefix.

        For inputs, labels are ignored.
        See :py:meth:`.Action.get_parameter_names` for more information.

        Parameters
        ----------
        prefix
            One of "inputs", "outputs", "input_files", "output_files".

        """
        return self.action.get_parameter_names(prefix)


@final
class ActionScope(JSONLike):
    """Class to represent the identification of a subset of task schema actions by a
    filtering process.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="typ",
            json_like_name="type",
            class_name="ActionScopeType",
            is_enum=True,
        ),
    )

    __ACTION_SCOPE_RE: ClassVar[Pattern] = re.compile(r"(\w*)(?:\[(.*)\])?")

    def __init__(self, typ: ActionScopeType | str, **kwargs):
        if isinstance(typ, str):
            #: Action scope type.
            self.typ = self._app.ActionScopeType[typ.upper()]
        else:
            self.typ = typ

        #: Any provided extra keyword arguments.
        self.kwargs = {k: v for k, v in kwargs.items() if v is not None}

        if bad_keys := set(kwargs) - ACTION_SCOPE_ALLOWED_KWARGS[self.typ.name]:
            raise TypeError(
                f"The following keyword arguments are unknown for ActionScopeType "
                f"{self.typ.name}: {bad_keys}."
            )

    def __repr__(self) -> str:
        kwargs_str = ""
        if self.kwargs:
            kwargs_str = ", ".join(f"{k}={v!r}" for k, v in self.kwargs.items())
        return f"{self.__class__.__name__}.{self.typ.name.lower()}({kwargs_str})"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.typ is other.typ and self.kwargs == other.kwargs

    class __customdict(dict):
        pass

    @classmethod
    def _parse_from_string(cls, string: str) -> dict[str, str]:
        if not (match := cls.__ACTION_SCOPE_RE.search(string)):
            raise TypeError(f"unparseable ActionScope: '{string}'")
        typ_str, kwargs_str = match.groups()
        # The types of the above two variables are idiotic, but bug reports to fix it
        # get closed because "it would break existing code that makes dumb assumptions"
        kwargs: dict[str, str] = cls.__customdict({"type": cast("str", typ_str)})
        if kwargs_str:
            for pair_str in kwargs_str.split(","):
                name, val = pair_str.split("=")
                kwargs[name.strip()] = val.strip()
        return kwargs

    def to_string(self) -> str:
        """
        Render this action scope as a string.
        """
        kwargs_str = ""
        if self.kwargs:
            kwargs_str = "[" + ", ".join(f"{k}={v}" for k, v in self.kwargs.items()) + "]"
        return f"{self.typ.name.lower()}{kwargs_str}"

    @classmethod
    def _from_json_like(
        cls,
        json_like: Mapping[str, Any] | Sequence[Mapping[str, Any]],
        shared_data: Mapping[str, Any],
    ) -> Self:
        if not isinstance(json_like, Mapping):
            raise TypeError("only mappings are supported for becoming an ActionScope")
        if not isinstance(json_like, cls.__customdict):
            # Wasn't processed by _parse_from_string() already
            json_like = {"type": json_like["type"], **json_like.get("kwargs", {})}
        return super()._from_json_like(json_like, shared_data)

    @classmethod
    def any(cls) -> ActionScope:
        """
        Any scope.
        """
        return cls(typ=ActionScopeType.ANY)

    @classmethod
    def main(cls) -> ActionScope:
        """
        The main scope.
        """
        return cls(typ=ActionScopeType.MAIN)

    @classmethod
    def processing(cls) -> ActionScope:
        """
        The processing scope.
        """
        return cls(typ=ActionScopeType.PROCESSING)

    @classmethod
    def input_file_generator(cls, file: str | None = None) -> ActionScope:
        """
        The scope of an input file generator.
        """
        return cls(typ=ActionScopeType.INPUT_FILE_GENERATOR, file=file)

    @classmethod
    def output_file_parser(cls, output: Parameter | str | None = None) -> ActionScope:
        """
        The scope of an output file parser.
        """
        return cls(typ=ActionScopeType.OUTPUT_FILE_PARSER, output=output)


@dataclass()
@hydrate
class ActionEnvironment(JSONLike):
    """
    The environment that an action is enacted within.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="scope",
            class_name="ActionScope",
        ),
    )

    #: The environment document.
    environment: Mapping[str, Any]
    #: The scope.
    scope: ActionScope

    def __init__(
        self, environment: str | dict[str, Any], scope: ActionScope | None = None
    ):
        if scope is None:
            self.scope = self._app.ActionScope.any()
        else:
            self.scope = scope

        if isinstance(environment, str):
            self.environment = {"name": environment}
        else:
            if "name" not in environment:
                raise ActionEnvironmentMissingNameError(environment)
            self.environment = copy.deepcopy(environment)


class ActionRule(JSONLike):
    """
    Class to represent a rule/condition that must be True if an action is to be
    included.

    Parameters
    ----------
    rule: ~hpcflow.app.Rule
        The rule to apply.
    check_exists: str
        A special rule that is enabled if this named attribute is present.
    check_missing: str
        A special rule that is enabled if this named attribute is absent.
    path: str
        Where to find the attribute to check.
    condition: dict | ConditionLike
        A more complex condition to apply.
    cast: str
        The name of a class to cast the attribute to before checking.
    doc: str
        Documentation for this rule, if any.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(name="rule", class_name="Rule"),
    )

    def __init__(
        self,
        rule: Rule | None = None,
        check_exists: str | None = None,
        check_missing: str | None = None,
        path: str | None = None,
        condition: dict[str, Any] | ConditionLike | None = None,
        cast: str | None = None,
        doc: str | None = None,
    ):
        if rule is None:
            #: The rule to apply.
            self.rule = self._app.Rule(
                check_exists=check_exists,
                check_missing=check_missing,
                path=path,
                condition=condition,
                cast=cast,
                doc=doc,
            )
        elif any(
            arg is not None
            for arg in (check_exists, check_missing, path, condition, cast, doc)
        ):
            raise TypeError(
                f"{self.__class__.__name__} `rule` specified in addition to rule "
                f"constructor arguments."
            )
        else:
            self.rule = rule

        #: The action that contains this rule.
        self.action: Action | None = None  # assigned by parent action
        #: The command that is guarded by this rule.
        self.command: Command | None = None  # assigned by parent command

    def __eq__(self, other: Any) -> bool:
        if type(other) is not self.__class__:
            return False
        return self.rule == other.rule

    @property
    def __parent_action(self) -> Action:
        if self.action:
            return self.action
        else:
            assert self.command
            act = self.command.action
            assert act
            return act

    @TimeIt.decorator
    def test(self, element_iteration: ElementIteration) -> bool:
        """
        Test if this rule holds for a particular iteration.

        Parameter
        ---------
        element_iteration:
            The iteration to apply this rule to.
        """

        return self.rule.test(
            element_like=element_iteration,
            action=self.__parent_action,
        )

    @classmethod
    def check_exists(cls, check_exists: str) -> ActionRule:
        """
        Make an action rule that checks if a named attribute is present.

        Parameter
        ---------
        check_exists:
            The path to the attribute to check for.
        """
        return cls(rule=cls._app.Rule(check_exists=check_exists))

    @classmethod
    def check_missing(cls, check_missing: str) -> ActionRule:
        """
        Make an action rule that checks if a named attribute is absent.

        Parameter
        ---------
        check_missing:
            The path to the attribute to check for.
        """
        return cls(rule=cls._app.Rule(check_missing=check_missing))


_ALL_OTHER_SYM = "*"


class Action(JSONLike):
    """
    An atomic component of a workflow that will be enacted within an iteration
    structure.

    Parameters
    ----------
    environments: list[ActionEnvironment]
        The environments in which this action can run.
    commands: list[~hpcflow.app.Command]
        The commands to be run by this action.
    script: str
        The name of the Python script to run.
    script_data_in: str
        Information about data input to the script.
    script_data_out: str
        Information about data output from the script.
    script_data_files_use_opt: bool
        If True, script data input and output file paths will be passed to the script
        execution command line with an option like ``--input-json`` or ``--output-hdf5``
        etc. If False, the file paths will be passed on their own. For Python scripts,
        options are always passed, and this parameter is overwritten to be True,
        regardless of its initial value.
    script_exe: str
        The executable to use to run the script.
    script_pass_env_spec: bool
        Whether to pass the environment details to the script.
    abortable: bool
        Whether this action can be aborted.
    input_file_generators: list[~hpcflow.app.InputFileGenerator]
        Any applicable input file generators.
    output_file_parsers: list[~hpcflow.app.OutputFileParser]
        Any applicable output file parsers.
    input_files: list[~hpcflow.app.FileSpec]
        The input files to the action's commands.
    output_files: list[~hpcflow.app.FileSpec]
        The output files from the action's commands.
    rules: list[ActionRule]
        How to determine whether to run the action.
    save_files: list[str]
        The names of files to be explicitly saved after each step.
    clean_up: list[str]
        The names of files to be deleted after each step.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="commands",
            class_name="Command",
            is_multiple=True,
            parent_ref="action",
        ),
        ChildObjectSpec(
            name="input_file_generators",
            is_multiple=True,
            class_name="InputFileGenerator",
            dict_key_attr="input_file",
        ),
        ChildObjectSpec(
            name="output_file_parsers",
            is_multiple=True,
            class_name="OutputFileParser",
            dict_key_attr="output",
        ),
        ChildObjectSpec(
            name="input_files",
            is_multiple=True,
            class_name="FileSpec",
            shared_data_name="command_files",
        ),
        ChildObjectSpec(
            name="output_files",
            is_multiple=True,
            class_name="FileSpec",
            shared_data_name="command_files",
        ),
        ChildObjectSpec(
            name="environments",
            class_name="ActionEnvironment",
            is_multiple=True,
        ),
        ChildObjectSpec(
            name="rules",
            class_name="ActionRule",
            is_multiple=True,
            parent_ref="action",
        ),
        ChildObjectSpec(
            name="save_files",
            class_name="FileSpec",
            is_multiple=True,
            shared_data_primary_key="label",
            shared_data_name="command_files",
        ),
        ChildObjectSpec(
            name="clean_up",
            class_name="FileSpec",
            is_multiple=True,
            shared_data_primary_key="label",
            shared_data_name="command_files",
        ),
    )
    _script_data_formats: ClassVar[tuple[str, ...]] = ("direct", "json", "hdf5")

    def __init__(
        self,
        environments: list[ActionEnvironment] | None = None,
        commands: list[Command] | None = None,
        script: str | None = None,
        script_data_in: str | Mapping[str, str | ScriptData] | None = None,
        script_data_out: str | Mapping[str, str | ScriptData] | None = None,
        script_data_files_use_opt: bool = False,
        script_exe: str | None = None,
        script_pass_env_spec: bool = False,
        abortable: bool = False,
        input_file_generators: list[InputFileGenerator] | None = None,
        output_file_parsers: list[OutputFileParser] | None = None,
        input_files: list[FileSpec] | None = None,
        output_files: list[FileSpec] | None = None,
        rules: list[ActionRule] | None = None,
        save_files: list[FileSpec] | None = None,
        clean_up: list[str] | None = None,
        requires_dir: bool | None = None,
    ):
        #: The commands to be run by this action.
        self.commands = commands or []
        #: The name of the Python script to run.
        self.script = script
        #: Information about data input to the script.
        self.script_data_in: dict[str, ScriptData] | None = None
        self._script_data_in = script_data_in
        #: Information about data output from the script.
        self.script_data_out: dict[str, ScriptData] | None = None
        self._script_data_out = script_data_out
        #: If True, script data input and output file paths will be passed to the script
        #: execution command line with an option like `--input-json` or `--output-hdf5`
        #: etc. If False, the file paths will be passed on their own. For Python scripts,
        #: options are always passed, and this parameter is overwritten to be True,
        #: regardless of its initial value.
        self.script_data_files_use_opt = (
            script_data_files_use_opt if not self.script_is_python_snippet else True
        )
        #: The executable to use to run the script.
        self.script_exe = script_exe.lower() if script_exe else None
        #: Whether to pass the environment details to the script.
        self.script_pass_env_spec = script_pass_env_spec
        #: The environments in which this action can run.
        self.environments = environments or [
            self._app.ActionEnvironment(environment="null_env")
        ]
        #: Whether this action can be aborted.
        self.abortable = abortable
        #: Any applicable input file generators.
        self.input_file_generators = input_file_generators or []
        #: Any applicable output file parsers.
        self.output_file_parsers = output_file_parsers or []
        #: The input files to the action's commands.
        self.input_files = self.__resolve_input_files(input_files or [])
        #: The output files from the action's commands.
        self.output_files = self.__resolve_output_files(output_files or [])
        #: How to determine whether to run the action.
        self.rules = rules or []
        #: The names of files to be explicitly saved after each step.
        self.save_files = save_files or []
        #: The names of files to be deleted after each step.
        self.clean_up = clean_up or []

        if requires_dir is None:
            requires_dir = (
                True if self.input_file_generators or self.output_file_parsers else False
            )
        self.requires_dir = requires_dir

        self._task_schema: TaskSchema | None = None  # assigned by parent TaskSchema
        self._from_expand = False  # assigned on creation of new Action by `expand`

        self._set_parent_refs()

    def process_script_data_formats(self) -> None:
        """
        Convert script data information into standard form.
        """
        self.script_data_in = self.__process_script_data(self._script_data_in, "inputs")
        self.script_data_out = self.__process_script_data(
            self._script_data_out, "outputs"
        )

    def __process_script_data_str(
        self, data_fmt: str, param_names: Iterable[str]
    ) -> dict[str, ScriptData]:
        # include all input parameters, using specified data format
        data_fmt = data_fmt.lower()
        return {k: {"format": data_fmt} for k in param_names}

    def __process_script_data_dict(
        self,
        data_fmt: Mapping[str, str | ScriptData],
        prefix: str,
        param_names: Iterable[str],
    ) -> dict[str, ScriptData]:
        all_params: dict[str, ScriptData] = {}
        for nm, v in data_fmt.items():
            # values might be strings, or dicts with "format" and potentially other
            # kwargs:
            if isinstance(v, dict):
                # Make sure format is first key
                v2: ScriptData = {
                    "format": v["format"],
                }
                all_params[nm] = v2
                v2.update(v)
            else:
                all_params[nm] = {"format": v.lower()}

        if prefix == "inputs":
            # expand unlabelled-multiple inputs to multiple labelled inputs:
            multi_types = set(self.task_schema.multi_input_types)
            multis: dict[str, ScriptData] = {}
            for nm in tuple(all_params):
                if nm in multi_types:
                    k_fmt = all_params.pop(nm)
                    for name in param_names:
                        if name.startswith(nm):
                            multis[name] = copy.deepcopy(k_fmt)
            if multis:
                all_params = {
                    **multis,
                    **all_params,
                }

        if _ALL_OTHER_SYM in all_params:
            # replace catch-all with all other input/output names:
            other_fmt = all_params[_ALL_OTHER_SYM]
            all_params = {k: v for k, v in all_params.items() if k != _ALL_OTHER_SYM}
            for name in set(param_names).difference(all_params):
                all_params[name] = copy.deepcopy(other_fmt)
        return all_params

    def __process_script_data(
        self, data_fmt: str | Mapping[str, str | ScriptData] | None, prefix: str
    ) -> dict[str, ScriptData]:
        if not data_fmt:
            return {}

        param_names = self.get_parameter_names(prefix)
        if isinstance(data_fmt, str):
            all_params = self.__process_script_data_str(data_fmt, param_names)
        else:
            all_params = self.__process_script_data_dict(data_fmt, prefix, param_names)

        # validation:
        allowed_keys = ("format", "all_iterations")
        for k, v in all_params.items():
            # validate parameter name (sub-parameters are allowed):
            if k.split(".")[0] not in param_names:
                raise UnknownScriptDataParameter(k, prefix, param_names)
            # validate format:
            if v["format"] not in self._script_data_formats:
                raise UnsupportedScriptDataFormat(
                    v, prefix[:-1], k, self._script_data_formats
                )
            if any((bad_key := k2) for k2 in v if k2 not in allowed_keys):
                raise UnknownScriptDataKey(bad_key, allowed_keys)

        return all_params

    @property
    def script_data_in_grouped(self) -> Mapping[str, Mapping[str, Mapping[str, str]]]:
        """Get input parameter types by script data-in format."""
        if self.script_data_in is None:
            self.process_script_data_formats()
            assert self.script_data_in is not None
        return swap_nested_dict_keys(
            dct=cast("dict", self.script_data_in), inner_key="format"
        )

    @property
    def script_data_out_grouped(self) -> Mapping[str, Mapping[str, Mapping[str, str]]]:
        """Get output parameter types by script data-out format."""
        if self.script_data_out is None:
            self.process_script_data_formats()
            assert self.script_data_out is not None
        return swap_nested_dict_keys(
            dct=cast("dict", self.script_data_out), inner_key="format"
        )

    @property
    def script_data_in_has_files(self) -> bool:
        """Return True if the script requires some inputs to be passed via an
        intermediate file format."""
        # TODO: should set `requires_dir` to True if this is True?
        return bool(set(self.script_data_in_grouped) - {"direct"})  # TODO: test

    @property
    def script_data_out_has_files(self) -> bool:
        """Return True if the script produces some outputs via an intermediate file
        format."""
        # TODO: should set `requires_dir` to True if this is True?
        return bool(set(self.script_data_out_grouped) - {"direct"})  # TODO: test

    @property
    def script_data_in_has_direct(self) -> bool:
        """Return True if the script requires some inputs to be passed directly from the
        app."""
        return "direct" in self.script_data_in_grouped  # TODO: test

    @property
    def script_data_out_has_direct(self) -> bool:
        """Return True if the script produces some outputs to be passed directly to the
        app."""
        return "direct" in self.script_data_out_grouped  # TODO: test

    @property
    def script_is_python_snippet(self) -> bool:
        """Return True if the script is a Python snippet script (determined by the file
        extension)"""
        if self.script and (snip_path := self.get_snippet_script_path(self.script)):
            return snip_path.suffix == ".py"
        return False

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        d = super()._postprocess_to_dict(d)
        d["script_data_in"] = d.pop("_script_data_in")
        d["script_data_out"] = d.pop("_script_data_out")
        return d

    @property
    def is_IFG(self):
        return bool(self.input_file_generators)

    @property
    def is_OFP(self):
        return bool(self.output_file_parsers)

    def __deepcopy__(self, memo: dict[int, Any]) -> Self:
        kwargs = self.to_dict()
        _from_expand = kwargs.pop("_from_expand")
        _task_schema = kwargs.pop("_task_schema", None)
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._from_expand = _from_expand
        obj._task_schema = _task_schema
        return obj

    @property
    def task_schema(self) -> TaskSchema:
        """
        The task schema that this action came from.
        """
        assert self._task_schema is not None
        return self._task_schema

    def __resolve_input_files(self, input_files: list[FileSpec]) -> list[FileSpec]:
        in_files = input_files
        for ifg in self.input_file_generators:
            if ifg.input_file not in in_files:
                in_files.append(ifg.input_file)
        return in_files

    def __resolve_output_files(self, output_files: list[FileSpec]) -> list[FileSpec]:
        out_files = output_files
        for ofp in self.output_file_parsers:
            for out_file in ofp.output_files:
                if out_file not in out_files:
                    out_files.append(out_file)
        return out_files

    def __repr__(self) -> str:
        IFGs = {
            ifg.input_file.label: [inp.typ for inp in ifg.inputs]
            for ifg in self.input_file_generators
        }
        OFPs = {
            ofp.output.typ if ofp.output else f"OFP_{idx}": [
                out_file.label for out_file in ofp.output_files
            ]
            for idx, ofp in enumerate(self.output_file_parsers)
        }

        out: list[str] = []
        if self.commands:
            out.append(f"commands={self.commands!r}")
        if self.script:
            out.append(f"script={self.script!r}")
        if self.environments:
            out.append(f"environments={self.environments!r}")
        if IFGs:
            out.append(f"input_file_generators={IFGs!r}")
        if OFPs:
            out.append(f"output_file_parsers={OFPs!r}")
        if self.rules:
            out.append(f"rules={self.rules!r}")

        return f"{self.__class__.__name__}({', '.join(out)})"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return (
            self.commands == other.commands
            and self.script == other.script
            and self.environments == other.environments
            and self.abortable == other.abortable
            and self.input_file_generators == other.input_file_generators
            and self.output_file_parsers == other.output_file_parsers
            and self.rules == other.rules
        )

    @staticmethod
    def env_spec_to_hashable(
        env_spec: Mapping[str, Any],
    ) -> tuple[tuple[str, ...], tuple[Any, ...]]:
        keys, values = zip(*env_spec.items()) if env_spec else ((), ())
        return tuple(keys), tuple(values)

    @staticmethod
    def env_spec_from_hashable(
        env_spec_h: tuple[tuple[str, ...], tuple[Any, ...]],
    ) -> dict[str, Any]:
        return dict(zip(*env_spec_h))

    def get_script_determinants(self) -> tuple:
        """Get the attributes that affect the script."""
        return (
            self.script,
            self.script_data_in,
            self.script_data_out,
            self.script_data_files_use_opt,
            self.script_exe,
        )

    def get_script_determinant_hash(self, env_specs: dict | None = None) -> int:
        """Get a hash of the instance attributes that uniquely determine the script.

        The hash is not stable across sessions or machines.

        """
        env_specs = env_specs or {}
        return get_hash(
            (self.get_script_determinants(), self.env_spec_to_hashable(env_specs))
        )

    @classmethod
    def _json_like_constructor(cls, json_like) -> Self:
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""
        _from_expand = json_like.pop("_from_expand", None)
        obj = cls(**json_like)
        obj._from_expand = _from_expand
        return obj

    def get_parameter_dependence(self, parameter: SchemaParameter) -> ParameterDependence:
        """Find if/where a given parameter is used by the action."""
        # names of input files whose generation requires this parameter
        writer_files = [
            ifg.input_file
            for ifg in self.input_file_generators
            if parameter.parameter in ifg.inputs
        ]
        # TODO: indices of commands in which this parameter appears
        commands: list[int] = []
        return {"input_file_writers": writer_files, "commands": commands}

    def __get_resolved_action_env(
        self,
        relevant_scopes: tuple[ActionScopeType, ...],
        input_file_generator: InputFileGenerator | None = None,
        output_file_parser: OutputFileParser | None = None,
        commands: list[Command] | None = None,
    ) -> ActionEnvironment:
        possible = [
            env
            for env in self.environments
            if env.scope and env.scope.typ in relevant_scopes
        ]
        if not possible:
            if input_file_generator:
                raise MissingCompatibleActionEnvironment(
                    f"input file generator {input_file_generator.input_file.label!r}"
                )
            elif output_file_parser:
                if output_file_parser.output is not None:
                    ofp_id = output_file_parser.output.typ
                else:
                    ofp_id = "<unnamed>"
                raise MissingCompatibleActionEnvironment(f"output file parser {ofp_id!r}")
            else:
                raise MissingCompatibleActionEnvironment(f"commands {commands!r}")

        # get max by scope type specificity:
        return max(possible, key=lambda i: i.scope.typ.value)

    def get_input_file_generator_action_env(
        self, input_file_generator: InputFileGenerator
    ) -> ActionEnvironment:
        """
        Get the actual environment to use for an input file generator.
        """
        return self.__get_resolved_action_env(
            relevant_scopes=(
                ActionScopeType.ANY,
                ActionScopeType.PROCESSING,
                ActionScopeType.INPUT_FILE_GENERATOR,
            ),
            input_file_generator=input_file_generator,
        )

    def get_output_file_parser_action_env(
        self, output_file_parser: OutputFileParser
    ) -> ActionEnvironment:
        """
        Get the actual environment to use for an output file parser.
        """
        return self.__get_resolved_action_env(
            relevant_scopes=(
                ActionScopeType.ANY,
                ActionScopeType.PROCESSING,
                ActionScopeType.OUTPUT_FILE_PARSER,
            ),
            output_file_parser=output_file_parser,
        )

    def get_commands_action_env(self) -> ActionEnvironment:
        """
        Get the actual environment to use for the action commands.
        """
        return self.__get_resolved_action_env(
            relevant_scopes=(ActionScopeType.ANY, ActionScopeType.MAIN),
            commands=self.commands,
        )

    def get_environment_name(self) -> str:
        """
        Get the name of the primary environment.
        """
        return self.get_environment_spec()["name"]

    def get_environment_spec(self) -> Mapping[str, Any]:
        """
        Get the specification for the primary envionment, assuming it has been expanded.
        """
        if not self._from_expand:
            raise RuntimeError(
                "Cannot choose a single environment from this action because it is not "
                "expanded, meaning multiple action environments might exist."
            )
        return self.environments[0].environment

    def get_environment(self) -> Environment:
        """
        Get the primary environment.
        """
        return self._app.envs.get(**self.get_environment_spec())

    @staticmethod
    def is_snippet_script(script: str | None) -> bool:
        """Returns True if the provided script string represents a script snippets that is
        to be modified before execution (e.g. to receive and provide parameter data)."""
        if script is None:
            return False
        return script.startswith("<<script:")

    __SCRIPT_NAME_RE: ClassVar[Pattern] = re.compile(
        r"\<\<script:(?:.*(?:\/|\\))*(.*)\>\>"
    )

    @classmethod
    def get_script_name(cls, script: str) -> str:
        """Return the script name.

        If `script` is a snippet script path, this method returns the name of the script
        (i.e. the final component of the path). If `script` is not a snippet script path
        (does not start with "<<script:"), then `script` is simply returned.

        """
        if cls.is_snippet_script(script):
            if not (match_obj := cls.__SCRIPT_NAME_RE.match(script)):
                raise ValueError("incomplete <<script:>>")
            return match_obj[1]
        # a script we can expect in the working directory, which might have been generated
        # by a previous action:
        return script

    @overload
    def get_script_artifact_name(
        self,
        env_spec: Mapping[str, Any],
        act_idx: int,
        ret_specifiers: Literal[False] = False,
        include_suffix: bool = True,
        specs_suffix_delim: str = ".",
    ) -> tuple[str, Path]: ...

    @overload
    def get_script_artifact_name(
        self,
        env_spec: Mapping[str, Any],
        act_idx: int,
        ret_specifiers: Literal[True],
        include_suffix: bool = True,
        specs_suffix_delim: str = ".",
    ) -> tuple[str, Path, dict]: ...

    def get_script_artifact_name(
        self,
        env_spec: Mapping[str, Any],
        act_idx: int,
        ret_specifiers: bool = False,
        include_suffix: bool = True,
        specs_suffix_delim: str = ".",
    ) -> tuple[str, Path] | tuple[str, Path, dict]:
        """Return the script name that is used when writing the script to the artifacts
        directory within the workflow.

        Like `Action.get_script_name`, this is only applicable for snippet scripts.

        """
        snip_path_specs = self.get_snippet_script_path(
            self.script,
            env_spec,
            ret_specifiers=True,
        )
        assert snip_path_specs
        snip_path, specifiers = snip_path_specs
        specs_suffix = "__".join(f"{k}_{v}" for k, v in specifiers.items())
        if specs_suffix:
            specs_suffix = f"{specs_suffix_delim}{specs_suffix}"

        name = f"{self.task_schema.name}_act_{act_idx}{specs_suffix}"
        if include_suffix:
            name += snip_path.suffix

        if ret_specifiers:
            return name, snip_path, specifiers
        else:
            return name, snip_path

    __SCRIPT_RE: ClassVar[Pattern] = re.compile(r"\<\<script:(.*:?)\>\>")
    __ENV_RE: ClassVar[Pattern] = re.compile(r"\<\<env:(.*?)\>\>")

    @overload
    @classmethod
    def get_snippet_script_str(
        cls,
        script: str,
        env_spec: Mapping[str, Any] | None = None,
        ret_specifiers: Literal[False] = False,
    ) -> str: ...

    @overload
    @classmethod
    def get_snippet_script_str(
        cls,
        script: str,
        env_spec: Mapping[str, Any] | None = None,
        *,
        ret_specifiers: Literal[True],
    ) -> tuple[str, dict[str, Any]]: ...

    @overload
    @classmethod
    def get_snippet_script_str(
        cls,
        script: str,
        env_spec: Mapping[str, Any] | None = None,
        *,
        ret_specifiers: bool,
    ) -> str | tuple[str, dict[str, Any]]: ...

    @classmethod
    def get_snippet_script_str(
        cls,
        script: str,
        env_spec: Mapping[str, Any] | None = None,
        ret_specifiers: bool = False,
    ) -> str | tuple[str, dict[str, Any]]:
        """Return the specified snippet `script` with variable substitutions completed.

        Parameters
        ----------
        ret_specifiers
            If True, also return a list of environment specifiers as a dict whose keys are
            specifier keys found in the `script` path and whose values are the
            corresponding values extracted from `env_spec`.

        """
        if not cls.is_snippet_script(script):
            raise ValueError(
                f"Must be an app-data script name (e.g. "
                f"<<script:path/to/app/data/script.py>>), but received {script}"
            )
        if not (match_obj := cls.__SCRIPT_RE.match(script)):
            raise ValueError("incomplete <<script:>>")
        out: str = match_obj[1]

        if env_spec is not None:
            specifiers: dict[str, Any] = {}

            def repl(match_obj):
                spec = match_obj[1]
                specifiers[spec] = env_spec[spec]
                return str(env_spec[spec])

            out = cls.__ENV_RE.sub(
                repl=repl,
                string=out,
            )
            if ret_specifiers:
                return (out, specifiers)
        return out

    @classmethod
    @overload
    def get_snippet_script_path(
        cls,
        script_path: str | None,
        env_spec: Mapping[str, Any] | None = None,
        *,
        ret_specifiers: Literal[True],
    ) -> tuple[Path, dict[str, Any]] | None: ...

    @classmethod
    @overload
    def get_snippet_script_path(
        cls,
        script_path: str | None,
        env_spec: Mapping[str, Any] | None = None,
        *,
        ret_specifiers: Literal[False] = False,
    ) -> Path | None: ...

    @classmethod
    def get_snippet_script_path(
        cls,
        script_path: str | None,
        env_spec: Mapping[str, Any] | None = None,
        *,
        ret_specifiers: bool = False,
    ) -> Path | tuple[Path, dict[str, Any]] | None:
        """Return the specified snippet `script` path, or None if there is no snippet.

        Parameters
        ----------
        ret_specifiers
            If True, also return a list of environment specifiers as a dict whose keys are
            specifier keys found in the `script` path and whose values are the
            corresponding values extracted from `env_spec`.

        """
        if not cls.is_snippet_script(script_path):
            return None

        assert script_path is not None
        path_ = cls.get_snippet_script_str(
            script_path, env_spec, ret_specifiers=ret_specifiers
        )
        if ret_specifiers:
            assert isinstance(path_, tuple)
            path_str, specifiers = path_
        else:
            assert isinstance(path_, str)
            path_str = path_

        path = Path(cls._app.scripts.get(path_str, path_str))

        if ret_specifiers:
            return path, specifiers
        else:
            return path

    @staticmethod
    def __get_param_dump_file_stem(block_act_key: BlockActionKey) -> str:
        return RunDirAppFiles.get_run_param_dump_file_prefix(block_act_key)

    @staticmethod
    def __get_param_load_file_stem(block_act_key: BlockActionKey) -> str:
        return RunDirAppFiles.get_run_param_load_file_prefix(block_act_key)

    def get_param_dump_file_path_JSON(
        self, block_act_key: BlockActionKey, directory: Path | None = None
    ) -> Path:
        """
        Get the path of the JSON dump file.
        """
        directory = directory or Path()
        return directory.joinpath(
            self.__get_param_dump_file_stem(block_act_key) + ".json"
        )

    def get_param_dump_file_path_HDF5(
        self, block_act_key: BlockActionKey, directory: Path | None = None
    ) -> Path:
        """
        Get the path of the HDF5 dump file.
        """
        directory = directory or Path()
        return directory.joinpath(self.__get_param_dump_file_stem(block_act_key) + ".h5")

    def get_param_load_file_path_JSON(
        self, block_act_key: BlockActionKey, directory: Path | None = None
    ) -> Path:
        """
        Get the path of the JSON load file.
        """
        directory = directory or Path()
        return directory.joinpath(
            self.__get_param_load_file_stem(block_act_key) + ".json"
        )

    def get_param_load_file_path_HDF5(
        self, block_act_key: BlockActionKey, directory: Path | None = None
    ) -> Path:
        """
        Get the path of the HDF5 load file.
        """
        directory = directory or Path()
        return directory.joinpath(self.__get_param_load_file_stem(block_act_key) + ".h5")

    def expand(self) -> Sequence[Action]:
        """
        Expand this action into a list of actions if necessary.
        This converts input file generators and output file parsers into their own actions.
        """
        if self._from_expand:
            # already expanded
            return [self]

        # run main if:
        #   - one or more output files are not passed
        # run IFG if:
        #   - one or more output files are not passed
        #   - AND input file is not passed
        # always run OPs, for now

        main_rules = self.rules + [
            self._app.ActionRule.check_missing(f"output_files.{of.label}")
            for of in self.output_files
        ]

        # note we keep the IFG/OPs in the new actions, so we can check the parameters
        # used/produced.

        inp_files: list[FileSpec] = []
        inp_acts: list[Action] = []

        app_caps = self._app.package_name.upper()

        script_cmd_vars = {
            "script_name": f"${app_caps}_RUN_SCRIPT_NAME",
            "script_name_no_ext": f"${app_caps}_RUN_SCRIPT_NAME_NO_EXT",
            "script_dir": f"${app_caps}_RUN_SCRIPT_DIR",
            "script_path": f"${app_caps}_RUN_SCRIPT_PATH",
        }

        for ifg in self.input_file_generators:
            script_exe = "python_script"
            exe = f"<<executable:{script_exe}>>"
            variables = script_cmd_vars if ifg.script else {}
            act_i = self._app.Action(
                commands=[self._app.Command(executable=exe, variables=variables)],
                input_file_generators=[ifg],
                environments=[self.get_input_file_generator_action_env(ifg)],
                rules=main_rules + ifg.get_action_rules(),
                script=ifg.script,
                script_data_in="direct",
                script_data_out="direct",
                script_exe=script_exe,
                script_pass_env_spec=ifg.script_pass_env_spec,
                abortable=ifg.abortable,
                requires_dir=ifg.requires_dir,
            )
            act_i._task_schema = self.task_schema
            if ifg.input_file not in inp_files:
                inp_files.append(ifg.input_file)
            act_i.process_script_data_formats()
            act_i._from_expand = True
            inp_acts.append(act_i)

        out_files: list[FileSpec] = []
        out_acts: list[Action] = []
        for ofp in self.output_file_parsers:
            script_exe = "python_script"
            exe = f"<<executable:{script_exe}>>"
            variables = script_cmd_vars if ofp.script else {}
            act_i = self._app.Action(
                commands=[self._app.Command(executable=exe, variables=variables)],
                output_file_parsers=[ofp],
                environments=[self.get_output_file_parser_action_env(ofp)],
                rules=list(self.rules) + ofp.get_action_rules(),
                script=ofp.script,
                script_data_in="direct",
                script_data_out="direct",
                script_exe=script_exe,
                script_pass_env_spec=ofp.script_pass_env_spec,
                abortable=ofp.abortable,
                requires_dir=ofp.requires_dir,
            )
            act_i._task_schema = self.task_schema
            for j in ofp.output_files:
                if j not in out_files:
                    out_files.append(j)
            act_i.process_script_data_formats()
            act_i._from_expand = True
            out_acts.append(act_i)

        commands = self.commands
        if self.script:
            exe = f"<<executable:{self.script_exe}>>"
            variables = script_cmd_vars if self.script else {}
            args = self.get_script_input_output_file_command_args()
            commands += [
                self._app.Command(executable=exe, arguments=args, variables=variables)
            ]

        # TODO: store script_args? and build command with executable syntax?
        main_act = self._app.Action(
            commands=commands,
            script=self.script,
            script_data_in=self.script_data_in,
            script_data_out=self.script_data_out,
            script_exe=self.script_exe,
            script_pass_env_spec=self.script_pass_env_spec,
            environments=[self.get_commands_action_env()],
            abortable=self.abortable,
            rules=main_rules,
            input_files=inp_files,
            output_files=out_files,
            save_files=self.save_files,
            clean_up=self.clean_up,
            requires_dir=self.requires_dir,
        )
        main_act._task_schema = self.task_schema
        main_act._from_expand = True
        main_act.process_script_data_formats()

        return [*inp_acts, main_act, *out_acts]

    # note: we use "parameter" rather than "input", because it could be a schema input
    # or schema output.
    __PARAMS_RE: ClassVar[Pattern] = re.compile(
        r"\<\<(?:\w+(?:\[(?:.*)\])?\()?parameter:(.*?)\)?\>\>"
    )

    def get_command_input_types(self, sub_parameters: bool = False) -> tuple[str, ...]:
        """Get parameter types from commands.

        Parameters
        ----------
        sub_parameters:
            If True, sub-parameters (i.e. dot-delimited parameter types) will be returned
            untouched. If False (default), only return the root parameter type and
            disregard the sub-parameter part.
        """
        params: set[str] = set()
        for command in self.commands:
            params.update(
                val[1] if sub_parameters else val[1].split(".")[0]
                for val in self.__PARAMS_RE.finditer(command.command or "")
            )
            for arg in command.arguments or ():
                params.update(
                    val[1] if sub_parameters else val[1].split(".")[0]
                    for val in self.__PARAMS_RE.finditer(arg)
                )
            # TODO: consider stdin?
        return tuple(params)

    __FILES_RE: ClassVar[Pattern] = re.compile(r"\<\<file:(.*?)\>\>")

    def get_command_file_labels(self) -> tuple[str, ...]:
        """Get input files types from commands."""
        files: set[str] = set()
        for command in self.commands:
            files.update(self.__FILES_RE.findall(command.command or ""))
            for arg in command.arguments or ():
                files.update(self.__FILES_RE.findall(arg))
            # TODO: consider stdin?
        return tuple(files)

    def get_command_output_types(self) -> tuple[str, ...]:
        """Get parameter types from command stdout and stderr arguments."""
        params: set[str] = set()
        for command in self.commands:
            out_params = command.get_output_types()
            if out_params["stdout"]:
                params.add(out_params["stdout"])
            if out_params["stderr"]:
                params.add(out_params["stderr"])
        return tuple(params)

    def get_command_parameter_types(
        self, sub_parameters: bool = False
    ) -> tuple[str, ...]:
        """Get all parameter types that appear in the commands of this action.

        Parameters
        ----------
        sub_parameters
            If True, sub-parameter inputs (i.e. dot-delimited input types) will be
            returned untouched. If False (default), only return the root parameter type
            and disregard the sub-parameter part.
        """
        # TODO: not sure if we need `input_files`
        return tuple(
            f"inputs.{i}" for i in self.get_command_input_types(sub_parameters)
        ) + tuple(f"input_files.{i}" for i in self.get_command_file_labels())

    def get_input_types(self, sub_parameters: bool = False) -> tuple[str, ...]:
        """Get the input types that are consumed by commands and input file generators of
        this action.

        Parameters
        ----------
        sub_parameters:
            If True, sub-parameters (i.e. dot-delimited parameter types) in command line
            inputs will be returned untouched. If False (default), only return the root
            parameter type and disregard the sub-parameter part.
        """
        if (
            self.script
            and not self.input_file_generators
            and not self.output_file_parsers
        ):
            # TODO: refine this according to `script_data_in`, since this can be used
            # to control the inputs/outputs of a script.
            params = set(self.task_schema.input_types)
        else:
            params = set(self.get_command_input_types(sub_parameters))
            for ifg in self.input_file_generators:
                params.update(inp.typ for inp in ifg.inputs)
            for ofp in self.output_file_parsers:
                params.update(ofp.inputs or ())
        return tuple(params)

    def get_output_types(self) -> tuple[str, ...]:
        """Get the output types that are produced by command standard outputs and errors,
        and by output file parsers of this action."""
        if (
            self.script
            and not self.input_file_generators
            and not self.output_file_parsers
        ):
            params = set(self.task_schema.output_types)
            # TODO: refine this according to `script_data_out`, since this can be used
            # to control the inputs/outputs of a script.
        else:
            params = set(self.get_command_output_types())
            for ofp in self.output_file_parsers:
                if ofp.output is not None:
                    params.add(ofp.output.typ)
                params.update(ofp.outputs or ())
        return tuple(params)

    def get_input_file_labels(self) -> tuple[str, ...]:
        """
        Get the labels from the input files.
        """
        return tuple(in_f.label for in_f in self.input_files)

    def get_output_file_labels(self) -> tuple[str, ...]:
        """
        Get the labels from the output files.
        """
        return tuple(out_f.label for out_f in self.output_files)

    @TimeIt.decorator
    def generate_data_index(
        self,
        act_idx: int,
        EAR_ID: int,
        schema_data_idx: DataIndex,
        all_data_idx: dict[tuple[int, int], DataIndex],
        workflow: Workflow,
        param_source: ParamSource,
    ) -> list[int | list[int]]:
        """Generate the data index for this action of an element iteration whose overall
        data index is passed.

        This mutates `all_data_idx`.
        """

        # output keys must be processed first for this to work, since when processing an
        # output key, we may need to update the index of an output in a previous action's
        # data index, which could affect the data index in an input of this action.
        keys = [f"outputs.{typ}" for typ in self.get_output_types()]
        keys.extend(f"inputs.{typ}" for typ in self.get_input_types())
        keys.extend(f"input_files.{file.label}" for file in self.input_files)
        keys.extend(f"output_files.{file.label}" for file in self.output_files)

        # these are consumed by the OFP, so should not be considered to generate new data:
        OFP_outs = {j for ofp in self.output_file_parsers for j in ofp.outputs or ()}

        # keep all resources and repeats data:
        sub_data_idx = {
            k: v
            for k, v in schema_data_idx.items()
            if ("resources" in k or "repeats" in k)
        }
        param_src_update: list[int | list[int]] = []
        for key in keys:
            sub_param_idx: dict[str, int | list[int]] = {}
            if (
                key.startswith("input_files")
                or key.startswith("output_files")
                or key.startswith("inputs")
                or (
                    key.startswith("outputs") and key.removeprefix("outputs.") in OFP_outs
                )
            ):
                # look for an index in previous data indices (where for inputs we look
                # for *output* parameters of the same name):
                k_idx: int | list[int] | None = None
                for prev_data_idx in all_data_idx.values():
                    if key.startswith("inputs"):
                        k_param = key.removeprefix("inputs.")
                        k_out = f"outputs.{k_param}"
                        if k_out in prev_data_idx:
                            k_idx = prev_data_idx[k_out]
                    elif key in prev_data_idx:
                        k_idx = prev_data_idx[key]

                if k_idx is None:
                    # otherwise take from the schema_data_idx:
                    if key in schema_data_idx:
                        k_idx = schema_data_idx[key]
                        prefix = f"{key}."  # sub-parameter (note dot)
                        # add any associated sub-parameters:
                        sub_param_idx.update(
                            (k, v)
                            for k, v in schema_data_idx.items()
                            if k.startswith(prefix)
                        )
                    else:
                        # otherwise we need to allocate a new parameter datum:
                        # (for input/output_files keys)
                        k_idx = workflow._add_unset_parameter_data(param_source)

            else:
                # outputs
                k_idx = None
                for (_, EAR_ID_i), prev_data_idx in all_data_idx.items():
                    if key in prev_data_idx:
                        k_idx = prev_data_idx[key]

                        # allocate a new parameter datum for this intermediate output:
                        param_source_i = copy.copy(param_source)
                        param_source_i["EAR_ID"] = EAR_ID_i
                        new_k_idx = workflow._add_unset_parameter_data(param_source_i)

                        # mutate `all_data_idx`:
                        prev_data_idx[key] = new_k_idx

                if k_idx is None:
                    # otherwise take from the schema_data_idx:
                    k_idx = schema_data_idx[key]

                # can now set the EAR/act idx in the associated parameter source
                param_src_update.append(k_idx)

            sub_data_idx[key] = k_idx
            sub_data_idx.update(sub_param_idx)

        all_data_idx[act_idx, EAR_ID] = sub_data_idx

        return param_src_update

    def get_possible_scopes(self) -> tuple[ActionScope, ...]:
        """Get the action scopes that are inclusive of this action, ordered by decreasing
        specificity."""

        scope = self.get_precise_scope()
        if self.input_file_generators:
            return (
                scope,
                self._app.ActionScope.input_file_generator(),
                self._app.ActionScope.processing(),
                self._app.ActionScope.any(),
            )
        elif self.output_file_parsers:
            return (
                scope,
                self._app.ActionScope.output_file_parser(),
                self._app.ActionScope.processing(),
                self._app.ActionScope.any(),
            )
        else:
            return (scope, self._app.ActionScope.any())

    def _get_possible_scopes_reversed(self) -> Iterator[ActionScope]:
        """Get the action scopes that are inclusive of this action, ordered by increasing
        specificity."""

        # Fail early if a failure is possible
        precise_scope = self.get_precise_scope()
        yield self._app.ActionScope.any()
        if self.input_file_generators:
            yield self._app.ActionScope.processing()
            yield self._app.ActionScope.input_file_generator()
        elif self.output_file_parsers:
            yield self._app.ActionScope.processing()
            yield self._app.ActionScope.output_file_parser()
        yield precise_scope

    def get_precise_scope(self) -> ActionScope:
        """
        Get the exact scope of this action.
        The action must have been expanded prior to calling this.
        """
        if not self._from_expand:
            raise RuntimeError(
                "Precise scope cannot be unambiguously defined until the Action has been "
                "expanded."
            )

        if self.input_file_generators:
            return self._app.ActionScope.input_file_generator(
                file=self.input_file_generators[0].input_file.label
            )
        elif self.output_file_parsers:
            if self.output_file_parsers[0].output is not None:
                return self._app.ActionScope.output_file_parser(
                    output=self.output_file_parsers[0].output
                )
            else:
                return self._app.ActionScope.output_file_parser()
        else:
            return self._app.ActionScope.main()

    def is_input_type_required(
        self, typ: str, provided_files: Container[FileSpec]
    ) -> bool:
        """
        Determine if the given input type is required by this action.
        """
        # TODO: for now assume a script takes all inputs
        if (
            self.script
            and not self.input_file_generators
            and not self.output_file_parsers
        ):
            return True

        # typ is required if is appears in any command:
        if typ in self.get_command_input_types():
            return True

        # typ is required if used in any input file generators and input file is not
        # provided:
        for ifg in self.input_file_generators:
            if typ in (inp.typ for inp in ifg.inputs) and (
                ifg.input_file not in provided_files
            ):
                return True

        # typ is required if used in any output file parser
        return any(typ in (ofp.inputs or ()) for ofp in self.output_file_parsers)

    @TimeIt.decorator
    def test_rules(self, element_iter: ElementIteration) -> tuple[bool, list[int]]:
        """Test all rules against the specified element iteration."""
        if any(not rule.test(element_iteration=element_iter) for rule in self.rules):
            return False, []
        return True, [
            cmd_idx
            for cmd_idx, cmd in enumerate(self.commands)
            if all(rule.test(element_iteration=element_iter) for rule in cmd.rules)
        ]

    def get_required_executables(self) -> Iterator[str]:
        """Return executable labels required by this action."""
        for command in self.commands:
            yield from command.get_required_executables()

    def compose_source(self, snip_path: Path) -> str:
        """Generate the file contents of this source."""

        script_name = snip_path.name
        with snip_path.open("rt") as fp:
            script_str = fp.read()

        if not self.script_is_python_snippet:
            return script_str

        if self.is_OFP and self.output_file_parsers[0].output is None:
            # might be used just for saving files:
            return ""

        app_caps = self._app.package_name.upper()
        py_imports = dedent(
            """\
            import argparse
            import os
            from pathlib import Path

            import {app_module} as app

            std_path = os.getenv("{app_caps}_RUN_STD_PATH")
            log_path = os.getenv("{app_caps}_RUN_LOG_PATH")
            run_id = int(os.getenv("{app_caps}_RUN_ID"))
            wk_path = os.getenv("{app_caps}_WK_PATH")

            with app.redirect_std_to_file(std_path):

            """
        ).format(app_module=self._app.module, app_caps=app_caps)

        # we must load the workflow (must be python):
        # (note: we previously only loaded the workflow if there were any direct inputs
        # or outputs; now we always load so we can use the method
        # `get_py_script_func_kwargs`)
        py_main_block_workflow_load = dedent(
            """\
                app.load_config(
                    log_file_path=Path(log_path),
                    config_dir=r"{cfg_dir}",
                    config_key=r"{cfg_invoc_key}",
                )
                wk = app.Workflow(wk_path)
                EAR = wk.get_EARs_from_IDs([run_id])[0]
            """
        ).format(
            cfg_dir=self._app.config.config_directory,
            cfg_invoc_key=self._app.config.config_key,
            app_caps=app_caps,
        )

        tab_indent = "    "
        tab_indent_2 = 2 * tab_indent

        func_kwargs_str = dedent(
            """\
            blk_act_key = (
                os.environ["{app_caps}_JS_IDX"],
                os.environ["{app_caps}_BLOCK_IDX"],
                os.environ["{app_caps}_BLOCK_ACT_IDX"],
            )
            with EAR.raise_on_failure_threshold() as unset_params:
                func_kwargs = EAR.get_py_script_func_kwargs(
                    raise_on_unset=False,
                    add_script_files=True,
                    blk_act_key=blk_act_key,
                )            
        """
        ).format(app_caps=app_caps)

        script_main_func = Path(script_name).stem
        func_invoke_str = f"{script_main_func}(**func_kwargs)"
        if not self.is_OFP and "direct" in self.script_data_out_grouped:
            py_main_block_invoke = f"outputs = {func_invoke_str}"
            py_main_block_outputs = dedent(
                """\
                with app.redirect_std_to_file(std_path):
                    for name_i, out_i in outputs.items():
                        wk.set_parameter_value(param_id=EAR.data_idx[f"outputs.{name_i}"], value=out_i)
                """
            )
        elif self.is_OFP:
            py_main_block_invoke = f"output = {func_invoke_str}"
            assert self.output_file_parsers[0].output
            py_main_block_outputs = dedent(
                """\
                with app.redirect_std_to_file(std_path):
                    wk.save_parameter(name="outputs.{output_typ}", value=output, EAR_ID=run_id)
                """
            ).format(output_typ=self.output_file_parsers[0].output.typ)
        else:
            py_main_block_invoke = func_invoke_str
            py_main_block_outputs = ""

        wk_load = (
            "\n" + indent(py_main_block_workflow_load, tab_indent_2)
            if py_main_block_workflow_load
            else ""
        )
        py_main_block = dedent(
            """\
            if __name__ == "__main__":
            {py_imports}{wk_load}
            {func_kwargs}
            {invoke}
            {outputs}
            """
        ).format(
            py_imports=indent(py_imports, tab_indent),
            wk_load=wk_load,
            func_kwargs=indent(func_kwargs_str, tab_indent_2),
            invoke=indent(py_main_block_invoke, tab_indent),
            outputs=indent(dedent(py_main_block_outputs), tab_indent),
        )

        out = dedent(
            """\
            {script_str}
            {main_block}
        """
        ).format(
            script_str=script_str,
            main_block=py_main_block,
        )

        return out

    def get_parameter_names(self, prefix: str) -> list[str]:
        """Get parameter types associated with a given prefix.

        For example, with the prefix "inputs", this would return `['p1', 'p2']` for an
        action that has input types `p1` and `p2`. For inputs, labels are ignored. For
        example, for an action that accepts two inputs of the same type `p1`, with labels
        `one` and `two`, this method would return (for the "inputs" prefix):
        `['p1[one]', 'p1[two]']`.

        This method is distinct from `TaskSchema.get_parameter_names` in that it
        returns action-level input/output/file types/labels, whereas
        `TaskSchema.get_parameter_names` returns schema-level inputs/outputs.

        Parameters
        ----------
        prefix
            One of "inputs", "outputs", "input_files", "output_files".

        """
        if prefix == "inputs":
            single_lab_lookup = self.task_schema._get_single_label_lookup()
            return [single_lab_lookup.get(i, i) for i in self.get_input_types()]
        elif prefix == "outputs":
            return list(self.get_output_types())
        elif prefix == "input_files":
            return list(self.get_input_file_labels())
        elif prefix == "output_files":
            return list(self.get_output_file_labels())
        else:
            raise ValueError(f"unexpected prefix: {prefix}")

    def get_commands_file_hash(self, data_idx: DataIndex, action_idx: int) -> int:
        """Get a hash that can be used to group together runs that will have the same
        commands file.

        This hash is not stable across sessions or machines.

        """

        # filter data index by input parameters that appear in the commands, or are used in
        # rules in conditional commands:
        param_types = self.get_command_parameter_types()

        relevant_paths: list[str] = []
        for i in param_types:
            relevant_paths.extend(
                list(WorkflowTask._get_relevant_paths(data_idx, i.split(".")).keys())
            )

        # hash any relevant data index from rule path
        for cmd in self.commands:
            for act_rule in cmd.rules:
                rule_path = act_rule.rule.path
                assert rule_path
                rule_path_split = rule_path.split(".")
                if rule_path.startswith("resources."):
                    # include all resource paths for now:
                    relevant_paths.extend(
                        list(
                            WorkflowTask._get_relevant_paths(
                                data_idx, ["resources"]
                            ).keys()
                        )
                    )
                else:
                    relevant_paths.extend(
                        list(
                            WorkflowTask._get_relevant_paths(
                                data_idx, rule_path_split
                            ).keys()
                        )
                    )

        # note we don't need to consider action-level rules, since these determine
        # whether a run will be included in a submission or not; this method is only
        # called on runs that are part of a submission, at which point action-level rules
        # are irrelevant.

        relevant_data_idx = {k: v for k, v in data_idx.items() if k in relevant_paths}

        try:
            schema_name = self.task_schema.name
        except AssertionError:
            # allows for testing without making a schema
            schema_name = ""

        return get_hash(
            (
                schema_name,
                action_idx,
                relevant_data_idx,
            )
        )

    @classmethod
    def get_block_act_idx_shell_vars(cls) -> BlockActionKey:
        """Return a the jobscript index, block index, and block action idx shell
        environment variable names formatted for shell substitution.

        Notes
        -----
        This seem so be shell-agnostic, at least for those currently supported.

        """
        app_caps = cls._app.package_name.upper()
        return (
            f"${{{app_caps}_JS_IDX}}",
            f"${{{app_caps}_BLOCK_IDX}}",
            f"${{{app_caps}_BLOCK_ACT_IDX}}",
        )

    def get_script_input_output_file_paths(
        self,
        block_act_key: BlockActionKey,
        directory: Path | None = None,
    ) -> dict[str, dict[str, Path]]:
        """Get the names (as `Path`s) of script input and output files for this action."""
        in_out_paths: dict[str, dict[str, Path]] = {
            "inputs": {},
            "outputs": {},
        }
        for fmt in self.script_data_in_grouped:
            if fmt == "json":
                path = self.get_param_dump_file_path_JSON(
                    block_act_key, directory=directory
                )
            elif fmt == "hdf5":
                path = self.get_param_dump_file_path_HDF5(
                    block_act_key, directory=directory
                )
            else:
                continue
            in_out_paths["inputs"][fmt] = path

        for fmt in self.script_data_out_grouped:
            if fmt == "json":
                path = self.get_param_load_file_path_JSON(
                    block_act_key, directory=directory
                )
            elif fmt == "hdf5":
                path = self.get_param_load_file_path_HDF5(
                    block_act_key, directory=directory
                )
            else:
                continue
            in_out_paths["outputs"][fmt] = path

        return in_out_paths

    def get_script_input_output_file_command_args(self) -> list[str]:
        """Get the script input and output file names as command line arguments."""
        in_out_names = self.get_script_input_output_file_paths(
            self.get_block_act_idx_shell_vars()
        )
        args: list[str] = []
        for fmt, path in in_out_names["inputs"].items():
            if self.script_data_files_use_opt:
                args.append(f"--inputs-{fmt}")
            args.append(str(path))
        for fmt, path in in_out_names["outputs"].items():
            if self.script_data_files_use_opt:
                args.append(f"--outputs-{fmt}")
            args.append(str(path))

        return args
