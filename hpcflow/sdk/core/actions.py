from __future__ import annotations
import copy
from dataclasses import dataclass
from datetime import datetime
import enum
import json
import pprint
from pathlib import Path
import re
from textwrap import indent, dedent
from typing import Any, Dict, List, Optional, Tuple, Union

from valida.conditions import ConditionLike

from watchdog.utils.dirsnapshot import DirectorySnapshotDiff

from hpcflow.sdk import app
from hpcflow.sdk.core import ABORT_EXIT_CODE
from hpcflow.sdk.core.skip_reason import SkipReason
from hpcflow.sdk.core.errors import (
    ActionEnvironmentMissingNameError,
    MissingCompatibleActionEnvironment,
    OutputFileParserNoOutputError,
    UnknownScriptDataKey,
    UnknownScriptDataParameter,
    UnsupportedScriptDataFormat,
    UnsetParameterDataError,
)
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.utils import (
    JSONLikeDirSnapShot,
    split_param_label,
    swap_nested_dict_keys,
    get_relative_path,
)
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.core.run_dir_files import RunDirAppFiles
from hpcflow.sdk.submission.submission import Submission
from hpcflow.sdk.utils.hashing import get_hash

ACTION_SCOPE_REGEX = r"(\w*)(?:\[(.*)\])?"


class ActionScopeType(enum.Enum):
    ANY = 0
    MAIN = 1
    PROCESSING = 2
    INPUT_FILE_GENERATOR = 3
    OUTPUT_FILE_PARSER = 4


ACTION_SCOPE_ALLOWED_KWARGS = {
    ActionScopeType.ANY.name: set(),
    ActionScopeType.MAIN.name: set(),
    ActionScopeType.PROCESSING.name: set(),
    ActionScopeType.INPUT_FILE_GENERATOR.name: {"file"},
    ActionScopeType.OUTPUT_FILE_PARSER.name: {"output"},
}


class EARStatus(enum.Enum):
    """Enumeration of all possible EAR statuses, and their associated status colour."""

    def __new__(cls, value, symbol, colour, doc=None):
        member = object.__new__(cls)
        member._value_ = value
        member.colour = colour
        member.symbol = symbol
        member.__doc__ = doc
        return member

    pending = (
        0,
        ".",
        "grey46",
        "Not yet associated with a submission.",
    )
    prepared = (
        1,
        ".",
        "grey46",
        "Associated with a prepared submission that is not yet submitted.",
    )
    submitted = (
        2,
        ".",
        "grey46",
        "Submitted for execution.",
    )
    running = (
        3,
        "●",
        "dodger_blue1",
        "Executing now.",
    )
    skipped = (
        4,
        "s",
        "dark_orange",
        (
            "Not attempted due to a failure of an upstream action on which this depends, "
            "or a loop termination condition being satisfied."
        ),
    )
    aborted = (
        5,
        "A",
        "deep_pink4",
        "Aborted by the user; downstream actions will be attempted.",
    )
    success = (
        6,
        "■",
        "green3",
        "Probably exited successfully.",
    )
    error = (
        7,
        "E",
        "red3",
        "Probably failed.",
    )

    @classmethod
    def get_non_running_submitted_states(cls):
        """Return the set of all non-running states, excluding those before submission."""
        return {
            cls.skipped,
            cls.aborted,
            cls.success,
            cls.error,
        }

    @property
    def rich_repr(self):
        return f"[{self.colour}]{self.symbol}[/{self.colour}]"


class ElementActionRun:
    _app_attr = "app"

    def __init__(
        self,
        id_: int,
        is_pending: bool,
        element_action,
        index: int,
        data_idx: Dict,
        commands_idx: List[int],
        start_time: Union[datetime, None],
        end_time: Union[datetime, None],
        snapshot_start: Union[Dict, None],
        snapshot_end: Union[Dict, None],
        submission_idx: Union[int, None],
        commands_file_ID: Union[int, None],
        success: Union[bool, None],
        skip: int,
        exit_code: Union[int, None],
        metadata: Dict,
        run_hostname: Union[str, None],
        port_number: Union[int, None],
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
        self._inputs = None
        self._outputs = None
        self._resources = None
        self._input_files = None
        self._output_files = None
        self._ss_start_obj = None
        self._ss_end_obj = None
        self._ss_diff_obj = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"id={self.id_!r}, index={self.index!r}, "
            f"element_action={self.element_action!r})"
        )

    @property
    def id_(self) -> int:
        return self._id

    @property
    def is_pending(self) -> bool:
        return self._is_pending

    @property
    def element_action(self):
        return self._element_action

    @property
    def index(self):
        """Run index."""
        return self._index

    @property
    def action(self):
        return self.element_action.action

    @property
    def element_iteration(self):
        return self.element_action.element_iteration

    @property
    def element(self):
        return self.element_iteration.element

    @property
    def workflow(self):
        return self.element_iteration.workflow

    @property
    def data_idx(self):
        return self._data_idx

    @property
    def commands_idx(self):
        return self._commands_idx

    @property
    def metadata(self):
        return self._metadata

    @property
    def run_hostname(self):
        return self._run_hostname

    @property
    def port_number(self):
        return self._port_number

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time

    @property
    def submission_idx(self):
        return self._submission_idx

    @property
    def commands_file_ID(self):
        return self._commands_file_ID

    @property
    def success(self):
        return self._success

    @property
    def skip(self):
        return self._skip

    @property
    def skip_reason(self):
        return SkipReason(self.skip)

    @property
    def snapshot_start(self):
        if self._ss_start_obj is None and self._snapshot_start:
            self._ss_start_obj = JSONLikeDirSnapShot(
                root_path=".",
                **self._snapshot_start,
            )
        return self._ss_start_obj

    @property
    def snapshot_end(self):
        if self._ss_end_obj is None and self._snapshot_end:
            self._ss_end_obj = JSONLikeDirSnapShot(root_path=".", **self._snapshot_end)
        return self._ss_end_obj

    @property
    def dir_diff(self) -> DirectorySnapshotDiff:
        """Get the changes to the EAR working directory due to the execution of this
        EAR."""
        if self._ss_diff_obj is None and self.snapshot_end:
            self._ss_diff_obj = DirectorySnapshotDiff(
                self.snapshot_start, self.snapshot_end
            )
        return self._ss_diff_obj

    @property
    def exit_code(self):
        return self._exit_code

    @property
    def task(self):
        return self.element_action.task

    @property
    def status(self):
        """Return the state of this EAR."""

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

            if wk_sub_stat.name == "PENDING":
                return EARStatus.prepared

            elif wk_sub_stat.name == "SUBMITTED":
                return EARStatus.submitted

            else:
                RuntimeError(f"Workflow submission status not understood: {wk_sub_stat}.")

        return EARStatus.pending

    def get_parameter_names(self, prefix: str) -> List[str]:
        """Get parameter types associated with a given prefix.

        For inputs, labels are ignored. See `Action.get_parameter_names` for more
        information.

        Parameters
        ----------
        prefix
            One of "inputs", "outputs", "input_files", "output_files".

        """
        return self.action.get_parameter_names(prefix)

    def get_data_idx(self, path: str = None):
        return self.element_iteration.get_data_idx(
            path,
            action_idx=self.element_action.action_idx,
            run_idx=self.index,
        )

    @TimeIt.decorator
    def get_parameter_sources(
        self,
        path: str = None,
        typ: str = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ):
        return self.element_iteration.get_parameter_sources(
            path,
            action_idx=self.element_action.action_idx,
            run_idx=self.index,
            typ=typ,
            as_strings=as_strings,
            use_task_index=use_task_index,
        )

    def get(
        self,
        path: str = None,
        default: Any = None,
        raise_on_missing: bool = False,
        raise_on_unset: bool = False,
    ):
        return self.element_iteration.get(
            path=path,
            action_idx=self.element_action.action_idx,
            run_idx=self.index,
            default=default,
            raise_on_missing=raise_on_missing,
            raise_on_unset=raise_on_unset,
        )

    @TimeIt.decorator
    def get_EAR_dependencies(self, as_objects=False):
        """Get EARs that this EAR depends on."""

        out = []
        for src in self.get_parameter_sources(typ="EAR_output").values():
            if not isinstance(src, list):
                src = [src]
            for src_i in src:
                EAR_ID_i = src_i["EAR_ID"]
                if EAR_ID_i != self.id_:
                    # don't record a self dependency!
                    out.append(EAR_ID_i)

        out = sorted(out)

        if as_objects:
            out = self.workflow.get_EARs_from_IDs(out)

        return out

    def get_input_dependencies(self):
        """Get information about locally defined input, sequence, and schema-default
        values that this EAR depends on. Note this does not get values from this EAR's
        task/schema, because the aim of this method is to help determine which upstream
        tasks this EAR depends on."""

        out = {}
        for k, v in self.get_parameter_sources().items():
            if not isinstance(v, list):
                v = [v]
            for v_i in v:
                if (
                    v_i["type"] in ["local_input", "default_input"]
                    and v_i["task_insert_ID"] != self.task.insert_ID
                ):
                    out[k] = v_i

        return out

    def get_dependent_EARs(
        self, as_objects=False
    ) -> List[Union[int, app.ElementActionRun]]:
        """Get downstream EARs that depend on this EAR."""
        deps = []
        for task in self.workflow.tasks[self.task.index :]:
            for elem in task.elements[:]:
                for iter_ in elem.iterations:
                    for run in iter_.action_runs:
                        for dep_EAR_i in run.get_EAR_dependencies(as_objects=True):
                            # does dep_EAR_i belong to self?
                            if dep_EAR_i.id_ == self._id:
                                deps.append(run.id_)
        deps = sorted(deps)
        if as_objects:
            deps = self.workflow.get_EARs_from_IDs(deps)

        return deps

    @property
    def inputs(self):
        if not self._inputs:
            self._inputs = self.app.ElementInputs(element_action_run=self)
        return self._inputs

    @property
    def outputs(self):
        if not self._outputs:
            self._outputs = self.app.ElementOutputs(element_action_run=self)
        return self._outputs

    @property
    @TimeIt.decorator
    def resources(self):
        if not self._resources:
            self._resources = self.app.ElementResources(**self.get_resources())
        return self._resources

    @property
    def input_files(self):
        if not self._input_files:
            self._input_files = self.app.ElementInputFiles(element_action_run=self)
        return self._input_files

    @property
    def output_files(self):
        if not self._output_files:
            self._output_files = self.app.ElementOutputFiles(element_action_run=self)
        return self._output_files

    @property
    @TimeIt.decorator
    def env_spec(self) -> Dict[str, Any]:
        return self.resources.environments[self.action.get_environment_name()]

    @property
    @TimeIt.decorator
    def env_spec_hashable(self) -> Tuple:
        return self.action.env_spec_to_hashable(self.env_spec)

    def get_directory(self) -> Path:
        return self.workflow.get_run_directories(run_ids=[self.id_])[0]

    def get_app_log_path(self) -> Path:
        return Submission.get_app_log_file_path(
            self.workflow.submissions_path,
            self.submission_idx,
            self.id_,
        )

    def get_app_std_path(self) -> Path:
        std_dir = Submission.get_app_std_path(
            self.workflow.submissions_path,
            self.submission_idx,
        )
        return std_dir / f"{self.id_}.txt"  # TODO: refactor

    @TimeIt.decorator
    def get_resources(self):
        """Resolve specific resources for this EAR, considering all applicable scopes and
        template-level resources."""
        return self.element_iteration.get_resources(self.action)

    def get_environment_spec(self) -> str:
        return self.action.get_environment_spec()

    def get_environment(self) -> app.Environment:
        return self.action.get_environment()

    def get_all_previous_iteration_runs(self, include_self: bool = True):
        """Get a list of run over all iterations that correspond to this run, optionally
        including this run."""
        self_iter = self.element_iteration
        self_elem = self_iter.element
        self_act_idx = self.element_action.action_idx
        max_idx = self_iter.index + 1 if include_self else self_iter.index
        all_runs = []
        for iter_i in self_elem.iterations[:max_idx]:
            all_runs.append(iter_i.actions[self_act_idx].runs[-1])
        return all_runs

    def get_input_values(
        self,
        inputs: Optional[Union[List[str], Dict[str, Dict]]] = None,
        label_dict: bool = True,
        raise_on_unset: Optional[bool] = False,
    ) -> Dict[str, Any]:
        """Get a dict of (optionally a subset of) inputs values for this run.

        Parameters
        ----------
        inputs
            If specified, a list of input parameter types to include, or a dict whose keys
            are input parameter types to include. For schema inputs that have
            `multiple=True`, the input type should be labelled. If a dict is passed, and
            the key "all_iterations` is present and `True`, the return for that input
            will be structured to include values for all previous iterations.
        label_dict
            If True, arrange the values of schema inputs with multiple=True as a dict
            whose keys are the labels. If False, labels will be included in the top level
            keys.

        """
        if not inputs:
            inputs = self.get_parameter_names("inputs")

        out = {}
        for inp_name in inputs:
            path_i, label_i = split_param_label(inp_name)

            try:
                all_iters = inputs[inp_name]["all_iterations"]
            except (TypeError, KeyError):
                all_iters = False

            if all_iters:
                all_runs = self.get_all_previous_iteration_runs(include_self=True)
                val_i = {
                    f"iteration_{run_i.element_iteration.index}": {
                        "loop_idx": run_i.element_iteration.loop_idx,
                        "value": run_i.get(
                            f"inputs.{inp_name}", raise_on_unset=raise_on_unset
                        ),
                    }
                    for run_i in all_runs
                }
            else:
                val_i = self.get(f"inputs.{inp_name}", raise_on_unset=raise_on_unset)

            key = inp_name
            if label_dict and label_i:
                key = path_i  # exclude label from key

            if "." in key:
                # for sub-parameters, take only the final part as the dict key:
                key = key.split(".")[-1]

            if label_dict and label_i:
                if key not in out:
                    out[key] = {}
                out[key][label_i] = val_i
            else:
                out[key] = val_i

        if self.action.script_pass_env_spec:
            out["env_spec"] = self.env_spec

        return out

    def get_input_values_direct(
        self, label_dict: bool = True, raise_on_unset: Optional[bool] = False
    ):
        """Get a dict of input values that are to be passed directly to a Python script
        function."""
        inputs = self.action.script_data_in_grouped.get("direct", {})
        return self.get_input_values(
            inputs=inputs, label_dict=label_dict, raise_on_unset=raise_on_unset
        )

    def get_IFG_input_values(
        self, raise_on_unset: Optional[bool] = False
    ) -> Dict[str, Any]:
        if not self.action._from_expand:
            raise RuntimeError(
                f"Cannot get input file generator inputs from this EAR because the "
                f"associated action is not expanded, meaning multiple IFGs might exists."
            )
        input_types = [i.typ for i in self.action.input_file_generators[0].inputs]
        inputs = {
            typ_i: self.get(f"inputs.{typ_i}", raise_on_unset=raise_on_unset)
            for typ_i in input_types
        }

        if self.action.script_pass_env_spec:
            inputs["env_spec"] = self.env_spec

        return inputs

    def get_OFP_output_files(self) -> Dict[str, Union[str, List[str]]]:
        # TODO: can this return multiple files for a given FileSpec?
        if not self.action._from_expand:
            raise RuntimeError(
                f"Cannot get output file parser files from this from EAR because the "
                f"associated action is not expanded, meaning multiple OFPs might exist."
            )
        out_files = {}
        for file_spec in self.action.output_file_parsers[0].output_files:
            out_files[file_spec.label] = Path(file_spec.name.value())
        return out_files

    def get_OFP_inputs(
        self, raise_on_unset: Optional[bool] = False
    ) -> Dict[str, Union[str, List[str]]]:
        if not self.action._from_expand:
            raise RuntimeError(
                f"Cannot get output file parser inputs from this from EAR because the "
                f"associated action is not expanded, meaning multiple OFPs might exist."
            )
        inputs = {}
        for inp_typ in self.action.output_file_parsers[0].inputs or []:
            inputs[inp_typ] = self.get(f"inputs.{inp_typ}", raise_on_unset=raise_on_unset)

        if self.action.script_pass_env_spec:
            inputs["env_spec"] = self.env_spec

        return inputs

    def get_OFP_outputs(
        self, raise_on_unset: Optional[bool] = False
    ) -> Dict[str, Union[str, List[str]]]:
        if not self.action._from_expand:
            raise RuntimeError(
                f"Cannot get output file parser outputs from this from EAR because the "
                f"associated action is not expanded, meaning multiple OFPs might exist."
            )
        outputs = {}
        for out_typ in self.action.output_file_parsers[0].outputs or []:
            outputs[out_typ] = self.get(
                f"outputs.{out_typ}", raise_on_unset=raise_on_unset
            )
        return outputs

    def get_py_script_func_kwargs(
        self,
        inp_files: Optional[Dict] = None,
        out_files: Optional[Dict] = None,
        raise_on_unset: Optional[bool] = False,
    ) -> Dict[str, Any]:
        # TODO: use this in compose_source as well?
        kwargs = {}
        if self.action.is_IFG:
            ifg = self.action.input_file_generators[0]
            kwargs["path"] = Path(ifg.input_file.name.value())
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

        if inp_files is not None:
            kwargs["_inputs"] = inp_files
        if out_files is not None:
            kwargs["_outputs"] = out_files

        return kwargs

    def write_script_input_files(self, block_act_key: Tuple[int, int, int]):

        for fmt, ins in self.action.script_data_in_grouped.items():
            if fmt == "json":
                in_vals = self.get_input_values(
                    inputs=ins, label_dict=False, raise_on_unset=True
                )
                dump_path = self.action.get_param_dump_file_path_JSON(block_act_key)
                in_vals_processed = {}
                for k, v in in_vals.items():
                    try:
                        v = v.prepare_JSON_dump()
                    except (AttributeError, NotImplementedError):
                        pass
                    in_vals_processed[k] = v

                with dump_path.open("wt") as fp:
                    json.dump(in_vals_processed, fp)

            elif fmt == "hdf5":
                import h5py

                in_vals = self.get_input_values(
                    inputs=ins, label_dict=False, raise_on_unset=True
                )
                dump_path = self.action.get_param_dump_file_path_HDF5(block_act_key)
                with h5py.File(dump_path, mode="w") as f:
                    for k, v in in_vals.items():
                        grp_k = f.create_group(k)
                        try:
                            v.dump_to_HDF5_group(grp_k)
                        except AttributeError:
                            # probably an element group (i.e. v is a list of
                            # `ParameterValue` objects):
                            v[0].dump_element_group_to_HDF5_group(v, grp_k)

    def _param_save(self, block_act_key: Tuple[int, int, int]):
        """Save script-generated parameters that are stored within the supported script
        data output formats (HDF5, JSON, etc)."""

        for fmt in self.action.script_data_out_grouped:
            if fmt == "json":
                load_path = self.action.get_param_load_file_path_JSON(block_act_key)
                with load_path.open(mode="rt") as f:
                    file_data = json.load(f)
                    for param_name, param_dat in file_data.items():
                        param_id = self.data_idx[f"outputs.{param_name}"]
                        param_cls = self.app.parameters.get(param_name)._value_class
                        try:
                            param_cls.save_from_JSON(param_dat, param_id, self.workflow)
                            continue
                        except (AttributeError, NotImplementedError):
                            pass
                        # try to save as a primitive:
                        self.workflow.set_parameter_value(
                            param_id=param_id, value=param_dat
                        )

            elif fmt == "hdf5":
                import h5py

                load_path = self.action.get_param_load_file_path_HDF5(block_act_key)
                with h5py.File(load_path, mode="r") as f:
                    for param_name, h5_grp in f.items():
                        param_id = self.data_idx[f"outputs.{param_name}"]
                        param_cls = self.app.parameters.get(param_name)._value_class
                        param_cls.save_from_HDF5_group(h5_grp, param_id, self.workflow)

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
        self,
        environments,
        shell,
    ) -> Tuple[str, List[str], List[int]]:
        """
        Returns
        -------
        commands
        shell_vars
            Dict whose keys are command indices, and whose values are lists of tuples,
            where each tuple contains: (parameter name, shell variable name,
            "stdout"/"stderr").
        """
        self.app.persistence_logger.debug("EAR.compose_commands")
        env_spec = self.env_spec

        for ofp in self.action.output_file_parsers:
            # TODO: there should only be one at this stage if expanded?
            if ofp.output is None:
                raise OutputFileParserNoOutputError()

        command_lns = []
        env = environments.get(**env_spec)
        if env.setup:
            command_lns += list(env.setup)

        shell_vars = {}  # keys are cmd_idx, each value is a list of tuples
        for cmd_idx, command in enumerate(self.action.commands):
            if cmd_idx in self.commands_idx:
                # only execute commands that have no rules, or all valid rules:
                cmd_str, shell_vars_i = command.get_command_line(
                    EAR=self, shell=shell, env=env
                )
                shell_vars[cmd_idx] = shell_vars_i
                command_lns.append(cmd_str)

        commands = "\n".join(command_lns) + "\n"

        return commands, shell_vars

    def get_commands_file_hash(self) -> int:
        """Get a hash that can be used to group together runs that will have the same
        commands file.

        This hash is not stable across sessions or machines.

        """
        return self.action.get_commands_file_hash(
            data_idx=self.get_data_idx(),
            action_idx=self.element_action.action_idx,
        )

    def try_write_commands(
        self,
        jobscript,
        environments,
        raise_on_unset: bool = False,
    ) -> Union[None, Path]:
        app_name = self.app.package_name
        try:
            commands, shell_vars = self.compose_commands(
                environments=environments,
                shell=jobscript.shell,
            )
        except UnsetParameterDataError:
            if raise_on_unset:
                raise
            self.app.submission_logger.debug(
                f"cannot yet write commands file for run ID {self.id_}; unset parameters"
            )
            return

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
            stem = self.id_

        cmd_file_name = f"{stem}{jobscript.shell.JS_EXT}"
        cmd_file_path = jobscript.submission.commands_path / cmd_file_name
        with cmd_file_path.open("wt", newline="\n") as fp:
            fp.write(commands_fmt)

        return cmd_file_path


class ElementAction:
    _app_attr = "app"

    def __init__(self, element_iteration, action_idx, runs):
        self._element_iteration = element_iteration
        self._action_idx = action_idx
        self._runs = runs

        # assigned on first access of corresponding properties:
        self._run_objs = None
        self._inputs = None
        self._outputs = None
        self._resources = None
        self._input_files = None
        self._output_files = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"iter_ID={self.element_iteration.id_}, "
            f"scope={self.action.get_precise_scope().to_string()!r}, "
            f"action_idx={self.action_idx}, num_runs={self.num_runs}"
            f")"
        )

    @property
    def element_iteration(self):
        return self._element_iteration

    @property
    def element(self):
        return self.element_iteration.element

    @property
    def num_runs(self):
        return len(self._runs)

    @property
    def runs(self):
        if self._run_objs is None:
            self._run_objs = [
                self.app.ElementActionRun(
                    element_action=self,
                    index=idx,
                    **{
                        k: v
                        for k, v in i.items()
                        if k not in ("elem_iter_ID", "action_idx")
                    },
                )
                for idx, i in enumerate(self._runs)
            ]
        return self._run_objs

    @property
    def task(self):
        return self.element_iteration.task

    @property
    def action_idx(self):
        return self._action_idx

    @property
    def action(self):
        return self.task.template.get_schema_action(self.action_idx)

    @property
    def inputs(self):
        if not self._inputs:
            self._inputs = self.app.ElementInputs(element_action=self)
        return self._inputs

    @property
    def outputs(self):
        if not self._outputs:
            self._outputs = self.app.ElementOutputs(element_action=self)
        return self._outputs

    @property
    def input_files(self):
        if not self._input_files:
            self._input_files = self.app.ElementInputFiles(element_action=self)
        return self._input_files

    @property
    def output_files(self):
        if not self._output_files:
            self._output_files = self.app.ElementOutputFiles(element_action=self)
        return self._output_files

    def get_data_idx(self, path: str = None, run_idx: int = -1):
        return self.element_iteration.get_data_idx(
            path,
            action_idx=self.action_idx,
            run_idx=run_idx,
        )

    def get_parameter_sources(
        self,
        path: str = None,
        run_idx: int = -1,
        typ: str = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ):
        return self.element_iteration.get_parameter_sources(
            path,
            action_idx=self.action_idx,
            run_idx=run_idx,
            typ=typ,
            as_strings=as_strings,
            use_task_index=use_task_index,
        )

    def get(
        self,
        path: str = None,
        run_idx: int = -1,
        default: Any = None,
        raise_on_missing: bool = False,
        raise_on_unset: bool = False,
    ):
        return self.element_iteration.get(
            path=path,
            action_idx=self.action_idx,
            run_idx=run_idx,
            default=default,
            raise_on_missing=raise_on_missing,
            raise_on_unset=raise_on_unset,
        )

    def get_parameter_names(self, prefix: str) -> List[str]:
        """Get parameter types associated with a given prefix.

        For inputs, labels are ignored. See `Action.get_parameter_names` for more
        information.

        Parameters
        ----------
        prefix
            One of "inputs", "outputs", "input_files", "output_files".

        """
        return self.action.get_parameter_names(prefix)


class ActionScope(JSONLike):
    """Class to represent the identification of a subset of task schema actions by a
    filtering process.
    """

    _child_objects = (
        ChildObjectSpec(
            name="typ",
            json_like_name="type",
            class_name="ActionScopeType",
            is_enum=True,
        ),
    )

    def __init__(self, typ: Union[app.ActionScopeType, str], **kwargs):
        if isinstance(typ, str):
            typ = getattr(self.app.ActionScopeType, typ.upper())

        self.typ = typ
        self.kwargs = {k: v for k, v in kwargs.items() if v is not None}

        bad_keys = set(kwargs.keys()) - ACTION_SCOPE_ALLOWED_KWARGS[self.typ.name]
        if bad_keys:
            raise TypeError(
                f"The following keyword arguments are unknown for ActionScopeType "
                f"{self.typ.name}: {bad_keys}."
            )

    def __repr__(self):
        kwargs_str = ""
        if self.kwargs:
            kwargs_str = ", ".join(f"{k}={v!r}" for k, v in self.kwargs.items())
        return f"{self.__class__.__name__}.{self.typ.name.lower()}({kwargs_str})"

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        if self.typ is other.typ and self.kwargs == other.kwargs:
            return True
        return False

    @classmethod
    def _parse_from_string(cls, string):
        typ_str, kwargs_str = re.search(ACTION_SCOPE_REGEX, string).groups()
        kwargs = {}
        if kwargs_str:
            for i in kwargs_str.split(","):
                name, val = i.split("=")
                kwargs[name.strip()] = val.strip()
        return {"type": typ_str, **kwargs}

    def to_string(self):
        kwargs_str = ""
        if self.kwargs:
            kwargs_str = "[" + ", ".join(f"{k}={v}" for k, v in self.kwargs.items()) + "]"
        return f"{self.typ.name.lower()}{kwargs_str}"

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        if isinstance(json_like, str):
            json_like = cls._parse_from_string(json_like)
        else:
            typ = json_like.pop("type")
            json_like = {"type": typ, **json_like.pop("kwargs", {})}
        return super().from_json_like(json_like, shared_data)

    @classmethod
    def any(cls):
        return cls(typ=ActionScopeType.ANY)

    @classmethod
    def main(cls):
        return cls(typ=ActionScopeType.MAIN)

    @classmethod
    def processing(cls):
        return cls(typ=ActionScopeType.PROCESSING)

    @classmethod
    def input_file_generator(cls, file=None):
        return cls(typ=ActionScopeType.INPUT_FILE_GENERATOR, file=file)

    @classmethod
    def output_file_parser(cls, output=None):
        return cls(typ=ActionScopeType.OUTPUT_FILE_PARSER, output=output)


@dataclass
class ActionEnvironment(JSONLike):
    _app_attr = "app"

    _child_objects = (
        ChildObjectSpec(
            name="scope",
            class_name="ActionScope",
        ),
    )

    environment: Union[str, Dict[str, Any]]
    scope: Optional[app.ActionScope] = None

    def __post_init__(self):
        if self.scope is None:
            self.scope = self.app.ActionScope.any()

        orig_env = copy.deepcopy(self.environment)
        if isinstance(self.environment, str):
            self.environment = {"name": self.environment}

        if "name" not in self.environment:
            raise ActionEnvironmentMissingNameError(
                f"The action-environment environment specification must include a string "
                f"`name` key, or be specified as string that is that name. Provided "
                f"environment key was {orig_env!r}."
            )


class ActionRule(JSONLike):
    """Class to represent a rule/condition that must be True if an action is to be
    included."""

    _child_objects = (ChildObjectSpec(name="rule", class_name="Rule"),)

    def __init__(
        self,
        rule: Optional[app.Rule] = None,
        check_exists: Optional[str] = None,
        check_missing: Optional[str] = None,
        path: Optional[str] = None,
        condition: Optional[Union[Dict, ConditionLike]] = None,
        cast: Optional[str] = None,
        doc: Optional[str] = None,
    ):
        if rule is None:
            rule = app.Rule(
                check_exists=check_exists,
                check_missing=check_missing,
                path=path,
                condition=condition,
                cast=cast,
                doc=doc,
            )
        elif any(
            i is not None
            for i in (check_exists, check_missing, path, condition, cast, doc)
        ):
            raise TypeError(
                f"{self.__class__.__name__} `rule` specified in addition to rule "
                f"constructor arguments."
            )

        self.rule = rule
        self.action = None  # assigned by parent action
        self.command = None  # assigned by parent command

    def __eq__(self, other):
        if type(other) is not self.__class__:
            return False
        if self.rule == other.rule:
            return True
        return False

    @TimeIt.decorator
    def test(self, element_iteration: app.ElementIteration) -> bool:
        return self.rule.test(
            element_like=element_iteration,
            action=self.action or self.command.action,
        )

    @classmethod
    def check_exists(cls, check_exists):
        return cls(rule=app.Rule(check_exists=check_exists))

    @classmethod
    def check_missing(cls, check_missing):
        return cls(rule=app.Rule(check_missing=check_missing))


class Action(JSONLike):
    """"""

    _app_attr = "app"
    _child_objects = (
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
    _script_data_formats = ("direct", "json", "hdf5")

    def __init__(
        self,
        environments: Optional[List[app.ActionEnvironment]] = None,
        commands: Optional[List[app.Command]] = None,
        script: Optional[str] = None,
        script_data_in: Optional[str] = None,
        script_data_out: Optional[str] = None,
        script_data_files_use_opt: Optional[bool] = False,
        script_exe: Optional[str] = None,
        script_pass_env_spec: Optional[bool] = False,
        abortable: Optional[bool] = False,
        input_file_generators: Optional[List[app.InputFileGenerator]] = None,
        output_file_parsers: Optional[List[app.OutputFileParser]] = None,
        input_files: Optional[List[app.FileSpec]] = None,
        output_files: Optional[List[app.FileSpec]] = None,
        rules: Optional[List[app.ActionRule]] = None,
        save_files: Optional[List[str]] = None,
        clean_up: Optional[List[str]] = None,
        requires_dir: Optional[bool] = None,
    ):
        """
        Parameters
        ----------
        script_data_files_use_opt
            If True, script data input and output file paths will be passed to the script
            execution command line with an option like `--input-json` or `--output-hdf5`
            etc. If False, the file paths will be passed on their own. For Python scripts,
            options are always passed, and this parameter is overwritten to be True,
            regardless of its initial value.

        """
        self.commands = commands or []
        self.script = script
        self.script_data_in = script_data_in
        self.script_data_out = script_data_out
        self.script_data_files_use_opt = (
            script_data_files_use_opt if not self.script_is_python_snippet else True
        )
        self.script_exe = script_exe.lower() if script_exe else None
        self.script_pass_env_spec = script_pass_env_spec
        self.environments = environments or [
            self.app.ActionEnvironment(environment="null_env")
        ]
        self.abortable = abortable
        self.input_file_generators = input_file_generators or []
        self.output_file_parsers = output_file_parsers or []
        self.input_files = self._resolve_input_files(input_files or [])
        self.output_files = self._resolve_output_files(output_files or [])
        self.rules = rules or []
        self.save_files = save_files or []
        self.clean_up = clean_up or []

        if requires_dir is None:
            requires_dir = (
                True if self.input_file_generators or self.output_file_parsers else False
            )
        self.requires_dir = requires_dir

        self._task_schema = None  # assigned by parent TaskSchema
        self._from_expand = False  # assigned on creation of new Action by `expand`

        self._set_parent_refs()

    def process_script_data_formats(self):
        self.script_data_in = self._process_script_data_in(self.script_data_in)
        self.script_data_out = self._process_script_data_out(self.script_data_out)

    def _process_script_data_format(
        self, data_fmt: Union[str, Dict[str, Union[str, Dict[str, str]]]], prefix: str
    ) -> Dict[str, str]:
        if not data_fmt:
            return {}

        _all_other_sym = "*"
        param_names = self.get_parameter_names(prefix)
        if isinstance(data_fmt, str):
            # include all input parameters, using specified data format
            data_fmt = data_fmt.lower()
            all_params = {k: {"format": data_fmt} for k in param_names}
        else:
            all_params = copy.copy(data_fmt)
            for k, v in all_params.items():
                # values might be strings, or dicts with "format" and potentially other
                # kwargs:
                try:
                    fmt = v["format"]
                except TypeError:
                    fmt = v
                    kwargs = {}
                else:
                    kwargs = {k2: v2 for k2, v2 in v.items() if k2 != "format"}
                finally:
                    all_params[k] = {"format": fmt.lower(), **kwargs}

            if prefix == "inputs":
                # expand unlabelled-multiple inputs to multiple labelled inputs:
                multi_types = self.task_schema.multi_input_types
                multis = {}
                for k in list(all_params.keys()):
                    if k in multi_types:
                        k_fmt = all_params.pop(k)
                        for i in param_names:
                            if i.startswith(k):
                                multis[i] = copy.deepcopy(k_fmt)
                all_params = {
                    **multis,
                    **all_params,
                }

            if _all_other_sym in all_params:
                # replace catch-all with all other input/output names:
                other_fmt = all_params[_all_other_sym]
                all_params = {k: v for k, v in all_params.items() if k != _all_other_sym}
                other = set(param_names) - set(all_params.keys())
                for i in other:
                    all_params[i] = copy.deepcopy(other_fmt)

        # validation:
        allowed_keys = ("format", "all_iterations")
        for k, v in all_params.items():
            # validate parameter name (sub-parameters are allowed):
            if k.split(".")[0] not in param_names:
                raise UnknownScriptDataParameter(
                    f"Script data parameter {k!r} is not a known parameter of the "
                    f"action. Parameters ({prefix}) are: {param_names!r}."
                )
            # validate format:
            if v["format"] not in self._script_data_formats:
                raise UnsupportedScriptDataFormat(
                    f"Script data format {v!r} for {prefix[:-1]} parameter {k!r} is not "
                    f"understood. Available script data formats are: "
                    f"{self._script_data_formats!r}."
                )

            for k2 in v:
                if k2 not in allowed_keys:
                    raise UnknownScriptDataKey(
                        f"Script data key {k2!r} is not understood. Allowed keys are: "
                        f"{allowed_keys!r}."
                    )

        return all_params

    def _process_script_data_in(
        self, data_fmt: Union[str, Dict[str, str]]
    ) -> Dict[str, str]:
        return self._process_script_data_format(data_fmt, "inputs")

    def _process_script_data_out(
        self, data_fmt: Union[str, Dict[str, str]]
    ) -> Dict[str, str]:
        return self._process_script_data_format(data_fmt, "outputs")

    @property
    def script_data_in_grouped(self) -> Dict[str, List[str]]:
        """Get input parameter types by script data-in format."""
        return swap_nested_dict_keys(dct=self.script_data_in, inner_key="format")

    @property
    def script_data_out_grouped(self) -> Dict[str, List[str]]:
        """Get output parameter types by script data-out format."""
        return swap_nested_dict_keys(dct=self.script_data_out, inner_key="format")

    @property
    def script_data_in_has_files(self) -> bool:
        """Return True if the script requires some inputs to be passed via an
        intermediate file format."""
        return bool(set(self.script_data_in_grouped.keys()) - {"direct"})  # TODO: test

    @property
    def script_data_out_has_files(self) -> bool:
        """Return True if the script produces some outputs via an intermediate file
        format."""
        return bool(set(self.script_data_out_grouped.keys()) - {"direct"})  # TODO: test

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
        if self.script:
            snip_path = self.get_snippet_script_path(self.script)
            if snip_path:
                return snip_path.suffix == ".py"
        return False

    @property
    def is_IFG(self):
        return bool(self.input_file_generators)

    @property
    def is_OFP(self):
        return bool(self.output_file_parsers)

    def __deepcopy__(self, memo):
        kwargs = self.to_dict()
        _from_expand = kwargs.pop("_from_expand")
        _task_schema = kwargs.pop("_task_schema", None)
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._from_expand = _from_expand
        obj._task_schema = _task_schema
        return obj

    @property
    def task_schema(self):
        return self._task_schema

    def _resolve_input_files(self, input_files):
        in_files = input_files
        for i in self.input_file_generators:
            if i.input_file not in in_files:
                in_files.append(i.input_file)
        return in_files

    def _resolve_output_files(self, output_files):
        out_files = output_files
        for i in self.output_file_parsers:
            for j in i.output_files:
                if j not in out_files:
                    out_files.append(j)
        return out_files

    def __repr__(self) -> str:
        IFGs = {
            i.input_file.label: [j.typ for j in i.inputs]
            for i in self.input_file_generators
        }
        OFPs = {}
        for idx, i in enumerate(self.output_file_parsers):
            if i.output is not None:
                key = i.output.typ
            else:
                key = f"OFP_{idx}"
            OFPs[key] = [j.label for j in i.output_files]

        out = []
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

    def __eq__(self, other):
        if type(other) is not self.__class__:
            return False
        if (
            self.commands == other.commands
            and self.script == other.script
            and self.environments == other.environments
            and self.abortable == other.abortable
            and self.input_file_generators == other.input_file_generators
            and self.output_file_parsers == other.output_file_parsers
            and self.rules == other.rules
        ):
            return True
        return False

    @staticmethod
    def env_spec_to_hashable(env_spec: Dict) -> Tuple:
        return tuple(zip(*env_spec.items()))

    @staticmethod
    def env_spec_from_hashable(env_spec_h: Tuple) -> Dict:
        return dict(zip(*env_spec_h))

    def get_script_determinants(self) -> Tuple:
        """Get the attributes that affect the script."""
        return (
            self.script,
            self.script_data_in,
            self.script_data_out,
            self.script_data_files_use_opt,
            self.script_exe,
        )

    def get_script_determinant_hash(self, env_specs: Optional[Dict] = None) -> int:
        """Get a hash of the instance attributes that uniquely determine the script.

        The hash is not stable accross sessions or machines.

        """
        env_specs = env_specs or {}
        return get_hash(
            (self.get_script_determinants(), self.env_spec_to_hashable(env_specs))
        )

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""
        _from_expand = json_like.pop("_from_expand", None)
        obj = cls(**json_like)
        obj._from_expand = _from_expand
        return obj

    def get_parameter_dependence(self, parameter: app.SchemaParameter):
        """Find if/where a given parameter is used by the action."""
        writer_files = [
            i.input_file
            for i in self.input_file_generators
            if parameter.parameter in i.inputs
        ]  # names of input files whose generation requires this parameter
        commands = []  # TODO: indices of commands in which this parameter appears
        out = {"input_file_writers": writer_files, "commands": commands}
        return out

    def get_resolved_action_env(
        self,
        relevant_scopes: Tuple[app.ActionScopeType],
        input_file_generator: app.InputFileGenerator = None,
        output_file_parser: app.OutputFileParser = None,
        commands: List[app.Command] = None,
    ):
        possible = [i for i in self.environments if i.scope.typ in relevant_scopes]
        if not possible:
            if input_file_generator:
                msg = f"input file generator {input_file_generator.input_file.label!r}"
            elif output_file_parser:
                if output_file_parser.output is not None:
                    ofp_id = output_file_parser.output.typ
                else:
                    ofp_id = "<unnamed>"
                msg = f"output file parser {ofp_id!r}"
            else:
                msg = f"commands {commands!r}"
            raise MissingCompatibleActionEnvironment(
                f"No compatible environment is specified for the {msg}."
            )

        # sort by scope type specificity:
        possible_srt = sorted(possible, key=lambda i: i.scope.typ.value, reverse=True)
        return possible_srt[0]

    def get_input_file_generator_action_env(
        self, input_file_generator: app.InputFileGenerator
    ):
        return self.get_resolved_action_env(
            relevant_scopes=(
                ActionScopeType.ANY,
                ActionScopeType.PROCESSING,
                ActionScopeType.INPUT_FILE_GENERATOR,
            ),
            input_file_generator=input_file_generator,
        )

    def get_output_file_parser_action_env(self, output_file_parser: app.OutputFileParser):
        return self.get_resolved_action_env(
            relevant_scopes=(
                ActionScopeType.ANY,
                ActionScopeType.PROCESSING,
                ActionScopeType.OUTPUT_FILE_PARSER,
            ),
            output_file_parser=output_file_parser,
        )

    def get_commands_action_env(self):
        return self.get_resolved_action_env(
            relevant_scopes=(ActionScopeType.ANY, ActionScopeType.MAIN),
            commands=self.commands,
        )

    def get_environment_name(self) -> str:
        return self.get_environment_spec()["name"]

    def get_environment_spec(self) -> Dict[str, Any]:
        if not self._from_expand:
            raise RuntimeError(
                f"Cannot choose a single environment from this action because it is not "
                f"expanded, meaning multiple action environments might exist."
            )
        return self.environments[0].environment

    def get_environment(self) -> app.Environment:
        return self.app.envs.get(**self.get_environment_spec())

    @staticmethod
    def is_snippet_script(script: str) -> bool:
        """Returns True if the provided script string represents a script snippets that is
        to be modified before execution (e.g. to receive and provide parameter data)."""
        return script.startswith("<<script:")

    @classmethod
    def get_script_name(cls, script: str) -> str:
        """Return the script name.

        If `script` is a snippet script path, this method returns the name of the script
        (i.e. the final component of the path). If `script` is not a snippet script path
        (does not start with "<<script:"), then `script` is simply returned.

        """
        if cls.is_snippet_script(script):
            pattern = r"\<\<script:(?:.*(?:\/|\\))*(.*)\>\>"
            match_obj = re.match(pattern, script)
            return match_obj.group(1)
        else:
            # a script we can expect in the working directory, which might have been
            # generated by a previous action
            return script

    def get_script_artifact_name(
        self,
        env_spec: Dict,
        act_idx: int,
        ret_specifiers=False,
        include_suffix: Optional[bool] = True,
        specs_suffix_delim: Optional[str] = ".",
    ) -> Union[Tuple[str, Path], Tuple[str, Path, Dict]]:
        """Return the script name that is used when writing the script to the artifacts
        directory within the workflow.

        Like `Action.get_script_name`, this is only applicable for snippet scripts.

        """
        if not self.is_snippet_script(self.script):
            raise ValueError("Not a snippet script!")

        snip_path, specifiers = self.get_snippet_script_path(
            self.script,
            env_spec,
            ret_specifiers=True,
        )
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

    @classmethod
    def get_snippet_script_str(
        cls,
        script,
        env_spec: Optional[Dict[str, Any]] = None,
        ret_specifiers: bool = False,
    ) -> str:
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
        pattern = r"\<\<script:(.*:?)\>\>"
        match_obj = re.match(pattern, script)
        out = match_obj.group(1)

        if env_spec is not None:
            specifiers = {}

            def repl(match_obj):
                spec = match_obj.group(1)
                specifiers[spec] = env_spec[spec]
                return str(env_spec[spec])

            out = re.sub(
                pattern=r"\<\<env:(.*?)\>\>",
                repl=repl,
                string=out,
            )
            if ret_specifiers:
                out = (out, specifiers)
        return out

    @classmethod
    def get_snippet_script_path(
        cls,
        script_path,
        env_spec: Optional[Dict[str, Any]] = None,
        ret_specifiers: bool = False,
    ) -> Path:
        """Return the specified snippet `script` path.

        Parameters
        ----------
        ret_specifiers
            If True, also return a list of environment specifiers as a dict whose keys are
            specifier keys found in the `script` path and whose values are the
            corresponding values extracted from `env_spec`.

        """
        if not cls.is_snippet_script(script_path):
            return False

        path = cls.get_snippet_script_str(script_path, env_spec, ret_specifiers)
        if ret_specifiers:
            path, specifiers = path

        if path in cls.app.scripts:
            path = cls.app.scripts.get(path)

        path = Path(path)

        if ret_specifiers:
            return path, specifiers
        else:
            return path

    @staticmethod
    def get_param_dump_file_stem(block_act_key: Tuple[int, int, int]):
        return RunDirAppFiles.get_run_param_dump_file_prefix(block_act_key)

    @staticmethod
    def get_param_load_file_stem(block_act_key: Tuple[int, int, int]):
        return RunDirAppFiles.get_run_param_load_file_prefix(block_act_key)

    def get_param_dump_file_path_JSON(self, block_act_key: Tuple[int, int, int]):
        return Path(self.get_param_dump_file_stem(block_act_key) + ".json")

    def get_param_dump_file_path_HDF5(self, block_act_key: Tuple[int, int, int]):
        return Path(self.get_param_dump_file_stem(block_act_key) + ".h5")

    def get_param_load_file_path_JSON(self, block_act_key: Tuple[int, int, int]):
        return Path(self.get_param_load_file_stem(block_act_key) + ".json")

    def get_param_load_file_path_HDF5(self, block_act_key: Tuple[int, int, int]):
        return Path(self.get_param_load_file_stem(block_act_key) + ".h5")

    def expand(self):
        if self._from_expand:
            # already expanded
            return [self]

        else:
            # run main if:
            #   - one or more output files are not passed
            # run IFG if:
            #   - one or more output files are not passed
            #   - AND input file is not passed
            # always run OPs, for now

            main_rules = self.rules + [
                self.app.ActionRule.check_missing(f"output_files.{i.label}")
                for i in self.output_files
            ]

            # note we keep the IFG/OPs in the new actions, so we can check the parameters
            # used/produced.

            inp_files = []
            inp_acts = []

            app_caps = self.app.package_name.upper()
            run_ID_var = "$RUN_ID"

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
                act_i = self.app.Action(
                    commands=[app.Command(executable=exe, variables=variables)],
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

            out_files = []
            out_acts = []
            for ofp in self.output_file_parsers:
                script_exe = "python_script"
                exe = f"<<executable:{script_exe}>>"
                variables = script_cmd_vars if ofp.script else {}
                act_i = self.app.Action(
                    commands=[app.Command(executable=exe, variables=variables)],
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
                args = []
                fn_args = (
                    f"${{{app_caps}_JS_IDX}}",
                    f"${{{app_caps}_BLOCK_IDX}}",
                    f"${{{app_caps}_BLOCK_ACT_IDX}}",
                )
                for fmt in self.script_data_in_grouped:
                    if fmt == "json":
                        if self.script_data_files_use_opt:
                            args.append("--inputs-json")
                        args.append(str(self.get_param_dump_file_path_JSON(fn_args)))
                    elif fmt == "hdf5":
                        if self.script_data_files_use_opt:
                            args.append("--inputs-hdf5")
                        args.append(str(self.get_param_dump_file_path_HDF5(fn_args)))

                for fmt in self.script_data_out_grouped:
                    if fmt == "json":
                        if self.script_data_files_use_opt:
                            args.append("--outputs-json")
                        args.append(str(self.get_param_load_file_path_JSON(fn_args)))
                    elif fmt == "hdf5":
                        if self.script_data_files_use_opt:
                            args.append("--outputs-hdf5")
                        args.append(str(self.get_param_load_file_path_HDF5(fn_args)))

                commands += [
                    self.app.Command(executable=exe, arguments=args, variables=variables)
                ]

            # TODO: store script_args? and build command with executable syntax?
            main_act = self.app.Action(
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

            cmd_acts = inp_acts + [main_act] + out_acts

            return cmd_acts

    def get_command_input_types(self, sub_parameters: bool = False) -> Tuple[str]:
        """Get parameter types from commands.

        Parameters
        ----------
        sub_parameters
            If True, sub-parameters (i.e. dot-delimited parameter types) will be returned
            untouched. If False (default), only return the root parameter type and
            disregard the sub-parameter part.
        """
        params = []
        # note: we use "parameter" rather than "input", because it could be a schema input
        # or schema output.
        vars_regex = r"\<\<(?:\w+(?:\[(?:.*)\])?\()?parameter:(.*?)\)?\>\>"
        for command in self.commands:
            for val in re.findall(vars_regex, command.command or ""):
                if not sub_parameters:
                    val = val.split(".")[0]
                params.append(val)
            for arg in command.arguments or []:
                for val in re.findall(vars_regex, arg):
                    if not sub_parameters:
                        val = val.split(".")[0]
                    params.append(val)
            # TODO: consider stdin?
        return tuple(set(params))

    def get_command_file_labels(self) -> Tuple[str]:
        """Get input files types from commands."""
        files = []
        vars_regex = r"\<\<file:(.*?)\>\>"
        for command in self.commands:
            for val in re.findall(vars_regex, command.command or ""):
                files.append(val)
            for arg in command.arguments or []:
                for val in re.findall(vars_regex, arg):
                    files.append(val)
            # TODO: consider stdin?
        return tuple(set(files))

    def get_command_output_types(self) -> Tuple[str]:
        """Get parameter types from command stdout and stderr arguments."""
        params = []
        for command in self.commands:
            out_params = command.get_output_types()
            if out_params["stdout"]:
                params.append(out_params["stdout"])
            if out_params["stderr"]:
                params.append(out_params["stderr"])

        return tuple(set(params))

    def get_command_parameter_types(self, sub_parameters: bool = False) -> Tuple[str]:
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

    def get_input_types(self, sub_parameters: bool = False) -> Tuple[str]:
        """Get the input types that are consumed by commands and input file generators of
        this action.

        Parameters
        ----------
        sub_parameters
            If True, sub-parameters (i.e. dot-delimited parameter types) in command line
            inputs will be returned untouched. If False (default), only return the root
            parameter type and disregard the sub-parameter part.
        """
        is_script = (
            self.script
            and not self.input_file_generators
            and not self.output_file_parsers
        )
        if is_script:
            params = self.task_schema.input_types
        else:
            params = list(self.get_command_input_types(sub_parameters))
            for i in self.input_file_generators:
                params.extend([j.typ for j in i.inputs])
            for i in self.output_file_parsers:
                params.extend([j for j in i.inputs or []])
        return tuple(set(params))

    def get_output_types(self) -> Tuple[str]:
        """Get the output types that are produced by command standard outputs and errors,
        and by output file parsers of this action."""
        is_script = (
            self.script
            and not self.input_file_generators
            and not self.output_file_parsers
        )
        if is_script:
            params = self.task_schema.output_types
        else:
            params = list(self.get_command_output_types())
            for i in self.output_file_parsers:
                if i.output is not None:
                    params.append(i.output.typ)
                params.extend([j for j in i.outputs or []])
        return tuple(set(params))

    def get_input_file_labels(self):
        return tuple(i.label for i in self.input_files)

    def get_output_file_labels(self):
        return tuple(i.label for i in self.output_files)

    @TimeIt.decorator
    def generate_data_index(
        self,
        act_idx,
        EAR_ID,
        schema_data_idx,
        all_data_idx,
        workflow,
        param_source,
    ) -> List[int]:
        """Generate the data index for this action of an element iteration whose overall
        data index is passed.

        This mutates `all_data_idx`.

        """

        # output keys must be processed first for this to work, since when processing an
        # output key, we may need to update the index of an output in a previous action's
        # data index, which could affect the data index in an input of this action.
        keys = [f"outputs.{i}" for i in self.get_output_types()]
        keys += [f"inputs.{i}" for i in self.get_input_types()]
        for i in self.input_files:
            keys.append(f"input_files.{i.label}")
        for i in self.output_files:
            keys.append(f"output_files.{i.label}")

        # these are consumed by the OFP, so should not be considered to generate new data:
        OFP_outs = [j for i in self.output_file_parsers for j in i.outputs or []]

        # keep all resources and repeats data:
        sub_data_idx = {
            k: v
            for k, v in schema_data_idx.items()
            if ("resources" in k or "repeats" in k)
        }
        param_src_update = []
        for key in keys:
            sub_param_idx = {}
            if (
                key.startswith("input_files")
                or key.startswith("output_files")
                or key.startswith("inputs")
                or (key.startswith("outputs") and key.split("outputs.")[1] in OFP_outs)
            ):
                # look for an index in previous data indices (where for inputs we look
                # for *output* parameters of the same name):
                k_idx = None
                for prev_data_idx in all_data_idx.values():
                    if key.startswith("inputs"):
                        k_param = key.split("inputs.")[1]
                        k_out = f"outputs.{k_param}"
                        if k_out in prev_data_idx:
                            k_idx = prev_data_idx[k_out]

                    else:
                        if key in prev_data_idx:
                            k_idx = prev_data_idx[key]

                if k_idx is None:
                    # otherwise take from the schema_data_idx:
                    if key in schema_data_idx:
                        k_idx = schema_data_idx[key]
                        # add any associated sub-parameters:
                        for k, v in schema_data_idx.items():
                            if k.startswith(f"{key}."):  # sub-parameter (note dot)
                                sub_param_idx[k] = v
                    else:
                        # otherwise we need to allocate a new parameter datum:
                        # (for input/output_files keys)
                        k_idx = workflow._add_unset_parameter_data(param_source)

            else:
                # outputs
                k_idx = None
                for (act_idx_i, EAR_ID_i), prev_data_idx in all_data_idx.items():
                    if key in prev_data_idx:
                        k_idx = prev_data_idx[key]

                        # allocate a new parameter datum for this intermediate output:
                        param_source_i = copy.deepcopy(param_source)
                        # param_source_i["action_idx"] = act_idx_i
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

        all_data_idx[(act_idx, EAR_ID)] = sub_data_idx

        return param_src_update

    def get_possible_scopes(self) -> Tuple[app.ActionScope]:
        """Get the action scopes that are inclusive of this action, ordered by decreasing
        specificity."""

        scope = self.get_precise_scope()

        if self.input_file_generators:
            scopes = (
                scope,
                self.app.ActionScope.input_file_generator(),
                self.app.ActionScope.processing(),
                self.app.ActionScope.any(),
            )
        elif self.output_file_parsers:
            scopes = (
                scope,
                self.app.ActionScope.output_file_parser(),
                self.app.ActionScope.processing(),
                self.app.ActionScope.any(),
            )
        else:
            scopes = (scope, self.app.ActionScope.any())

        return scopes

    def get_precise_scope(self) -> app.ActionScope:
        if not self._from_expand:
            raise RuntimeError(
                "Precise scope cannot be unambiguously defined until the Action has been "
                "expanded."
            )

        if self.input_file_generators:
            return self.app.ActionScope.input_file_generator(
                file=self.input_file_generators[0].input_file.label
            )
        elif self.output_file_parsers:
            if self.output_file_parsers[0].output is not None:
                return self.app.ActionScope.output_file_parser(
                    output=self.output_file_parsers[0].output
                )
            else:
                return self.app.ActionScope.output_file_parser()
        else:
            return self.app.ActionScope.main()

    def is_input_type_required(
        self, typ: str, provided_files: List[app.FileSpec]
    ) -> bool:
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
        for IFG in self.input_file_generators:
            if typ in (i.typ for i in IFG.inputs):
                if IFG.input_file not in provided_files:
                    return True

        # typ is required if used in any output file parser
        for OFP in self.output_file_parsers:
            if typ in (OFP.inputs or []):
                return True

    @TimeIt.decorator
    def test_rules(self, element_iter) -> Tuple[bool, List[int]]:
        """Test all rules against the specified element iteration."""
        rules_valid = [rule.test(element_iteration=element_iter) for rule in self.rules]
        action_valid = all(rules_valid)
        commands_idx = []
        if action_valid:
            for cmd_idx, cmd in enumerate(self.commands):
                if any(not i.test(element_iteration=element_iter) for i in cmd.rules):
                    continue
                commands_idx.append(cmd_idx)
        return action_valid, commands_idx

    def get_required_executables(self) -> Tuple[str]:
        """Return executable labels required by this action."""
        exec_labs = []
        for command in self.commands:
            exec_labs.extend(command.get_required_executables())
        return tuple(set(exec_labs))

    def compose_source(self, snip_path: Path) -> str:
        """Generate the file contents of this source."""

        script_name = snip_path.name
        with snip_path.open("rt") as fp:
            script_str = fp.read()

        if not self.script_is_python_snippet:
            return script_str

        if self.is_OFP and self.output_file_parsers[0].output is None:
            # might be used just for saving files:
            return

        app_caps = self.app.package_name.upper()
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

                parser = argparse.ArgumentParser()
                parser.add_argument("--inputs-json")
                parser.add_argument("--inputs-hdf5")
                parser.add_argument("--outputs-json")
                parser.add_argument("--outputs-hdf5")
                args = parser.parse_args()
            """
        ).format(app_module=self.app.module, app_caps=app_caps)

        # if any direct inputs/outputs, we must load the workflow (must be python):
        if self.script_data_in_has_direct or self.script_data_out_has_direct:
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
                cfg_dir=self.app.config.config_directory,
                cfg_invoc_key=self.app.config.config_key,
                app_caps=app_caps,
            )
        else:
            py_main_block_workflow_load = ""

        kwargs_lns = []
        double_splat_lns = []
        if (
            not any((self.is_IFG, self.is_OFP))
            and "direct" in self.script_data_in_grouped
        ):
            double_splat_lns.append("**EAR.get_input_values_direct()")

        elif self.is_IFG:
            new_file_path = self.input_file_generators[0].input_file.name.value()
            double_splat_lns.append("**EAR.get_IFG_input_values()")
            kwargs_lns.append(f'"path": Path("{new_file_path}")')

        elif self.is_OFP:
            double_splat_lns.extend(
                (
                    "**EAR.get_OFP_output_files()",
                    "**EAR.get_OFP_inputs()",
                    "**EAR.get_OFP_outputs()",
                )
            )

        if self.script_data_in_has_files:
            # TODO: add parser.add_argument s here
            # need to pass "_input_files" keyword argument to script main function:
            input_files_str = dedent(
                """\
                inp_files = {}
                if args.inputs_json:
                    inp_files["json"] = Path(args.inputs_json)
                if args.inputs_hdf5:
                    inp_files["hdf5"] = Path(args.inputs_hdf5)
            """
            )
            kwargs_lns.append('"_input_files": inp_files')
        else:
            input_files_str = ""

        if self.script_data_out_has_files:
            # TODO: add parser.add_argument s here
            # need to pass "_output_files" keyword argument to script main function:
            output_files_str = dedent(
                """\
                out_files = {}
                if args.outputs_json:
                    out_files["json"] = Path(args.outputs_json)
                if args.outputs_hdf5:
                    out_files["hdf5"] = Path(args.outputs_hdf5)
            """
            )
            kwargs_lns.append('"_output_files": out_files')

        else:
            output_files_str = ""

        tab_indent = "    "
        tab_indent_2 = 2 * tab_indent

        all_kwargs_lns = kwargs_lns + double_splat_lns
        all_kwargs_str = ",\n".join(all_kwargs_lns) + ","
        func_kwargs_str = dedent(
            """\
            func_kwargs = {{
            {kwargs_str}
            }}
        """
        ).format(kwargs_str=indent(all_kwargs_str, tab_indent))

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
            py_main_block_outputs = dedent(
                """\
                with app.redirect_std_to_file(std_path):
                    wk.save_parameter(name="outputs.{output_typ}", value=output, EAR_ID=run_id)
                """
            ).format(output_typ=self.output_file_parsers[0].output.typ)
        else:
            py_main_block_invoke = func_invoke_str
            py_main_block_outputs = ""

        in_files = "\n" + indent(input_files_str, tab_indent_2) if input_files_str else ""
        out_files = (
            "\n" + indent(output_files_str, tab_indent_2) if output_files_str else ""
        )
        wk_load = (
            "\n" + indent(py_main_block_workflow_load, tab_indent_2)
            if py_main_block_workflow_load
            else ""
        )
        py_main_block = dedent(
            """\
            if __name__ == "__main__":
            {py_imports}{wk_load}{in_files}{out_files}
            {func_kwargs}
            {invoke}
            {outputs}
            """
        ).format(
            py_imports=indent(py_imports, tab_indent),
            wk_load=wk_load,
            in_files=in_files,
            out_files=out_files,
            func_kwargs=indent(func_kwargs_str, tab_indent_2),
            invoke=indent(py_main_block_invoke, tab_indent),
            outputs=indent(py_main_block_outputs, tab_indent),
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

    def get_parameter_names(self, prefix: str) -> List[str]:
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
            out = list(single_lab_lookup.get(i, i) for i in self.get_input_types())
        elif prefix == "outputs":
            out = list(f"{i}" for i in self.get_output_types())
        elif prefix == "input_files":
            out = list(f"{i}" for i in self.get_input_file_labels())
        elif prefix == "output_files":
            out = list(f"{i}" for i in self.get_output_file_labels())
        return out

    def get_commands_file_hash(self, data_idx: Dict[str, int], action_idx: int) -> int:
        """Get a hash that can be used to group together runs that will have the same
        commands file.

        This hash is not stable across sessions or machines.

        """

        def _get_relevant_paths(data_index, path, children_of: str = None):
            # TODO: refactor: this is the same function as used in
            # `WorkflowTask._get_merged_parameter_data`; duplicating to avoid
            # anticipated merge conflicts

            relevant_paths = {}
            # first extract out relevant paths in `data_index`:
            for path_i in data_index:
                path_i_split = path_i.split(".")
                try:
                    rel_path = get_relative_path(path, path_i_split)
                    relevant_paths[path_i] = {"type": "parent", "relative_path": rel_path}
                except ValueError:
                    try:
                        update_path = get_relative_path(path_i_split, path)
                        relevant_paths[path_i] = {
                            "type": "update",
                            "update_path": update_path,
                        }
                    except ValueError:
                        # no intersection between paths
                        if children_of and path_i.startswith(children_of):
                            relevant_paths[path_i] = {"type": "sibling"}
                        continue
            return relevant_paths

        # filter data index by input parameters that appear in the commands, or are used in
        # rules in conditional commands:
        param_types = self.get_command_parameter_types()

        relevant_paths = []
        for i in param_types:
            relevant_paths.extend(
                list(_get_relevant_paths(data_idx, i.split(".")).keys())
            )

        # hash any relevant data index from rule path
        for cmd in self.commands:
            for act_rule in cmd.rules:
                rule_path = act_rule.rule.path
                rule_path_split = rule_path.split(".")
                if rule_path.startswith("resources."):
                    # include all resource paths for now:
                    relevant_paths.extend(
                        list(_get_relevant_paths(data_idx, ["resources"]).keys())
                    )
                else:
                    relevant_paths.extend(
                        list(_get_relevant_paths(data_idx, rule_path_split).keys())
                    )

        # note we don't need to consider action-level rules, since these determine
        # whether a run will be included in a submission or not; this method is only
        # called on runs that are part of a submission, at which point action-level rules
        # are irrelevent.

        relevant_data_idx = {k: v for k, v in data_idx.items() if k in relevant_paths}

        try:
            schema_name = self.task_schema.name
        except AttributeError:
            # allows for testing without making a schema
            schema_name = ""

        return get_hash(
            (
                schema_name,
                action_idx,
                relevant_data_idx,
            )
        )
