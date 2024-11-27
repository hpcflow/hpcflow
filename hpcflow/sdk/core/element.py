from __future__ import annotations
import copy
from dataclasses import dataclass, field, fields
from operator import attrgetter
import os
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from valida.conditions import ConditionLike
from valida.rules import Rule

from hpcflow.sdk import app
from hpcflow.sdk.core.skip_reason import SkipReason
from hpcflow.sdk.core.errors import UnsupportedOSError, UnsupportedSchedulerError
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.parallel import ParallelMode
from hpcflow.sdk.core.utils import (
    check_valid_py_identifier,
    dict_values_process_flat,
    get_enum_by_name_or_val,
    split_param_label,
)
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.submission.shells import get_shell
from hpcflow.sdk.utils.hashing import get_hash


class _ElementPrefixedParameter:
    _app_attr = "_app"

    def __init__(
        self,
        prefix: str,
        element_iteration: Optional[app.Element] = None,
        element_action: Optional[app.ElementAction] = None,
        element_action_run: Optional[app.ElementActionRun] = None,
    ) -> None:
        self._prefix = prefix
        self._element_iteration = element_iteration
        self._element_action = element_action
        self._element_action_run = element_action_run

        self._prefixed_names_unlabelled = None  # assigned on first access

    def __getattr__(self, name):
        if name not in self.prefixed_names_unlabelled:
            raise ValueError(
                f"No {self._prefix} named {name!r}. Available {self._prefix} are: "
                f"{self.prefixed_names_unlabelled_str}."
            )

        labels = self.prefixed_names_unlabelled.get(name)
        if labels:
            # is multiple; return a dict of `ElementParameter`s
            out = {}
            for label_i in labels:
                path_i = f"{self._prefix}.{name}[{label_i}]"
                out[label_i] = self._app.ElementParameter(
                    path=path_i,
                    task=self._task,
                    parent=self._parent,
                    element=self._element_iteration_obj,
                )

        else:
            # could be labelled still, but with `multiple=False`
            path_i = f"{self._prefix}.{name}"
            out = self._app.ElementParameter(
                path=path_i,
                task=self._task,
                parent=self._parent,
                element=self._element_iteration_obj,
            )
        return out

    def __dir__(self):
        return super().__dir__() + self.prefixed_names_unlabelled

    @property
    def _parent(self):
        return self._element_iteration or self._element_action or self._element_action_run

    @property
    def _element_iteration_obj(self):
        if self._element_iteration:
            return self._element_iteration
        else:
            return self._parent.element_iteration

    @property
    def _task(self):
        return self._parent.task

    @property
    def prefixed_names_unlabelled(self) -> Dict[str, List[str]]:
        """Get a mapping between inputs types and associated labels.

        If the schema input for a given input type has `multiple=False` (even if a label
        is defined), the values for that input type will be an empty list.

        """
        if self._prefixed_names_unlabelled is None:
            self._prefixed_names_unlabelled = self._get_prefixed_names_unlabelled()
        return self._prefixed_names_unlabelled

    @property
    def prefixed_names_unlabelled_str(self):
        return ", ".join(i for i in self.prefixed_names_unlabelled)

    def __repr__(self):
        # If there are one or more labels present, then replace with a single name
        # indicating there could be multiple (using a `*` prefix):
        names = []
        for unlabelled, labels in self.prefixed_names_unlabelled.items():
            name_i = unlabelled
            if labels:
                name_i = "*" + name_i
            names.append(name_i)
        names_str = ", ".join(i for i in names)
        return f"{self.__class__.__name__}({names_str})"

    def _get_prefixed_names(self):
        return sorted(self._parent.get_parameter_names(self._prefix))

    def _get_prefixed_names_unlabelled(self) -> Dict[str, List[str]]:
        names = self._get_prefixed_names()
        all_names = {}
        for i in list(names):
            if "[" in i:
                unlab_i, label_i = split_param_label(i)
                if unlab_i not in all_names:
                    all_names[unlab_i] = []
                all_names[unlab_i].append(label_i)
            else:
                all_names[i] = []
        return all_names

    def __iter__(self):
        for name in self.prefixed_names_unlabelled:
            yield getattr(self, name)


class ElementInputs(_ElementPrefixedParameter):
    def __init__(
        self,
        element_iteration: Optional[app.ElementIteration] = None,
        element_action: Optional[app.ElementAction] = None,
        element_action_run: Optional[app.ElementActionRun] = None,
    ) -> None:
        super().__init__("inputs", element_iteration, element_action, element_action_run)


class ElementOutputs(_ElementPrefixedParameter):
    def __init__(
        self,
        element_iteration: Optional[app.ElementIteration] = None,
        element_action: Optional[app.ElementAction] = None,
        element_action_run: Optional[app.ElementActionRun] = None,
    ) -> None:
        super().__init__("outputs", element_iteration, element_action, element_action_run)


class ElementInputFiles(_ElementPrefixedParameter):
    def __init__(
        self,
        element_iteration: Optional[app.ElementIteration] = None,
        element_action: Optional[app.ElementAction] = None,
        element_action_run: Optional[app.ElementActionRun] = None,
    ) -> None:
        super().__init__(
            "input_files", element_iteration, element_action, element_action_run
        )


class ElementOutputFiles(_ElementPrefixedParameter):
    def __init__(
        self,
        element_iteration: Optional[app.ElementIteration] = None,
        element_action: Optional[app.ElementAction] = None,
        element_action_run: Optional[app.ElementActionRun] = None,
    ) -> None:
        super().__init__(
            "output_files", element_iteration, element_action, element_action_run
        )


@dataclass
class ElementResources(JSONLike):
    # TODO: how to specify e.g. high-memory requirement?

    scratch: Optional[str] = None
    parallel_mode: Optional[ParallelMode] = None
    num_cores: Optional[int] = None
    num_cores_per_node: Optional[int] = None
    num_threads: Optional[int] = None
    num_nodes: Optional[int] = None
    scheduler: Optional[str] = None
    shell: Optional[str] = None
    use_job_array: Optional[bool] = None
    max_array_items: Optional[int] = None
    write_app_logs: Optional[bool] = False
    combine_jobscript_std: Optional[bool] = field(default_factory=lambda: os.name != "nt")
    combine_scripts: Optional[bool] = None
    time_limit: Optional[str] = None

    scheduler_args: Optional[Dict] = field(default_factory=dict)
    shell_args: Optional[Dict] = field(default_factory=dict)
    os_name: Optional[str] = None
    environments: Optional[Dict] = None

    # SGE scheduler specific:
    SGE_parallel_env: str = None

    # SLURM scheduler specific:
    SLURM_partition: str = None
    SLURM_num_tasks: str = None
    SLURM_num_tasks_per_node: str = None
    SLURM_num_nodes: str = None
    SLURM_num_cpus_per_task: str = None

    def __post_init__(self):
        if (
            self.num_cores is None
            and self.num_cores_per_node is None
            and self.num_threads is None
            and self.num_nodes is None
        ):
            self.num_cores = 1

        if self.parallel_mode:
            self.parallel_mode = get_enum_by_name_or_val(ParallelMode, self.parallel_mode)

    def _get_repr_fields(self) -> dict:
        """Retrieve attributes that are non-default/non-default-factory values."""
        items = {}
        for f in fields(self):
            if isinstance(f.default_factory, Callable):
                compare = f.default_factory()
            else:
                compare = f.default
            val = attrgetter(f.name)(self)
            if val != compare:
                items[f.name] = val
        return items

    def __repr__(self):
        """Only include non-default/non-default-factory values."""
        items = self._get_repr_fields()
        items_repr = ", ".join(f"{name}={value!r}" for name, value in items.items())
        return f"{self.__class__.__name__}({items_repr})"

    def __eq__(self, other) -> bool:
        if type(self) != type(other):
            return False
        else:
            return self.__dict__ == other.__dict__

    @TimeIt.decorator
    def get_jobscript_hash(self):
        """Get hash from all arguments that distinguish jobscripts."""

        exclude = ["time_limit"]
        if not self.combine_scripts:
            # usually environment selection need not distinguish jobscripts because
            # environments become effective/active within the command files, but if we
            # are combining scripts, then the environments must be the same:
            exclude.append("environments")

        dct = {k: copy.deepcopy(v) for k, v in self.__dict__.items() if k not in exclude}

        # `combine_scripts==False` and `combine_scripts==None` should have an equivalent
        # contribution to the hash, so always set it to `False` if unset at this point:
        if self.combine_scripts is None:
            dct["combine_scripts"] = False

        return get_hash(dct)

    @property
    def is_parallel(self) -> bool:
        """Returns True if any scheduler-agnostic arguments indicate a parallel job."""
        return (
            (self.num_cores and self.num_cores != 1)
            or (self.num_cores_per_node and self.num_cores_per_node != 1)
            or (self.num_nodes and self.num_nodes != 1)
            or (self.num_threads and self.num_threads != 1)
        )

    @property
    def SLURM_is_parallel(self) -> bool:
        """Returns True if any SLURM-specific arguments indicate a parallel job."""
        return (
            (self.SLURM_num_tasks and self.SLURM_num_tasks != 1)
            or (self.SLURM_num_tasks_per_node and self.SLURM_num_tasks_per_node != 1)
            or (self.SLURM_num_nodes and self.SLURM_num_nodes != 1)
            or (self.SLURM_num_cpus_per_task and self.SLURM_num_cpus_per_task != 1)
        )

    @staticmethod
    def get_env_instance_filterable_attributes() -> Tuple[str]:
        """Get a tuple of resource attributes that are used to filter environment
        executable instances at submit- and run-time."""
        return ("num_cores",)  # TODO: filter on `parallel_mode` later

    @staticmethod
    @TimeIt.decorator
    def get_default_os_name():
        return os.name

    @classmethod
    @TimeIt.decorator
    def get_default_shell(cls):
        return cls.app.config.default_shell

    @classmethod
    @TimeIt.decorator
    def get_default_scheduler(cls, os_name, shell_name):
        if os_name == "nt" and "wsl" in shell_name:
            # provide a "*_posix" default scheduler on windows if shell is WSL:
            return "direct_posix"
        return cls.app.config.default_scheduler

    @TimeIt.decorator
    def set_defaults(self):
        if self.os_name is None:
            self.os_name = self.get_default_os_name()
        if self.shell is None:
            self.shell = self.get_default_shell()
        if self.scheduler is None:
            self.scheduler = self.get_default_scheduler(self.os_name, self.shell)

        # merge defaults shell args from config:
        self.shell_args = {
            **self.app.config.shells.get(self.shell, {}).get("defaults", {}),
            **self.shell_args,
        }

        # "direct_posix" scheduler is valid on Windows if using WSL:
        cfg_lookup = f"{self.scheduler}_posix" if "wsl" in self.shell else self.scheduler
        cfg_sched = copy.deepcopy(self.app.config.schedulers.get(cfg_lookup, {}))

        # merge defaults scheduler args from config:
        cfg_defs = cfg_sched.get("defaults", {})
        cfg_opts = cfg_defs.pop("options", {})
        opts = {**cfg_opts, **self.scheduler_args.get("options", {})}
        if opts:
            self.scheduler_args["options"] = opts
        self.scheduler_args = {**cfg_defs, **self.scheduler_args}

    @TimeIt.decorator
    def validate_against_machine(self):
        """Validate the values for `os_name`, `shell` and `scheduler` against those
        supported on this machine (as specified by the app configuration)."""
        if self.os_name != os.name:
            raise UnsupportedOSError(os_name=self.os_name)
        if self.scheduler not in self.app.config.schedulers:
            raise UnsupportedSchedulerError(
                scheduler=self.scheduler,
                supported=self.app.config.schedulers,
            )

        if self.os_name == "nt" and self.combine_jobscript_std:
            raise NotImplementedError(
                "`combine_jobscript_std` is not yet supported on Windows."
            )

        # might raise `UnsupportedShellError`:
        get_shell(shell_name=self.shell, os_name=self.os_name)

        # Validate num_cores/num_nodes against options in config and set scheduler-
        # specific resources (e.g. SGE parallel environmentPE, and SLURM partition)
        if "_" in self.scheduler:  # e.g. WSL on windows uses *_posix
            key = tuple(self.scheduler.split("_"))
        else:
            key = (self.scheduler.lower(), self.os_name.lower())
        scheduler_cls = self.app.scheduler_lookup[key]
        scheduler_cls.process_resources(self, self.app.config.schedulers[self.scheduler])


class ElementIteration:
    _app_attr = "app"

    def __init__(
        self,
        id_: int,
        is_pending: bool,
        index: int,
        element: app.Element,
        data_idx: Dict,
        EARs_initialised: bool,
        EAR_IDs: Dict[int, int],
        EARs: Union[List[Dict], None],
        schema_parameters: List[str],
        loop_idx: Dict,
    ):
        self._id = id_
        self._is_pending = is_pending
        self._index = index
        self._element = element
        self._data_idx = data_idx
        self._loop_idx = loop_idx
        self._schema_parameters = schema_parameters
        self._EARs_initialised = EARs_initialised
        self._EARs = EARs
        self._EAR_IDs = EAR_IDs

        # assigned on first access of corresponding properties:
        self._inputs = None
        self._outputs = None
        self._input_files = None
        self._output_files = None
        self._action_objs = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(id={self.id_!r}, "
            f"index={self.index!r}, element={self.element!r}, "
            f"EARs_initialised={self.EARs_initialised!r}"
            f")"
        )

    @property
    def data_idx(self):
        """The overall element iteration data index, before resolution of EARs."""
        return self._data_idx

    @property
    def EARs_initialised(self):
        """Whether or not the EARs have been initialised."""
        return self._EARs_initialised

    @property
    def element(self):
        return self._element

    @property
    def index(self):
        return self._index

    @property
    def id_(self) -> int:
        return self._id

    @property
    def is_pending(self) -> bool:
        return self._is_pending

    @property
    def task(self):
        return self.element.task

    @property
    def workflow(self):
        return self.element.workflow

    @property
    def loop_idx(self) -> Dict[str, int]:
        return self._loop_idx

    @property
    def schema_parameters(self) -> List[str]:
        return self._schema_parameters

    @property
    def EAR_IDs(self) -> Dict[int, int]:
        return self._EAR_IDs

    @property
    def loop_skipped(self) -> bool:
        """True if the the iteration was skipped entirely due to a loop termination."""
        if not self.action_runs:
            # this includes when runs are not initialised
            return False
        else:
            return all(
                i.skip_reason is SkipReason.LOOP_TERMINATION for i in self.action_runs
            )

    @property
    def EAR_IDs_flat(self):
        return [j for i in self.EAR_IDs.values() for j in i]

    @property
    def actions(self) -> Dict[app.ElementAction]:
        if self._action_objs is None:
            self._action_objs = {
                act_idx: self.app.ElementAction(
                    element_iteration=self,
                    action_idx=act_idx,
                    runs=runs,
                )
                for act_idx, runs in (self._EARs or {}).items()
            }
        return self._action_objs

    @property
    def action_runs(self) -> List[app.ElementActionRun]:
        """Get a list of element action runs, where only the final run is taken for each
        element action."""
        return [i.runs[-1] for i in self.actions.values()]

    @property
    def inputs(self) -> app.ElementInputs:
        if not self._inputs:
            self._inputs = self.app.ElementInputs(element_iteration=self)
        return self._inputs

    @property
    def outputs(self) -> app.ElementOutputs:
        if not self._outputs:
            self._outputs = self.app.ElementOutputs(element_iteration=self)
        return self._outputs

    @property
    def input_files(self) -> app.ElementInputFiles:
        if not self._input_files:
            self._input_files = self.app.ElementInputFiles(element_iteration=self)
        return self._input_files

    @property
    def output_files(self) -> app.ElementOutputFiles:
        if not self._output_files:
            self._output_files = self.app.ElementOutputFiles(element_iteration=self)
        return self._output_files

    def get_parameter_names(self, prefix: str) -> List[str]:
        """Get parameter types associated with a given prefix.

        For example, with the prefix "inputs", this would return `['p1', 'p2']` for a task
        schema that has input types `p1` and `p2`. For inputs, labels are ignored. For
        example, for a task schema that accepts two inputs of the same type `p1`, with
        labels `one` and `two`, this method would return (for the "inputs" prefix):
        `['p1[one]', 'p1[two]']`.

        This method is distinct from `Action.get_parameter_names` in that it returns
        schema-level inputs/outputs, whereas `Action.get_parameter_names` returns
        action-level input/output/file types/labels.

        Parameters
        ----------
        prefix
            One of "inputs", "outputs".

        """
        single_label_lookup = self.task.template._get_single_label_lookup("inputs")
        return list(
            ".".join(single_label_lookup.get(i, i).split(".")[1:])
            for i in self.schema_parameters
            if i.startswith(prefix)
        )

    @TimeIt.decorator
    def get_data_idx(
        self,
        path: str = None,
        action_idx: int = None,
        run_idx: int = -1,
    ) -> Dict[str, int]:
        """
        Parameters
        ----------
        action_idx
            The index of the action within the schema.
        """

        if not self.actions:
            data_idx = self.data_idx

        elif action_idx is None:
            # inputs should be from first action where that input is defined, and outputs
            # should include modifications from all actions; we can't just take
            # `self.data_idx`, because 1) this is used for initial runs, and subsequent
            # runs might have different parametrisations, and 2) we want to include
            # intermediate input/output_files:
            data_idx = {}
            for action in self.actions.values():
                for k, v in action.runs[run_idx].data_idx.items():
                    is_input = k.startswith("inputs")
                    if (is_input and k not in data_idx) or not is_input:
                        data_idx[k] = v

        else:
            elem_act = self.actions[action_idx]
            data_idx = elem_act.runs[run_idx].data_idx

        if path:
            data_idx = {k: v for k, v in data_idx.items() if k.startswith(path)}

        return copy.deepcopy(data_idx)

    @TimeIt.decorator
    def get_parameter_sources(
        self,
        path: str = None,
        action_idx: int = None,
        run_idx: int = -1,
        typ: str = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ) -> Dict[str, Union[str, Dict[str, Any]]]:
        """
        Parameters
        ----------
        use_task_index
            If True, use the task index within the workflow, rather than the task insert
            ID.
        """
        data_idx = self.get_data_idx(path, action_idx, run_idx)

        # the value associated with `repeats.*` is the repeats index, not a parameter ID:
        for k in list(data_idx.keys()):
            if k.startswith("repeats."):
                data_idx.pop(k)

        out = dict_values_process_flat(
            data_idx,
            callable=self.workflow.get_parameter_sources,
        )
        task_key = "task_insert_ID"

        if use_task_index:
            task_key = "task_idx"
            out_task_idx = {}
            for k, v in out.items():
                insert_ID = v.pop("task_insert_ID", None)
                if insert_ID is not None:
                    v[task_key] = self.workflow.tasks.get(insert_ID=insert_ID).index
                out_task_idx[k] = v
            out = out_task_idx

        if typ:
            out_ = {}
            for k, v in out.items():
                is_multi = False
                if isinstance(v, list):
                    is_multi = True
                else:
                    v = [v]

                sources_k = []
                for src_i in v:
                    if src_i["type"] == typ:
                        if not is_multi:
                            sources_k = src_i
                            break
                        else:
                            sources_k.append(src_i)

                if sources_k:
                    out_[k] = sources_k

            out = out_

        if as_strings:
            # format as a dict with compact string values
            self_task_val = (
                self.task.index if task_key == "task_idx" else self.task.insert_ID
            )
            out_strs = {}
            for k, v in out.items():
                if v["type"] == "local_input":
                    if v[task_key] == self_task_val:
                        out_strs[k] = "local"
                    else:
                        out_strs[k] = f"task.{v[task_key]}.input"
                elif v["type"] == "default_input":
                    out_strs == "default"
                else:
                    out_strs[k] = (
                        f"task.{v[task_key]}.element.{v['element_idx']}."
                        f"action.{v['action_idx']}.run.{v['run_idx']}"
                    )
            out = out_strs

        return out

    @TimeIt.decorator
    def get(
        self,
        path: str = None,
        action_idx: int = None,
        run_idx: int = -1,
        default: Any = None,
        raise_on_missing: bool = False,
        raise_on_unset: bool = False,
    ) -> Any:
        """Get element data from the persistent store."""
        # TODO include a "stats" parameter which when set we know the run has been
        # executed (or if start time is set but not end time, we know it's running or
        # failed.)

        data_idx = self.get_data_idx(action_idx=action_idx, run_idx=run_idx)
        single_label_lookup = self.task.template._get_single_label_lookup(prefix="inputs")

        if single_label_lookup:
            # For any non-multiple `SchemaParameter`s of this task with non-empty labels,
            # remove the trivial label:
            for key in list(data_idx.keys()):
                if (path or "").startswith(key):
                    # `path` uses labelled type, so no need to convert to non-labelled
                    continue
                lookup_val = single_label_lookup.get(key)
                if lookup_val:
                    data_idx[lookup_val] = data_idx.pop(key)

        return self.task._get_merged_parameter_data(
            data_index=data_idx,
            path=path,
            raise_on_missing=raise_on_missing,
            raise_on_unset=raise_on_unset,
            default=default,
        )

    @TimeIt.decorator
    def get_EAR_dependencies(
        self,
        as_objects: Optional[bool] = False,
    ) -> List[Union[int, app.ElementActionRun]]:
        """Get EARs that this element iteration depends on (excluding EARs of this element
        iteration)."""
        # TODO: test this includes EARs of upstream iterations of this iteration's element
        if self.action_runs:
            out = sorted(
                set(
                    EAR_ID
                    for i in self.action_runs
                    for EAR_ID in i.get_EAR_dependencies(as_objects=False)
                    if not EAR_ID in self.EAR_IDs_flat
                )
            )
        else:
            # if an "input-only" task schema, then there will be no action runs, but the
            # ElementIteration can still depend on other EARs if inputs are sourced from
            # upstream tasks:
            out = []
            for src in self.get_parameter_sources(typ="EAR_output").values():
                if not isinstance(src, list):
                    src = [src]
                for src_i in src:
                    EAR_ID_i = src_i["EAR_ID"]
                    out.append(EAR_ID_i)
            out = sorted(set(out))

        if as_objects:
            out = self.workflow.get_EARs_from_IDs(out)
        return out

    @TimeIt.decorator
    def get_element_iteration_dependencies(
        self, as_objects: bool = False
    ) -> List[Union[int, app.ElementIteration]]:
        """Get element iterations that this element iteration depends on."""
        # TODO: test this includes previous iterations of this iteration's element
        EAR_IDs = self.get_EAR_dependencies(as_objects=False)
        out = sorted(set(self.workflow.get_element_iteration_IDs_from_EAR_IDs(EAR_IDs)))
        if as_objects:
            out = self.workflow.get_element_iterations_from_IDs(out)
        return out

    @TimeIt.decorator
    def get_element_dependencies(
        self,
        as_objects: Optional[bool] = False,
    ) -> List[Union[int, app.Element]]:
        """Get elements that this element iteration depends on."""
        # TODO: this will be used in viz.
        EAR_IDs = self.get_EAR_dependencies(as_objects=False)
        out = sorted(set(self.workflow.get_element_IDs_from_EAR_IDs(EAR_IDs)))
        if as_objects:
            out = self.workflow.get_elements_from_IDs(out)
        return out

    def get_input_dependencies(self) -> Dict[str, Dict]:
        """Get locally defined inputs/sequences/defaults from other tasks that this
        element iteration depends on."""
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

    def get_task_dependencies(
        self, as_objects: bool = False
    ) -> List[Union[int, app.WorkflowTask]]:
        """Get tasks (insert ID or WorkflowTask objects) that this element iteration
        depends on.

        Dependencies may come from either elements from upstream tasks, or from locally
        defined inputs/sequences/defaults from upstream tasks."""

        out = self.workflow.get_task_IDs_from_element_IDs(
            self.get_element_dependencies(as_objects=False)
        )
        for i in self.get_input_dependencies().values():
            out.append(i["task_insert_ID"])

        out = sorted(set(out))

        if as_objects:
            out = [self.workflow.tasks.get(insert_ID=i) for i in out]

        return out

    @TimeIt.decorator
    def get_dependent_EARs(
        self, as_objects: bool = False
    ) -> List[Union[int, app.ElementActionRun]]:
        """Get EARs of downstream iterations and tasks that depend on this element
        iteration."""
        # TODO: test this includes EARs of downstream iterations of this iteration's element
        deps = []
        for task in self.workflow.tasks[self.task.index :]:
            for elem in task.elements[:]:
                for iter_ in elem.iterations:
                    if iter_.id_ == self.id_:
                        # don't include EARs of this iteration
                        continue
                    for run in iter_.action_runs:
                        for dep_EAR_i in run.get_EAR_dependencies(as_objects=True):
                            # does dep_EAR_i belong to self?
                            if dep_EAR_i.id_ in self.EAR_IDs_flat and run.id_ not in deps:
                                deps.append(run.id_)
        deps = sorted(deps)
        if as_objects:
            deps = self.workflow.get_EARs_from_IDs(deps)

        return deps

    @TimeIt.decorator
    def get_dependent_element_iterations(
        self, as_objects: bool = False
    ) -> List[Union[int, app.ElementIteration]]:
        """Get elements iterations of downstream iterations and tasks that depend on this
        element iteration."""
        # TODO: test this includes downstream iterations of this iteration's element?
        deps = []
        for task in self.workflow.tasks[self.task.index :]:
            for elem in task.elements:
                for iter_i in elem.iterations:
                    if iter_i.id_ == self.id_:
                        continue
                    for dep_iter_i in iter_i.get_element_iteration_dependencies(
                        as_objects=True
                    ):
                        if dep_iter_i.id_ == self.id_ and iter_i.id_ not in deps:
                            deps.append(iter_i.id_)
        deps = sorted(deps)
        if as_objects:
            deps = self.workflow.get_element_iterations_from_IDs(deps)

        return deps

    @TimeIt.decorator
    def get_dependent_elements(
        self,
        as_objects: bool = False,
    ) -> List[Union[int, app.Element]]:
        """Get elements of downstream tasks that depend on this element iteration."""
        deps = []
        for task in self.task.downstream_tasks:
            for element in task.elements[:]:
                for iter_i in element.iterations:
                    for dep_iter_i in iter_i.get_element_iteration_dependencies(
                        as_objects=True
                    ):
                        if dep_iter_i.id_ == self.id_ and element.id_ not in deps:
                            deps.append(element.id_)

        deps = sorted(deps)
        if as_objects:
            deps = self.workflow.get_elements_from_IDs(deps)

        return deps

    def get_dependent_tasks(
        self,
        as_objects: bool = False,
    ) -> List[Union[int, app.WorkflowTask]]:
        """Get downstream tasks that depend on this element iteration."""
        deps = []
        for task in self.task.downstream_tasks:
            for element in task.elements[:]:
                for iter_i in element.iterations:
                    for dep_iter_i in iter_i.get_element_iteration_dependencies(
                        as_objects=True
                    ):
                        if dep_iter_i.id_ == self.id_ and task.insert_ID not in deps:
                            deps.append(task.insert_ID)
        deps = sorted(deps)
        if as_objects:
            deps = [self.workflow.tasks.get(insert_ID=i) for i in deps]

        return deps

    def get_template_resources(self) -> Dict:
        """Get template-level resources."""
        out = {}
        for res_i in self.workflow.template.resources:
            out[res_i.scope.to_string()] = res_i._get_value()
        return out

    @TimeIt.decorator
    def get_resources(self, action: app.Action, set_defaults: bool = False) -> Dict:
        """Resolve specific resources for the specified action of this iteration,
        considering all applicable scopes.

        Parameters
        ----------
        set_defaults
            If `True`, include machine-defaults for `os_name`, `shell` and `scheduler`.

        """

        # This method is currently accurate for both `ElementIteration` and `EAR` objects
        # because when generating the EAR data index we copy (from the schema data index)
        # anything that starts with "resources". BUT: when we support adding a run, the
        # user should be able to modify the resources! Which would invalidate this
        # assumption!!!!!

        # --- so need to rethink...
        # question is perhaps "what would the resources be if this action were to become
        # an EAR?" which would then allow us to test a resources-based action rule.

        resource_specs = copy.deepcopy(self.get("resources"))

        env_spec = action.get_environment_spec()
        env_name = env_spec["name"]

        # set default env specifiers, if none set:
        if "any" not in resource_specs:
            resource_specs["any"] = {}
        if "environments" not in resource_specs["any"]:
            resource_specs["any"]["environments"] = {env_name: copy.deepcopy(env_spec)}

        for scope, dat in resource_specs.items():
            if "environments" in dat:
                # keep only relevant user-provided environment specifiers:
                resource_specs[scope]["environments"] = {
                    k: v for k, v in dat["environments"].items() if k == env_name
                }
                # merge user-provided specifiers into action specifiers:
                resource_specs[scope]["environments"][env_name] = {
                    **resource_specs[scope]["environments"].get(env_name, {}),
                    **copy.deepcopy(env_spec),
                }

        resources = {}
        for scope in action.get_possible_scopes()[::-1]:
            # loop in reverse so higher-specificity scopes take precedence:
            scope_s = scope.to_string()
            scope_res = resource_specs.get(scope_s, {})
            resources.update({k: v for k, v in scope_res.items() if v is not None})

        if set_defaults:
            # used in e.g. `Rule.test` if testing resource rules on element iterations:
            if "os_name" not in resources:
                resources["os_name"] = self.app.ElementResources.get_default_os_name()
            if "shell" not in resources:
                resources["shell"] = self.app.ElementResources.get_default_shell()
            if "scheduler" not in resources:
                resources["scheduler"] = self.app.ElementResources.get_default_scheduler(
                    resources["os_name"], resources["shell"]
                )

        # unset inapplicable items:
        if "combine_scripts" in resources and not action.script_is_python_snippet:
            del resources["combine_scripts"]

        return resources

    def get_resources_obj(
        self, action: app.Action, set_defaults: bool = False
    ) -> app.ElementResources:
        return self.app.ElementResources(**self.get_resources(action, set_defaults))


class Element:
    _app_attr = "app"

    # TODO: use slots
    # TODO:
    #   - add `iterations` property which returns `ElementIteration`
    #   - also map iteration properties of the most recent iteration to this object

    def __init__(
        self,
        id_: int,
        is_pending: bool,
        task: app.WorkflowTask,
        index: int,
        es_idx: int,
        seq_idx: Dict[str, int],
        src_idx: Dict[str, int],
        iteration_IDs: List[int],
        iterations: List[Dict],
    ) -> None:
        self._id = id_
        self._is_pending = is_pending
        self._task = task
        self._index = index
        self._es_idx = es_idx
        self._seq_idx = seq_idx
        self._src_idx = src_idx

        self._iteration_IDs = iteration_IDs
        self._iterations = iterations

        # assigned on first access:
        self._iteration_objs = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(id={self.id_!r}, "
            f"index={self.index!r}, task={self.task.unique_name!r}"
            f")"
        )

    @property
    def id_(self) -> int:
        return self._id

    @property
    def is_pending(self) -> bool:
        return self._is_pending

    @property
    def task(self) -> app.WorkflowTask:
        return self._task

    @property
    def index(self) -> int:
        """Get the index of the element within the task.

        Note: the `global_idx` attribute returns the index of the element within the
        workflow, across all tasks."""

        return self._index

    @property
    def element_set_idx(self) -> int:
        return self._es_idx

    @property
    def element_set(self):
        return self.task.template.element_sets[self.element_set_idx]

    @property
    def sequence_idx(self) -> Dict[str, int]:
        return self._seq_idx

    @property
    def input_source_idx(self) -> Dict[str, int]:
        return self._src_idx

    @property
    def input_sources(self) -> Dict[str, app.InputSource]:
        return {
            k: self.element_set.input_sources[k.split("inputs.")[1]][v]
            for k, v in self.input_source_idx.items()
        }

    @property
    def workflow(self) -> app.Workflow:
        return self.task.workflow

    @property
    def iteration_IDs(self) -> List[int]:
        return self._iteration_IDs

    @property
    @TimeIt.decorator
    def iterations(self) -> Dict[app.ElementAction]:
        # TODO: fix this
        if self._iteration_objs is None:
            self._iteration_objs = [
                self.app.ElementIteration(
                    element=self,
                    index=idx,
                    **{k: v for k, v in iter_i.items() if k != "element_ID"},
                )
                for idx, iter_i in enumerate(self._iterations)
            ]
        return self._iteration_objs

    @property
    def dir_name(self):
        return f"e_{self.index}"

    @property
    def latest_iteration(self):
        """Get the latest iteration."""
        return self.iterations[-1]

    @property
    def latest_iteration_non_skipped(self):
        """Get the latest iteration that is not loop-skipped."""
        for iter_i in self.iterations[::-1]:
            if not iter_i.loop_skipped:
                return iter_i

    @property
    def inputs(self) -> app.ElementInputs:
        return self.latest_iteration_non_skipped.inputs

    @property
    def outputs(self) -> app.ElementOutputs:
        return self.latest_iteration_non_skipped.outputs

    @property
    def input_files(self) -> app.ElementInputFiles:
        return self.latest_iteration_non_skipped.input_files

    @property
    def output_files(self) -> app.ElementOutputFiles:
        return self.latest_iteration_non_skipped.output_files

    @property
    def schema_parameters(self) -> List[str]:
        return self.latest_iteration_non_skipped.schema_parameters

    @property
    def actions(self) -> Dict[app.ElementAction]:
        return self.latest_iteration_non_skipped.actions

    @property
    def action_runs(self) -> List[app.ElementActionRun]:
        """Get a list of element action runs from the latest non-skipped iteration,
        where only the final run is taken for each element action."""
        return self.latest_iteration_non_skipped.action_runs

    def to_element_set_data(self):
        """Generate lists of workflow-bound InputValues and ResourceList."""
        inputs = []
        resources = []
        for k, v in self.get_data_idx().items():
            k_s = k.split(".")

            if k_s[0] == "inputs":
                inp_val = self.app.InputValue(
                    parameter=k_s[1],
                    path=k_s[2:] or None,
                    value=None,
                )
                inp_val._value_group_idx = v
                inp_val._workflow = self.workflow
                inputs.append(inp_val)

            elif k_s[0] == "resources":
                scope = self.app.ActionScope.from_json_like(k_s[1])
                res = self.app.ResourceSpec(scope=scope)
                res._value_group_idx = v
                res._workflow = self.workflow
                resources.append(res)

        return inputs, resources

    def get_sequence_value(self, sequence_path: str) -> Any:
        seq = self.element_set.get_sequence_from_path(sequence_path)
        if not seq:
            raise ValueError(
                f"No sequence with path {sequence_path!r} in this element's originating "
                f"element set."
            )
        return seq.values[self.sequence_idx[sequence_path]]

    def get_data_idx(
        self,
        path: str = None,
        action_idx: int = None,
        run_idx: int = -1,
    ) -> Dict[str, int]:
        """Get the data index of the most recent element iteration that
        is not loop-skipped.

        Parameters
        ----------
        action_idx
            The index of the action within the schema.
        """
        return self.latest_iteration_non_skipped.get_data_idx(
            path=path,
            action_idx=action_idx,
            run_idx=run_idx,
        )

    def get_parameter_sources(
        self,
        path: str = None,
        action_idx: int = None,
        run_idx: int = -1,
        typ: str = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ) -> Dict[str, Union[str, Dict[str, Any]]]:
        """ "Get the parameter sources of the most recent element iteration.

        Parameters
        ----------
        use_task_index
            If True, use the task index within the workflow, rather than the task insert
            ID.
        """
        return self.latest_iteration.get_parameter_sources(
            path=path,
            action_idx=action_idx,
            run_idx=run_idx,
            typ=typ,
            as_strings=as_strings,
            use_task_index=use_task_index,
        )

    def get(
        self,
        path: str = None,
        action_idx: int = None,
        run_idx: int = -1,
        default: Any = None,
        raise_on_missing: bool = False,
        raise_on_unset: bool = False,
    ) -> Any:
        """Get element data of the most recent iteration that is not
        loop-skipped."""
        return self.latest_iteration_non_skipped.get(
            path=path,
            action_idx=action_idx,
            run_idx=run_idx,
            default=default,
            raise_on_missing=raise_on_missing,
            raise_on_unset=raise_on_unset,
        )

    @TimeIt.decorator
    def get_EAR_dependencies(
        self, as_objects: bool = False
    ) -> List[Union[int, app.ElementActionRun]]:
        """Get EARs that the most recent iteration of this element depends on."""
        return self.latest_iteration.get_EAR_dependencies(as_objects=as_objects)

    def get_element_iteration_dependencies(
        self, as_objects: bool = False
    ) -> List[Union[int, app.ElementIteration]]:
        """Get element iterations that the most recent iteration of this element depends
        on."""
        return self.latest_iteration.get_element_iteration_dependencies(
            as_objects=as_objects
        )

    def get_element_dependencies(
        self, as_objects: bool = False
    ) -> List[Union[int, app.Element]]:
        """Get elements that the most recent iteration of this element depends on."""
        return self.latest_iteration.get_element_dependencies(as_objects=as_objects)

    def get_input_dependencies(self) -> Dict[str, Dict]:
        """Get locally defined inputs/sequences/defaults from other tasks that this
        the most recent iteration of this element depends on."""
        return self.latest_iteration.get_input_dependencies()

    def get_task_dependencies(
        self, as_objects: bool = False
    ) -> List[Union[int, app.WorkflowTask]]:
        """Get tasks (insert ID or WorkflowTask objects) that the most recent iteration of
        this element depends on.

        Dependencies may come from either elements from upstream tasks, or from locally
        defined inputs/sequences/defaults from upstream tasks."""
        return self.latest_iteration.get_task_dependencies(as_objects=as_objects)

    def get_dependent_EARs(
        self, as_objects: bool = False
    ) -> List[Union[int, app.ElementActionRun]]:
        """Get EARs that depend on the most recent iteration of this element."""
        return self.latest_iteration.get_dependent_EARs(as_objects=as_objects)

    def get_dependent_element_iterations(
        self, as_objects: bool = False
    ) -> List[Union[int, app.ElementIteration]]:
        """Get element iterations that depend on the most recent iteration of this
        element."""
        return self.latest_iteration.get_dependent_element_iterations(
            as_objects=as_objects
        )

    def get_dependent_elements(
        self, as_objects: bool = False
    ) -> List[Union[int, app.Element]]:
        """Get elements that depend on the most recent iteration of this element."""
        return self.latest_iteration.get_dependent_elements(as_objects=as_objects)

    def get_dependent_tasks(
        self, as_objects: bool = False
    ) -> List[Union[int, app.WorkflowTask]]:
        """Get tasks that depend on the most recent iteration of this element."""
        return self.latest_iteration.get_dependent_tasks(as_objects=as_objects)

    @TimeIt.decorator
    def get_dependent_elements_recursively(self, task_insert_ID=None):
        """Get downstream elements that depend on this element, including recursive
        dependencies.

        Dependencies are resolved using the initial iteration only. This method is used to
        identify from which element in the previous iteration a new iteration should be
        parametrised.

        Parameters
        ----------
        task_insert_ID
            If specified, only return elements from this task.

        """

        def get_deps(element):
            deps = element.iterations[0].get_dependent_elements(as_objects=False)
            deps_objs = self.workflow.get_elements_from_IDs(deps)
            return set(deps).union(
                [dep_j for deps_i in deps_objs for dep_j in get_deps(deps_i)]
            )

        all_deps = get_deps(self)

        if task_insert_ID is not None:
            elem_ID_subset = self.workflow.tasks.get(insert_ID=task_insert_ID).element_IDs
            all_deps = [i for i in all_deps if i in elem_ID_subset]

        return self.workflow.get_elements_from_IDs(sorted(all_deps))


@dataclass
class ElementParameter:
    _app_attr = "app"

    task: app.WorkflowTask
    path: str
    parent: Union[Element, app.ElementAction, app.ElementActionRun, app.Parameters]
    element: Element

    @property
    def data_idx(self):
        return self.parent.get_data_idx(path=self.path)

    @property
    def value(self) -> Any:
        return self.parent.get(path=self.path)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(element={self.element!r}, path={self.path!r})"

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, self.__class__):
            return False
        if self.task == __o.task and self.path == __o.path:
            return True

    @property
    def data_idx_is_set(self):
        return {
            k: self.task.workflow.is_parameter_set(v) for k, v in self.data_idx.items()
        }

    @property
    def is_set(self):
        return all(self.data_idx_is_set.values())

    def get_size(self, **store_kwargs):
        raise NotImplementedError


@dataclass
class ElementFilter(JSONLike):
    _child_objects = (ChildObjectSpec(name="rules", is_multiple=True, class_name="Rule"),)

    rules: List[app.Rule] = field(default_factory=list)

    def filter(
        self, element_iters: List[app.ElementIteration]
    ) -> List[app.ElementIteration]:
        out = []
        for i in element_iters:
            if all(rule_j.test(i) for rule_j in self.rules):
                out.append(i)
        return out


@dataclass
class ElementGroup(JSONLike):
    name: str
    where: Optional[ElementFilter] = None
    group_by_distinct: Optional[app.ParameterPath] = None

    def __post_init__(self):
        self.name = check_valid_py_identifier(self.name)


@dataclass
class ElementRepeats:
    number: int
    where: Optional[ElementFilter] = None
