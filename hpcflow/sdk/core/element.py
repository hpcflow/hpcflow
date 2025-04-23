"""
Elements are components of tasks.
"""

from __future__ import annotations
import copy
from dataclasses import dataclass, field, fields
from operator import attrgetter
from itertools import chain
import os
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    cast,
    overload,
    TYPE_CHECKING,
)

from hpcflow.sdk.core.enums import ParallelMode
from hpcflow.sdk.core.skip_reason import SkipReason
from hpcflow.sdk.core.errors import UnsupportedOSError, UnsupportedSchedulerError
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.loop_cache import LoopIndex
from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.app_aware import AppAware
from hpcflow.sdk.core.utils import (
    check_valid_py_identifier,
    dict_values_process_flat,
    get_enum_by_name_or_val,
    split_param_label,
)
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.submission.shells import get_shell
from hpcflow.sdk.utils.hashing import get_hash

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence
    from typing import Any, ClassVar, Literal
    from ..app import BaseApp
    from ..typing import DataIndex, ParamSource
    from .actions import Action, ElementAction, ElementActionRun
    from .parameters import InputSource, ParameterPath, InputValue, ResourceSpec
    from .rule import Rule
    from .task import WorkflowTask, ElementSet
    from .workflow import Workflow


class _ElementPrefixedParameter(AppAware):
    def __init__(
        self,
        prefix: str,
        element_iteration: ElementIteration | None = None,
        element_action: ElementAction | None = None,
        element_action_run: ElementActionRun | None = None,
    ) -> None:
        self._prefix = prefix
        self._element_iteration = element_iteration
        self._element_action = element_action
        self._element_action_run = element_action_run

        # assigned on first access
        self._prefixed_names_unlabelled: Mapping[str, Sequence[str]] | None = None

    def __getattr__(self, name: str) -> ElementParameter | Mapping[str, ElementParameter]:
        if name not in self.prefixed_names_unlabelled:
            if names_str := self.prefixed_names_unlabelled_str:
                msg_info = f"Available {self._prefix} are: {names_str}."
            else:
                msg_info = f"There are no {self._prefix} available."
            raise ValueError(f"No {self._prefix} named {name!r}. {msg_info}")

        if labels := self.prefixed_names_unlabelled.get(name):
            # is multiple; return a dict of `ElementParameter`s
            return {
                label_i: self.__parameter(f"{self._prefix}.{name}[{label_i}]")
                for label_i in labels
            }
        else:
            # could be labelled still, but with `multiple=False`
            return self.__parameter(f"{self._prefix}.{name}")

    def __dir__(self) -> Iterator[str]:
        yield from super().__dir__()
        yield from self.prefixed_names_unlabelled

    @property
    def __parent(self) -> ElementIteration | ElementActionRun | ElementAction:
        p = self._element_iteration or self._element_action or self._element_action_run
        assert p is not None
        return p

    def __parameter(self, name: str) -> ElementParameter:
        """Manufacture an ElementParameter with the given name."""
        p = self.__parent
        return self._app.ElementParameter(
            path=name,
            task=self._task,
            parent=p,
            element=p if isinstance(p, ElementIteration) else p.element_iteration,
        )

    @property
    def _task(self) -> WorkflowTask:
        return self.__parent.task

    @property
    def prefixed_names_unlabelled(self) -> Mapping[str, Sequence[str]]:
        """
        A mapping between input types and associated labels.

        If the schema input for a given input type has `multiple=False` (even if a label
        is defined), the values for that input type will be an empty list.

        """
        if self._prefixed_names_unlabelled is None:
            self._prefixed_names_unlabelled = self.__get_prefixed_names_unlabelled()
        return self._prefixed_names_unlabelled

    @property
    def prefixed_names_unlabelled_str(self) -> str:
        """
        A description of the prefixed names.
        """
        return ", ".join(self.prefixed_names_unlabelled)

    def __repr__(self) -> str:
        # If there are one or more labels present, then replace with a single name
        # indicating there could be multiple (using a `*` prefix):
        names = ", ".join(
            "*" + unlabelled if labels else unlabelled
            for unlabelled, labels in self.prefixed_names_unlabelled.items()
        )
        return f"{self.__class__.__name__}({names})"

    def _get_prefixed_names(self) -> list[str]:
        return sorted(self.__parent.get_parameter_names(self._prefix))

    def __get_prefixed_names_unlabelled(self) -> Mapping[str, Sequence[str]]:
        all_names: dict[str, list[str]] = {}
        for name in self._get_prefixed_names():
            if "[" in name:
                unlab_i, label_i = split_param_label(name)
                if unlab_i is not None and label_i is not None:
                    all_names.setdefault(unlab_i, []).append(label_i)
            else:
                all_names[name] = []
        return all_names

    def __iter__(self) -> Iterator[ElementParameter | Mapping[str, ElementParameter]]:
        for name in self.prefixed_names_unlabelled:
            yield getattr(self, name)


class ElementInputs(_ElementPrefixedParameter):
    """
    The inputs to an element.

    Parameters
    ----------
    element_iteration: ElementIteration
        Which iteration does this refer to?
    element_action: ~hpcflow.app.ElementAction
        Which action does this refer to?
    element_action_run: ~hpcflow.app.ElementActionRun
        Which EAR does this refer to?
    """

    def __init__(
        self,
        element_iteration: ElementIteration | None = None,
        element_action: ElementAction | None = None,
        element_action_run: ElementActionRun | None = None,
    ) -> None:
        super().__init__("inputs", element_iteration, element_action, element_action_run)


class ElementOutputs(_ElementPrefixedParameter):
    """
    The outputs from an element.

    Parameters
    ----------
    element_iteration: ElementIteration
        Which iteration does this refer to?
    element_action: ~hpcflow.app.ElementAction
        Which action does this refer to?
    element_action_run: ~hpcflow.app.ElementActionRun
        Which EAR does this refer to?
    """

    def __init__(
        self,
        element_iteration: ElementIteration | None = None,
        element_action: ElementAction | None = None,
        element_action_run: ElementActionRun | None = None,
    ) -> None:
        super().__init__("outputs", element_iteration, element_action, element_action_run)


class ElementInputFiles(_ElementPrefixedParameter):
    """
    The input files to an element.

    Parameters
    ----------
    element_iteration: ElementIteration
        Which iteration does this refer to?
    element_action: ~hpcflow.app.ElementAction
        Which action does this refer to?
    element_action_run: ~hpcflow.app.ElementActionRun
        Which EAR does this refer to?
    """

    def __init__(
        self,
        element_iteration: ElementIteration | None = None,
        element_action: ElementAction | None = None,
        element_action_run: ElementActionRun | None = None,
    ) -> None:
        super().__init__(
            "input_files", element_iteration, element_action, element_action_run
        )


class ElementOutputFiles(_ElementPrefixedParameter):
    """
    The output files from an element.

    Parameters
    ----------
    element_iteration: ElementIteration
        Which iteration does this refer to?
    element_action: ~hpcflow.app.ElementAction
        Which action does this refer to?
    element_action_run: ~hpcflow.app.ElementActionRun
        Which EAR does this refer to?
    """

    def __init__(
        self,
        element_iteration: ElementIteration | None = None,
        element_action: ElementAction | None = None,
        element_action_run: ElementActionRun | None = None,
    ) -> None:
        super().__init__(
            "output_files", element_iteration, element_action, element_action_run
        )


@dataclass
@hydrate
class ElementResources(JSONLike):
    """
    The resources an element requires.

    Note
    ----
    It is common to leave most of these unspecified.
    Many of them have complex interactions with each other.

    Parameters
    ----------
    scratch: str
        Which scratch space to use.
    parallel_mode: ParallelMode
        Which parallel mode to use.
    num_cores: int
        How many cores to request.
    num_cores_per_node: int
        How many cores per compute node to request.
    num_threads: int
        How many threads to request.
    num_nodes: int
        How many compute nodes to request.
    scheduler: str
        Which scheduler to use.
    shell: str
        Which system shell to use.
    use_job_array: bool
        Whether to use array jobs.
    max_array_items: int
        If using array jobs, up to how many items should be in the job array.
    write_app_logs: bool
        Whether an app log file should be written.
    combine_jobscript_std: bool
        Whether jobscript standard output and error streams should be combined.
    combine_scripts: bool
        Whether Python scripts should be combined.
    time_limit: str
        How long to run for.
    scheduler_args: dict[str, Any]
        Additional arguments to pass to the scheduler.
    shell_args: dict[str, Any]
        Additional arguments to pass to the shell.
    os_name: str
        Which OS to use.
    environments: dict
        Which execution environments to use.
    resources_id: int
        An arbitrary integer that can be used to force multiple jobscripts.
    skip_downstream_on_failure: bool
        Whether to skip downstream dependents on failure.
    allow_failed_dependencies: int | float | bool | None
        The failure tolerance with respect to dependencies, specified as a number or
        proportion.
    SGE_parallel_env: str
        Which SGE parallel environment to request.
    SLURM_partition: str
        Which SLURM partition to request.
    SLURM_num_tasks: str
        How many SLURM tasks to request.
    SLURM_num_tasks_per_node: str
        How many SLURM tasks per compute node to request.
    SLURM_num_nodes: str
        How many compute nodes to request.
    SLURM_num_cpus_per_task: str
        How many CPU cores to ask for per SLURM task.
    """

    # TODO: how to specify e.g. high-memory requirement?

    #: Which scratch space to use.
    scratch: str | None = None
    #: Which parallel mode to use.
    parallel_mode: ParallelMode | None = None
    #: How many cores to request.
    num_cores: int | None = None
    #: How many cores per compute node to request.
    num_cores_per_node: int | None = None
    #: How many threads to request.
    num_threads: int | None = None
    #: How many compute nodes to request.
    num_nodes: int | None = None

    #: Which scheduler to use.
    scheduler: str | None = None
    #: Which system shell to use.
    shell: str | None = None
    #: Whether to use array jobs.
    use_job_array: bool | None = None
    #: If using array jobs, up to how many items should be in the job array.
    max_array_items: int | None = None
    #: Whether an app log file should be written.
    write_app_logs: bool = False
    #: Whether jobscript standard output and error streams should be combined.
    combine_jobscript_std: bool = field(default_factory=lambda: os.name != "nt")
    #: Whether Python scripts should be combined.
    combine_scripts: bool | None = None
    #: How long to run for.
    time_limit: str | None = None

    #: Additional arguments to pass to the scheduler.
    scheduler_args: dict[str, Any] = field(default_factory=dict)
    #: Additional arguments to pass to the shell.
    shell_args: dict[str, Any] = field(default_factory=dict)
    #: Which OS to use.
    os_name: str | None = None
    #: Which execution environments to use.
    environments: dict[str, dict[str, Any]] | None = None
    #: An arbitrary integer that can be used to force multiple jobscripts.
    resources_id: int | None = None
    #: Whether to skip downstream dependents on failure.
    skip_downstream_on_failure: bool = True
    #: The failure tolerance with respect to dependencies, specified as a number or
    #: proportion.
    allow_failed_dependencies: int | float | bool | None = False

    # SGE scheduler specific:
    #: Which SGE parallel environment to request.
    SGE_parallel_env: str | None = None

    # SLURM scheduler specific:
    #: Which SLURM partition to request.
    SLURM_partition: str | None = None
    #: How many SLURM tasks to request.
    SLURM_num_tasks: int | None = None
    #: How many SLURM tasks per compute node to request.
    SLURM_num_tasks_per_node: int | None = None
    #: How many compute nodes to request.
    SLURM_num_nodes: int | None = None
    #: How many CPU cores to ask for per SLURM task.
    SLURM_num_cpus_per_task: int | None = None

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

        self.scheduler_args = self.scheduler_args or {}
        self.shell_args = self.shell_args or {}

    def __eq__(self, other) -> bool:
        if type(self) != type(other):
            return False
        else:
            return self.__dict__ == other.__dict__

    @TimeIt.decorator
    def get_jobscript_hash(self) -> int:
        """Get hash from all arguments that distinguish jobscripts."""

        exclude = ["time_limit", "skip_downstream_on_failure"]
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
        return bool(
            (self.num_cores and self.num_cores != 1)
            or (self.num_cores_per_node and self.num_cores_per_node != 1)
            or (self.num_nodes and self.num_nodes != 1)
            or (self.num_threads and self.num_threads != 1)
        )

    @property
    def SLURM_is_parallel(self) -> bool:
        """Returns True if any SLURM-specific arguments indicate a parallel job."""
        return bool(
            (self.SLURM_num_tasks and self.SLURM_num_tasks != 1)
            or (self.SLURM_num_tasks_per_node and self.SLURM_num_tasks_per_node != 1)
            or (self.SLURM_num_nodes and self.SLURM_num_nodes != 1)
            or (self.SLURM_num_cpus_per_task and self.SLURM_num_cpus_per_task != 1)
        )

    @staticmethod
    def get_env_instance_filterable_attributes() -> tuple[str, ...]:
        """Get a tuple of resource attributes that are used to filter environment
        executable instances at submit- and run-time."""
        return ("num_cores",)  # TODO: filter on `parallel_mode` later

    @staticmethod
    @TimeIt.decorator
    def get_default_os_name() -> str:
        """
        Get the default value for OS name.
        """
        return os.name

    @classmethod
    @TimeIt.decorator
    def get_default_shell(cls) -> str:
        """
        Get the default value for name.
        """
        return cls._app.config.default_shell

    @classmethod
    @TimeIt.decorator
    def get_default_scheduler(cls, os_name: str, shell_name: str) -> str:
        """
        Get the default value for scheduler.
        """
        if os_name == "nt" and "wsl" in shell_name:
            # provide a "*_posix" default scheduler on windows if shell is WSL:
            return "direct_posix"
        return cls._app.config.default_scheduler

    @TimeIt.decorator
    def set_defaults(self):
        """
        Set defaults for unspecified values that need defaults.
        """
        if self.os_name is None:
            self.os_name = self.get_default_os_name()
        if self.shell is None:
            self.shell = self.get_default_shell()
        if self.scheduler is None:
            self.scheduler = self.get_default_scheduler(self.os_name, self.shell)

        # merge defaults shell args from config:
        self.shell_args = {
            **self._app.config.shells.get(self.shell, {}).get("defaults", {}),
            **self.shell_args,
        }

        # "direct_posix" scheduler is valid on Windows if using WSL:
        cfg_lookup = f"{self.scheduler}_posix" if "wsl" in self.shell else self.scheduler
        cfg_sched = copy.deepcopy(self._app.config.schedulers.get(cfg_lookup, {}))

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
        if self.scheduler not in self._app.config.schedulers:
            raise UnsupportedSchedulerError(
                scheduler=self.scheduler,
                supported=self._app.config.schedulers,
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
        scheduler_cls = self._app.scheduler_lookup[key]
        scheduler_cls.process_resources(self, self._app.config.schedulers[self.scheduler])


class ElementIteration(AppAware):
    """
    A particular iteration of an element.

    Parameters
    ----------
    id_ : int
        The ID of this iteration.
    is_pending: bool
        Whether this iteration is pending execution.
    index: int
        The index of this iteration in its parent element.
    element: Element
        The element this is an iteration of.
    data_idx: dict
        The overall element iteration data index, before resolution of EARs.
    EARs_initialised: bool
        Whether EARs have been set up for the iteration.
    EAR_IDs: dict[int, int]
        Mapping from iteration number to EAR ID, where known.
    EARs: list[dict]
        Data about EARs.
    schema_parameters: list[str]
        Parameters from the schema.
    loop_idx: dict[str, int]
        Indexing information from the loop.
    """

    def __init__(
        self,
        id_: int,
        is_pending: bool,
        index: int,
        element: Element,
        data_idx: DataIndex,
        EARs_initialised: bool,
        EAR_IDs: dict[int, list[int]],
        EARs: dict[int, dict[Mapping[str, Any], Any]] | None,
        schema_parameters: list[str],
        loop_idx: Mapping[str, int],
    ):
        self._id = id_
        self._is_pending = is_pending
        self._index = index
        self._element = element
        self._data_idx = data_idx
        self._loop_idx = LoopIndex(loop_idx)
        self._schema_parameters = schema_parameters
        self._EARs_initialised = EARs_initialised
        self._EARs = EARs
        self._EAR_IDs = EAR_IDs

        # assigned on first access of corresponding properties:
        self._inputs: ElementInputs | None = None
        self._outputs: ElementOutputs | None = None
        self._input_files: ElementInputFiles | None = None
        self._output_files: ElementOutputFiles | None = None
        self._action_objs: dict[int, ElementAction] | None = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(id={self.id_!r}, "
            f"index={self.index!r}, element={self.element!r}, "
            f"EARs_initialised={self.EARs_initialised!r}"
            f")"
        )

    @property
    def data_idx(self) -> DataIndex:
        """The overall element iteration data index, before resolution of EARs."""
        return self._data_idx

    @property
    def EARs_initialised(self) -> bool:
        """Whether or not the EARs have been initialised."""
        return self._EARs_initialised

    @property
    def element(self) -> Element:
        """
        The element this is an iteration of.
        """
        return self._element

    @property
    def index(self) -> int:
        """
        The index of this iteration in its parent element.
        """
        return self._index

    @property
    def id_(self) -> int:
        """
        The ID of this iteration.
        """
        return self._id

    @property
    def is_pending(self) -> bool:
        """
        Whether this iteration is pending execution.
        """
        return self._is_pending

    @property
    def task(self) -> WorkflowTask:
        """
        The task this is an iteration of an element for.
        """
        return self.element.task

    @property
    def workflow(self) -> Workflow:
        """
        The workflow this is a part of.
        """
        return self.element.workflow

    @property
    def loop_idx(self) -> LoopIndex[str, int]:
        """
        Indexing information from the loop.
        """
        return self._loop_idx

    @property
    def schema_parameters(self) -> Sequence[str]:
        """
        Parameters from the schema.
        """
        return self._schema_parameters

    @property
    def EAR_IDs(self) -> Mapping[int, Sequence[int]]:
        """
        Mapping from action index to EAR ID, where known.
        """
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
    def EAR_IDs_flat(self) -> Iterable[int]:
        """
        The EAR IDs.
        """
        return chain.from_iterable(self.EAR_IDs.values())

    @property
    def actions(self) -> Mapping[int, ElementAction]:
        """
        The actions of this iteration.
        """
        if self._action_objs is None:
            self._action_objs = {
                act_idx: self._app.ElementAction(self, act_idx, runs)
                for act_idx, runs in (self._EARs or {}).items()
            }
        return self._action_objs

    @property
    def action_runs(self) -> Sequence[ElementActionRun]:
        """
        A list of element action runs, where only the final run is taken for each
        element action.
        """
        return [act.runs[-1] for act in self.actions.values()]

    @property
    def inputs(self) -> ElementInputs:
        """
        The inputs to this element.
        """
        if not self._inputs:
            self._inputs = self._app.ElementInputs(element_iteration=self)
        return self._inputs

    @property
    def outputs(self) -> ElementOutputs:
        """
        The outputs from this element.
        """
        if not self._outputs:
            self._outputs = self._app.ElementOutputs(element_iteration=self)
        return self._outputs

    @property
    def input_files(self) -> ElementInputFiles:
        """
        The input files to this element.
        """
        if not self._input_files:
            self._input_files = self._app.ElementInputFiles(element_iteration=self)
        return self._input_files

    @property
    def output_files(self) -> ElementOutputFiles:
        """
        The output files from this element.
        """
        if not self._output_files:
            self._output_files = self._app.ElementOutputFiles(element_iteration=self)
        return self._output_files

    def get_parameter_names(self, prefix: str) -> list[str]:
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
        return [
            ".".join(single_label_lookup.get(param_name, param_name).split(".")[1:])
            for param_name in self.schema_parameters
            if param_name.startswith(prefix)
        ]

    @TimeIt.decorator
    def get_data_idx(
        self,
        path: str | None = None,
        action_idx: int | None = None,
        run_idx: int = -1,
    ) -> DataIndex:
        """
        Get the data index.

        Parameters
        ----------
        path:
            If specified, filters the data indices to the ones relevant to this path.
        action_idx:
            The index of the action within the schema.
        run_idx:
            The index of the run within the action.
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
                    if not k.startswith("inputs") or k not in data_idx:
                        data_idx[k] = v

        else:
            elem_act = self.actions[action_idx]
            data_idx = elem_act.runs[run_idx].data_idx

        if path:
            data_idx = {k: v for k, v in data_idx.items() if k.startswith(path)}

        return copy.deepcopy(data_idx)

    def __get_parameter_sources(
        self, data_idx: DataIndex, filter_type: str | None, use_task_index: bool
    ) -> Mapping[str, ParamSource | list[ParamSource]]:
        # the value associated with `repeats.*` is the repeats index, not a parameter ID:
        for k in tuple(data_idx):
            if k.startswith("repeats."):
                data_idx.pop(k)

        out: Mapping[str, ParamSource | list[ParamSource]] = dict_values_process_flat(
            data_idx,
            callable=self.workflow.get_parameter_sources,
        )

        if use_task_index:
            for k, v in out.items():
                assert isinstance(v, dict)
                if (insert_ID := v.pop("task_insert_ID", None)) is not None:
                    # Modify the contents of out
                    v["task_idx"] = self.workflow.tasks.get(insert_ID=insert_ID).index

        if not filter_type:
            return out

        # Filter to just the elements that have the right type property
        filtered = (
            (k, self.__filter_param_source_by_type(v, filter_type))
            for k, v in out.items()
        )
        return {k: v for k, v in filtered if v is not None}

    @staticmethod
    def __filter_param_source_by_type(
        value: ParamSource | list[ParamSource], filter_type: str
    ) -> ParamSource | list[ParamSource] | None:
        if isinstance(value, list):
            if sources := [src for src in value if src["type"] == filter_type]:
                return sources
        else:
            if value["type"] == filter_type:
                return value
        return None

    @overload
    def get_parameter_sources(
        self,
        path: str | None,
        *,
        action_idx: int | None,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: Literal[True],
        use_task_index: bool = False,
    ) -> Mapping[str, str]: ...

    @overload
    def get_parameter_sources(
        self,
        path: str | None = None,
        *,
        action_idx: int | None = None,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: Literal[False] = False,
        use_task_index: bool = False,
    ) -> Mapping[str, ParamSource | list[ParamSource]]: ...

    @TimeIt.decorator
    def get_parameter_sources(
        self,
        path: str | None = None,
        *,
        action_idx: int | None = None,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ) -> Mapping[str, str] | Mapping[str, ParamSource | list[ParamSource]]:
        """
        Get the origin of parameters.

        Parameters
        ----------
        use_task_index
            If True, use the task index within the workflow, rather than the task insert
            ID.
        """
        data_idx = self.get_data_idx(path, action_idx, run_idx)
        out = self.__get_parameter_sources(data_idx, typ or "", use_task_index)
        if not as_strings:
            return out

        # format as a dict with compact string values
        out_strs: dict[str, str] = {}
        for k, v in out.items():
            assert isinstance(v, dict)
            if v["type"] == "local_input":
                if use_task_index:
                    if v["task_idx"] == self.task.index:
                        out_strs[k] = "local"
                    else:
                        out_strs[k] = f"task.{v['task_idx']}.input"
                else:
                    if v["task_insert_ID"] == self.task.insert_ID:
                        out_strs[k] = "local"
                    else:
                        out_strs[k] = f"task.{v['task_insert_ID']}.input"
            elif v["type"] == "default_input":
                out_strs == "default"
            else:
                idx = v["task_idx"] if use_task_index else v["task_insert_ID"]
                out_strs[k] = (
                    f"task.{idx}.element.{v['element_idx']}."
                    f"action.{v['action_idx']}.run.{v['run_idx']}"
                )
        return out_strs

    @TimeIt.decorator
    def get(
        self,
        path: str | None = None,
        action_idx: int | None = None,
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
            for key in tuple(data_idx):
                if (path or "").startswith(key):
                    # `path` uses labelled type, so no need to convert to non-labelled
                    continue
                if lookup_val := single_label_lookup.get(key):
                    data_idx[lookup_val] = data_idx.pop(key)

        return self.task._get_merged_parameter_data(
            data_index=data_idx,
            path=path,
            raise_on_missing=raise_on_missing,
            raise_on_unset=raise_on_unset,
            default=default,
        )

    @overload
    def get_EAR_dependencies(
        self,
        as_objects: Literal[False] = False,
    ) -> set[int]: ...

    @overload
    def get_EAR_dependencies(
        self,
        as_objects: Literal[True],
    ) -> list[ElementActionRun]: ...

    @TimeIt.decorator
    def get_EAR_dependencies(
        self,
        as_objects: bool = False,
    ) -> set[int] | list[ElementActionRun]:
        """Get EARs that this element iteration depends on (excluding EARs of this element
        iteration)."""
        # TODO: test this includes EARs of upstream iterations of this iteration's element
        if self.action_runs:
            EAR_IDs_set = frozenset(self.EAR_IDs_flat)
            out = {
                id_
                for ear in self.action_runs
                for id_ in ear.get_EAR_dependencies()
                if id_ not in EAR_IDs_set
            }
        else:
            # if an "input-only" task schema, then there will be no action runs, but the
            # ElementIteration can still depend on other EARs if inputs are sourced from
            # upstream tasks:
            out = {
                src_i["EAR_ID"]
                for src in self.get_parameter_sources(typ="EAR_output").values()
                for src_i in (src if isinstance(src, list) else [src])
            }

        if as_objects:
            return self.workflow.get_EARs_from_IDs(sorted(out))
        return out

    @overload
    def get_element_iteration_dependencies(
        self, as_objects: Literal[True]
    ) -> list[ElementIteration]: ...

    @overload
    def get_element_iteration_dependencies(
        self, as_objects: Literal[False] = False
    ) -> set[int]: ...

    @TimeIt.decorator
    def get_element_iteration_dependencies(
        self, as_objects: bool = False
    ) -> set[int] | list[ElementIteration]:
        """Get element iterations that this element iteration depends on."""
        # TODO: test this includes previous iterations of this iteration's element
        EAR_IDs = self.get_EAR_dependencies()
        out = set(self.workflow.get_element_iteration_IDs_from_EAR_IDs(EAR_IDs))
        if as_objects:
            return self.workflow.get_element_iterations_from_IDs(sorted(out))
        return out

    @overload
    def get_element_dependencies(
        self,
        as_objects: Literal[False] = False,
    ) -> set[int]: ...

    @overload
    def get_element_dependencies(
        self,
        as_objects: Literal[True],
    ) -> list[Element]: ...

    @TimeIt.decorator
    def get_element_dependencies(
        self,
        as_objects: bool = False,
    ) -> set[int] | list[Element]:
        """Get elements that this element iteration depends on."""
        # TODO: this will be used in viz.
        EAR_IDs = self.get_EAR_dependencies()
        out = set(self.workflow.get_element_IDs_from_EAR_IDs(EAR_IDs))
        if as_objects:
            return self.workflow.get_elements_from_IDs(sorted(out))
        return out

    def get_input_dependencies(self) -> Mapping[str, ParamSource]:
        """Get locally defined inputs/sequences/defaults from other tasks that this
        element iteration depends on."""
        out: dict[str, ParamSource] = {}
        for k, v in self.get_parameter_sources().items():
            for v_i in v if isinstance(v, list) else [v]:
                if (
                    v_i["type"] in ["local_input", "default_input"]
                    and v_i["task_insert_ID"] != self.task.insert_ID
                ):
                    out[k] = v_i
        return out

    @overload
    def get_task_dependencies(self, as_objects: Literal[False] = False) -> set[int]: ...

    @overload
    def get_task_dependencies(self, as_objects: Literal[True]) -> list[WorkflowTask]: ...

    def get_task_dependencies(
        self, as_objects: bool = False
    ) -> set[int] | list[WorkflowTask]:
        """Get tasks (insert ID or WorkflowTask objects) that this element iteration
        depends on.

        Dependencies may come from either elements from upstream tasks, or from locally
        defined inputs/sequences/defaults from upstream tasks."""

        out = set(
            self.workflow.get_task_IDs_from_element_IDs(self.get_element_dependencies())
        )
        for p_src in self.get_input_dependencies().values():
            out.add(p_src["task_insert_ID"])

        if as_objects:
            return [self.workflow.tasks.get(insert_ID=id_) for id_ in sorted(out)]
        return out

    @property
    def __elements(self) -> Iterator[Element]:
        """
        This iteration's element and its downstream elements.
        """
        for task in self.workflow.tasks[self.task.index :]:
            yield from task.elements[:]

    @property
    def __iterations(self) -> Iterator[ElementIteration]:
        """
        This iteration and its downstream iterations.
        """
        for elem in self.__elements:
            yield from elem.iterations

    @overload
    def get_dependent_EARs(self, as_objects: Literal[False] = False) -> set[int]: ...

    @overload
    def get_dependent_EARs(self, as_objects: Literal[True]) -> list[ElementActionRun]: ...

    @TimeIt.decorator
    def get_dependent_EARs(
        self, as_objects: bool = False
    ) -> set[int] | list[ElementActionRun]:
        """Get EARs of downstream iterations and tasks that depend on this element
        iteration."""
        # TODO: test this includes EARs of downstream iterations of this iteration's element
        deps: set[int] = set()
        for iter_ in self.__iterations:
            if iter_.id_ == self.id_:
                # don't include EARs of this iteration
                continue
            for run in iter_.action_runs:
                if run.get_EAR_dependencies().intersection(self.EAR_IDs_flat):
                    deps.add(run.id_)
        if as_objects:
            return self.workflow.get_EARs_from_IDs(sorted(deps))
        return deps

    @overload
    def get_dependent_element_iterations(
        self, as_objects: Literal[True]
    ) -> list[ElementIteration]: ...

    @overload
    def get_dependent_element_iterations(
        self, as_objects: Literal[False] = False
    ) -> set[int]: ...

    @TimeIt.decorator
    def get_dependent_element_iterations(
        self, as_objects: bool = False
    ) -> set[int] | list[ElementIteration]:
        """Get elements iterations of downstream iterations and tasks that depend on this
        element iteration."""
        # TODO: test this includes downstream iterations of this iteration's element?
        deps: set[int] = set()
        for iter_i in self.__iterations:
            if iter_i.id_ == self.id_:
                continue
            if self.id_ in iter_i.get_element_iteration_dependencies():
                deps.add(iter_i.id_)
        if as_objects:
            return self.workflow.get_element_iterations_from_IDs(sorted(deps))
        return deps

    @overload
    def get_dependent_elements(
        self,
        as_objects: Literal[True],
    ) -> list[Element]: ...

    @overload
    def get_dependent_elements(
        self,
        as_objects: Literal[False] = False,
    ) -> set[int]: ...

    @TimeIt.decorator
    def get_dependent_elements(
        self,
        as_objects: bool = False,
    ) -> set[int] | list[Element]:
        """Get elements of downstream tasks that depend on this element iteration."""
        deps: set[int] = set()
        for task in self.task.downstream_tasks:
            for element in task.elements[:]:
                if any(
                    self.id_ in iter_i.get_element_iteration_dependencies()
                    for iter_i in element.iterations
                ):
                    deps.add(element.id_)

        if as_objects:
            return self.workflow.get_elements_from_IDs(sorted(deps))
        return deps

    @overload
    def get_dependent_tasks(
        self,
        as_objects: Literal[True],
    ) -> list[WorkflowTask]: ...

    @overload
    def get_dependent_tasks(
        self,
        as_objects: Literal[False] = False,
    ) -> set[int]: ...

    def get_dependent_tasks(
        self,
        as_objects: bool = False,
    ) -> set[int] | list[WorkflowTask]:
        """Get downstream tasks that depend on this element iteration."""
        deps: set[int] = set()
        for task in self.task.downstream_tasks:
            if any(
                self.id_ in iter_i.get_element_iteration_dependencies()
                for element in task.elements[:]
                for iter_i in element.iterations
            ):
                deps.add(task.insert_ID)
        if as_objects:
            return [self.workflow.tasks.get(insert_ID=id_) for id_ in sorted(deps)]
        return deps

    def get_template_resources(self) -> Mapping[str, Any]:
        """Get template-level resources."""
        res = self.workflow.template._resources
        return {res_i.normalised_resources_path: res_i._get_value() for res_i in res}

    @TimeIt.decorator
    def get_resources(
        self, action: Action, set_defaults: bool = False
    ) -> Mapping[str, Any]:
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

        # FIXME: Use a TypedDict?
        resource_specs: dict[str, dict[str, dict[str, Any]]] = copy.deepcopy(
            self.get("resources")
        )

        env_spec = action.get_environment_spec()
        env_name: str = env_spec["name"]

        # set default env specifiers, if none set:
        if "environments" not in (any_specs := resource_specs.setdefault("any", {})):
            any_specs["environments"] = {env_name: copy.deepcopy(env_spec)}

        for dat in resource_specs.values():
            if "environments" in dat:
                # keep only relevant user-provided environment specifiers:
                dat["environments"] = {
                    k: v for k, v in dat["environments"].items() if k == env_name
                }
                # merge user-provided specifiers into action specifiers:
                dat["environments"].setdefault(env_name, {}).update(
                    copy.deepcopy(env_spec)
                )

        resources: dict[str, Any] = {}
        for scope in action._get_possible_scopes_reversed():
            # loop from least-specific to most so higher-specificity scopes take precedence:
            if scope_res := resource_specs.get(scope.to_string()):
                resources.update((k, v) for k, v in scope_res.items() if v is not None)

        if set_defaults:
            # used in e.g. `Rule.test` if testing resource rules on element iterations:
            ER = self._app.ElementResources
            resources.setdefault("os_name", ER.get_default_os_name())
            resources.setdefault("shell", ER.get_default_shell())
            if "scheduler" not in resources:
                resources["scheduler"] = ER.get_default_scheduler(
                    resources["os_name"], resources["shell"]
                )

        # unset inapplicable items:
        if "combine_scripts" in resources and not action.script_is_python_snippet:
            del resources["combine_scripts"]

        return resources

    def get_resources_obj(
        self, action: Action, set_defaults: bool = False
    ) -> ElementResources:
        """
        Get the resources for an action (see :py:meth:`get_resources`)
        as a searchable model.
        """
        return self._app.ElementResources(**self.get_resources(action, set_defaults))


class Element(AppAware):
    """
    A basic component of a workflow. Elements are enactments of tasks.

    Parameters
    ----------
    id_ : int
        The ID of this element.
    is_pending: bool
        Whether this element is pending execution.
    task: ~hpcflow.app.WorkflowTask
        The task this is part of the enactment of.
    index: int
        The index of this element.
    es_idx: int
        The index within the task of the element set containing this element.
    seq_idx: dict[str, int]
        The sequence index IDs.
    src_idx: dict[str, int]
        The input source indices.
    iteration_IDs: list[int]
        The known IDs of iterations,
    iterations: list[dict]
        Data for creating iteration objects.
    """

    # TODO: use slots
    # TODO:
    #   - add `iterations` property which returns `ElementIteration`
    #   - also map iteration properties of the most recent iteration to this object

    def __init__(
        self,
        id_: int,
        is_pending: bool,
        task: WorkflowTask,
        index: int,
        es_idx: int,
        seq_idx: Mapping[str, int],
        src_idx: Mapping[str, int],
        iteration_IDs: list[int],
        iterations: list[dict[str, Any]],
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
        self._iteration_objs: list[ElementIteration] | None = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(id={self.id_!r}, "
            f"index={self.index!r}, task={self.task.unique_name!r}"
            f")"
        )

    @property
    def id_(self) -> int:
        """
        The ID of this element.
        """
        return self._id

    @property
    def is_pending(self) -> bool:
        """
        Whether this element is pending execution.
        """
        return self._is_pending

    @property
    def task(self) -> WorkflowTask:
        """
        The task this is part of the enactment of.
        """
        return self._task

    @property
    def index(self) -> int:
        """Get the index of the element within the task.

        Note: the `global_idx` attribute returns the index of the element within the
        workflow, across all tasks."""

        return self._index

    @property
    def element_set_idx(self) -> int:
        """
        The index within the task of the element set containing this element.
        """
        return self._es_idx

    @property
    def element_set(self) -> ElementSet:
        """
        The element set containing this element.
        """
        return self.task.template.element_sets[self.element_set_idx]

    @property
    def sequence_idx(self) -> Mapping[str, int]:
        """
        The sequence index IDs.
        """
        return self._seq_idx

    @property
    def input_source_idx(self) -> Mapping[str, int]:
        """
        The input source indices.
        """
        return self._src_idx

    @property
    def input_sources(self) -> Mapping[str, InputSource]:
        """
        The sources of the inputs to this element.
        """
        return {
            k: self.element_set.input_sources[k.removeprefix("inputs.")][v]
            for k, v in self.input_source_idx.items()
        }

    @property
    def workflow(self) -> Workflow:
        """
        The workflow containing this element.
        """
        return self.task.workflow

    @property
    def iteration_IDs(self) -> Sequence[int]:
        """
        The IDs of the iterations of this element.
        """
        return self._iteration_IDs

    @property
    @TimeIt.decorator
    def iterations(self) -> Sequence[ElementIteration]:
        """
        The iterations of this element.
        """
        # TODO: fix this
        if self._iteration_objs is None:
            self._iteration_objs = [
                self._app.ElementIteration(
                    element=self,
                    index=idx,
                    **{k: v for k, v in iter_i.items() if k != "element_ID"},
                )
                for idx, iter_i in enumerate(self._iterations)
            ]
        return self._iteration_objs

    @property
    def dir_name(self) -> str:
        """
        The name of the directory for containing temporary files for this element.
        """
        return f"e_{self.index}"

    @property
    def latest_iteration(self) -> ElementIteration:
        """
        The most recent iteration of this element.
        """
        return self.iterations[-1]

    @property
    def latest_iteration_non_skipped(self):
        """Get the latest iteration that is not loop-skipped."""
        for iter_i in self.iterations[::-1]:
            if not iter_i.loop_skipped:
                return iter_i

    @property
    def inputs(self) -> ElementInputs:
        """
        The inputs to this element's most recent iteration (that was not skipped due to
        loop termination).
        """
        return self.latest_iteration_non_skipped.inputs

    @property
    def outputs(self) -> ElementOutputs:
        """
        The outputs from this element's most recent iteration (that was not skipped due to
        loop termination).
        """
        return self.latest_iteration_non_skipped.outputs

    @property
    def input_files(self) -> ElementInputFiles:
        """
        The input files to this element's most recent iteration (that was not skipped due
        to loop termination).
        """
        return self.latest_iteration_non_skipped.input_files

    @property
    def output_files(self) -> ElementOutputFiles:
        """
        The output files from this element's most recent iteration (that was not skipped
        due to loop termination).
        """
        return self.latest_iteration_non_skipped.output_files

    @property
    def schema_parameters(self) -> Sequence[str]:
        """
        The schema-defined parameters to this element's most recent iteration (that was
        not skipped due to loop termination).
        """
        return self.latest_iteration_non_skipped.schema_parameters

    @property
    def actions(self) -> Mapping[int, ElementAction]:
        """
        The actions of this element's most recent iteration (that was not skipped due to
        loop termination).
        """
        return self.latest_iteration_non_skipped.actions

    @property
    def action_runs(self) -> Sequence[ElementActionRun]:
        """
        A list of element action runs from the latest iteration, where only the
        final run is taken for each element action.
        """
        return self.latest_iteration_non_skipped.action_runs

    def to_element_set_data(self) -> tuple[list[InputValue], list[ResourceSpec]]:
        """Generate lists of workflow-bound InputValues and ResourceList."""
        inputs: list[InputValue] = []
        resources: list[ResourceSpec] = []
        for k, v in self.get_data_idx().items():
            kind, parameter_or_scope, *path = k.split(".")

            if kind == "inputs":
                inp_val = self._app.InputValue(
                    parameter=parameter_or_scope,
                    path=cast("str", path) or None,  # FIXME: suspicious cast!
                    value=None,
                )
                inp_val._value_group_idx = v
                inp_val._workflow = self.workflow
                inputs.append(inp_val)

            elif kind == "resources":
                scope = self._app.ActionScope.from_json_like(parameter_or_scope)
                res = self._app.ResourceSpec(scope=scope)
                res._value_group_idx = v
                res._workflow = self.workflow
                resources.append(res)

        return inputs, resources

    def get_sequence_value(self, sequence_path: str) -> Any:
        """
        Get the value of a sequence that applies.
        """

        if not (seq := self.element_set.get_sequence_from_path(sequence_path)):
            raise ValueError(
                f"No sequence with path {sequence_path!r} in this element's originating "
                f"element set."
            )
        if (values := seq.values) is None:
            raise ValueError(
                f"Sequence with path {sequence_path!r} has no defined values."
            )
        return values[self.sequence_idx[sequence_path]]

    def get_data_idx(
        self,
        path: str | None = None,
        action_idx: int | None = None,
        run_idx: int = -1,
    ) -> DataIndex:
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

    @overload
    def get_parameter_sources(
        self,
        path: str | None = None,
        *,
        action_idx: int | None = None,
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
        action_idx: int | None = None,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: Literal[True],
        use_task_index: bool = False,
    ) -> Mapping[str, str]: ...

    def get_parameter_sources(
        self,
        path: str | None = None,
        *,
        action_idx: int | None = None,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ) -> Mapping[str, str] | Mapping[str, ParamSource | list[ParamSource]]:
        """ "Get the parameter sources of the most recent element iteration.

        Parameters
        ----------
        use_task_index
            If True, use the task index within the workflow, rather than the task insert
            ID.
        """
        if as_strings:
            return self.latest_iteration.get_parameter_sources(
                path=path,
                action_idx=action_idx,
                run_idx=run_idx,
                typ=typ,
                as_strings=True,
                use_task_index=use_task_index,
            )
        return self.latest_iteration.get_parameter_sources(
            path=path,
            action_idx=action_idx,
            run_idx=run_idx,
            typ=typ,
            use_task_index=use_task_index,
        )

    def get(
        self,
        path: str | None = None,
        action_idx: int | None = None,
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

    @overload
    def get_EAR_dependencies(
        self, as_objects: Literal[True]
    ) -> list[ElementActionRun]: ...

    @overload
    def get_EAR_dependencies(self, as_objects: Literal[False] = False) -> set[int]: ...

    @TimeIt.decorator
    def get_EAR_dependencies(
        self, as_objects: bool = False
    ) -> set[int] | list[ElementActionRun]:
        """Get EARs that the most recent iteration of this element depends on."""
        if as_objects:
            return self.latest_iteration.get_EAR_dependencies(as_objects=True)
        return self.latest_iteration.get_EAR_dependencies()

    @overload
    def get_element_iteration_dependencies(
        self, as_objects: Literal[True]
    ) -> list[ElementIteration]: ...

    @overload
    def get_element_iteration_dependencies(
        self, as_objects: Literal[False] = False
    ) -> set[int]: ...

    def get_element_iteration_dependencies(
        self, as_objects: bool = False
    ) -> set[int] | list[ElementIteration]:
        """Get element iterations that the most recent iteration of this element depends
        on."""
        if as_objects:
            return self.latest_iteration.get_element_iteration_dependencies(
                as_objects=True
            )
        return self.latest_iteration.get_element_iteration_dependencies()

    @overload
    def get_element_dependencies(self, as_objects: Literal[True]) -> list[Element]: ...

    @overload
    def get_element_dependencies(
        self, as_objects: Literal[False] = False
    ) -> set[int]: ...

    def get_element_dependencies(
        self, as_objects: bool = False
    ) -> set[int] | list[Element]:
        """Get elements that the most recent iteration of this element depends on."""
        if as_objects:
            return self.latest_iteration.get_element_dependencies(as_objects=True)
        return self.latest_iteration.get_element_dependencies()

    def get_input_dependencies(self) -> Mapping[str, ParamSource]:
        """Get locally defined inputs/sequences/defaults from other tasks that this
        the most recent iteration of this element depends on."""
        return self.latest_iteration.get_input_dependencies()

    @overload
    def get_task_dependencies(self, as_objects: Literal[True]) -> list[WorkflowTask]: ...

    @overload
    def get_task_dependencies(self, as_objects: Literal[False] = False) -> set[int]: ...

    def get_task_dependencies(
        self, as_objects: bool = False
    ) -> set[int] | list[WorkflowTask]:
        """Get tasks (insert ID or WorkflowTask objects) that the most recent iteration of
        this element depends on.

        Dependencies may come from either elements from upstream tasks, or from locally
        defined inputs/sequences/defaults from upstream tasks."""
        if as_objects:
            return self.latest_iteration.get_task_dependencies(as_objects=True)
        return self.latest_iteration.get_task_dependencies()

    @overload
    def get_dependent_EARs(self, as_objects: Literal[True]) -> list[ElementActionRun]: ...

    @overload
    def get_dependent_EARs(self, as_objects: Literal[False] = False) -> set[int]: ...

    def get_dependent_EARs(
        self, as_objects: bool = False
    ) -> set[int] | list[ElementActionRun]:
        """Get EARs that depend on the most recent iteration of this element."""
        if as_objects:
            return self.latest_iteration.get_dependent_EARs(as_objects=True)
        return self.latest_iteration.get_dependent_EARs()

    @overload
    def get_dependent_element_iterations(
        self, as_objects: Literal[True]
    ) -> list[ElementIteration]: ...

    @overload
    def get_dependent_element_iterations(
        self, as_objects: Literal[False] = False
    ) -> set[int]: ...

    def get_dependent_element_iterations(
        self, as_objects: bool = False
    ) -> set[int] | list[ElementIteration]:
        """Get element iterations that depend on the most recent iteration of this
        element."""
        if as_objects:
            return self.latest_iteration.get_dependent_element_iterations(as_objects=True)
        return self.latest_iteration.get_dependent_element_iterations()

    @overload
    def get_dependent_elements(self, as_objects: Literal[True]) -> list[Element]: ...

    @overload
    def get_dependent_elements(self, as_objects: Literal[False] = False) -> set[int]: ...

    def get_dependent_elements(
        self, as_objects: bool = False
    ) -> set[int] | list[Element]:
        """Get elements that depend on the most recent iteration of this element."""
        if as_objects:
            return self.latest_iteration.get_dependent_elements(as_objects=True)
        return self.latest_iteration.get_dependent_elements()

    @overload
    def get_dependent_tasks(self, as_objects: Literal[True]) -> list[WorkflowTask]: ...

    @overload
    def get_dependent_tasks(self, as_objects: Literal[False] = False) -> set[int]: ...

    def get_dependent_tasks(
        self, as_objects: bool = False
    ) -> set[int] | list[WorkflowTask]:
        """Get tasks that depend on the most recent iteration of this element."""
        if as_objects:
            return self.latest_iteration.get_dependent_tasks(as_objects=True)
        return self.latest_iteration.get_dependent_tasks()

    @TimeIt.decorator
    def get_dependent_elements_recursively(
        self, task_insert_ID: int | None = None
    ) -> list[Element]:
        """Get downstream elements that depend on this element, including recursive
        dependencies.

        Dependencies are resolved using the initial iteration only. This method is used to
        identify from which element in the previous iteration a new iteration should be
        parametrised.

        Parameters
        ----------
        task_insert_ID: int
            If specified, only return elements from this task.

        """

        def get_deps(element: Element) -> set[int]:
            deps = element.iterations[0].get_dependent_elements()
            deps_objs = self.workflow.get_elements_from_IDs(deps)
            return deps.union(dep_j for deps_i in deps_objs for dep_j in get_deps(deps_i))

        all_deps = get_deps(self)
        if task_insert_ID is not None:
            all_deps.intersection_update(
                self.workflow.tasks.get(insert_ID=task_insert_ID).element_IDs
            )
        return self.workflow.get_elements_from_IDs(sorted(all_deps))


@dataclass(repr=False, eq=False)
@hydrate
class ElementParameter:
    """
    A parameter to an :py:class:`.Element`.

    Parameters
    ----------
    task: ~hpcflow.app.WorkflowTask
        The task that this is part of.
    path: str
        The path to this parameter.
    parent: Element | ~hpcflow.app.ElementAction | ~hpcflow.app.ElementActionRun | ~hpcflow.app.Parameters
        The entity that owns this parameter.
    element: Element
        The element that this is a parameter of.
    """

    # Intended to be subclassed, so public
    #: Application context.
    app: ClassVar[BaseApp]
    _app_attr: ClassVar[str] = "app"

    #: The task that this is part of.
    task: WorkflowTask
    #: The path to this parameter.
    path: str
    #: The entity that owns this parameter.
    parent: Element | ElementAction | ElementActionRun | ElementIteration
    #: The element that this is a parameter of.
    element: Element | ElementIteration

    @property
    def data_idx(self) -> DataIndex:
        """
        The data indices associated with this parameter.
        """
        return self.parent.get_data_idx(path=self.path)

    @property
    def value(self) -> Any:
        """
        The value of this parameter.
        """
        return self.parent.get(path=self.path)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(element={self.element!r}, path={self.path!r})"

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, self.__class__):
            return False
        return self.task == __o.task and self.path == __o.path

    @property
    def data_idx_is_set(self) -> Mapping[str, bool]:
        """
        The associated data indices for which this is set.
        """
        return {
            k: self.task.workflow.is_parameter_set(cast("int", v))
            for k, v in self.data_idx.items()
        }

    @property
    def is_set(self) -> bool:
        """
        Whether this parameter is set.
        """
        return all(self.data_idx_is_set.values())

    def get_size(self, **store_kwargs):
        """
        Get the size of the parameter.
        """
        raise NotImplementedError


@dataclass
@hydrate
class ElementFilter(JSONLike):
    """
    A filter for iterations.

    Parameters
    ----------
    rules: list[~hpcflow.app.Rule]
        The filtering rules to use.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(name="rules", is_multiple=True, class_name="Rule"),
    )

    #: The filtering rules to use.
    rules: list[Rule] = field(default_factory=list)

    def filter(self, element_iters: list[ElementIteration]) -> list[ElementIteration]:
        """
        Apply the filter rules to select a subsequence of iterations.
        """
        return [
            el_iter
            for el_iter in element_iters
            if all(rule_j.test(el_iter) for rule_j in self.rules)
        ]


@dataclass
class ElementGroup(JSONLike):
    """
    A grouping rule for element iterations.

    Parameters
    ----------
    name:
        The name of the grouping rule.
    where:
        A filtering rule to select which iterations to use in the group.
    group_by_distinct:
        If specified, the name of the property to group iterations by.
    """

    #: The name of the grouping rule.
    name: str
    #: A filtering rule to select which iterations to use in the group.
    where: ElementFilter | None = None
    #: If specified, the name of the property to group iterations by.
    group_by_distinct: ParameterPath | None = None

    def __post_init__(self):
        self.name = check_valid_py_identifier(self.name)


@dataclass
class ElementRepeats:
    """
    A repetition rule.

    Parameters
    ----------
    number:
        The number of times to repeat.
    where:
        A filtering rule for what to repeat.
    """

    #: The number of times to repeat.
    number: int
    #: A filtering rule for what to repeat.
    where: ElementFilter | None = None
