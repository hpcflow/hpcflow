"""
Types to support the core SDK.
"""

from __future__ import annotations
from typing import Any, Literal, Protocol, TYPE_CHECKING
from typing_extensions import NotRequired, TypeAlias, TypedDict

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime, timedelta
    import numpy as np
    from valida.conditions import ConditionLike  # type: ignore
    from .actions import ActionScope
    from .command_files import FileSpec
    from .enums import ParallelMode, ParameterPropagationMode
    from .object_list import ResourceList
    from .parameters import (
        InputSource,
        InputValue,
        Parameter,
        ResourceSpec,
    )
    from .task import InputStatus
    from ..persistence.types import ParamSource


class ParameterDependence(TypedDict):
    """
    Dependency descriptor for a parameter.
    """

    #: The input file writers that can use the parameter.
    input_file_writers: list[FileSpec]
    #: The commands that can use the parameter.
    commands: list[int]


class ScriptData(TypedDict, total=False):
    """
    Descriptor for data relating to a script.
    """

    #: The format of the data.
    format: str
    #: Whether the data is required for all iterations.
    all_iterations: NotRequired[bool]


class JobscriptSubmissionFailureArgs(TypedDict):
    """
    Arguments that can be expanded to create a
    :class:`JobscriptSubmissionFailure`.
    """

    #: The command that was submitted.
    submit_cmd: list[str]
    #: The jobscript index.
    js_idx: int
    #: The jobscript path.
    js_path: str
    #: Where to write stdout.
    stdout: NotRequired[str]
    #: Where to write stderr.
    stderr: NotRequired[str]
    #: The exception from the exec of the subprocess.
    subprocess_exc: NotRequired[Exception]
    #: The exception from parsing the job ID.
    job_ID_parse_exc: NotRequired[Exception]


class ElementDescriptor(TypedDict):
    """
    Descriptor for elements.
    """

    #: The statuses of inputs.
    input_statuses: Mapping[str, InputStatus]
    #: The sources of inputs.
    input_sources: Mapping[str, InputSource]
    #: The insertion ID.
    task_insert_ID: int


class _DependentDescriptor(TypedDict):
    #: The names of groups of dependents.
    group_names: tuple[str, ...]


class DependentDescriptor(_DependentDescriptor, total=False):
    """
    Descriptor for dependents.
    """


class IterableParam(TypedDict):
    """
    The type of the descriptor for an iterable parameter.
    """

    #: Identifier for the input task supplying the parameter.
    input_task: int
    #: Identifiers for the output tasks consuming the parameter.
    output_tasks: list[int]


#: Type of an address.
Address: TypeAlias = "list[int | float | str]"
#: Type of something numeric.
Numeric: TypeAlias = "int | float | np.number"


class LabelInfo(TypedDict):
    """
    Information about a label.
    """

    #: The label propagation mode, if known.
    propagation_mode: NotRequired[ParameterPropagationMode]
    #: The group containing the label, if known.
    group: NotRequired[str]
    #: The default value for the label, if known.
    default_value: NotRequired[InputValue]


class LabellingDescriptor(TypedDict):
    """
    Descriptor for a labelling.
    """

    #: The type with the label.
    labelled_type: str
    #: The propagation mode for the label.
    propagation_mode: ParameterPropagationMode
    #: The group containing the label.
    group: str | None
    #: The default value for the label, if known.
    default_value: NotRequired[InputValue]


class ResourceSpecArgs(TypedDict):
    """
    Supported keyword arguments for a ResourceSpec.
    """

    #: Which scope does this apply to.
    scope: NotRequired[ActionScope | str]
    #: Which scratch space to use.
    scratch: NotRequired[str]
    #: Which parallel mode to use.
    parallel_mode: NotRequired[str | ParallelMode]
    #: How many cores to request.
    num_cores: NotRequired[int]
    #: How many cores per compute node to request.
    num_cores_per_node: NotRequired[int]
    #: How many threads to request.
    num_threads: NotRequired[int]
    #: How many compute nodes to request.
    num_nodes: NotRequired[int]
    #: Which scheduler to use.
    scheduler: NotRequired[str]
    #: Which system shell to use.
    shell: NotRequired[str]
    #: Whether to use array jobs.
    use_job_array: NotRequired[bool]
    #: If using array jobs, up to how many items should be in the job array.
    max_array_items: NotRequired[int]
    #: How long to run for.
    time_limit: NotRequired[str | timedelta]
    #: Additional arguments to pass to the scheduler.
    scheduler_args: NotRequired[dict[str, Any]]
    #: Additional arguments to pass to the shell.
    shell_args: NotRequired[dict[str, Any]]
    #: Which OS to use.
    os_name: NotRequired[str]
    #: Which execution environments to use.
    environments: NotRequired[dict[str, dict[str, Any]]]
    #: Which SGE parallel environment to request.
    SGE_parallel_env: NotRequired[str]
    #: Which SLURM partition to request.
    SLURM_partition: NotRequired[str]
    #: How many SLURM tasks to request.
    SLURM_num_tasks: NotRequired[str]
    #: How many SLURM tasks per compute node to request.
    SLURM_num_tasks_per_node: NotRequired[str]
    #: How many compute nodes to request.
    SLURM_num_nodes: NotRequired[str]
    #: How many CPU cores to ask for per SLURM task.
    SLURM_num_cpus_per_task: NotRequired[str]


# Used in declaration of Resources below
_R: TypeAlias = "ResourceSpec | ResourceSpecArgs | dict"
#: The type of things we can normalise to a :py:class:`ResourceList`.
Resources: TypeAlias = "_R | ResourceList | None | Sequence[_R]"


class SchemaInputKwargs(TypedDict):
    """
    Just used when deep copying `SchemaInput`.
    """

    #: The parameter.
    parameter: Parameter | str
    #: Whether this is multiple.
    multiple: bool
    #: The labels.
    labels: dict[str, LabelInfo] | None
    #: The number or proportion of permitted unset parameter data found when resolving
    #: this input from upstream outputs.
    allow_failed_dependencies: int | float | bool | None


class RuleArgs(TypedDict):
    """
    The keyword arguments that may be used to create a Rule.
    """

    #: If present, check this attribute exists.
    check_exists: NotRequired[str]
    #: If present, check this attribute does *not* exist.
    check_missing: NotRequired[str]
    #: Where to look up the attribute to check.
    #: If not present, determined by context.
    path: NotRequired[str]
    #: If present, a general condition to check (or kwargs used to generate one).
    condition: NotRequired[dict[str, Any] | ConditionLike]
    #: If present, a cast to apply prior to running the general check.
    cast: NotRequired[str]
    #: Optional descriptive text.
    doc: NotRequired[str]


class ActParameterDependence(TypedDict):
    """
    Action parameter dependency descriptor.
    """

    #: The input file writers that produce the parameter.
    input_file_writers: list[tuple[int, FileSpec]]
    #: The commands that produce the parameter.
    commands: list[tuple[int, int]]


#: A relevant path when applying an update.
RelevantPath: TypeAlias = "ParentPath | UpdatePath | SiblingPath"


class RepeatsDescriptor(TypedDict):
    """
    Descriptor for repeats.
    """

    #: Name of the repeat.
    name: str
    #: The repeat count.
    number: int
    #: The nesting order. Normally an integer; non-integer values have special meanings.
    nesting_order: float


class MultiplicityDescriptor(TypedDict):
    """
    Descriptor for multiplicities.
    """

    #: The size of the multiplicity.
    multiplicity: int
    #: The nesting order. Normally an integer; non-integer values have special meanings.
    nesting_order: float
    #: The path to the multiplicity.
    path: str


class ParentPath(TypedDict):
    """
    A `RelevantPath` that is a path to a parent.
    """

    #: Type ID.
    type: Literal["parent"]
    relative_path: Sequence[str]


class UpdatePath(TypedDict):
    """
    A `RelevantPath` that is a path to an update.
    """

    #: Type ID.
    type: Literal["update"]
    update_path: Sequence[str]


class SiblingPath(TypedDict):
    """
    A `RelevantPath` that is a path to a sibling.
    """

    #: Type ID.
    type: Literal["sibling"]


class RelevantData(TypedDict):
    """
    Data relevant to performing an update.
    """

    #: The data to set.
    data: list[Any] | Any
    #: Which method to use for handling the data, if any.
    value_class_method: list[str | None] | str | None
    #: Whether the value is set.
    is_set: bool | list[bool]
    #: Whether the value is multiple.
    is_multi: bool


class CreationInfo(TypedDict):
    """
    Descriptor for creation information about a workflow.
    """

    #: Description of information about the application.
    app_info: dict[str, Any]
    #: When the workflow was created.
    create_time: datetime
    #: Unique identifier for the workflow.
    id: str


class WorkflowTemplateTaskData(TypedDict):
    """
    Descriptor for information about tasks described in a workflow template.
    """

    #: The schema, if known.
    schema: NotRequired[Any | list[Any]]
    #: The element sets, if known.
    element_sets: NotRequired[list[WorkflowTemplateElementSetData]]
    #: The output labels, if known.
    output_labels: NotRequired[list[str]]


class WorkflowTemplateElementSetData(TypedDict):
    """
    Descriptor for element set data within a workflow template parametrisation.
    """

    #: Inputs to the set of elements.
    inputs: NotRequired[list[dict[str, Any]]]
    #: Input files to the set of elements.
    input_files: NotRequired[list[dict[str, Any]]]
    #: Description of how to repeat the set of elements.
    repeats: NotRequired[int | list[RepeatsDescriptor]]
    #: Groupings in the set of elements.
    groups: NotRequired[list[dict[str, Any]]]
    #: Resources to use for the set of elements.
    resources: NotRequired[dict[str, Any]]
    #: Input value sequences to parameterise over.
    sequences: NotRequired[list[dict[str, Any]]]
    #: Input value multi-path sequences to parameterise over.
    multi_path_sequences: NotRequired[list[dict[str, Any]]]
    #: Input source descriptors.
    input_sources: NotRequired[dict[str, list]]
    #: How to handle nesting of iterations.
    nesting_order: NotRequired[dict[str, float]]
    #: Which environment preset to use.
    env_preset: NotRequired[str]
    #: Environment descriptors to use.
    environments: NotRequired[dict[str, dict[str, Any]]]
    #: List of global element iteration indices from which inputs for
    #: the new elements associated with this element set may be sourced.
    #: If ``None``, all iterations are valid.
    sourceable_elem_iters: NotRequired[list[int]]
    #: Whether to allow sources to come from distinct element sub-sets.
    allow_non_coincident_task_sources: NotRequired[bool]
    #: Whether this initialisation is the first for this data (i.e. not a
    #: reconstruction from persistent workflow data), in which case, we merge
    #: ``environments`` into ``resources`` using the "any" scope, and merge any multi-
    #: path sequences into the sequences list.
    is_creation: NotRequired[bool]


class Pending(TypedDict):
    """
    Pending update information. Internal use only.
    """

    #: Template components to update.
    template_components: dict[str, list[int]]
    #: Tasks to update.
    tasks: list[int]
    #: Loops to update.
    loops: list[int]
    #: Submissions to update.
    submissions: list[int]


class AbstractFileSystem(Protocol):
    """
    Type constraints for an abstract file system.
    """

    # Because a dependency is not fully typed...
    def exists(self, path: str) -> bool:
        """Test if a path points to a file or directory that exists."""

    def rename(self, from_: str, to: str, *, recursive: bool = False) -> None:
        """Rename a file or directory."""

    def rm(self, path: str, *, recursive: bool = False) -> None:
        """Delete a file or directory."""

    def glob(self, pattern: str) -> list[str]:
        """List files in a directory that match a pattern."""


class ResourcePersistingWorkflow(Protocol):
    """
    An object to pass to :py:meth:`ResourceSpec.make_persistent` that handles
    persisting resources.
    """

    def _add_parameter_data(self, data: Any, source: ParamSource) -> int: ...

    def check_parameters_exist(self, id_lst: int | list[int]) -> bool:
        """
        Check if all the parameters exist.
        """


BlockActionKey: TypeAlias = "tuple[int | str, int | str, int | str]"
"""
The type of indices that locate an action within a submission. The indices represent,
respectively, the jobscript index, the jobscript-block index, and the block-action index.
Usually, these are integers, but in the case of strings, they will correspond to shell
environment variables.
"""
