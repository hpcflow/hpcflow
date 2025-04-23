"""
Tasks are components of workflows.
"""

from __future__ import annotations
from collections import defaultdict
import copy
from dataclasses import dataclass, field
from itertools import chain
from pathlib import Path
from typing import NamedTuple, cast, overload, TYPE_CHECKING
from typing_extensions import override

from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.object_list import AppDataList
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.core.app_aware import AppAware
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.element import ElementGroup
from hpcflow.sdk.core.enums import InputSourceType, TaskSourceType
from hpcflow.sdk.core.errors import (
    ContainerKeyError,
    ExtraInputs,
    InapplicableInputSourceElementIters,
    MalformedNestingOrderPath,
    MayNeedObjectError,
    MissingElementGroup,
    MissingInputs,
    NoAvailableElementSetsError,
    NoCoincidentInputSources,
    TaskTemplateInvalidNesting,
    TaskTemplateMultipleInputValues,
    TaskTemplateMultipleSchemaObjectives,
    TaskTemplateUnexpectedInput,
    TaskTemplateUnexpectedSequenceInput,
    UnavailableInputSource,
    UnknownEnvironmentPresetError,
    UnrequiredInputSources,
    UnsetParameterDataError,
)
from hpcflow.sdk.core.parameters import ParameterValue
from hpcflow.sdk.core.utils import (
    get_duplicate_items,
    get_in_container,
    get_item_repeat_index,
    get_relative_path,
    group_by_dict_key_values,
    set_in_container,
    split_param_label,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
    from typing import Any, ClassVar, Literal, TypeVar
    from typing_extensions import Self, TypeAlias, TypeIs
    from ..typing import DataIndex, ParamSource
    from .actions import Action
    from .command_files import InputFile
    from .element import (
        Element,
        ElementIteration,
        ElementFilter,
        ElementParameter,
        _ElementPrefixedParameter as EPP,
    )
    from .parameters import (
        InputValue,
        InputSource,
        ValueSequence,
        MultiPathSequence,
        SchemaInput,
        SchemaOutput,
        ParameterPath,
    )
    from .rule import Rule
    from .task_schema import TaskObjective, TaskSchema, MetaTaskSchema
    from .types import (
        MultiplicityDescriptor,
        RelevantData,
        RelevantPath,
        Resources,
        RepeatsDescriptor,
    )
    from .workflow import Workflow, WorkflowTemplate

    StrSeq = TypeVar("StrSeq", bound=Sequence[str])


INPUT_SOURCE_TYPES = ("local", "default", "task", "import")


@dataclass
class InputStatus:
    """Information about a given schema input and its parametrisation within an element
    set.

    Parameters
    ----------
    has_default
        True if a default value is available.
    is_required
        True if the input is required by one or more actions. An input may not be required
        if it is only used in the generation of inputs files, and those input files are
        passed to the element set directly.
    is_provided
        True if the input is locally provided in the element set.

    """

    #: True if a default value is available.
    has_default: bool
    #: True if the input is required by one or more actions. An input may not be required
    #: if it is only used in the generation of inputs files, and those input files are
    #: passed to the element set directly.
    is_required: bool
    #: True if the input is locally provided in the element set.
    is_provided: bool

    @property
    def is_extra(self) -> bool:
        """True if the input is provided but not required."""
        return self.is_provided and not self.is_required


class ElementSet(JSONLike):
    """Class to represent a parameterisation of a new set of elements.

    Parameters
    ----------
    inputs: list[~hpcflow.app.InputValue]
        Inputs to the set of elements.
    input_files: list[~hpcflow.app.InputFile]
        Input files to the set of elements.
    sequences: list[~hpcflow.app.ValueSequence]
        Input value sequences to parameterise over.
    multi_path_sequences: list[~hpcflow.app.MultiPathSequence]
        Multi-path sequences to parameterise over.
    resources: ~hpcflow.app.ResourceList
        Resources to use for the set of elements.
    repeats: list[dict]
        Description of how to repeat the set of elements.
    groups: list[~hpcflow.app.ElementGroup]
        Groupings in the set of elements.
    input_sources: dict[str, ~hpcflow.app.InputSource]
        Input source descriptors.
    nesting_order: dict[str, int]
        How to handle nesting of iterations.
    env_preset: str
        Which environment preset to use. Don't use at same time as ``environments``.
    environments: dict
        Environment descriptors to use. Don't use at same time as ``env_preset``.
    sourceable_elem_iters: list[int]
        If specified, a list of global element iteration indices from which inputs for
        the new elements associated with this element set may be sourced. If not
        specified, all workflow element iterations are considered sourceable.
    allow_non_coincident_task_sources: bool
        If True, if more than one parameter is sourced from the same task, then allow
        these sources to come from distinct element sub-sets. If False (default),
        only the intersection of element sub-sets for all parameters are included.
    is_creation: bool
        If True, merge ``environments`` into ``resources`` using the "any" scope, and
        merge sequences belonging to multi-path sequences into the value-sequences list.
        If False, ``environments`` are ignored. This is required on first initialisation,
        but not on subsequent re-initialisation from a persistent workflow.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="inputs",
            class_name="InputValue",
            is_multiple=True,
            dict_key_attr="parameter",
            dict_val_attr="value",
            parent_ref="_element_set",
        ),
        ChildObjectSpec(
            name="input_files",
            class_name="InputFile",
            is_multiple=True,
            dict_key_attr="file",
            dict_val_attr="path",
            parent_ref="_element_set",
        ),
        ChildObjectSpec(
            name="resources",
            class_name="ResourceList",
            parent_ref="_element_set",
        ),
        ChildObjectSpec(
            name="sequences",
            class_name="ValueSequence",
            is_multiple=True,
            parent_ref="_element_set",
        ),
        ChildObjectSpec(
            name="multi_path_sequences",
            class_name="MultiPathSequence",
            is_multiple=True,
            parent_ref="_element_set",
        ),
        ChildObjectSpec(
            name="input_sources",
            class_name="InputSource",
            is_multiple=True,
            is_dict_values=True,
            is_dict_values_ensure_list=True,
        ),
        ChildObjectSpec(
            name="groups",
            class_name="ElementGroup",
            is_multiple=True,
        ),
    )

    def __init__(
        self,
        inputs: list[InputValue] | dict[str, Any] | None = None,
        input_files: list[InputFile] | None = None,
        sequences: list[ValueSequence] | None = None,
        multi_path_sequences: list[MultiPathSequence] | None = None,
        resources: Resources = None,
        repeats: list[RepeatsDescriptor] | int | None = None,
        groups: list[ElementGroup] | None = None,
        input_sources: dict[str, list[InputSource]] | None = None,
        nesting_order: dict[str, float] | None = None,
        env_preset: str | None = None,
        environments: Mapping[str, Mapping[str, Any]] | None = None,
        sourceable_elem_iters: list[int] | None = None,
        allow_non_coincident_task_sources: bool = False,
        is_creation: bool = True,
    ):
        #: Inputs to the set of elements.
        self.inputs = self.__decode_inputs(inputs or [])
        #: Input files to the set of elements.
        self.input_files = input_files or []
        #: Description of how to repeat the set of elements.
        self.repeats = self.__decode_repeats(repeats or [])
        #: Groupings in the set of elements.
        self.groups = groups or []
        #: Resources to use for the set of elements.
        self.resources = self._app.ResourceList.normalise(resources)
        #: Input value sequences to parameterise over.
        self.sequences = sequences or []
        #: Input value multi-path sequences to parameterise over.
        self.multi_path_sequences = multi_path_sequences or []
        #: Input source descriptors.
        self.input_sources = input_sources or {}
        #: How to handle nesting of iterations.
        self.nesting_order = nesting_order or {}
        #: Which environment preset to use.
        self.env_preset = env_preset
        #: Environment descriptors to use.
        self.environments = environments
        #: List of global element iteration indices from which inputs for
        #: the new elements associated with this element set may be sourced.
        #: If ``None``, all iterations are valid.
        self.sourceable_elem_iters = sourceable_elem_iters
        #: Whether to allow sources to come from distinct element sub-sets.
        self.allow_non_coincident_task_sources = allow_non_coincident_task_sources
        #: Whether this initialisation is the first for this data (i.e. not a
        #: reconstruction from persistent workflow data), in which case, we merge
        #: ``environments`` into ``resources`` using the "any" scope, and merge any multi-
        #: path sequences into the sequences list.
        self.is_creation = is_creation
        self.original_input_sources: dict[str, list[InputSource]] | None = None
        self.original_nesting_order: dict[str, float] | None = None

        self._validate()
        self._set_parent_refs()

        # assigned by parent Task
        self._task_template: Task | None = None
        # assigned on _task_template assignment
        self._defined_input_types: set[str] | None = None
        # assigned by WorkflowTask._add_element_set
        self._element_local_idx_range: list[int] | None = None

        if self.is_creation:

            # merge `environments` into element set resources (this mutates `resources`, and
            # should only happen on creation of the element set, not re-initialisation from a
            # persistent workflow):
            if self.environments:
                self.resources.merge_one(
                    self._app.ResourceSpec(scope="any", environments=self.environments)
                )
            # note: `env_preset` is merged into resources by the Task init.

            # merge sequences belonging to multi-path sequences into the value-sequences list:
            if self.multi_path_sequences:
                for mp_seq in self.multi_path_sequences:
                    mp_seq._move_to_sequence_list(self.sequences)

            self.is_creation = False

    def __deepcopy__(self, memo: dict[int, Any] | None) -> Self:
        dct = self.to_dict()
        orig_inp = dct.pop("original_input_sources", None)
        orig_nest = dct.pop("original_nesting_order", None)
        elem_local_idx_range = dct.pop("_element_local_idx_range", None)
        obj = self.__class__(**copy.deepcopy(dct, memo))
        obj._task_template = self._task_template
        obj._defined_input_types = self._defined_input_types
        obj.original_input_sources = copy.deepcopy(orig_inp)
        obj.original_nesting_order = copy.copy(orig_nest)
        obj._element_local_idx_range = copy.copy(elem_local_idx_range)
        return obj

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.to_dict() == other.to_dict()

    @classmethod
    def _json_like_constructor(cls, json_like) -> Self:
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""
        orig_inp = json_like.pop("original_input_sources", None)
        orig_nest = json_like.pop("original_nesting_order", None)
        elem_local_idx_range = json_like.pop("_element_local_idx_range", None)
        obj = cls(**json_like)
        obj.original_input_sources = orig_inp
        obj.original_nesting_order = orig_nest
        obj._element_local_idx_range = elem_local_idx_range
        return obj

    def prepare_persistent_copy(self) -> Self:
        """Return a copy of self, which will then be made persistent, and save copies of
        attributes that may be changed during integration with the workflow."""
        obj = copy.deepcopy(self)
        obj.original_nesting_order = self.nesting_order
        obj.original_input_sources = self.input_sources
        return obj

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        dct = super()._postprocess_to_dict(d)
        del dct["_defined_input_types"]
        del dct["_task_template"]
        return dct

    @property
    def task_template(self) -> Task:
        """
        The abstract task this was derived from.
        """
        assert self._task_template is not None
        return self._task_template

    @task_template.setter
    def task_template(self, value: Task) -> None:
        self._task_template = value
        self.__validate_against_template()

    @property
    def input_types(self) -> list[str]:
        """
        The input types of the inputs to this element set.
        """
        return [in_.labelled_type for in_ in self.inputs]

    @property
    def element_local_idx_range(self) -> tuple[int, ...]:
        """Indices of elements belonging to this element set."""
        return tuple(self._element_local_idx_range or ())

    @classmethod
    def __decode_inputs(
        cls, inputs: list[InputValue] | dict[str, Any]
    ) -> list[InputValue]:
        """support inputs passed as a dict"""
        if isinstance(inputs, dict):
            _inputs: list[InputValue] = []
            for k, v in inputs.items():
                param, label = split_param_label(k)
                assert param is not None
                _inputs.append(cls._app.InputValue(parameter=param, label=label, value=v))
            return _inputs
        else:
            return inputs

    @classmethod
    def __decode_repeats(
        cls, repeats: list[RepeatsDescriptor] | int
    ) -> list[RepeatsDescriptor]:
        # support repeats as an int:
        if isinstance(repeats, int):
            return [
                {
                    "name": "",
                    "number": repeats,
                    "nesting_order": 0.0,
                }
            ]
        else:
            return repeats

    _ALLOWED_NESTING_PATHS: ClassVar[frozenset[str]] = frozenset(
        {"inputs", "resources", "repeats"}
    )

    def _validate(self) -> None:
        # check `nesting_order` paths:
        for k in self.nesting_order:
            if k.split(".")[0] not in self._ALLOWED_NESTING_PATHS:
                raise MalformedNestingOrderPath(k, self._ALLOWED_NESTING_PATHS)

        inp_paths = [in_.normalised_inputs_path for in_ in self.inputs]
        if dup_paths := get_duplicate_items(inp_paths):
            raise TaskTemplateMultipleInputValues(
                f"The following inputs parameters are associated with multiple input value "
                f"definitions: {dup_paths!r}."
            )

        inp_seq_paths = [
            cast("str", seq.normalised_inputs_path)
            for seq in self.sequences
            if seq.input_type
        ]
        if dup_paths := get_duplicate_items(inp_seq_paths):
            raise TaskTemplateMultipleInputValues(
                f"The following input parameters are associated with multiple sequence "
                f"value definitions: {dup_paths!r}."
            )

        if inp_and_seq := set(inp_paths).intersection(inp_seq_paths):
            raise TaskTemplateMultipleInputValues(
                f"The following input parameters are specified in both the `inputs` and "
                f"`sequences` lists: {list(inp_and_seq)!r}, but must be specified in at "
                f"most one of these."
            )

        for src_key, sources in self.input_sources.items():
            if not sources:
                raise ValueError(
                    f"If specified in `input_sources`, at least one input source must be "
                    f"provided for parameter {src_key!r}."
                )

        # disallow both `env_preset` and `environments` specifications:
        if self.env_preset and self.environments:
            raise ValueError("Specify at most one of `env_preset` and `environments`.")

    def __validate_against_template(self) -> None:
        expected_types = self.task_template.all_schema_input_types
        if unexpected_types := set(self.input_types) - expected_types:
            raise TaskTemplateUnexpectedInput(unexpected_types)

        defined_inp_types = set(self.input_types)
        for seq in self.sequences:
            if inp_type := seq.labelled_type:
                if inp_type not in expected_types:
                    raise TaskTemplateUnexpectedSequenceInput(
                        inp_type, expected_types, seq
                    )
                defined_inp_types.add(inp_type)
            if seq.path not in self.nesting_order and seq.nesting_order is not None:
                self.nesting_order[seq.path] = seq.nesting_order

        for rep_spec in self.repeats:
            if (reps_path_i := f'repeats.{rep_spec["name"]}') not in self.nesting_order:
                self.nesting_order[reps_path_i] = rep_spec["nesting_order"]

        for k, v in self.nesting_order.items():
            if v < 0:
                raise TaskTemplateInvalidNesting(k, v)

        self._defined_input_types = defined_inp_types

    @classmethod
    def ensure_element_sets(
        cls,
        inputs: list[InputValue] | dict[str, Any] | None = None,
        input_files: list[InputFile] | None = None,
        sequences: list[ValueSequence] | None = None,
        multi_path_sequences: list[MultiPathSequence] | None = None,
        resources: Resources = None,
        repeats: list[RepeatsDescriptor] | int | None = None,
        groups: list[ElementGroup] | None = None,
        input_sources: dict[str, list[InputSource]] | None = None,
        nesting_order: dict[str, float] | None = None,
        env_preset: str | None = None,
        environments: Mapping[str, Mapping[str, Any]] | None = None,
        allow_non_coincident_task_sources: bool = False,
        element_sets: list[Self] | None = None,
        sourceable_elem_iters: list[int] | None = None,
    ) -> list[Self]:
        """
        Make an instance after validating some argument combinations.
        """
        args = (
            inputs,
            input_files,
            sequences,
            multi_path_sequences,
            resources,
            repeats,
            groups,
            input_sources,
            nesting_order,
            env_preset,
            environments,
        )

        if any(arg is not None for arg in args):
            if element_sets is not None:
                raise ValueError(
                    "If providing an `element_set`, no other arguments are allowed."
                )
            element_sets = [
                cls(
                    *args,
                    sourceable_elem_iters=sourceable_elem_iters,
                    allow_non_coincident_task_sources=allow_non_coincident_task_sources,
                )
            ]
        else:
            if element_sets is None:
                element_sets = [
                    cls(
                        *args,
                        sourceable_elem_iters=sourceable_elem_iters,
                        allow_non_coincident_task_sources=allow_non_coincident_task_sources,
                    )
                ]

        return element_sets

    @property
    def defined_input_types(self) -> set[str]:
        """
        The input types to this element set.
        """
        assert self._defined_input_types is not None
        return self._defined_input_types

    @property
    def undefined_input_types(self) -> set[str]:
        """
        The input types to the abstract task that aren't related to this element set.
        """
        return self.task_template.all_schema_input_types - self.defined_input_types

    def get_sequence_from_path(self, sequence_path: str) -> ValueSequence | None:
        """
        Get the value sequence for the given path, if it exists.
        """
        return next((seq for seq in self.sequences if seq.path == sequence_path), None)

    def get_defined_parameter_types(self) -> list[str]:
        """
        Get the parameter types of this element set.
        """
        out: list[str] = []
        for inp in self.inputs:
            if not inp.is_sub_value:
                out.append(inp.normalised_inputs_path)
        for seq in self.sequences:
            if seq.parameter and not seq.is_sub_value:  # ignore resource sequences
                assert seq.normalised_inputs_path is not None
                out.append(seq.normalised_inputs_path)
        return out

    def get_defined_sub_parameter_types(self) -> list[str]:
        """
        Get the sub-parameter types of this element set.
        """
        out: list[str] = []
        for inp in self.inputs:
            if inp.is_sub_value:
                out.append(inp.normalised_inputs_path)
        for seq in self.sequences:
            if seq.parameter and seq.is_sub_value:  # ignore resource sequences
                assert seq.normalised_inputs_path is not None
                out.append(seq.normalised_inputs_path)
        return out

    def get_locally_defined_inputs(self) -> list[str]:
        """
        Get the input types that this element set defines.
        """
        return self.get_defined_parameter_types() + self.get_defined_sub_parameter_types()

    @property
    def index(self) -> int | None:
        """
        The index of this element set in its' template task's collection of sets.
        """
        return next(
            (
                idx
                for idx, element_set in enumerate(self.task_template.element_sets)
                if element_set is self
            ),
            None,
        )

    @property
    def task(self) -> WorkflowTask:
        """
        The concrete task corresponding to this element set.
        """
        t = self.task_template.workflow_template
        assert t
        w = t.workflow
        assert w
        i = self.task_template.index
        assert i is not None
        return w.tasks[i]

    @property
    def elements(self) -> list[Element]:
        """
        The elements in this element set.
        """
        return self.task.elements[slice(*self.element_local_idx_range)]

    @property
    def element_iterations(self) -> list[ElementIteration]:
        """
        The iterations in this element set.
        """
        return list(chain.from_iterable(elem.iterations for elem in self.elements))

    @property
    def elem_iter_IDs(self) -> list[int]:
        """
        The IDs of the iterations in this element set.
        """
        return [it.id_ for it in self.element_iterations]

    @overload
    def get_task_dependencies(self, as_objects: Literal[False] = False) -> set[int]: ...

    @overload
    def get_task_dependencies(self, as_objects: Literal[True]) -> list[WorkflowTask]: ...

    def get_task_dependencies(
        self, as_objects: bool = False
    ) -> list[WorkflowTask] | set[int]:
        """Get upstream tasks that this element set depends on."""
        deps: set[int] = set()
        for element in self.elements:
            deps.update(element.get_task_dependencies())
        if as_objects:
            return [self.task.workflow.tasks.get(insert_ID=id_) for id_ in sorted(deps)]
        return deps

    def is_input_type_provided(self, labelled_path: str) -> bool:
        """Check if an input is provided locally as an InputValue or a ValueSequence."""
        return any(
            labelled_path == inp.normalised_inputs_path for inp in self.inputs
        ) or any(
            seq.parameter
            # i.e. not a resource:
            and labelled_path == seq.normalised_inputs_path
            for seq in self.sequences
        )


@hydrate
class OutputLabel(JSONLike):
    """
    Schema input labels that should be applied to a subset of task outputs.

    Parameters
    ----------
    parameter:
        Name of a parameter.
    label:
        Label to apply to the parameter.
    where: ~hpcflow.app.ElementFilter
        Optional filtering rule
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="where",
            class_name="ElementFilter",
        ),
    )

    def __init__(
        self,
        parameter: str,
        label: str,
        where: Rule | None = None,
    ) -> None:
        #: Name of a parameter.
        self.parameter = parameter
        #: Label to apply to the parameter.
        self.label = label
        #: Filtering rule.
        self.where = where


@hydrate
class Task(JSONLike):
    """
    Parametrisation of an isolated task for which a subset of input values are given
    "locally". The remaining input values are expected to be satisfied by other
    tasks/imports in the workflow.

    Parameters
    ----------
    schema: ~hpcflow.app.TaskSchema | list[~hpcflow.app.TaskSchema]
        A (list of) `TaskSchema` object(s) and/or a (list of) strings that are task
        schema names that uniquely identify a task schema. If strings are provided,
        the `TaskSchema` object will be fetched from the known task schemas loaded by
        the app configuration.
    repeats: list[dict]
    groups: list[~hpcflow.app.ElementGroup]
    resources: dict
    inputs: list[~hpcflow.app.InputValue]
        A list of `InputValue` objects.
    input_files: list[~hpcflow.app.InputFile]
    sequences: list[~hpcflow.app.ValueSequence]
        Input value sequences to parameterise over.
    multi_path_sequences: list[~hpcflow.app.MultiPathSequence]
        Multi-path sequences to parameterise over.
    input_sources: dict[str, ~hpcflow.app.InputSource]
    nesting_order: list
    env_preset: str
    environments: dict[str, dict]
    allow_non_coincident_task_sources: bool
        If True, if more than one parameter is sourced from the same task, then allow
        these sources to come from distinct element sub-sets. If False (default),
        only the intersection of element sub-sets for all parameters are included.
    element_sets: list[ElementSet]
    output_labels: list[OutputLabel]
    sourceable_elem_iters: list[int]
    merge_envs: bool
        If True, merge environment presets (set via the element set `env_preset` key)
        into `resources` using the "any" scope. If False, these presets are ignored.
        This is required on first initialisation, but not on subsequent
        re-initialisation from a persistent workflow.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="schema",
            class_name="TaskSchema",
            is_multiple=True,
            shared_data_name="task_schemas",
            shared_data_primary_key="name",
            parent_ref="_task_template",
        ),
        ChildObjectSpec(
            name="element_sets",
            class_name="ElementSet",
            is_multiple=True,
            parent_ref="task_template",
        ),
        ChildObjectSpec(
            name="output_labels",
            class_name="OutputLabel",
            is_multiple=True,
        ),
    )

    @classmethod
    def __is_TaskSchema(cls, value) -> TypeIs[TaskSchema]:
        return isinstance(value, cls._app.TaskSchema)

    def __init__(
        self,
        schema: TaskSchema | str | list[TaskSchema] | list[str],
        repeats: list[RepeatsDescriptor] | int | None = None,
        groups: list[ElementGroup] | None = None,
        resources: Resources = None,
        inputs: list[InputValue] | dict[str, Any] | None = None,
        input_files: list[InputFile] | None = None,
        sequences: list[ValueSequence] | None = None,
        multi_path_sequences: list[MultiPathSequence] | None = None,
        input_sources: dict[str, list[InputSource]] | None = None,
        nesting_order: dict[str, float] | None = None,
        env_preset: str | None = None,
        environments: Mapping[str, Mapping[str, Any]] | None = None,
        allow_non_coincident_task_sources: bool = False,
        element_sets: list[ElementSet] | None = None,
        output_labels: list[OutputLabel] | None = None,
        sourceable_elem_iters: list[int] | None = None,
        merge_envs: bool = True,
    ):
        # TODO: allow init via specifying objective and/or method and/or implementation
        # (lists of) strs e.g.: Task(
        #   objective='simulate_VE_loading',
        #   method=['CP_FFT', 'taylor'],
        #   implementation=['damask', 'damask']
        # )
        # where method and impl must be single strings of lists of the same length
        # and method/impl are optional/required only if necessary to disambiguate
        #
        # this would be like Task(schemas=[
        #   'simulate_VE_loading_CP_FFT_damask',
        #   'simulate_VE_loading_taylor_damask'
        # ])

        _schemas: list[TaskSchema] = []
        for item in schema if isinstance(schema, list) else [schema]:
            if isinstance(item, str):
                try:
                    _schemas.append(
                        self._app.TaskSchema.get_by_key(item)
                    )  # TODO: document that we need to use the actual app instance here?
                    continue
                except KeyError:
                    raise KeyError(f"TaskSchema {item!r} not found.")
            elif self.__is_TaskSchema(item):
                _schemas.append(item)
            else:
                raise TypeError(f"Not a TaskSchema object: {item!r}")

        self._schemas = _schemas

        self._element_sets = self._app.ElementSet.ensure_element_sets(
            inputs=inputs,
            input_files=input_files,
            sequences=sequences,
            multi_path_sequences=multi_path_sequences,
            resources=resources,
            repeats=repeats,
            groups=groups,
            input_sources=input_sources,
            nesting_order=nesting_order,
            env_preset=env_preset,
            environments=environments,
            element_sets=element_sets,
            allow_non_coincident_task_sources=allow_non_coincident_task_sources,
            sourceable_elem_iters=sourceable_elem_iters,
        )
        self._output_labels = output_labels or []
        #: Whether to merge ``environments`` into ``resources`` using the "any" scope
        #: on first initialisation.
        self.merge_envs = merge_envs
        self.__groups: AppDataList[ElementGroup] = AppDataList(
            groups or [], access_attribute="name"
        )

        # appended to when new element sets are added and reset on dump to disk:
        self._pending_element_sets: list[ElementSet] = []

        self._validate()
        self._name = self.__get_name()

        #: The template workflow that this task is within.
        self.workflow_template: WorkflowTemplate | None = (
            None  # assigned by parent WorkflowTemplate
        )
        self._insert_ID: int | None = None
        self._dir_name: str | None = None

        if self.merge_envs:
            self.__merge_envs_into_resources()

        # TODO: consider adding a new element_set; will need to merge new environments?

        self._set_parent_refs({"schema": "schemas"})

    def __merge_envs_into_resources(self) -> None:
        # for each element set, merge `env_preset` into `resources` (this mutates
        # `resources`, and should only happen on creation of the task, not
        # re-initialisation from a persistent workflow):
        self.merge_envs = False

        # TODO: required so we don't raise below; can be removed once we consider multiple
        # schemas:
        for es in self.element_sets:
            if es.env_preset or any(seq.path == "env_preset" for seq in es.sequences):
                break
        else:
            # No presets
            return

        try:
            env_presets = self.schema.environment_presets
        except ValueError as e:
            # TODO: consider multiple schemas
            raise NotImplementedError(
                "Cannot merge environment presets into a task with multiple schemas."
            ) from e

        for es in self.element_sets:
            if es.env_preset:
                # retrieve env specifiers from presets defined in the schema:
                try:
                    env_specs = env_presets[es.env_preset]  # type: ignore[index]
                except (TypeError, KeyError):
                    raise UnknownEnvironmentPresetError(es.env_preset, self.schema.name)
                es.resources.merge_one(
                    self._app.ResourceSpec(scope="any", environments=env_specs)
                )

            for seq in es.sequences:
                if seq.path == "env_preset":
                    # change to a resources path:
                    seq.path = "resources.any.environments"
                    _values = []
                    for val in seq.values or ():
                        try:
                            _values.append(env_presets[val])  # type: ignore[index]
                        except (TypeError, KeyError) as e:
                            raise UnknownEnvironmentPresetError(
                                val, self.schema.name
                            ) from e
                    seq._values = _values

    def _reset_pending_element_sets(self) -> None:
        self._pending_element_sets = []

    def _accept_pending_element_sets(self) -> None:
        self._element_sets += self._pending_element_sets
        self._reset_pending_element_sets()

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.to_dict() == other.to_dict()

    def _add_element_set(self, element_set: ElementSet):
        """Invoked by WorkflowTask._add_element_set."""
        self._pending_element_sets.append(element_set)
        wt = self.workflow_template
        assert wt
        w = wt.workflow
        assert w
        w._store.add_element_set(
            self.insert_ID, cast("Mapping", element_set.to_json_like()[0])
        )

    @classmethod
    def _json_like_constructor(cls, json_like: dict) -> Self:
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""
        insert_ID = json_like.pop("insert_ID", None)
        dir_name = json_like.pop("dir_name", None)
        obj = cls(**json_like)
        obj._insert_ID = insert_ID
        obj._dir_name = dir_name
        return obj

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"

    def __deepcopy__(self, memo: dict[int, Any] | None) -> Self:
        kwargs = self.to_dict()
        _insert_ID = kwargs.pop("insert_ID")
        _dir_name = kwargs.pop("dir_name")
        # _pending_element_sets = kwargs.pop("pending_element_sets")
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._insert_ID = _insert_ID
        obj._dir_name = _dir_name
        obj._name = self._name
        obj.workflow_template = self.workflow_template
        obj._pending_element_sets = self._pending_element_sets
        return obj

    def to_persistent(
        self, workflow: Workflow, insert_ID: int
    ) -> tuple[Self, list[int | list[int]]]:
        """Return a copy where any schema input defaults are saved to a persistent
        workflow. Element set data is not made persistent."""

        obj = copy.deepcopy(self)
        source: ParamSource = {"type": "default_input", "task_insert_ID": insert_ID}
        new_refs = list(
            chain.from_iterable(
                schema.make_persistent(workflow, source) for schema in obj.schemas
            )
        )

        return obj, new_refs

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        out = super()._postprocess_to_dict(d)
        out["_schema"] = out.pop("_schemas")
        res = {
            k.lstrip("_"): v
            for k, v in out.items()
            if k not in ("_name", "_pending_element_sets", "_Task__groups")
        }
        return res

    def set_sequence_parameters(self, element_set: ElementSet) -> None:
        """
        Set up parameters parsed by value sequences.
        """
        # set ValueSequence Parameter objects:
        for seq in element_set.sequences:
            if seq.input_type:
                for schema_i in self.schemas:
                    for inp_j in schema_i.inputs:
                        if inp_j.typ == seq.input_type:
                            seq._parameter = inp_j.parameter

    def _validate(self) -> None:
        # TODO: check a nesting order specified for each sequence?

        if len(names := set(schema.objective.name for schema in self.schemas)) > 1:
            raise TaskTemplateMultipleSchemaObjectives(names)

    def __get_name(self) -> str:
        out = self.objective.name
        for idx, schema_i in enumerate(self.schemas, start=1):
            need_and = idx < len(self.schemas) and (
                self.schemas[idx].method or self.schemas[idx].implementation
            )
            out += (
                f"{f'_{schema_i.method}' if schema_i.method else ''}"
                f"{f'_{schema_i.implementation}' if schema_i.implementation else ''}"
                f"{f'_and' if need_and else ''}"
            )
        return out

    @staticmethod
    def get_task_unique_names(tasks: list[Task]) -> Sequence[str]:
        """Get the unique name of each in a list of tasks.

        Returns
        -------
        list of str
        """

        task_name_rep_idx = get_item_repeat_index(
            tasks,
            item_callable=lambda x: x.name,
            distinguish_singular=True,
        )

        return [
            (
                f"{task.name}_{task_name_rep_idx[idx]}"
                if task_name_rep_idx[idx] > 0
                else task.name
            )
            for idx, task in enumerate(tasks)
        ]

    @TimeIt.decorator
    def _prepare_persistent_outputs(
        self, workflow: Workflow, local_element_idx_range: Sequence[int]
    ) -> Mapping[str, Sequence[int]]:
        # TODO: check that schema is present when adding task? (should this be here?)

        # allocate schema-level output parameter; precise EAR index will not be known
        # until we initialise EARs:
        output_data_indices: dict[str, list[int]] = {}
        for schema in self.schemas:
            for output in schema.outputs:
                # TODO: consider multiple schemas in action index?

                path = f"outputs.{output.typ}"
                output_data_indices[path] = [
                    # iteration_idx, action_idx, and EAR_idx are not known until
                    # `initialise_EARs`:
                    workflow._add_unset_parameter_data(
                        {
                            "type": "EAR_output",
                            # "task_insert_ID": self.insert_ID,
                            # "element_idx": idx,
                            # "run_idx": 0,
                        }
                    )
                    for idx in range(*local_element_idx_range)
                ]

        return output_data_indices

    def prepare_element_resolution(
        self, element_set: ElementSet, input_data_indices: Mapping[str, Sequence]
    ) -> list[MultiplicityDescriptor]:
        """
        Set up the resolution of details of elements
        (especially multiplicities and how iterations are nested)
        within an element set.
        """
        multiplicities: list[MultiplicityDescriptor] = [
            {
                "multiplicity": len(inp_idx_i),
                "nesting_order": element_set.nesting_order.get(path_i, -1.0),
                "path": path_i,
            }
            for path_i, inp_idx_i in input_data_indices.items()
        ]

        # if all inputs with non-unit multiplicity have the same multiplicity and a
        # default nesting order of -1 or 0 (which will have probably been set by a
        # `ValueSequence` default), set the non-unit multiplicity inputs to a nesting
        # order of zero:
        non_unit_multis: dict[int, int] = {}
        unit_multis: list[int] = []
        change = True
        for idx, descriptor in enumerate(multiplicities):
            if descriptor["multiplicity"] == 1:
                unit_multis.append(idx)
            elif descriptor["nesting_order"] in (-1.0, 0.0):
                non_unit_multis[idx] = descriptor["multiplicity"]
            else:
                change = False
                break

        if change and len(set(non_unit_multis.values())) == 1:
            for i_idx in non_unit_multis:
                multiplicities[i_idx]["nesting_order"] = 0

        return multiplicities

    @property
    def index(self) -> int | None:
        """
        The index of this task within the workflow's tasks.
        """
        if self.workflow_template:
            return self.workflow_template.tasks.index(self)
        else:
            return None

    @property
    def output_labels(self) -> Sequence[OutputLabel]:
        """
        The labels on the outputs of the task.
        """
        return self._output_labels

    @property
    def _element_indices(self) -> list[int] | None:
        if (
            self.workflow_template
            and self.workflow_template.workflow
            and self.index is not None
        ):
            task = self.workflow_template.workflow.tasks[self.index]
            return [element._index for element in task.elements]
        return None

    def __get_task_source_element_iters(
        self, in_or_out: str, src_task: Task, labelled_path: str, element_set: ElementSet
    ) -> list[int]:
        """Get a sorted list of element iteration IDs that provide either inputs or
        outputs from the provided source task."""

        if in_or_out == "input":
            # input parameter might not be provided e.g. if it is only used
            # to generate an input file, and that input file is passed
            # directly, so consider only source task element sets that
            # provide the input locally:
            es_idx = src_task.get_param_provided_element_sets(labelled_path)
            for es_i in src_task.element_sets:
                # add any element set that has task sources for this parameter
                es_i_idx = es_i.index
                if (
                    es_i_idx is not None
                    and es_i_idx not in es_idx
                    and any(
                        inp_src_i.source_type is InputSourceType.TASK
                        for inp_src_i in es_i.input_sources.get(labelled_path, ())
                    )
                ):
                    es_idx.append(es_i_idx)
        else:
            # outputs are always available, so consider all source task
            # element sets:
            es_idx = list(range(src_task.num_element_sets))

        if not es_idx:
            raise NoAvailableElementSetsError()

        src_elem_iters: list[int] = []
        for es_i_idx in es_idx:
            es_i = src_task.element_sets[es_i_idx]
            src_elem_iters.extend(es_i.elem_iter_IDs)  # should be sorted already

        if element_set.sourceable_elem_iters is not None:
            # can only use a subset of element iterations (this is the
            # case where this element set is generated from an upstream
            # element set, in which case we only want to consider newly
            # added upstream elements when adding elements from this
            # element set):
            src_elem_iters = sorted(
                set(element_set.sourceable_elem_iters).intersection(src_elem_iters)
            )

        return src_elem_iters

    @staticmethod
    def __get_common_path(labelled_path: str, inputs_path: str) -> str | None:
        lab_s = labelled_path.split(".")
        inp_s = inputs_path.split(".")
        try:
            get_relative_path(lab_s, inp_s)
            return labelled_path
        except ValueError:
            pass
        try:
            get_relative_path(inp_s, lab_s)
            return inputs_path
        except ValueError:
            # no intersection between paths
            return None

    @staticmethod
    def __filtered_iters(wk_task: WorkflowTask, where: Rule) -> list[int]:
        param_path = cast("str", where.path)
        param_prefix, param_name, *param_tail = param_path.split(".")
        src_elem_iters: list[int] = []

        for elem in wk_task.elements:
            params: EPP = getattr(elem, param_prefix)
            param: ElementParameter = getattr(params, param_name)
            param_dat = param.value

            # for remaining paths components try both getattr and
            # getitem:
            for path_k in param_tail:
                try:
                    param_dat = param_dat[path_k]
                except TypeError:
                    param_dat = getattr(param_dat, path_k)

            if where._valida_check(param_dat):
                src_elem_iters.append(elem.iterations[0].id_)

        return src_elem_iters

    def get_available_task_input_sources(
        self,
        element_set: ElementSet,
        source_tasks: Sequence[WorkflowTask] = (),
    ) -> Mapping[str, Sequence[InputSource]]:
        """For each input parameter of this task, generate a list of possible input sources
        that derive from inputs or outputs of this and other provided tasks.

        Note this only produces a subset of available input sources for each input
        parameter; other available input sources may exist from workflow imports."""

        # ensure parameters provided by later tasks are added to the available sources
        # list first, meaning they take precedence when choosing an input source:
        source_tasks = sorted(source_tasks, key=lambda x: x.index, reverse=True)

        available: dict[str, list[InputSource]] = {}
        for inputs_path, inp_status in self.get_input_statuses(element_set).items():
            # local specification takes precedence:
            if inputs_path in element_set.get_locally_defined_inputs():
                available.setdefault(inputs_path, []).append(
                    self._app.InputSource.local()
                )

            # search for task sources:
            for src_wk_task_i in source_tasks:
                # ensure we process output types before input types, so they appear in the
                # available sources list first, meaning they take precedence when choosing
                # an input source:
                src_task_i = src_wk_task_i.template
                for in_or_out, labelled_path in sorted(
                    src_task_i.provides_parameters(),
                    key=lambda x: x[0],
                    reverse=True,
                ):
                    src_elem_iters: list[int] = []
                    common = self.__get_common_path(labelled_path, inputs_path)
                    if common is not None:
                        avail_src_path = common
                    else:
                        # no intersection between paths
                        inputs_path_label = None
                        out_label = None
                        unlabelled, inputs_path_label = split_param_label(inputs_path)
                        if unlabelled is None:
                            continue
                        try:
                            get_relative_path(
                                unlabelled.split("."), labelled_path.split(".")
                            )
                            avail_src_path = inputs_path
                        except ValueError:
                            continue
                        if not inputs_path_label:
                            continue
                        for out_lab_i in src_task_i.output_labels:
                            if out_lab_i.label == inputs_path_label:
                                out_label = out_lab_i

                        # consider output labels
                        if out_label and in_or_out == "output":
                            # find element iteration IDs that match the output label
                            # filter:
                            if out_label.where:
                                src_elem_iters = self.__filtered_iters(
                                    src_wk_task_i, out_label.where
                                )
                            else:
                                src_elem_iters = [
                                    elem_i.iterations[0].id_
                                    for elem_i in src_wk_task_i.elements
                                ]

                    if not src_elem_iters:
                        try:
                            src_elem_iters = self.__get_task_source_element_iters(
                                in_or_out=in_or_out,
                                src_task=src_task_i,
                                labelled_path=labelled_path,
                                element_set=element_set,
                            )
                        except NoAvailableElementSetsError:
                            continue
                        if not src_elem_iters:
                            continue

                    available.setdefault(avail_src_path, []).append(
                        self._app.InputSource.task(
                            task_ref=src_task_i.insert_ID,
                            task_source_type=in_or_out,
                            element_iters=src_elem_iters,
                        )
                    )

            if inp_status.has_default:
                available.setdefault(inputs_path, []).append(
                    self._app.InputSource.default()
                )
        return available

    @property
    def schemas(self) -> list[TaskSchema]:
        """
        All the task schemas.
        """
        return self._schemas

    @property
    def schema(self) -> TaskSchema:
        """The single task schema, if only one, else raises."""
        if len(self._schemas) == 1:
            return self._schemas[0]
        else:
            raise ValueError(
                "Multiple task schemas are associated with this task. Access the list "
                "via the `schemas` property."
            )

    @property
    def element_sets(self) -> list[ElementSet]:
        """
        The element sets.
        """
        return self._element_sets + self._pending_element_sets

    @property
    def num_element_sets(self) -> int:
        """
        The number of element sets.
        """
        return len(self._element_sets) + len(self._pending_element_sets)

    @property
    def insert_ID(self) -> int:
        """
        Insertion ID.
        """
        assert self._insert_ID is not None
        return self._insert_ID

    @property
    def dir_name(self) -> str:
        """
        Artefact directory name.
        """
        assert self._dir_name is not None
        return self._dir_name

    @property
    def name(self) -> str:
        """
        Task name.
        """
        return self._name

    @property
    def objective(self) -> TaskObjective:
        """
        The goal of this task.
        """
        obj = self.schemas[0].objective
        return obj

    @property
    def all_schema_inputs(self) -> tuple[SchemaInput, ...]:
        """
        The inputs to this task's schemas.
        """
        return tuple(inp_j for schema_i in self.schemas for inp_j in schema_i.inputs)

    @property
    def all_schema_outputs(self) -> tuple[SchemaOutput, ...]:
        """
        The outputs from this task's schemas.
        """
        return tuple(inp_j for schema_i in self.schemas for inp_j in schema_i.outputs)

    @property
    def all_schema_input_types(self) -> set[str]:
        """
        The set of all schema input types (over all specified schemas).
        """
        return {inp_j for schema_i in self.schemas for inp_j in schema_i.input_types}

    @property
    def all_schema_input_normalised_paths(self) -> set[str]:
        """
        Normalised paths for all schema input types.
        """
        return {f"inputs.{typ}" for typ in self.all_schema_input_types}

    @property
    def all_schema_output_types(self) -> set[str]:
        """
        The set of all schema output types (over all specified schemas).
        """
        return {out_j for schema_i in self.schemas for out_j in schema_i.output_types}

    def get_schema_action(self, idx: int) -> Action:  #
        """
        Get the schema action at the given index.
        """
        _idx = 0
        for schema in self.schemas:
            for action in schema.actions:
                if _idx == idx:
                    return action
                _idx += 1
        raise ValueError(f"No action in task {self.name!r} with index {idx!r}.")

    def all_schema_actions(self) -> Iterator[tuple[int, Action]]:
        """
        Get all the schema actions and their indices.
        """
        idx = 0
        for schema in self.schemas:
            for action in schema.actions:
                yield (idx, action)
                idx += 1

    @property
    def num_all_schema_actions(self) -> int:
        """
        The total number of schema actions.
        """
        return sum(len(schema.actions) for schema in self.schemas)

    @property
    def all_sourced_normalised_paths(self) -> set[str]:
        """
        All the sourced normalised paths, including of sub-values.
        """
        sourced_input_types: set[str] = set()
        for elem_set in self.element_sets:
            sourced_input_types.update(
                inp.normalised_path for inp in elem_set.inputs if inp.is_sub_value
            )
            sourced_input_types.update(
                seq.normalised_path for seq in elem_set.sequences if seq.is_sub_value
            )
        return sourced_input_types | self.all_schema_input_normalised_paths

    def is_input_type_required(self, typ: str, element_set: ElementSet) -> bool:
        """Check if an given input type must be specified in the parametrisation of this
        element set.

        A schema input need not be specified if it is only required to generate an input
        file, and that input file is passed directly."""

        provided_files = {in_file.file for in_file in element_set.input_files}
        for schema in self.schemas:
            if not schema.actions:
                return True  # for empty tasks that are used merely for defining inputs
            if any(
                act.is_input_type_required(typ, provided_files) for act in schema.actions
            ):
                return True

        return False

    def get_param_provided_element_sets(self, labelled_path: str) -> list[int]:
        """Get the element set indices of this task for which a specified parameter type
        is locally provided.

        Note
        ----
        Caller may freely modify this result.
        """
        return [
            idx
            for idx, src_es in enumerate(self.element_sets)
            if src_es.is_input_type_provided(labelled_path)
        ]

    def get_input_statuses(self, elem_set: ElementSet) -> Mapping[str, InputStatus]:
        """Get a dict whose keys are normalised input paths (without the "inputs" prefix),
        and whose values are InputStatus objects.

        Parameters
        ----------
        elem_set
            The element set for which input statuses should be returned.
        """

        status: dict[str, InputStatus] = {}
        for schema_input in self.all_schema_inputs:
            for lab_info in schema_input.labelled_info():
                labelled_type = lab_info["labelled_type"]
                status[labelled_type] = InputStatus(
                    has_default="default_value" in lab_info,
                    is_provided=elem_set.is_input_type_provided(labelled_type),
                    is_required=self.is_input_type_required(labelled_type, elem_set),
                )

        for inp_path in elem_set.get_defined_sub_parameter_types():
            root_param = inp_path.split(".")[0]
            # If the root parameter is required then the sub-parameter should also be
            # required, otherwise there would be no point in specifying it:
            status[inp_path] = InputStatus(
                has_default=False,
                is_provided=True,
                is_required=status[root_param].is_required,
            )

        return status

    @property
    def universal_input_types(self) -> set[str]:
        """Get input types that are associated with all schemas"""
        raise NotImplementedError()

    @property
    def non_universal_input_types(self) -> set[str]:
        """Get input types for each schema that are non-universal."""
        raise NotImplementedError()

    @property
    def defined_input_types(self) -> set[str]:
        """
        The input types defined by this task, being the input types defined by any of
        its element sets.
        """
        dit: set[str] = set()
        for es in self.element_sets:
            dit.update(es.defined_input_types)
        return dit
        # TODO: Is this right?

    @property
    def undefined_input_types(self) -> set[str]:
        """
        The schema's input types that this task doesn't define.
        """
        return self.all_schema_input_types - self.defined_input_types

    @property
    def undefined_inputs(self) -> list[SchemaInput]:
        """
        The task's inputs that are undefined.
        """
        return [
            inp_j
            for schema_i in self.schemas
            for inp_j in schema_i.inputs
            if inp_j.typ in self.undefined_input_types
        ]

    def provides_parameters(self) -> tuple[tuple[str, str], ...]:
        """Get all provided parameter labelled types and whether they are inputs and
        outputs, considering all element sets.

        """
        out: dict[tuple[str, str], None] = {}
        for schema in self.schemas:
            out.update(dict.fromkeys(schema.provides_parameters))

        # add sub-parameter input values and sequences:
        for es_i in self.element_sets:
            for inp_j in es_i.inputs:
                if inp_j.is_sub_value:
                    out["input", inp_j.normalised_inputs_path] = None
            for seq_j in es_i.sequences:
                if seq_j.is_sub_value and (path := seq_j.normalised_inputs_path):
                    out["input", path] = None

        return tuple(out)

    def add_group(
        self, name: str, where: ElementFilter, group_by_distinct: ParameterPath
    ):
        """
        Add an element group to this task.
        """
        group = ElementGroup(name=name, where=where, group_by_distinct=group_by_distinct)
        self.__groups.add_object(group)

    def _get_single_label_lookup(self, prefix: str = "") -> Mapping[str, str]:
        """Get a mapping between schema input types that have a single label (i.e.
        labelled but with `multiple=False`) and the non-labelled type string.

        For example, if a task schema has a schema input like:
        `SchemaInput(parameter="p1", labels={"one": {}}, multiple=False)`, this method
        would return a dict that includes: `{"p1[one]": "p1"}`. If the `prefix` argument
        is provided, this will be added to map key and value (and a terminating period
        will be added to the end of the prefix if it does not already end in one). For
        example, with `prefix="inputs"`, this method might return:
        `{"inputs.p1[one]": "inputs.p1"}`.

        """
        lookup: dict[str, str] = {}
        for schema in self.schemas:
            lookup.update(schema._get_single_label_lookup(prefix=prefix))
        return lookup


class _ESIdx(NamedTuple):
    ordered: list[int]
    uniq: frozenset[int]


class WorkflowTask(AppAware):
    """
    Represents a :py:class:`Task` that is bound to a :py:class:`Workflow`.

    Parameters
    ----------
    workflow:
        The workflow that the task is bound to.
    template:
        The task template that this binds.
    index:
        Where in the workflow's list of tasks is this one.
    element_IDs:
        The IDs of the elements of this task.
    """

    def __init__(
        self,
        workflow: Workflow,
        template: Task,
        index: int,
        element_IDs: list[int],
    ):
        self._workflow = workflow
        self._template = template
        self._index = index
        self._element_IDs = element_IDs

        # appended to when new elements are added and reset on dump to disk:
        self._pending_element_IDs: list[int] = []

        self._elements: Elements | None = None  # assigned on `elements` first access

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.unique_name!r})"

    def _reset_pending_element_IDs(self):
        self._pending_element_IDs = []

    def _accept_pending_element_IDs(self):
        self._element_IDs += self._pending_element_IDs
        self._reset_pending_element_IDs()

    @classmethod
    def new_empty_task(cls, workflow: Workflow, template: Task, index: int) -> Self:
        """
        Make a new instance without any elements set up yet.

        Parameters
        ----------
        workflow:
            The workflow that the task is bound to.
        template:
            The task template that this binds.
        index:
            Where in the workflow's list of tasks is this one.
        """
        return cls(
            workflow=workflow,
            template=template,
            index=index,
            element_IDs=[],
        )

    @property
    def workflow(self) -> Workflow:
        """
        The workflow this task is bound to.
        """
        return self._workflow

    @property
    def template(self) -> Task:
        """
        The template for this task.
        """
        return self._template

    @property
    def index(self) -> int:
        """
        The index of this task within its workflow.
        """
        return self._index

    @property
    def element_IDs(self) -> list[int]:
        """
        The IDs of elements associated with this task.
        """
        return self._element_IDs + self._pending_element_IDs

    @property
    @TimeIt.decorator
    def num_elements(self) -> int:
        """
        The number of elements associated with this task.
        """
        return len(self._element_IDs) + len(self._pending_element_IDs)

    @property
    def num_actions(self) -> int:
        """
        The number of actions in this task.
        """
        return self.template.num_all_schema_actions

    @property
    def name(self) -> str:
        """
        The name of this task based on its template.
        """
        return self.template.name

    @property
    def unique_name(self) -> str:
        """
        The unique name for this task specifically.
        """
        return self.workflow.get_task_unique_names()[self.index]

    @property
    def insert_ID(self) -> int:
        """
        The insertion ID of the template task.
        """
        return self.template.insert_ID

    @property
    def dir_name(self) -> str:
        """
        The name of the directory for the task's temporary files.
        """
        dn = self.template.dir_name
        assert dn is not None
        return dn

    @property
    def num_element_sets(self) -> int:
        """
        The number of element sets associated with this task.
        """
        return self.template.num_element_sets

    @property
    @TimeIt.decorator
    def elements(self) -> Elements:
        """
        The elements associated with this task.
        """
        if self._elements is None:
            self._elements = Elements(self)
        return self._elements

    def get_dir_name(self, loop_idx: Mapping[str, int] | None = None) -> str:
        """
        Get the directory name for a particular iteration.
        """
        if not loop_idx:
            return self.dir_name
        return self.dir_name + "_" + "_".join((f"{k}-{v}" for k, v in loop_idx.items()))

    def get_all_element_iterations(self) -> Mapping[int, ElementIteration]:
        """
        Get the iterations known by the task's elements.
        """
        return {itr.id_: itr for elem in self.elements for itr in elem.iterations}

    @staticmethod
    def __get_src_elem_iters(
        src_task: WorkflowTask, inp_src: InputSource
    ) -> tuple[Iterable[ElementIteration], list[int]]:
        src_iters = src_task.get_all_element_iterations()

        if inp_src.element_iters:
            # only include "sourceable" element iterations:
            src_iters_list = [src_iters[itr_id] for itr_id in inp_src.element_iters]
            set_indices = [el.element.element_set_idx for el in src_iters.values()]
            return src_iters_list, set_indices
        return src_iters.values(), []

    def __get_task_group_index(
        self,
        labelled_path_i: str,
        inp_src: InputSource,
        padded_elem_iters: Mapping[str, Sequence[int]],
        inp_group_name: str | None,
    ) -> None | Sequence[int | list[int]]:
        src_task = inp_src.get_task(self.workflow)
        assert src_task
        src_elem_iters, src_elem_set_idx = self.__get_src_elem_iters(src_task, inp_src)

        if not src_elem_iters:
            return None

        task_source_type = inp_src.task_source_type
        assert task_source_type is not None
        if task_source_type == TaskSourceType.OUTPUT and "[" in labelled_path_i:
            src_key = f"{task_source_type.name.lower()}s.{labelled_path_i.split('[')[0]}"
        else:
            src_key = f"{task_source_type.name.lower()}s.{labelled_path_i}"

        padded_iters = padded_elem_iters.get(labelled_path_i, [])
        grp_idx = [
            (itr.get_data_idx()[src_key] if itr_idx not in padded_iters else -1)
            for itr_idx, itr in enumerate(src_elem_iters)
        ]

        if not inp_group_name:
            return grp_idx

        group_dat_idx: list[int | list[int]] = []
        element_sets = src_task.template.element_sets
        for dat_idx, src_set_idx, src_iter in zip(
            grp_idx, src_elem_set_idx, src_elem_iters
        ):
            src_es = element_sets[src_set_idx]
            if any(inp_group_name == grp.name for grp in src_es.groups):
                group_dat_idx.append(dat_idx)
                continue
            # if for any recursive iteration dependency, this group is
            # defined, assign:
            src_iter_deps = self.workflow.get_element_iterations_from_IDs(
                src_iter.get_element_iteration_dependencies(),
            )

            if any(
                inp_group_name == grp.name
                for el_iter in src_iter_deps
                for grp in el_iter.element.element_set.groups
            ):
                group_dat_idx.append(dat_idx)
                continue

            # also check input dependencies
            for p_src in src_iter.element.get_input_dependencies().values():
                k_es = self.workflow.tasks.get(
                    insert_ID=p_src["task_insert_ID"]
                ).template.element_sets[p_src["element_set_idx"]]
                if any(inp_group_name == grp.name for grp in k_es.groups):
                    group_dat_idx.append(dat_idx)
                    break

            # TODO: this only goes to one level of dependency

        if not group_dat_idx:
            raise MissingElementGroup(self.unique_name, inp_group_name, labelled_path_i)

        return [cast("int", group_dat_idx)]  # TODO: generalise to multiple groups

    def __make_new_elements_persistent(
        self,
        element_set: ElementSet,
        element_set_idx: int,
        padded_elem_iters: Mapping[str, Sequence[int]],
    ) -> tuple[
        dict[str, list[int | list[int]]], dict[str, Sequence[int]], dict[str, list[int]]
    ]:
        """Save parameter data to the persistent workflow."""

        # TODO: rewrite. This method is a little hard to follow and results in somewhat
        # unexpected behaviour: if a local source and task source are requested for a
        # given input, the local source element(s) will always come first, regardless of
        # the ordering in element_set.input_sources.

        input_data_idx: dict[str, list[int | list[int]]] = {}
        sequence_idx: dict[str, Sequence[int]] = {}
        source_idx: dict[str, list[int]] = {}

        # Assign first assuming all locally defined values are to be used:
        param_src: ParamSource = {
            "type": "local_input",
            "task_insert_ID": self.insert_ID,
            "element_set_idx": element_set_idx,
        }
        loc_inp_src = self._app.InputSource.local()
        for res_i in element_set.resources:
            key, dat_ref, _ = res_i.make_persistent(self.workflow, param_src)
            input_data_idx[key] = list(dat_ref)

        for inp_i in element_set.inputs:
            key, dat_ref, _ = inp_i.make_persistent(self.workflow, param_src)
            input_data_idx[key] = list(dat_ref)
            key_ = key.removeprefix("inputs.")
            try:
                # TODO: wouldn't need to do this if we raise when an InputValue is
                # provided for a parameter whose inputs sources do not include the local
                # value.
                source_idx[key] = [element_set.input_sources[key_].index(loc_inp_src)]
            except ValueError:
                pass

        for inp_file_i in element_set.input_files:
            key, input_dat_ref, _ = inp_file_i.make_persistent(self.workflow, param_src)
            input_data_idx[key] = list(input_dat_ref)

        for seq_i in element_set.sequences:
            key, seq_dat_ref, _ = seq_i.make_persistent(self.workflow, param_src)
            input_data_idx[key] = list(seq_dat_ref)
            sequence_idx[key] = list(range(len(seq_dat_ref)))
            try:
                key_ = key.split("inputs.")[1]
            except IndexError:
                # e.g. "resources."
                key_ = ""
            try:
                # TODO: wouldn't need to do this if we raise when an ValueSequence is
                # provided for a parameter whose inputs sources do not include the local
                # value.
                if key_:
                    source_idx[key] = [
                        element_set.input_sources[key_].index(loc_inp_src)
                    ] * len(seq_dat_ref)
            except ValueError:
                pass

        for rep_spec in element_set.repeats:
            seq_key = f"repeats.{rep_spec['name']}"
            num_range = range(rep_spec["number"])
            input_data_idx[seq_key] = list(num_range)
            sequence_idx[seq_key] = num_range

        # Now check for task- and default-sources and overwrite or append to local sources:
        inp_stats = self.template.get_input_statuses(element_set)
        for labelled_path_i, sources_i in element_set.input_sources.items():
            if len(path_i_split := labelled_path_i.split(".")) > 1:
                path_i_root = path_i_split[0]
            else:
                path_i_root = labelled_path_i
            if not inp_stats[path_i_root].is_required:
                continue

            inp_group_name, def_val = None, None
            for schema_input in self.template.all_schema_inputs:
                for lab_info in schema_input.labelled_info():
                    if lab_info["labelled_type"] == path_i_root:
                        inp_group_name = lab_info["group"]
                        if "default_value" in lab_info:
                            def_val = lab_info["default_value"]
                        break

            key = f"inputs.{labelled_path_i}"

            for inp_src_idx, inp_src in enumerate(sources_i):
                if inp_src.source_type is InputSourceType.TASK:
                    grp_idx = self.__get_task_group_index(
                        labelled_path_i, inp_src, padded_elem_iters, inp_group_name
                    )
                    if grp_idx is None:
                        continue

                    if self._app.InputSource.local() in sources_i:
                        # add task source to existing local source:
                        input_data_idx[key].extend(grp_idx)
                        source_idx[key].extend([inp_src_idx] * len(grp_idx))

                    else:  # BUG: doesn't work for multiple task inputs sources
                        # overwrite existing local source (if it exists):
                        input_data_idx[key] = list(grp_idx)
                        source_idx[key] = [inp_src_idx] * len(grp_idx)
                        if key in sequence_idx:
                            sequence_idx.pop(key)
                            # TODO: Use the value retrieved below?
                            _ = element_set.get_sequence_from_path(key)

                elif inp_src.source_type is InputSourceType.DEFAULT:
                    assert def_val is not None
                    assert def_val._value_group_idx is not None
                    grp_idx_ = def_val._value_group_idx
                    if self._app.InputSource.local() in sources_i:
                        input_data_idx[key].append(grp_idx_)
                        source_idx[key].append(inp_src_idx)
                    else:
                        input_data_idx[key] = [grp_idx_]
                        source_idx[key] = [inp_src_idx]

        # sort smallest to largest path, so more-specific items overwrite less-specific
        # items in parameter retrieval in `WorkflowTask._get_merged_parameter_data`:
        input_data_idx = dict(sorted(input_data_idx.items()))

        return (input_data_idx, sequence_idx, source_idx)

    def ensure_input_sources(
        self, element_set: ElementSet
    ) -> Mapping[str, Sequence[int]]:
        """Check valid input sources are specified for a new task to be added to the
        workflow in a given position. If none are specified, set them according to the
        default behaviour.

        This method mutates `element_set.input_sources`.

        """

        # this depends on this schema, other task schemas and inputs/sequences:
        available_sources = self.template.get_available_task_input_sources(
            element_set=element_set,
            source_tasks=self.workflow.tasks[: self.index],
        )

        if unreq := set(element_set.input_sources).difference(available_sources):
            raise UnrequiredInputSources(unreq)

        # TODO: get available input sources from workflow imports

        all_stats = self.template.get_input_statuses(element_set)

        # an input is not required if it is only used to generate an input file that is
        # passed directly:
        req_types = set(k for k, v in all_stats.items() if v.is_required)

        # check any specified sources are valid, and replace them with those computed in
        # `available_sources` since these will have `element_iters` assigned:
        for path_i, avail_i in available_sources.items():
            # for each sub-path in available sources, if the "root-path" source is
            # required, then add the sub-path source to `req_types` as well:
            if len(path_i_split := path_i.split(".")) > 1:
                if path_i_split[0] in req_types:
                    req_types.add(path_i)

            for s_idx, specified_source in enumerate(
                element_set.input_sources.get(path_i, [])
            ):
                self.workflow._resolve_input_source_task_reference(
                    specified_source, self.unique_name
                )
                avail_idx = specified_source.is_in(avail_i)
                if avail_idx is None:
                    raise UnavailableInputSource(specified_source, path_i, avail_i)
                available_source: InputSource
                try:
                    available_source = avail_i[avail_idx]
                except TypeError:
                    raise UnavailableInputSource(
                        specified_source, path_i, avail_i
                    ) from None

                elem_iters_IDs = available_source.element_iters
                if specified_source.element_iters:
                    # user-specified iter IDs; these must be a subset of available
                    # element_iters:
                    if not set(specified_source.element_iters).issubset(
                        elem_iters_IDs or ()
                    ):
                        raise InapplicableInputSourceElementIters(
                            specified_source, elem_iters_IDs
                        )
                    elem_iters_IDs = specified_source.element_iters

                if specified_source.where:
                    # filter iter IDs by user-specified rules, maintaining order:
                    elem_iters = self.workflow.get_element_iterations_from_IDs(
                        elem_iters_IDs or ()
                    )
                    elem_iters_IDs = [
                        ei.id_ for ei in specified_source.where.filter(elem_iters)
                    ]

                available_source.element_iters = elem_iters_IDs
                element_set.input_sources[path_i][s_idx] = available_source

        # sorting ensures that root parameters come before sub-parameters, which is
        # necessary when considering if we want to include a sub-parameter, when setting
        # missing sources below:
        unsourced_inputs = sorted(req_types.difference(element_set.input_sources))

        if extra_types := {k for k, v in all_stats.items() if v.is_extra}:
            raise ExtraInputs(extra_types)

        # set source for any unsourced inputs:
        missing: list[str] = []
        # track which root params we have set according to default behaviour (not
        # specified by user):
        set_root_params: set[str] = set()
        for input_type in unsourced_inputs:
            input_split = input_type.split(".")
            has_root_param = input_split[0] if len(input_split) > 1 else None
            inp_i_sources = available_sources.get(input_type, [])

            source = None
            try:
                # first element is defined by default to take precedence in
                # `get_available_task_input_sources`:
                source = inp_i_sources[0]
            except IndexError:
                missing.append(input_type)

            if source is not None:
                if has_root_param and has_root_param in set_root_params:
                    # this is a sub-parameter, and the associated root parameter was not
                    # specified by the user either, so we previously set it according to
                    # default behaviour
                    root_src = element_set.input_sources[has_root_param][0]
                    # do not set a default task-input type source for this sub-parameter
                    # if the associated root parameter has a default-set task-output
                    # source from the same task:
                    if (
                        source.source_type is InputSourceType.TASK
                        and source.task_source_type is TaskSourceType.INPUT
                        and root_src.source_type is InputSourceType.TASK
                        and root_src.task_source_type is TaskSourceType.OUTPUT
                        and source.task_ref == root_src.task_ref
                    ):
                        continue

                element_set.input_sources[input_type] = [source]
                if not has_root_param:
                    set_root_params.add(input_type)

        # for task sources that span multiple element sets, pad out sub-parameter
        # `element_iters` to include the element iterations from other element sets in
        # which the "root" parameter is defined:
        sources_by_task: dict[int, dict[str, InputSource]] = defaultdict(dict)
        elem_iter_by_task: dict[int, dict[str, list[int]]] = defaultdict(dict)
        all_elem_iters: set[int] = set()
        for inp_type, sources in element_set.input_sources.items():
            source = sources[0]
            if source.source_type is InputSourceType.TASK:
                assert source.task_ref is not None
                assert source.element_iters is not None
                sources_by_task[source.task_ref][inp_type] = source
                all_elem_iters.update(source.element_iters)
                elem_iter_by_task[source.task_ref][inp_type] = source.element_iters

        all_elem_iters_by_ID = {
            el_iter.id_: el_iter
            for el_iter in self.workflow.get_element_iterations_from_IDs(all_elem_iters)
        }

        # element set indices:
        padded_elem_iters = defaultdict(list)
        es_idx_by_task: dict[int, dict[str, _ESIdx]] = defaultdict(dict)
        for task_ref, task_iters in elem_iter_by_task.items():
            for inp_type, inp_iters in task_iters.items():
                es_indices = [
                    all_elem_iters_by_ID[id_].element.element_set_idx for id_ in inp_iters
                ]
                es_idx_by_task[task_ref][inp_type] = _ESIdx(
                    es_indices, frozenset(es_indices)
                )
            for root_param in {k for k in task_iters if "." not in k}:
                rp_nesting = element_set.nesting_order.get(f"inputs.{root_param}", None)
                rp_elem_sets, rp_elem_sets_uniq = es_idx_by_task[task_ref][root_param]

                root_param_prefix = f"{root_param}."
                for sub_param_j in {
                    k for k in task_iters if k.startswith(root_param_prefix)
                }:
                    sub_param_nesting = element_set.nesting_order.get(
                        f"inputs.{sub_param_j}", None
                    )
                    if sub_param_nesting == rp_nesting:
                        sp_elem_sets_uniq = es_idx_by_task[task_ref][sub_param_j].uniq

                        if sp_elem_sets_uniq != rp_elem_sets_uniq:
                            # replace elem_iters in sub-param sequence with those from the
                            # root parameter, but re-order the elem iters to match their
                            # original order:
                            iters = elem_iter_by_task[task_ref][root_param]

                            # "mask" iter IDs corresponding to the sub-parameter's element
                            # sets, and keep track of the extra indices so they can be
                            # ignored later:
                            sp_iters_new: list[int | None] = []
                            for idx, (it_id, es_idx) in enumerate(
                                zip(iters, rp_elem_sets)
                            ):
                                if es_idx in sp_elem_sets_uniq:
                                    sp_iters_new.append(None)
                                else:
                                    sp_iters_new.append(it_id)
                                    padded_elem_iters[sub_param_j].append(idx)

                            # update sub-parameter element iters:
                            for src in element_set.input_sources[sub_param_j]:
                                if src.source_type is InputSourceType.TASK:
                                    # fill in sub-param elem_iters in their specified order
                                    sub_iters_it = iter(
                                        elem_iter_by_task[task_ref][sub_param_j]
                                    )
                                    src.element_iters = [
                                        it_id if it_id is not None else next(sub_iters_it)
                                        for it_id in sp_iters_new
                                    ]
                                    # assumes only a single task-type source for this
                                    # parameter
                                    break

        # TODO: collate all input sources separately, then can fall back to a different
        # input source (if it was not specified manually) and if the "top" input source
        # results in no available elements due to `allow_non_coincident_task_sources`.

        if not element_set.allow_non_coincident_task_sources:
            self.__enforce_some_sanity(sources_by_task, element_set)

        if missing:
            raise MissingInputs(self.template, missing)
        return padded_elem_iters

    def __enforce_some_sanity(
        self, sources_by_task: dict[int, dict[str, InputSource]], element_set: ElementSet
    ) -> None:
        """
        if multiple parameters are sourced from the same upstream task, only use
        element iterations for which all parameters are available (the set
        intersection)
        """
        for task_ref, sources in sources_by_task.items():
            # if a parameter has multiple labels, disregard from this by removing all
            # parameters:
            seen_labelled: dict[str, int] = defaultdict(int)
            for src_i in sources:
                if "[" in src_i:
                    unlabelled, _ = split_param_label(src_i)
                    assert unlabelled is not None
                    seen_labelled[unlabelled] += 1

            for prefix, count in seen_labelled.items():
                if count > 1:
                    # remove:
                    sources = {
                        k: v for k, v in sources.items() if not k.startswith(prefix)
                    }

            if len(sources) < 2:
                continue

            first_src = next(iter(sources.values()))
            intersect_task_i = set(first_src.element_iters or ())
            for inp_src in sources.values():
                intersect_task_i.intersection_update(inp_src.element_iters or ())
            if not intersect_task_i:
                raise NoCoincidentInputSources(self.name, task_ref)

            # now change elements for the affected input sources.
            # sort by original order of first_src.element_iters
            int_task_i_lst = [
                i for i in first_src.element_iters or () if i in intersect_task_i
            ]
            for inp_type in sources:
                element_set.input_sources[inp_type][0].element_iters = int_task_i_lst

    def generate_new_elements(
        self,
        input_data_indices: Mapping[str, Sequence[int | list[int]]],
        output_data_indices: Mapping[str, Sequence[int]],
        element_data_indices: Sequence[Mapping[str, int]],
        sequence_indices: Mapping[str, Sequence[int]],
        source_indices: Mapping[str, Sequence[int]],
    ) -> tuple[
        Sequence[DataIndex], Mapping[str, Sequence[int]], Mapping[str, Sequence[int]]
    ]:
        """
        Create information about new elements in this task.
        """
        new_elements: list[DataIndex] = []
        element_sequence_indices: dict[str, list[int]] = {}
        element_src_indices: dict[str, list[int]] = {}
        for i_idx, data_idx in enumerate(element_data_indices):
            elem_i = {
                k: input_data_indices[k][v]
                for k, v in data_idx.items()
                if input_data_indices[k][v] != -1
            }
            elem_i.update((k, v2[i_idx]) for k, v2 in output_data_indices.items())
            new_elements.append(elem_i)

            for k, v3 in data_idx.items():
                # track which sequence value indices (if any) are used for each new
                # element:
                if k in sequence_indices:
                    element_sequence_indices.setdefault(k, []).append(
                        sequence_indices[k][v3]
                    )

                # track original InputSource associated with each new element:
                if k in source_indices:
                    if input_data_indices[k][v3] != -1:
                        src_idx_k = source_indices[k][v3]
                    else:
                        src_idx_k = -1
                    element_src_indices.setdefault(k, []).append(src_idx_k)

        return new_elements, element_sequence_indices, element_src_indices

    @property
    def upstream_tasks(self) -> Iterator[WorkflowTask]:
        """All workflow tasks that are upstream from this task."""
        tasks = self.workflow.tasks
        for idx in range(0, self.index):
            yield tasks[idx]

    @property
    def downstream_tasks(self) -> Iterator[WorkflowTask]:
        """All workflow tasks that are downstream from this task."""
        tasks = self.workflow.tasks
        for idx in range(self.index + 1, len(tasks)):
            yield tasks[idx]

    @staticmethod
    def resolve_element_data_indices(
        multiplicities: list[MultiplicityDescriptor],
    ) -> Sequence[Mapping[str, int]]:
        """Find the index of the parameter group index list corresponding to each
        input data for all elements.

        Parameters
        ----------
        multiplicities : list of MultiplicityDescriptor
            Each list item represents a sequence of values with keys:
                multiplicity: int
                nesting_order: float
                path : str

        Returns
        -------
        element_dat_idx : list of dict
            Each list item is a dict representing a single task element and whose keys are
            input data paths and whose values are indices that index the values of the
            dict returned by the `task.make_persistent` method.

        Note
        ----
        Non-integer nesting orders result in doing the dot product of that sequence with
        all the current sequences instead of just with the other sequences at the same
        nesting order (or as a cross product for other nesting orders entire).
        """

        # order by nesting order (lower nesting orders will be slowest-varying):
        multi_srt = sorted(multiplicities, key=lambda x: x["nesting_order"])
        multi_srt_grp = group_by_dict_key_values(multi_srt, "nesting_order")

        element_dat_idx: list[dict[str, int]] = [{}]
        last_nest_ord: int | None = None
        for para_sequences in multi_srt_grp:
            # check all equivalent nesting_orders have equivalent multiplicities
            all_multis = {md["multiplicity"] for md in para_sequences}
            if len(all_multis) > 1:
                raise ValueError(
                    f"All inputs with the same `nesting_order` must have the same "
                    f"multiplicity, but for paths "
                    f"{[md['path'] for md in para_sequences]} with "
                    f"`nesting_order` {para_sequences[0]['nesting_order']} found "
                    f"multiplicities {[md['multiplicity'] for md in para_sequences]}."
                )

            cur_nest_ord = int(para_sequences[0]["nesting_order"])
            new_elements: list[dict[str, int]] = []
            for elem_idx, element in enumerate(element_dat_idx):
                if last_nest_ord is not None and cur_nest_ord == last_nest_ord:
                    # merge in parallel with existing elements:
                    new_elements.append(
                        {
                            **element,
                            **{md["path"]: elem_idx for md in para_sequences},
                        }
                    )
                else:
                    for val_idx in range(para_sequences[0]["multiplicity"]):
                        # nest with existing elements:
                        new_elements.append(
                            {
                                **element,
                                **{md["path"]: val_idx for md in para_sequences},
                            }
                        )
            element_dat_idx = new_elements
            last_nest_ord = cur_nest_ord

        return element_dat_idx

    @TimeIt.decorator
    def initialise_EARs(self, iter_IDs: list[int] | None = None) -> Sequence[int]:
        """Try to initialise any uninitialised EARs of this task."""
        if iter_IDs:
            iters = self.workflow.get_element_iterations_from_IDs(iter_IDs)
        else:
            iters = []
            for element in self.elements:
                # We don't yet cache Element objects, so `element`, and also it's
                # `ElementIterations, are transient. So there is no reason to update these
                # objects in memory to account for the new EARs. Subsequent calls to
                # `WorkflowTask.elements` will retrieve correct element data from the
                # store. This might need changing once/if we start caching Element
                # objects.
                iters.extend(element.iterations)

        initialised: list[int] = []
        for iter_i in iters:
            if not iter_i.EARs_initialised:
                try:
                    self.__initialise_element_iter_EARs(iter_i)
                    initialised.append(iter_i.id_)
                except UnsetParameterDataError:
                    # raised by `Action.test_rules`; cannot yet initialise EARs
                    self._app.logger.debug(
                        "UnsetParameterDataError raised: cannot yet initialise runs."
                    )
                    pass
                else:
                    iter_i._EARs_initialised = True
                    self.workflow.set_EARs_initialised(iter_i.id_)
        return initialised

    @TimeIt.decorator
    def __initialise_element_iter_EARs(self, element_iter: ElementIteration) -> None:
        # keys are (act_idx, EAR_idx):
        all_data_idx: dict[tuple[int, int], DataIndex] = {}
        action_runs: dict[tuple[int, int], dict[str, Any]] = {}

        # keys are parameter indices, values are EAR_IDs to update those sources to
        param_src_updates: dict[int, ParamSource] = {}

        count = 0
        for act_idx, action in self.template.all_schema_actions():
            log_common = (
                f"for action {act_idx} of element iteration {element_iter.index} of "
                f"element {element_iter.element.index} of task {self.unique_name!r}."
            )
            # TODO: when we support adding new runs, we will probably pass additional
            # run-specific data index to `test_rules` and `generate_data_index`
            # (e.g. if we wanted to increase the memory requirements of a action because
            # it previously failed)
            act_valid, cmds_idx = action.test_rules(element_iter=element_iter)
            if act_valid:
                self._app.logger.info(f"All action rules evaluated to true {log_common}")
                EAR_ID = self.workflow.num_EARs + count
                param_source: ParamSource = {
                    "type": "EAR_output",
                    "EAR_ID": EAR_ID,
                }
                psrc_update = (
                    action.generate_data_index(  # adds an item to `all_data_idx`
                        act_idx=act_idx,
                        EAR_ID=EAR_ID,
                        schema_data_idx=element_iter.data_idx,
                        all_data_idx=all_data_idx,
                        workflow=self.workflow,
                        param_source=param_source,
                    )
                )
                # with EARs initialised, we can update the pre-allocated schema-level
                # parameters with the correct EAR reference:
                for i in psrc_update:
                    param_src_updates[cast("int", i)] = {"EAR_ID": EAR_ID}
                run_0 = {
                    "elem_iter_ID": element_iter.id_,
                    "action_idx": act_idx,
                    "commands_idx": cmds_idx,
                    "metadata": {},
                }
                action_runs[act_idx, EAR_ID] = run_0
                count += 1
            else:
                self._app.logger.info(
                    f"Some action rules evaluated to false {log_common}"
                )

        # `generate_data_index` can modify data index for previous actions, so only assign
        # this at the end:
        for (act_idx, EAR_ID_i), run in action_runs.items():
            self.workflow._store.add_EAR(
                elem_iter_ID=element_iter.id_,
                action_idx=act_idx,
                commands_idx=run["commands_idx"],
                data_idx=all_data_idx[act_idx, EAR_ID_i],
            )

        self.workflow._store.update_param_source(param_src_updates)

    @TimeIt.decorator
    def _add_element_set(self, element_set: ElementSet) -> list[int]:
        """
        Returns
        -------
        element_indices : list of int
            Global indices of newly added elements.

        """

        self.template.set_sequence_parameters(element_set)

        # may modify element_set.input_sources:
        padded_elem_iters = self.ensure_input_sources(element_set)

        (input_data_idx, seq_idx, src_idx) = self.__make_new_elements_persistent(
            element_set=element_set,
            element_set_idx=self.num_element_sets,
            padded_elem_iters=padded_elem_iters,
        )
        element_set.task_template = self.template  # may modify element_set.nesting_order

        multiplicities = self.template.prepare_element_resolution(
            element_set, input_data_idx
        )

        element_inp_data_idx = self.resolve_element_data_indices(multiplicities)

        local_element_idx_range = [
            self.num_elements,
            self.num_elements + len(element_inp_data_idx),
        ]

        element_set._element_local_idx_range = local_element_idx_range
        self.template._add_element_set(element_set)

        output_data_idx = self.template._prepare_persistent_outputs(
            workflow=self.workflow,
            local_element_idx_range=local_element_idx_range,
        )

        (element_data_idx, element_seq_idx, element_src_idx) = self.generate_new_elements(
            input_data_idx,
            output_data_idx,
            element_inp_data_idx,
            seq_idx,
            src_idx,
        )

        iter_IDs: list[int] = []
        elem_IDs: list[int] = []
        for elem_idx, data_idx in enumerate(element_data_idx):
            schema_params = set(i for i in data_idx if len(i.split(".")) == 2)
            elem_ID_i = self.workflow._store.add_element(
                task_ID=self.insert_ID,
                es_idx=self.num_element_sets - 1,
                seq_idx={k: v[elem_idx] for k, v in element_seq_idx.items()},
                src_idx={k: v[elem_idx] for k, v in element_src_idx.items() if v != -1},
            )
            iter_ID_i = self.workflow._store.add_element_iteration(
                element_ID=elem_ID_i,
                data_idx=data_idx,
                schema_parameters=list(schema_params),
            )
            iter_IDs.append(iter_ID_i)
            elem_IDs.append(elem_ID_i)

        self._pending_element_IDs += elem_IDs
        self.initialise_EARs()

        return iter_IDs

    @overload
    def add_elements(
        self,
        *,
        base_element: Element | None = None,
        inputs: list[InputValue] | dict[str, Any] | None = None,
        input_files: list[InputFile] | None = None,
        sequences: list[ValueSequence] | None = None,
        resources: Resources = None,
        repeats: list[RepeatsDescriptor] | int | None = None,
        input_sources: dict[str, list[InputSource]] | None = None,
        nesting_order: dict[str, float] | None = None,
        element_sets: list[ElementSet] | None = None,
        sourceable_elem_iters: list[int] | None = None,
        propagate_to: (
            list[ElementPropagation]
            | Mapping[str, ElementPropagation | Mapping[str, Any]]
            | None
        ) = None,
        return_indices: Literal[True],
    ) -> list[int]: ...

    @overload
    def add_elements(
        self,
        *,
        base_element: Element | None = None,
        inputs: list[InputValue] | dict[str, Any] | None = None,
        input_files: list[InputFile] | None = None,
        sequences: list[ValueSequence] | None = None,
        resources: Resources = None,
        repeats: list[RepeatsDescriptor] | int | None = None,
        input_sources: dict[str, list[InputSource]] | None = None,
        nesting_order: dict[str, float] | None = None,
        element_sets: list[ElementSet] | None = None,
        sourceable_elem_iters: list[int] | None = None,
        propagate_to: (
            list[ElementPropagation]
            | Mapping[str, ElementPropagation | Mapping[str, Any]]
            | None
        ) = None,
        return_indices: Literal[False] = False,
    ) -> None: ...

    def add_elements(
        self,
        *,
        base_element: Element | None = None,
        inputs: list[InputValue] | dict[str, Any] | None = None,
        input_files: list[InputFile] | None = None,
        sequences: list[ValueSequence] | None = None,
        resources: Resources = None,
        repeats: list[RepeatsDescriptor] | int | None = None,
        input_sources: dict[str, list[InputSource]] | None = None,
        nesting_order: dict[str, float] | None = None,
        element_sets: list[ElementSet] | None = None,
        sourceable_elem_iters: list[int] | None = None,
        propagate_to: (
            list[ElementPropagation]
            | Mapping[str, ElementPropagation | Mapping[str, Any]]
            | None
        ) = None,
        return_indices=False,
    ) -> list[int] | None:
        """
        Add elements to this task.

        Parameters
        ----------
        sourceable_elem_iters : list of int, optional
            If specified, a list of global element iteration indices from which inputs
            may be sourced. If not specified, all workflow element iterations are
            considered sourceable.
        propagate_to : dict[str, ElementPropagation]
            Propagate the new elements downstream to the specified tasks.
        return_indices : bool
            If True, return the list of indices of the newly added elements. False by
            default.

        """
        real_propagate_to = self._app.ElementPropagation._prepare_propagate_to_dict(
            propagate_to, self.workflow
        )
        with self.workflow.batch_update():
            indices = self._add_elements(
                base_element=base_element,
                inputs=inputs,
                input_files=input_files,
                sequences=sequences,
                resources=resources,
                repeats=repeats,
                input_sources=input_sources,
                nesting_order=nesting_order,
                element_sets=element_sets,
                sourceable_elem_iters=sourceable_elem_iters,
                propagate_to=real_propagate_to,
            )
        return indices if return_indices else None

    @TimeIt.decorator
    def _add_elements(
        self,
        *,
        base_element: Element | None = None,
        inputs: list[InputValue] | dict[str, Any] | None = None,
        input_files: list[InputFile] | None = None,
        sequences: list[ValueSequence] | None = None,
        resources: Resources = None,
        repeats: list[RepeatsDescriptor] | int | None = None,
        input_sources: dict[str, list[InputSource]] | None = None,
        nesting_order: dict[str, float] | None = None,
        element_sets: list[ElementSet] | None = None,
        sourceable_elem_iters: list[int] | None = None,
        propagate_to: dict[str, ElementPropagation],
    ) -> list[int] | None:
        """Add more elements to this task.

        Parameters
        ----------
        sourceable_elem_iters : list[int]
            If specified, a list of global element iteration indices from which inputs
            may be sourced. If not specified, all workflow element iterations are
            considered sourceable.
        propagate_to : dict[str, ElementPropagation]
            Propagate the new elements downstream to the specified tasks.
        """

        if base_element is not None:
            if base_element.task is not self:
                raise ValueError("If specified, `base_element` must belong to this task.")
            b_inputs, b_resources = base_element.to_element_set_data()
            inputs = inputs or b_inputs
            resources = resources or b_resources

        element_sets = self._app.ElementSet.ensure_element_sets(
            inputs=inputs,
            input_files=input_files,
            sequences=sequences,
            resources=resources,
            repeats=repeats,
            input_sources=input_sources,
            nesting_order=nesting_order,
            element_sets=element_sets,
            sourceable_elem_iters=sourceable_elem_iters,
        )

        elem_idx: list[int] = []
        for elem_set_i in element_sets:
            # copy and add the new element set:
            elem_idx.extend(self._add_element_set(elem_set_i.prepare_persistent_copy()))

        if not propagate_to:
            return elem_idx

        for task in self.get_dependent_tasks(as_objects=True):
            if (elem_prop := propagate_to.get(task.unique_name)) is None:
                continue

            if all(
                self.unique_name != task.unique_name
                for task in elem_prop.element_set.get_task_dependencies(as_objects=True)
            ):
                # TODO: why can't we just do
                #  `if self in not elem_propagate.element_set.task_dependencies:`?
                continue

            # TODO: generate a new ElementSet for this task;
            #       Assume for now we use a single base element set.
            #       Later, allow combining multiple element sets.
            src_elem_iters = elem_idx + [
                j for el_set in element_sets for j in el_set.sourceable_elem_iters or ()
            ]

            # note we must pass `resources` as a list since it is already persistent:
            elem_set_i = self._app.ElementSet(
                inputs=elem_prop.element_set.inputs,
                input_files=elem_prop.element_set.input_files,
                sequences=elem_prop.element_set.sequences,
                resources=elem_prop.element_set.resources[:],
                repeats=elem_prop.element_set.repeats,
                nesting_order=elem_prop.nesting_order,
                input_sources=elem_prop.input_sources,
                sourceable_elem_iters=src_elem_iters,
            )

            del propagate_to[task.unique_name]
            prop_elem_idx = task._add_elements(
                element_sets=[elem_set_i],
                propagate_to=propagate_to,
            )
            elem_idx.extend(prop_elem_idx or ())

        return elem_idx

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

    def get_element_dependencies(
        self,
        as_objects: bool = False,
    ) -> set[int] | list[Element]:
        """Get elements from upstream tasks that this task depends on."""

        deps: set[int] = set()
        for element in self.elements:
            for iter_i in element.iterations:
                deps.update(
                    dep_elem_i.id_
                    for dep_elem_i in iter_i.get_element_dependencies(as_objects=True)
                    if dep_elem_i.task.insert_ID != self.insert_ID
                )

        if as_objects:
            return self.workflow.get_elements_from_IDs(sorted(deps))
        return deps

    @overload
    def get_task_dependencies(self, as_objects: Literal[False] = False) -> set[int]: ...

    @overload
    def get_task_dependencies(self, as_objects: Literal[True]) -> list[WorkflowTask]: ...

    def get_task_dependencies(
        self,
        as_objects: bool = False,
    ) -> set[int] | list[WorkflowTask]:
        """Get tasks (insert ID or WorkflowTask objects) that this task depends on.

        Dependencies may come from either elements from upstream tasks, or from locally
        defined inputs/sequences/defaults from upstream tasks."""

        # TODO: this method might become insufficient if/when we start considering a
        # new "task_iteration" input source type, which may take precedence over any
        # other input source types.

        deps: set[int] = set()
        for element_set in self.template.element_sets:
            for sources in element_set.input_sources.values():
                deps.update(
                    src.task_ref
                    for src in sources
                    if (
                        src.source_type is InputSourceType.TASK
                        and src.task_ref is not None
                    )
                )

        if as_objects:
            return [self.workflow.tasks.get(insert_ID=id_) for id_ in sorted(deps)]
        return deps

    @overload
    def get_dependent_elements(
        self,
        as_objects: Literal[False] = False,
    ) -> set[int]: ...

    @overload
    def get_dependent_elements(self, as_objects: Literal[True]) -> list[Element]: ...

    def get_dependent_elements(
        self,
        as_objects: bool = False,
    ) -> set[int] | list[Element]:
        """Get elements from downstream tasks that depend on this task."""
        deps: set[int] = set()
        for task in self.downstream_tasks:
            deps.update(
                element.id_
                for element in task.elements
                if any(
                    self.insert_ID in iter_i.get_task_dependencies()
                    for iter_i in element.iterations
                )
            )

        if as_objects:
            return self.workflow.get_elements_from_IDs(sorted(deps))
        return deps

    @overload
    def get_dependent_tasks(self, as_objects: Literal[False] = False) -> set[int]: ...

    @overload
    def get_dependent_tasks(self, as_objects: Literal[True]) -> list[WorkflowTask]: ...

    @TimeIt.decorator
    def get_dependent_tasks(
        self,
        as_objects: bool = False,
    ) -> set[int] | list[WorkflowTask]:
        """Get tasks (insert ID or WorkflowTask objects) that depends on this task."""

        # TODO: this method might become insufficient if/when we start considering a
        # new "task_iteration" input source type, which may take precedence over any
        # other input source types.

        deps: set[int] = set()
        for task in self.downstream_tasks:
            if task.insert_ID not in deps and any(
                src.source_type is InputSourceType.TASK and src.task_ref == self.insert_ID
                for element_set in task.template.element_sets
                for sources in element_set.input_sources.values()
                for src in sources
            ):
                deps.add(task.insert_ID)
        if as_objects:
            return [self.workflow.tasks.get(insert_ID=id_) for id_ in sorted(deps)]
        return deps

    @property
    def inputs(self) -> TaskInputParameters:
        """
        Inputs to this task.
        """
        return self._app.TaskInputParameters(self)

    @property
    def outputs(self) -> TaskOutputParameters:
        """
        Outputs from this task.
        """
        return self._app.TaskOutputParameters(self)

    def get(
        self, path: str, *, raise_on_missing=False, default: Any | None = None
    ) -> Parameters:
        """
        Get a parameter known to this task by its path.
        """
        return self._app.Parameters(
            self,
            path=path,
            return_element_parameters=False,
            raise_on_missing=raise_on_missing,
            default=default,
        )

    def _paths_to_PV_classes(self, *paths: str | None) -> dict[str, type[ParameterValue]]:
        """Return a dict mapping dot-delimited string input paths to `ParameterValue`
        classes."""

        params: dict[str, type[ParameterValue]] = {}
        for path in paths:
            if not path:
                # Skip None/empty
                continue
            path_split = path.split(".")
            if len(path_split) == 1 or path_split[0] not in ("inputs", "outputs"):
                continue

            # top-level parameter can be found via the task schema:
            key_0 = ".".join(path_split[:2])

            if key_0 not in params:
                if path_split[0] == "inputs":
                    path_1, _ = split_param_label(
                        path_split[1]
                    )  # remove label if present
                    for schema in self.template.schemas:
                        for inp in schema.inputs:
                            if inp.parameter.typ == path_1 and inp.parameter._value_class:
                                params[key_0] = inp.parameter._value_class

                elif path_split[0] == "outputs":
                    for schema in self.template.schemas:
                        for out in schema.outputs:
                            if (
                                out.parameter.typ == path_split[1]
                                and out.parameter._value_class
                            ):
                                params[key_0] = out.parameter._value_class

            if path_split[2:]:
                pv_classes = {cls._typ: cls for cls in ParameterValue.__subclasses__()}

            # now proceed by searching for sub-parameters in each ParameterValue
            # sub-class:
            for idx, part_i in enumerate(path_split[2:], start=2):
                parent = path_split[:idx]  # e.g. ["inputs", "p1"]
                child = path_split[: idx + 1]  # e.g. ["inputs", "p1", "sub_param"]
                key_i = ".".join(child)
                if key_i in params:
                    continue
                if parent_param := params.get(".".join(parent)):
                    for attr_name, sub_type in parent_param._sub_parameters.items():
                        if part_i == attr_name:
                            # find the class with this `typ` attribute:
                            if cls := pv_classes.get(sub_type):
                                params[key_i] = cls

        return params

    @staticmethod
    def _get_relevant_paths(
        data_index: Mapping[str, Any], path: list[str], children_of: str | None = None
    ) -> Mapping[str, RelevantPath]:
        relevant_paths: dict[str, RelevantPath] = {}
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

    def __get_relevant_data_item(
        self,
        path: str | None,
        path_i: str,
        data_idx_ij: int,
        raise_on_unset: bool,
        len_dat_idx: int = 1,
    ) -> tuple[Any, bool, str | None]:
        if path_i.startswith("repeats."):
            # data is an integer repeats index, rather than a parameter ID:
            return data_idx_ij, True, None

        meth_i: str | None = None
        data_j: Any
        param_j = self.workflow.get_parameter(data_idx_ij)
        is_set_i = param_j.is_set
        if param_j.file:
            if param_j.file["store_contents"]:
                file_j = Path(self.workflow.path) / param_j.file["path"]
            else:
                file_j = Path(param_j.file["path"])
            data_j = file_j.as_posix()
        else:
            meth_i = param_j.source.get("value_class_method")
            if param_j.is_pending:
                # if pending, we need to convert `ParameterValue` objects
                # to their dict representation, so they can be merged with
                # other data:
                try:
                    data_j = cast("ParameterValue", param_j.data).to_dict()
                except AttributeError:
                    data_j = param_j.data
            else:
                # if not pending, data will be the result of an encode-
                # decode cycle, and it will not be initialised as an
                # object if the parameter is associated with a
                # `ParameterValue` class.
                data_j = param_j.data
        if raise_on_unset and not is_set_i:
            raise UnsetParameterDataError(path, path_i)
        if not is_set_i and self.workflow._is_tracking_unset:
            src_run_id = param_j.source.get("EAR_ID")
            unset_trackers = self.workflow._tracked_unset
            assert src_run_id is not None
            assert unset_trackers is not None
            unset_trackers[path_i].run_ids.add(src_run_id)
            unset_trackers[path_i].group_size = len_dat_idx
        return data_j, is_set_i, meth_i

    def __get_relevant_data(
        self,
        relevant_data_idx: Mapping[str, list[int] | int],
        raise_on_unset: bool,
        path: str | None,
    ) -> Mapping[str, RelevantData]:
        relevant_data: dict[str, RelevantData] = {}
        for path_i, data_idx_i in relevant_data_idx.items():
            if not isinstance(data_idx_i, list):
                data, is_set, meth = self.__get_relevant_data_item(
                    path, path_i, data_idx_i, raise_on_unset
                )
                relevant_data[path_i] = {
                    "data": data,
                    "value_class_method": meth,
                    "is_set": is_set,
                    "is_multi": False,
                }
                continue

            data_i: list[Any] = []
            methods_i: list[str | None] = []
            is_param_set_i: list[bool] = []
            for data_idx_ij in data_idx_i:
                data_j, is_set_i, meth_i = self.__get_relevant_data_item(
                    path, path_i, data_idx_ij, raise_on_unset, len_dat_idx=len(data_idx_i)
                )
                data_i.append(data_j)
                methods_i.append(meth_i)
                is_param_set_i.append(is_set_i)

            relevant_data[path_i] = {
                "data": data_i,
                "value_class_method": methods_i,
                "is_set": is_param_set_i,
                "is_multi": True,
            }

        if not raise_on_unset:
            to_remove: set[str] = set()
            for key, dat_info in relevant_data.items():
                if not dat_info["is_set"] and (not path or path in key):
                    # remove sub-paths, as they cannot be merged with this parent
                    prefix = f"{key}."
                    to_remove.update(k for k in relevant_data if k.startswith(prefix))
            for key in to_remove:
                relevant_data.pop(key, None)

        return relevant_data

    @classmethod
    def __merge_relevant_data(
        cls,
        relevant_data: Mapping[str, RelevantData],
        relevant_paths: Mapping[str, RelevantPath],
        PV_classes,
        path: str | None,
        raise_on_missing: bool,
    ):
        current_val: list | dict | Any | None = None
        assigned_from_parent = False
        val_cls_method: str | None | list[str | None] = None
        path_is_multi = False
        path_is_set: bool | list[bool] = False
        all_multi_len: int | None = None
        for path_i, data_info_i in relevant_data.items():
            data_i = data_info_i["data"]
            if path_i == path:
                val_cls_method = data_info_i["value_class_method"]
                path_is_multi = data_info_i["is_multi"]
                path_is_set = data_info_i["is_set"]

            if data_info_i["is_multi"]:
                if all_multi_len:
                    if len(data_i) != all_multi_len:
                        raise RuntimeError(
                            "Cannot merge group values of different lengths."
                        )
                else:
                    # keep track of group lengths, only merge equal-length groups;
                    all_multi_len = len(data_i)

            path_info = relevant_paths[path_i]
            if path_info["type"] == "parent":
                try:
                    if data_info_i["is_multi"]:
                        current_val = [
                            get_in_container(
                                item,
                                path_info["relative_path"],
                                cast_indices=True,
                            )
                            for item in data_i
                        ]
                        path_is_multi = True
                        path_is_set = data_info_i["is_set"]
                        val_cls_method = data_info_i["value_class_method"]
                    else:
                        current_val = get_in_container(
                            data_i,
                            path_info["relative_path"],
                            cast_indices=True,
                        )
                except ContainerKeyError as err:
                    if path_i in PV_classes:
                        raise MayNeedObjectError(path=".".join([path_i, *err.path[:-1]]))
                    continue
                except (IndexError, ValueError) as err:
                    if raise_on_missing:
                        raise err
                    continue
                else:
                    assigned_from_parent = True
            elif path_info["type"] == "update":
                current_val = current_val or {}
                if all_multi_len:
                    if len(path_i.split(".")) == 2:
                        # groups can only be "created" at the parameter level
                        set_in_container(
                            cont=current_val,
                            path=path_info["update_path"],
                            value=data_i,
                            ensure_path=True,
                            cast_indices=True,
                        )
                    else:
                        # update group
                        update_path = path_info["update_path"]
                        if len(update_path) > 1:
                            for idx, j in enumerate(data_i):
                                set_in_container(
                                    cont=current_val,
                                    path=[*update_path[:1], idx, *update_path[1:]],
                                    value=j,
                                    ensure_path=True,
                                    cast_indices=True,
                                )
                        else:
                            for i, j in zip(current_val, data_i):
                                set_in_container(
                                    cont=i,
                                    path=update_path,
                                    value=j,
                                    ensure_path=True,
                                    cast_indices=True,
                                )

                else:
                    set_in_container(
                        current_val,
                        path_info["update_path"],
                        data_i,
                        ensure_path=True,
                        cast_indices=True,
                    )
        if path in PV_classes:
            if path not in relevant_data:
                # requested data must be a sub-path of relevant data, so we can assume
                # path is set (if the parent was not set the sub-paths would be
                # removed in `__get_relevant_data`):
                path_is_set = path_is_set or True

                if not assigned_from_parent:
                    # search for unset parents in `relevant_data`:
                    assert path is not None
                    for parent_i_span in range(
                        len(path_split := path.split(".")) - 1, 1, -1
                    ):
                        parent_path_i = ".".join(path_split[:parent_i_span])
                        if not (relevant_par := relevant_data.get(parent_path_i)):
                            continue
                        if not (par_is_set := relevant_par["is_set"]) or not all(
                            cast("list", par_is_set)
                        ):
                            val_cls_method = relevant_par["value_class_method"]
                            path_is_multi = relevant_par["is_multi"]
                            path_is_set = relevant_par["is_set"]
                            current_val = relevant_par["data"]
                            break

            # initialise objects
            PV_cls = PV_classes[path]
            if path_is_multi:
                current_val = [
                    (
                        cls.__map_parameter_value(PV_cls, meth_i, val_i)
                        if set_i and isinstance(val_i, dict)
                        else None
                    )
                    for set_i, meth_i, val_i in zip(
                        cast("list[bool]", path_is_set),
                        cast("list[str|None]", val_cls_method),
                        cast("list[Any]", current_val),
                    )
                ]
            elif path_is_set and isinstance(current_val, dict):
                assert not isinstance(val_cls_method, list)
                current_val = cls.__map_parameter_value(
                    PV_cls, val_cls_method, current_val
                )

        return current_val, all_multi_len

    @staticmethod
    def __map_parameter_value(
        PV_cls: type[ParameterValue], meth: str | None, val: dict
    ) -> Any | ParameterValue:
        if meth:
            method: Callable = getattr(PV_cls, meth)
            return method(**val)
        else:
            return PV_cls(**val)

    @TimeIt.decorator
    def _get_merged_parameter_data(
        self,
        data_index: Mapping[str, list[int] | int],
        path: str | None = None,
        *,
        raise_on_missing: bool = False,
        raise_on_unset: bool = False,
        default: Any | None = None,
    ):
        """Get element data from the persistent store."""
        path_split = [] if not path else path.split(".")

        if not (relevant_paths := self._get_relevant_paths(data_index, path_split)):
            if raise_on_missing:
                # TODO: custom exception?
                raise ValueError(f"Path {path!r} does not exist in the element data.")
            return default

        relevant_data_idx = {k: v for k, v in data_index.items() if k in relevant_paths}

        cache = self.workflow._merged_parameters_cache
        use_cache = (
            self.workflow._use_merged_parameters_cache
            and raise_on_missing is False
            and raise_on_unset is False
            and default is None  # cannot cache on default value, may not be hashable
        )
        add_to_cache = False
        if use_cache:
            # generate the key:
            dat_idx_cache: list[tuple[str, tuple[int, ...] | int]] = []
            for k, v in sorted(relevant_data_idx.items()):
                dat_idx_cache.append((k, tuple(v) if isinstance(v, list) else v))
            cache_key = (path, tuple(dat_idx_cache))

            # check for cache hit:
            if cache_key in cache:
                self._app.logger.debug(
                    f"_get_merged_parameter_data: cache hit with key: {cache_key}"
                )
                return cache[cache_key]
            else:
                add_to_cache = True

        PV_classes = self._paths_to_PV_classes(*relevant_paths, path)
        relevant_data = self.__get_relevant_data(relevant_data_idx, raise_on_unset, path)

        current_val = None
        is_assigned = False
        try:
            current_val, _ = self.__merge_relevant_data(
                relevant_data, relevant_paths, PV_classes, path, raise_on_missing
            )
        except MayNeedObjectError as err:
            path_to_init = err.path
            path_to_init_split = path_to_init.split(".")
            relevant_paths = self._get_relevant_paths(data_index, path_to_init_split)
            PV_classes = self._paths_to_PV_classes(*relevant_paths, path_to_init)
            relevant_data_idx = {
                k: v for k, v in data_index.items() if k in relevant_paths
            }
            relevant_data = self.__get_relevant_data(
                relevant_data_idx, raise_on_unset, path
            )
            # merge the parent data
            current_val, group_len = self.__merge_relevant_data(
                relevant_data, relevant_paths, PV_classes, path_to_init, raise_on_missing
            )
            # try to retrieve attributes via the initialised object:
            rel_path_split = get_relative_path(path_split, path_to_init_split)
            try:
                if group_len:
                    current_val = [
                        get_in_container(
                            cont=item,
                            path=rel_path_split,
                            cast_indices=True,
                            allow_getattr=True,
                        )
                        for item in current_val
                    ]
                else:
                    current_val = get_in_container(
                        cont=current_val,
                        path=rel_path_split,
                        cast_indices=True,
                        allow_getattr=True,
                    )
            except (KeyError, IndexError, ValueError):
                pass
            else:
                is_assigned = True

        except (KeyError, IndexError, ValueError):
            pass
        else:
            is_assigned = True

        if not is_assigned:
            if raise_on_missing:
                # TODO: custom exception?
                raise ValueError(f"Path {path!r} does not exist in the element data.")
            current_val = default

        if add_to_cache:
            self._app.logger.debug(
                f"_get_merged_parameter_data: adding to cache with key: {cache_key!r}"
            )
            # tuple[str | None, tuple[tuple[str, tuple[int, ...] | int], ...]]
            # tuple[str | None, tuple[tuple[str, tuple[int, ...] | int], ...]] | None
            cache[cache_key] = current_val

        return current_val


class Elements:
    """
    The elements of a task. Iterable.

    Parameters
    ----------
    task:
        The task this will be the elements of.
    """

    __slots__ = ("_task",)

    def __init__(self, task: WorkflowTask):
        self._task = task

        # TODO: cache Element objects

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(task={self.task.unique_name!r}, "
            f"num_elements={self.task.num_elements})"
        )

    @property
    def task(self) -> WorkflowTask:
        """
        The task this is the elements of.
        """
        return self._task

    @TimeIt.decorator
    def __get_selection(self, selection: int | slice | list[int]) -> list[int]:
        """Normalise an element selection into a list of element indices."""
        if isinstance(selection, int):
            return [selection]

        elif isinstance(selection, slice):
            return list(range(*selection.indices(self.task.num_elements)))

        elif isinstance(selection, list):
            return selection
        else:
            raise RuntimeError(
                f"{self.__class__.__name__} selection must be an `int`, `slice` object, "
                f"or list of `int`s, but received type {type(selection)}."
            )

    def __len__(self) -> int:
        return self.task.num_elements

    def __iter__(self) -> Iterator[Element]:
        yield from self.task.workflow.get_task_elements(self.task)

    @overload
    def __getitem__(
        self,
        selection: int,
    ) -> Element: ...

    @overload
    def __getitem__(
        self,
        selection: slice | list[int],
    ) -> list[Element]: ...

    @TimeIt.decorator
    def __getitem__(
        self,
        selection: int | slice | list[int],
    ) -> Element | list[Element]:
        elements = self.task.workflow.get_task_elements(
            self.task, self.__get_selection(selection)
        )

        if isinstance(selection, int):
            return elements[0]
        else:
            return elements


@dataclass
@hydrate
class Parameters(AppAware):
    """
    The parameters of a (workflow-bound) task. Iterable.

    Parameters
    ----------
    task: WorkflowTask
        The task these are the parameters of.
    path: str
        The path to the parameter or parameters.
    return_element_parameters: bool
        Whether to return element parameters.
    raise_on_missing: bool
        Whether to raise an exception on a missing parameter.
    raise_on_unset: bool
        Whether to raise an exception on an unset parameter.
    default:
        A default value to use when the parameter is absent.
    """

    #: The task these are the parameters of.
    task: WorkflowTask
    #: The path to the parameter or parameters.
    path: str
    #: Whether to return element parameters.
    return_element_parameters: bool
    #: Whether to raise an exception on a missing parameter.
    raise_on_missing: bool = False
    #: Whether to raise an exception on an unset parameter.
    raise_on_unset: bool = False
    #: A default value to use when the parameter is absent.
    default: Any | None = None

    @TimeIt.decorator
    def __get_selection(
        self, selection: int | slice | list[int] | tuple[int, ...]
    ) -> list[int]:
        """Normalise an element selection into a list of element indices."""
        if isinstance(selection, int):
            return [selection]
        elif isinstance(selection, slice):
            return list(range(*selection.indices(self.task.num_elements)))
        elif isinstance(selection, list):
            return selection
        elif isinstance(selection, tuple):
            return list(selection)
        else:
            raise RuntimeError(
                f"{self.__class__.__name__} selection must be an `int`, `slice` object, "
                f"or list of `int`s, but received type {type(selection)}."
            )

    def __iter__(self) -> Iterator[Any | ElementParameter]:
        yield from self.__getitem__(slice(None))

    @overload
    def __getitem__(self, selection: int) -> Any | ElementParameter: ...

    @overload
    def __getitem__(
        self, selection: slice | list[int]
    ) -> list[Any | ElementParameter]: ...

    def __getitem__(
        self,
        selection: int | slice | list[int],
    ) -> Any | ElementParameter | list[Any | ElementParameter]:
        idx_lst = self.__get_selection(selection)
        elements = self.task.workflow.get_task_elements(self.task, idx_lst)
        if self.return_element_parameters:
            params = (
                self._app.ElementParameter(
                    task=self.task,
                    path=self.path,
                    parent=elem,
                    element=elem,
                )
                for elem in elements
            )
        else:
            params = (
                elem.get(
                    path=self.path,
                    raise_on_missing=self.raise_on_missing,
                    raise_on_unset=self.raise_on_unset,
                    default=self.default,
                )
                for elem in elements
            )

        if isinstance(selection, int):
            return next(iter(params))
        else:
            return list(params)


@dataclass
@hydrate
class TaskInputParameters(AppAware):
    """
    For retrieving schema input parameters across all elements.
    Treat as an unmodifiable namespace.

    Parameters
    ----------
    task:
        The task that this represents the input parameters of.
    """

    #: The task that this represents the input parameters of.
    task: WorkflowTask
    __input_names: frozenset[str] | None = field(default=None, init=False, compare=False)

    def __getattr__(self, name: str) -> Parameters:
        if name not in self.__get_input_names():
            raise ValueError(f"No input named {name!r}.")
        return self._app.Parameters(self.task, f"inputs.{name}", True)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"{', '.join(f'{name!r}' for name in sorted(self.__get_input_names()))})"
        )

    def __dir__(self) -> Iterator[str]:
        yield from super().__dir__()
        yield from sorted(self.__get_input_names())

    def __get_input_names(self) -> frozenset[str]:
        if self.__input_names is None:
            self.__input_names = frozenset(self.task.template.all_schema_input_types)
        return self.__input_names


@dataclass
@hydrate
class TaskOutputParameters(AppAware):
    """
    For retrieving schema output parameters across all elements.
    Treat as an unmodifiable namespace.

    Parameters
    ----------
    task:
        The task that this represents the output parameters of.
    """

    #: The task that this represents the output parameters of.
    task: WorkflowTask
    __output_names: frozenset[str] | None = field(default=None, init=False, compare=False)

    def __getattr__(self, name: str) -> Parameters:
        if name not in self.__get_output_names():
            raise ValueError(f"No output named {name!r}.")
        return self._app.Parameters(self.task, f"outputs.{name}", True)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"{', '.join(map(repr, sorted(self.__get_output_names())))})"
        )

    def __dir__(self) -> Iterator[str]:
        yield from super().__dir__()
        yield from sorted(self.__get_output_names())

    def __get_output_names(self) -> frozenset[str]:
        if self.__output_names is None:
            self.__output_names = frozenset(self.task.template.all_schema_output_types)
        return self.__output_names


@dataclass
@hydrate
class ElementPropagation(AppAware):
    """
    Class to represent how a newly added element set should propagate to a given
    downstream task.

    Parameters
    ----------
    task:
        The task this is propagating to.
    nesting_order:
        The nesting order information.
    input_sources:
        The input source information.
    """

    #: The task this is propagating to.
    task: WorkflowTask
    #: The nesting order information.
    nesting_order: dict[str, float] | None = None
    #: The input source information.
    input_sources: dict[str, list[InputSource]] | None = None

    @property
    def element_set(self) -> ElementSet:
        """
        The element set that this propagates from.

        Note
        ----
        Temporary property. May be moved or reinterpreted.
        """
        # TEMP property; for now just use the first element set as the base:
        return self.task.template.element_sets[0]

    def __deepcopy__(self, memo: dict[int, Any] | None) -> Self:
        return self.__class__(
            task=self.task,
            nesting_order=copy.copy(self.nesting_order),
            input_sources=copy.deepcopy(self.input_sources, memo),
        )

    @classmethod
    def _prepare_propagate_to_dict(
        cls,
        propagate_to: (
            list[ElementPropagation]
            | Mapping[str, ElementPropagation | Mapping[str, Any]]
            | None
        ),
        workflow: Workflow,
    ) -> dict[str, ElementPropagation]:
        if not propagate_to:
            return {}
        propagate_to = copy.deepcopy(propagate_to)
        if isinstance(propagate_to, list):
            return {prop.task.unique_name: prop for prop in propagate_to}

        return {
            k: (
                v
                if isinstance(v, ElementPropagation)
                else cls(task=workflow.tasks.get(unique_name=k), **v)
            )
            for k, v in propagate_to.items()
        }


#: A task used as a template for other tasks.
TaskTemplate: TypeAlias = Task


class MetaTask(JSONLike):
    def __init__(self, schema: MetaTaskSchema, tasks: Sequence[Task]):
        self.schema = schema
        self.tasks = tasks

        # TODO: validate schema's inputs and outputs are inputs and outputs of `tasks`
        # schemas
