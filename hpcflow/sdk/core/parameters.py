"""
Parameters represent information passed around within a workflow.
"""

from __future__ import annotations
from collections.abc import Sequence
import copy
from dataclasses import dataclass, field
from datetime import timedelta
import enum
from pathlib import Path
import re
from typing import TypeVar, cast, TYPE_CHECKING
from typing_extensions import override, TypeIs

import numpy as np
from scipy.stats.qmc import LatinHypercube, scale
from valida import Schema as ValidaSchema  # type: ignore

from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.enums import (
    InputSourceType,
    ParallelMode,
    ParameterPropagationMode,
    TaskSourceType,
)
from hpcflow.sdk.core.errors import (
    MalformedParameterPathError,
    UnknownResourceSpecItemError,
    WorkflowParameterMissingError,
)
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.utils import (
    check_valid_py_identifier,
    get_enum_by_name_or_val,
    linspace_rect,
    process_string_nodes,
    split_param_label,
    timedelta_format,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping
    from typing import Any, ClassVar, Literal
    from typing_extensions import Self, TypeAlias
    from h5py import Group as HDF5Group  # type: ignore
    from numpy.typing import NDArray
    from ..app import BaseApp
    from ..typing import ParamSource
    from .actions import ActionScope
    from .element import ElementFilter
    from .object_list import ResourceList
    from .rule import Rule
    from .task import ElementSet, TaskSchema, TaskTemplate, WorkflowTask
    from .types import (
        Address,
        Numeric,
        LabelInfo,
        LabellingDescriptor,
        ResourcePersistingWorkflow,
        RuleArgs,
        SchemaInputKwargs,
    )
    from .workflow import Workflow, WorkflowTemplate
    from .validation import Schema


T = TypeVar("T")


def _process_demo_data_strings(app: BaseApp, value: T) -> T:
    demo_pattern = re.compile(r"\<\<demo_data_file:(.*)\>\>")

    def string_processor(str_in: str) -> str:
        str_out = demo_pattern.sub(
            repl=lambda x: str(app.get_demo_data_file_path(x[1])),
            string=str_in,
        )
        return str_out

    return process_string_nodes(value, string_processor)


@dataclass
@hydrate
class ParameterValue:
    """
    The value handler for a parameter.

    Intended to be subclassed.
    """

    _typ: ClassVar[str | None] = None
    _sub_parameters: ClassVar[dict[str, str]] = {}

    def to_dict(self) -> dict[str, Any]:
        """
        Serialise this parameter value as a dictionary.
        """
        if hasattr(self, "__dict__"):
            return self._postprocess_to_dict(dict(self.__dict__))
        elif hasattr(self, "__slots__"):
            return self._postprocess_to_dict(
                {k: getattr(self, k) for k in self.__slots__}
            )
        else:
            raise NotImplementedError

    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        """Postprocess the results of :meth:`to_dict`."""
        return d

    def prepare_JSON_dump(self) -> dict[str, Any]:
        """
        Prepare this parameter value for serialisation as JSON.
        """
        raise NotImplementedError

    def dump_to_HDF5_group(self, group: HDF5Group):
        """
        Write this parameter value to an HDF5 group.
        """
        raise NotImplementedError

    @classmethod
    def dump_element_group_to_HDF5_group(cls, objs: list[Self], group: HDF5Group):
        """
        Write a list (from an element group) of parameter values to an HDF5 group.
        """
        raise NotImplementedError

    @classmethod
    def save_from_HDF5_group(cls, group: HDF5Group, param_id: int, workflow: Workflow):
        """
        Extract a parameter value from an HDF5 group.
        """
        raise NotImplementedError

    @classmethod
    def save_from_JSON(cls, data, param_id: int | list[int], workflow: Workflow):
        """
        Extract a parameter value from JSON data.
        """
        raise NotImplementedError


@dataclass
class ParameterPath(JSONLike):
    """
    Path to a parameter.
    """

    # TODO: unused?
    #: The path to the parameter.
    path: Sequence[str | int | float]
    #: The task in which to look up the parameter.
    task: TaskTemplate | TaskSchema | None = None  # default is "current" task


@dataclass
@hydrate
class Parameter(JSONLike):
    """
    A general parameter to a workflow task.

    Parameters
    ----------
    typ:
        Type code.
        Used to look up the :py:class:`ParameterValue` for this parameter,
        if any.
    is_file:
        Whether this parameter represents a file.
    sub_parameters: list[SubParameter]
        Any parameters packed within this one.
    _value_class: type[ParameterValue]
        Class that provides the implementation of this parameter's values.
        Not normally directly user-managed.
    _hash_value:
        Hash of this class. Not normally user-managed.
    _validation:
        Validation schema.
    """

    _validation_schema: ClassVar[str] = "parameters_spec_schema.yaml"
    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="typ",
            json_like_name="type",
        ),
        ChildObjectSpec(
            name="_validation",
            class_obj=ValidaSchema,
        ),
    )

    #: Type code. Used to look up the :py:class:`ParameterValue` for this parameter,
    #: if any.
    typ: str
    #: Whether this parameter represents a file.
    is_file: bool = False
    #: Any parameters packed within this one.
    sub_parameters: list[SubParameter] = field(default_factory=list)
    _value_class: type[ParameterValue] | None = None
    _hash_value: str | None = field(default=None, repr=False)
    _validation: Schema | None = None

    def __repr__(self) -> str:
        is_file_str = ""
        if self.is_file:
            is_file_str = f", is_file={self.is_file!r}"

        sub_parameters_str = ""
        if self.sub_parameters:
            sub_parameters_str = f", sub_parameters={self.sub_parameters!r}"

        _value_class_str = ""
        if self._value_class is not None:
            _value_class_str = f", _value_class={self._value_class!r}"

        return (
            f"{self.__class__.__name__}("
            f"typ={self.typ!r}{is_file_str}{sub_parameters_str}{_value_class_str}"
            f")"
        )

    def __post_init__(self) -> None:
        self.typ = check_valid_py_identifier(self.typ)
        self._set_value_class()

    def _set_value_class(self) -> None:
        # custom parameter classes must inherit from `ParameterValue` not the app
        # subclass:
        if self._value_class is None:
            self._value_class = next(
                (
                    pv_class
                    for pv_class in ParameterValue.__subclasses__()
                    if pv_class._typ == self.typ
                ),
                None,
            )

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and self.typ == other.typ

    def __lt__(self, other: Parameter):
        return self.typ < other.typ

    def __deepcopy__(self, memo: dict[int, Any]):
        kwargs = self.to_dict()
        _validation = kwargs.pop("_validation")
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._validation = _validation
        return obj

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        dct = super()._postprocess_to_dict(d)
        del dct["_value_class"]
        if dct.get("name", None) is None:
            dct.pop("name", None)
        dct.pop("_task_schema", None)  # TODO: how do we have a _task_schema ref?
        return dct

    @property
    def url_slug(self) -> str:
        """
        Representation of this parameter as part of a URL.
        """
        return self.typ.lower().replace("_", "-")

    def _instantiate_value(self, source: ParamSource, val: dict) -> Any:
        """
        Convert the serialized form of this parameter to its "real" form,
        if that is valid to do at all.
        """
        if self._value_class is None:
            return val
        if (method_name := source.get("value_class_method")) is not None:
            method = getattr(self._value_class, method_name)
        else:
            method = self._value_class
        return method(**val)

    def _force_value_class(self) -> type[ParameterValue] | None:
        if (param_cls := self._value_class) is None:
            self._set_value_class()
            param_cls = self._value_class
        return param_cls


@dataclass
class SubParameter:
    """
    A parameter that is a component of another parameter.
    """

    #: How to find this within the containing parameter.
    address: Address
    #: The containing main parameter.
    parameter: Parameter


@dataclass
@hydrate
class SchemaParameter(JSONLike):
    """
    A parameter bound in a schema.

    Parameters
    ----------
    parameter: Parameter
        The parameter.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="parameter",
            class_name="Parameter",
            shared_data_name="parameters",
            shared_data_primary_key="typ",
        ),
    )

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if isinstance(self.parameter, str):
            self.parameter: Parameter = self._app.Parameter(typ=self.parameter)

    @property
    def typ(self) -> str:
        """
        The type code of the parameter.
        """
        return self.parameter.typ


class NullDefault(enum.Enum):
    """
    Sentinel value used to distinguish an explicit null.
    """

    #: Special sentinel.
    #: Used in situations where otherwise a JSON object or array would be.
    NULL = 0


@hydrate
class SchemaInput(SchemaParameter):
    """A Parameter as used within a particular schema, for which a default value may be
    applied.

    Parameters
    ----------
    parameter:
        The parameter (i.e. type) of this schema input.
    multiple:
        If True, expect one or more of these parameters defined in the workflow,
        distinguished by a string label in square brackets. For example `p1[0]` for a
        parameter `p1`.
    labels:
        Dict whose keys represent the string labels that distinguish multiple parameters
        if `multiple` is `True`. Use the key "*" to mean all labels not matching
        other label keys. If `multiple` is `False`, this will default to a
        single-item dict with an empty string key: `{{"": {{}}}}`. If `multiple` is
        `True`, this will default to a single-item dict with the catch-all key:
        `{{"*": {{}}}}`. On initialisation, remaining keyword-arguments are treated as default
        values for the dict values of `labels`.
    default_value:
        The default value for this input parameter. This is itself a default value that
        will be applied to all `labels` values if a "default_value" key does not exist.
    propagation_mode:
        Determines how this input should propagate through the workflow. This is a default
        value that will be applied to all `labels` values if a "propagation_mode" key does
        not exist. By default, the input is allowed to be used in downstream tasks simply
        because it has a compatible type (this is the "implicit" propagation mode). Other
        options are "explicit", meaning that the parameter must be explicitly specified in
        the downstream task `input_sources` for it to be used, and "never", meaning that
        the parameter must not be used in downstream tasks and will be inaccessible to
        those tasks.
    group:
        Determines the name of the element group from which this input should be sourced.
        This is a default value that will be applied to all `labels` if a "group" key
        does not exist.
    allow_failed_dependencies
        This controls whether failure to retrieve inputs (i.e. an
        `UnsetParameterDataError` is raised for one of the input sources) should be
        allowed. By default, the unset value, which is equivalent to `False`, means no
        failures are allowed. If set to `True`, any number of failures are allowed. If an
        integer is specified, that number of failures are permitted. Finally, if a float
        is specified, that proportion of failures are allowed.
    """

    _task_schema: TaskSchema | None = None  # assigned by parent TaskSchema

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="parameter",
            class_name="Parameter",
            shared_data_name="parameters",
            shared_data_primary_key="typ",
        ),
    )

    def __init__(
        self,
        parameter: Parameter | str,
        multiple: bool = False,
        labels: dict[str, LabelInfo] | None = None,
        default_value: InputValue | Any | NullDefault = NullDefault.NULL,
        propagation_mode: ParameterPropagationMode = ParameterPropagationMode.IMPLICIT,
        group: str | None = None,
        allow_failed_dependencies: int | float | bool | None = False,
    ):
        # TODO: can we define elements groups on local inputs as well, or should these be
        # just for elements from other tasks?

        # TODO: test we allow unlabelled with accepts-multiple True.
        # TODO: test we allow a single labelled with accepts-multiple False.

        if isinstance(parameter, str):
            try:
                #: The parameter (i.e. type) of this schema input.
                self.parameter = self._app.parameters.get(parameter)
            except ValueError:
                self.parameter = self._app.Parameter(parameter)
        else:
            self.parameter = parameter

        if allow_failed_dependencies is None:
            allow_failed_dependencies = 0.0
        elif isinstance(allow_failed_dependencies, bool):
            allow_failed_dependencies = float(allow_failed_dependencies)

        #: Whether to expect multiple labels for this parameter.
        self.multiple = multiple
        self.allow_failed_dependencies = allow_failed_dependencies

        #: Dict whose keys represent the string labels that distinguish multiple
        #: parameters if `multiple` is `True`.
        self.labels: dict[str, LabelInfo]
        if labels is None:
            if self.multiple:
                self.labels = {"*": {}}
            else:
                self.labels = {"": {}}
        else:
            self.labels = labels
            if not self.multiple:
                # check single-item:
                if len(self.labels) > 1:
                    raise ValueError(
                        f"If `{self.__class__.__name__}.multiple` is `False`, "
                        f"then `labels` must be a single-item `dict` if specified, but "
                        f"`labels` is: {self.labels!r}."
                    )

        labels_defaults: LabelInfo = {}
        if propagation_mode is not None:
            labels_defaults["propagation_mode"] = propagation_mode
        if group is not None:
            labels_defaults["group"] = group

        # apply defaults:
        for k, v in self.labels.items():
            labels_defaults_i = copy.deepcopy(labels_defaults)
            if default_value is not NullDefault.NULL:
                if isinstance(default_value, InputValue):
                    labels_defaults_i["default_value"] = default_value
                else:
                    labels_defaults_i["default_value"] = self._app.InputValue(
                        parameter=self.parameter,
                        value=default_value,
                        label=k,
                    )
            label_i: LabelInfo = {**labels_defaults_i, **v}
            if "propagation_mode" in label_i:
                label_i["propagation_mode"] = get_enum_by_name_or_val(
                    ParameterPropagationMode, label_i["propagation_mode"]
                )
            if "default_value" in label_i:
                label_i["default_value"]._schema_input = self
            self.labels[k] = label_i

        self._set_parent_refs()
        self._validate()

    def __repr__(self) -> str:
        default_str = ""
        group_str = ""
        labels_str = ""
        if not self.multiple and self.labels:
            label = next(iter(self.labels))  # the single key

            default_str = ""
            if "default_value" in self.labels[label]:
                default_str = (
                    f", default_value={self.labels[label]['default_value'].value!r}"
                )

            if (group := self.labels[label].get("group")) is not None:
                group_str = f", group={group!r}"

        else:
            labels_str = f", labels={str(self.labels)!r}"

        return (
            f"{self.__class__.__name__}("
            f"parameter={self.parameter.__class__.__name__}({self.parameter.typ!r}), "
            f"multiple={self.multiple!r}"
            f"{default_str}{group_str}{labels_str}"
            f")"
        )

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        dct = super()._postprocess_to_dict(d)
        v: dict[str, ParameterPropagationMode]
        for k, v in dct["labels"].items():
            if (prop_mode := v.get("parameter_propagation_mode")) is not None:
                dct["labels"][k]["parameter_propagation_mode"] = prop_mode.name
        return dct

    def _postprocess_to_json(self, json_like):
        for v in json_like["labels"].values():
            if "default_value" in v:
                v["default_value_is_input_value"] = True
        return json_like

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        for k, v in json_like.get("labels", {}).items():
            if "default_value" in v:
                if "default_value_is_input_value" in v:
                    inp_val_kwargs = v["default_value"]
                else:
                    inp_val_kwargs = {
                        "parameter": json_like["parameter"],
                        "value": v["default_value"],
                        "label": k,
                    }
                json_like["labels"][k]["default_value"] = (
                    cls._app.InputValue.from_json_like(
                        json_like=inp_val_kwargs,
                        shared_data=shared_data,
                    )
                )

        return super().from_json_like(json_like, shared_data)

    def __deepcopy__(self, memo: dict[int, Any]):
        kwargs: SchemaInputKwargs = {
            "parameter": copy.deepcopy(self.parameter, memo),
            "multiple": self.multiple,
            "labels": copy.deepcopy(self.labels, memo),
            "allow_failed_dependencies": self.allow_failed_dependencies,
        }
        obj = self.__class__(**kwargs)
        obj._task_schema = self._task_schema
        return obj

    @property
    def default_value(self) -> InputValue | Literal[NullDefault.NULL] | None:
        """
        The default value of the input.
        """
        if single_data := self.single_labelled_data:
            if "default_value" in single_data:
                return single_data["default_value"]
            else:
                return NullDefault.NULL
        return None

    @property
    def task_schema(self) -> TaskSchema:
        """
        The schema containing this input.
        """
        assert self._task_schema is not None
        return self._task_schema

    @property
    def all_labelled_types(self) -> list[str]:
        """
        The types of the input labels.
        """
        return [(f"{self.typ}[{i}]" if i else self.typ) for i in self.labels]

    @property
    def single_label(self) -> str | None:
        """
        The label of this input, assuming it is not multiple.
        """
        if not self.multiple:
            return next(iter(self.labels))
        return None

    @property
    def single_labelled_type(self) -> str | None:
        """
        The type code of this input, assuming it is not multiple.
        """
        if not self.multiple:
            return next(iter(self.labelled_info()))["labelled_type"]
        return None

    @property
    def single_labelled_data(self) -> LabelInfo | None:
        """
        The value of this input, assuming it is not multiple.
        """
        if (label := self.single_label) is not None:
            return self.labels[label]
        return None

    def labelled_info(self) -> Iterator[LabellingDescriptor]:
        """
        Get descriptors for all the labels associated with this input.
        """
        for k, v in self.labels.items():
            label = f"{self.parameter.typ}[{k}]" if k else self.parameter.typ
            dct: LabellingDescriptor = {
                "labelled_type": label,
                "propagation_mode": v["propagation_mode"],
                "group": v.get("group"),
            }
            if "default_value" in v:
                dct["default_value"] = v["default_value"]
            yield dct

    @property
    def _simple_labelled_info(self) -> Iterator[tuple[str, ParameterPropagationMode]]:
        """
        Cut-down version of :py:meth:`labelled_info` that has lower overheads.
        """
        for k, v in self.labels.items():
            label = f"{self.parameter.typ}[{k}]" if k else self.parameter.typ
            yield label, v["propagation_mode"]

    def _validate(self) -> None:
        super()._validate()
        for k, v in self.labels.items():
            if "default_value" in v:
                if not isinstance(v["default_value"], InputValue):
                    def_val = self._app.InputValue(
                        parameter=self.parameter,
                        value=v["default_value"],
                        label=k,
                    )
                    v["default_value"] = def_val
                else:
                    def_val = v["default_value"]
                if def_val.parameter != self.parameter or def_val.label != k:
                    raise ValueError(
                        f"{self.__class__.__name__} `default_value` for label {k!r} must "
                        f"be an `InputValue` for parameter: {self.parameter!r} with the "
                        f"same label, but specified `InputValue` is: "
                        f"{v['default_value']!r}."
                    )

    @property
    def input_or_output(self) -> str:
        """
        Whether this is an input or output. Always ``input``.
        """
        return "input"


@dataclass(init=False)
@hydrate
class SchemaOutput(SchemaParameter):
    """A Parameter as outputted from particular task."""

    #: The basic parameter this supplies.
    parameter: Parameter
    #: How this output propagates.
    propagation_mode: ParameterPropagationMode

    def __init__(
        self,
        parameter: Parameter | str,
        propagation_mode: ParameterPropagationMode = ParameterPropagationMode.IMPLICIT,
    ):
        if isinstance(parameter, str):
            self.parameter: Parameter = self._app.Parameter(typ=parameter)
        else:
            self.parameter = parameter
        self.propagation_mode = propagation_mode

    @property
    def input_or_output(self) -> str:
        """
        Whether this is an input or output. Always ``output``.
        """
        return "output"

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"parameter={self.parameter.__class__.__name__}({self.parameter.typ!r}), "
            f"propagation_mode={self.propagation_mode.name!r}"
            f")"
        )


@dataclass
class BuiltinSchemaParameter:
    """
    A parameter of a built-in schema.
    """

    # TODO: Is this used anywhere?
    # builtin inputs (resources,parameter_perturbations,method,implementation
    # builtin outputs (time, memory use, node/hostname etc)
    # - builtin parameters do not propagate to other tasks (since all tasks define the same
    #   builtin parameters).
    # - however, builtin parameters can be accessed if a downstream task schema specifically
    #   asks for them (e.g. for calculating/plotting a convergence test)
    pass


class _BaseSequence(JSONLike):
    """
    A base class for shared methods of `ValueSequence` and `MultiPathSequence`.
    """

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.to_dict() == other.to_dict()

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        if "path" in json_like:  # note: singular
            # only applicable to ValueSequence, although not well-defined/useful anyway,
            # I think.
            if "::" in json_like["path"]:
                path, cls_method = json_like["path"].split("::")
                json_like["path"] = path
                json_like["value_class_method"] = cls_method

        val_key = next((item for item in json_like if "values" in item), "")
        if "::" in val_key:
            # class method (e.g. `from_range`, `from_file` etc):
            _, method = val_key.split("::")
            _values_method_args = json_like.pop(val_key)

            if "paths" in json_like:  # note: plural
                # only applicable to `MultiPathSequence`, where it is useful to know
                # how many paths we are generating sequences for:
                _values_method_args["paths"] = json_like["paths"]

            _values_method = f"_values_{method}"
            _values_method_args = _process_demo_data_strings(
                cls._app, _values_method_args
            )
            json_like["values"] = getattr(cls, _values_method)(**_values_method_args)

        obj = super().from_json_like(json_like, shared_data)
        if "::" in val_key:
            obj._values_method = method
            obj._values_method_args = _values_method_args

        return obj


class ValueSequence(_BaseSequence):
    """
    A sequence of values.

    Parameters
    ----------
    path:
        The path to this sequence.
    values:
        The values in this sequence.
    nesting_order: int
        A nesting order for this sequence. Can be used to compose sequences together.
    label: str
        A label for this sequence.
    value_class_method: str
        Name of a method used to generate sequence values. Not normally used directly.
    """

    def __init__(
        self,
        path: str,
        values: Sequence[Any] | None,
        nesting_order: int | float | None = None,
        label: str | int | None = None,
        value_class_method: str | None = None,
    ):
        path_, label_ = self._validate_parameter_path(path, label)
        #: The path to this sequence.
        self.path = path_
        #: The label of this sequence.
        self.label = label_
        #: The nesting order for this sequence.
        self.nesting_order = None if nesting_order is None else float(nesting_order)
        #: Name of a method used to generate sequence values.
        self.value_class_method = value_class_method

        if values is not None:
            self._values: list[Any] | None = [
                _process_demo_data_strings(self._app, i) for i in values
            ]
        else:
            self._values = None

        self._values_group_idx: list[int] | None = None
        self._values_are_objs: list[bool] | None = (
            None  # assigned initially on `make_persistent`
        )

        self._workflow: Workflow | None = None  # assigned in `make_persistent`
        self._element_set: ElementSet | None = None  # assigned by parent `ElementSet`

        # assigned if this is an "inputs" sequence in `WorkflowTask._add_element_set`:
        self._parameter: Parameter | None = None

        self._path_split: list[str] | None = None  # assigned by property `path_split`

        self._values_method: str | None = None
        self._values_method_args: dict | None = None

    def __repr__(self):
        label_str = ""
        if self.label:
            label_str = f"label={self.label!r}, "
        vals_grp_idx = (
            f"values_group_idx={self._values_group_idx}, "
            if self._values_group_idx
            else ""
        )
        return (
            f"{self.__class__.__name__}("
            f"path={self.path!r}, "
            f"{label_str}"
            f"nesting_order={self.nesting_order}, "
            f"{vals_grp_idx}"
            f"values={self.values}"
            f")"
        )

    def __deepcopy__(self, memo: dict[int, Any]):
        kwargs = self.to_dict()
        kwargs["values"] = kwargs.pop("_values")

        _values_group_idx = kwargs.pop("_values_group_idx")
        _values_are_objs = kwargs.pop("_values_are_objs")
        _values_method = kwargs.pop("_values_method", None)
        _values_method_args = kwargs.pop("_values_method_args", None)

        obj = self.__class__(**copy.deepcopy(kwargs, memo))

        obj._values_group_idx = _values_group_idx
        obj._values_are_objs = _values_are_objs
        obj._values_method = _values_method
        obj._values_method_args = _values_method_args

        obj._workflow = self._workflow
        obj._element_set = self._element_set
        obj._path_split = self._path_split
        obj._parameter = self._parameter

        return obj

    @property
    def parameter(self) -> Parameter | None:
        """
        The parameter this sequence supplies.
        """
        return self._parameter

    @property
    def path_split(self) -> Sequence[str]:
        """
        The components of this path.
        """
        if self._path_split is None:
            self._path_split = self.path.split(".")
        return self._path_split

    @property
    def path_type(self) -> str:
        """
        The type of path this is.
        """
        return self.path_split[0]

    @property
    def input_type(self) -> str | None:
        """
        The type of input sequence this is, if it is one.
        """
        if self.path_type == "inputs":
            return self.path_split[1].replace(self._label_fmt, "")
        return None

    @property
    def input_path(self) -> str | None:
        """
        The path of the input sequence this is, if it is one.
        """
        if self.path_type == "inputs":
            return ".".join(self.path_split[2:])
        return None

    @property
    def resource_scope(self) -> str | None:
        """
        The scope of the resources this is, if it is one.
        """
        if self.path_type == "resources":
            return self.path_split[1]
        return None

    @property
    def is_sub_value(self) -> bool:
        """True if the values are for a sub part of the parameter."""
        return bool(self.input_path)

    @property
    def _label_fmt(self) -> str:
        return f"[{self.label}]" if self.label else ""

    @property
    def labelled_type(self) -> str | None:
        """
        The labelled type of input sequence this is, if it is one.
        """
        if self.input_type:
            return f"{self.input_type}{self._label_fmt}"
        return None

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""

        _values_group_idx = json_like.pop("_values_group_idx", None)
        _values_are_objs = json_like.pop("_values_are_objs", None)
        _values_method = json_like.pop("_values_method", None)
        _values_method_args = json_like.pop("_values_method_args", None)
        if "_values" in json_like:
            json_like["values"] = json_like.pop("_values")

        obj = cls(**json_like)
        obj._values_group_idx = _values_group_idx
        obj._values_are_objs = _values_are_objs
        obj._values_method = _values_method
        obj._values_method_args = _values_method_args
        return obj

    def _validate_parameter_path(
        self, path: str, label: str | int | None
    ) -> tuple[str, str | int | None]:
        """Parse the supplied path and perform basic checks on it.

        This method also adds the specified `SchemaInput` label to the path and checks for
        consistency if a label is already present.

        """
        label_arg = label

        if not isinstance(path, str):
            raise MalformedParameterPathError(
                f"`path` must be a string, but given path has type {type(path)} with value "
                f"{path!r}."
            )
        path_l = path.lower()
        path_split = path_l.split(".")
        ALLOWED_PATH_START = ("inputs", "resources", "environments", "env_preset")
        if not path_split[0] in ALLOWED_PATH_START:
            raise MalformedParameterPathError(
                f"`path` must start with one of: "
                f'{", ".join(f"{pfx!r}" for pfx in ALLOWED_PATH_START)}, but given path '
                f"is: {path!r}."
            )

        _, label_from_path = split_param_label(path_l)

        if path_split[0] == "inputs":
            if label_arg is not None and label_arg != "":
                if label_from_path is None:
                    # add label to path without lower casing any parts:
                    path_split_orig = path.split(".")
                    path_split_orig[1] += f"[{label_arg}]"
                    path = ".".join(path_split_orig)
                elif str(label_arg) != label_from_path:
                    raise ValueError(
                        f"{self.__class__.__name__} `label` argument is specified as "
                        f"{label_arg!r}, but a distinct label is implied by the sequence "
                        f"path: {path!r}."
                    )
            elif label_from_path:
                label = label_from_path

        elif path_split[0] == "resources":
            if label_from_path or label_arg:
                raise ValueError(
                    f"{self.__class__.__name__} `label` argument ({label_arg!r}) and/or "
                    f"label specification via `path` ({path!r}) is not supported for "
                    f"`resource` sequences."
                )
            try:
                self._app.ActionScope.from_json_like(path_split[1])
            except Exception as err:
                raise MalformedParameterPathError(
                    f"Cannot parse a resource action scope from the second component of the "
                    f"path: {path!r}. Exception was: {err}."
                ) from None

            if len(path_split) > 2:
                if path_split[2] not in ResourceSpec.ALLOWED_PARAMETERS:
                    raise UnknownResourceSpecItemError(
                        f"Resource item name {path_split[2]!r} is unknown. Allowed "
                        f"resource item names are: {ResourceSpec._allowed_params_quoted()}."
                    )
            label = ""

        elif path_split[0] == "environments":
            # rewrite as a resources path:
            path = f"resources.any.{path}"
            label = str(label) if label is not None else ""
        else:
            pass
            # note: `env_preset` paths also need to be transformed into `resources`
            # paths, but we cannot do that until the sequence is part of a task, since
            # the available environment presets are defined in the task schema.

        return path, label

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        out = super()._postprocess_to_dict(d)
        del out["_parameter"]
        del out["_path_split"]
        out.pop("_workflow", None)
        return out

    @property
    def normalised_path(self) -> str:
        """
        The path to this sequence.
        """
        return self.path

    @property
    def normalised_inputs_path(self) -> str | None:
        """
        The normalised path without the "inputs" prefix, if the sequence is an
        inputs sequence, else return None.
        """

        if self.input_type:
            if self.input_path:
                return f"{self.labelled_type}.{self.input_path}"
            else:
                return self.labelled_type
        return None

    def make_persistent(
        self, workflow: Workflow, source: ParamSource
    ) -> tuple[str, list[int], bool]:
        """Save value to a persistent workflow."""

        if self._values_group_idx is not None:
            if not workflow.check_parameters_exist(self._values_group_idx):
                raise RuntimeError(
                    f"{self.__class__.__name__} has a parameter group index "
                    f"({self._values_group_idx}), but does not exist in the workflow."
                )
            # TODO: log if already persistent.
            return self.normalised_path, self._values_group_idx, False

        data_ref: list[int] = []
        source = copy.deepcopy(source)
        if self.value_class_method:
            source["value_class_method"] = self.value_class_method
        are_objs: list[bool] = []
        assert self._values is not None
        for idx, item in enumerate(self._values):
            # record if ParameterValue sub-classes are passed for values, which allows
            # us to re-init the objects on access to `.value`:
            are_objs.append(isinstance(item, ParameterValue))
            source = copy.deepcopy(source)
            source["sequence_idx"] = idx
            pg_idx_i = workflow._add_parameter_data(item, source=source)
            data_ref.append(pg_idx_i)

        self._values_group_idx = data_ref
        self._workflow = workflow
        self._values = None
        self._values_are_objs = are_objs
        return self.normalised_path, data_ref, True

    @property
    def workflow(self) -> Workflow | None:
        """
        The workflow containing this sequence.
        """
        if self._workflow:
            # (assigned in `make_persistent`)
            return self._workflow
        elif self._element_set:
            # (assigned by parent `ElementSet`)
            if tmpl := self._element_set.task_template.workflow_template:
                return tmpl.workflow
        return None

    @property
    def values(self) -> Sequence[Any] | None:
        """
        The values in this sequence.
        """
        if self._values_group_idx is not None:
            vals: list[Any] = []
            for idx, pg_idx_i in enumerate(self._values_group_idx):
                if not (w := self.workflow):
                    continue
                param_i = w.get_parameter(pg_idx_i)
                if param_i.data is not None:
                    val_i = param_i.data
                else:
                    val_i = param_i.file

                # `val_i` might already be a `_value_class` object if the store has not
                # yet been committed to disk:
                if (
                    self.parameter
                    and self._values_are_objs
                    and self._values_are_objs[idx]
                    and isinstance(val_i, dict)
                ):
                    val_i = self.parameter._instantiate_value(param_i.source, val_i)

                vals.append(val_i)
            return vals
        else:
            return self._values

    @classmethod
    def _values_from_linear_space(
        cls, start: float, stop: float, num: int, **kwargs
    ) -> list[float]:
        return np.linspace(start, stop, num=num, **kwargs).tolist()

    @classmethod
    def _values_from_geometric_space(
        cls, start: float, stop: float, num: int, **kwargs
    ) -> list[float]:
        return np.geomspace(start, stop, num=num, **kwargs).tolist()  # type: ignore #  mypy bug for numpy~2.2.4 https://github.com/numpy/numpy/issues/27944

    @classmethod
    def _values_from_log_space(
        cls, start: float, stop: float, num: int, base: float = 10.0, **kwargs
    ) -> list[float]:
        return np.logspace(start, stop, num=num, base=base, **kwargs).tolist()  # type: ignore #  mypy bug for numpy~2.2.4 https://github.com/numpy/numpy/issues/27944

    @classmethod
    def _values_from_range(
        cls, start: int | float, stop: int | float, step: int | float, **kwargs
    ) -> list[float]:
        return np.arange(start, stop, step, **kwargs).tolist()  # type: ignore #  mypy bug for numpy~2.2.4 https://github.com/numpy/numpy/issues/27944

    @classmethod
    def _values_from_file(cls, file_path: str | Path) -> list[str]:
        with Path(file_path).open("rt") as fh:
            return [line.strip() for line in fh.readlines()]

    @classmethod
    def _values_from_rectangle(
        cls,
        start: Sequence[float],
        stop: Sequence[float],
        num: Sequence[int],
        coord: int | tuple[int, int] | None = None,
        include: Sequence[str] | None = None,
        **kwargs,
    ) -> list[float]:
        vals = linspace_rect(start=start, stop=stop, num=num, include=include, **kwargs)
        if coord is not None:
            return vals[coord].tolist()
        else:
            return (vals.T).tolist()  # type: ignore #  mypy bug for numpy~2.2.4 https://github.com/numpy/numpy/issues/27944

    @classmethod
    def _values_from_random_uniform(
        cls,
        num: int,
        low: float = 0.0,
        high: float = 1.0,
        seed: int | list[int] | None = None,
    ) -> list[float]:
        rng = np.random.default_rng(seed)
        return rng.uniform(low=low, high=high, size=num).tolist()  # type: ignore #  mypy bug for numpy~2.2.4 https://github.com/numpy/numpy/issues/27944

    @classmethod
    def from_linear_space(
        cls,
        path: str,
        start: float,
        stop: float,
        num: int,
        nesting_order: float = 0,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        """
        Build a sequence from a NumPy linear space.
        """
        # TODO: save persistently as an array?
        args = {"start": start, "stop": stop, "num": num, **kwargs}
        values = cls._values_from_linear_space(**args)
        obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
        obj._values_method = "from_linear_space"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_geometric_space(
        cls,
        path: str,
        start: float,
        stop: float,
        num: int,
        nesting_order: float = 0,
        endpoint=True,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        """
        Build a sequence from a NumPy geometric space.
        """
        args = {"start": start, "stop": stop, "num": num, "endpoint": endpoint, **kwargs}
        values = cls._values_from_geometric_space(**args)
        obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
        obj._values_method = "from_geometric_space"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_log_space(
        cls,
        path: str,
        start: float,
        stop: float,
        num: int,
        nesting_order: float = 0,
        base=10.0,
        endpoint=True,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        """
        Build a sequence from a NumPy logarithmic space.
        """
        args = {
            "start": start,
            "stop": stop,
            "num": num,
            "endpoint": endpoint,
            "base": base,
            **kwargs,
        }
        values = cls._values_from_log_space(**args)
        obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
        obj._values_method = "from_log_space"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_range(
        cls,
        path: str,
        start: float,
        stop: float,
        nesting_order: float = 0,
        step: int | float = 1,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        """
        Build a sequence from a range.
        """
        # TODO: save persistently as an array?
        args = {"start": start, "stop": stop, "step": step, **kwargs}
        if isinstance(step, int):
            values = cls._values_from_range(**args)
        else:
            # Use linspace for non-integer step, as recommended by Numpy:
            values = cls._values_from_linear_space(
                start=start,
                stop=stop,
                num=int((stop - start) / step),
                endpoint=False,
                **kwargs,
            )
        obj = cls(
            values=values,
            path=path,
            nesting_order=nesting_order,
            label=label,
        )
        obj._values_method = "from_range"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_file(
        cls,
        path: str,
        file_path: str | Path,
        nesting_order: float = 0,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        """
        Build a sequence from a simple file.
        """
        args = {"file_path": file_path, **kwargs}
        values = cls._values_from_file(**args)
        obj = cls(
            values=values,
            path=path,
            nesting_order=nesting_order,
            label=label,
        )

        obj._values_method = "from_file"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_rectangle(
        cls,
        path: str,
        start: Sequence[float],
        stop: Sequence[float],
        num: Sequence[int],
        coord: int | None = None,
        include: list[str] | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        """
        Build a sequence to cover a rectangle.

        Parameters
        ----------
        coord:
            Which coordinate to use. Either 0, 1, or `None`, meaning each value will be
            both coordinates.
        include
            If specified, include only the specified edges. Choose from "top", "right",
            "bottom", "left".
        """
        args = {
            "start": start,
            "stop": stop,
            "num": num,
            "coord": coord,
            "include": include,
            **kwargs,
        }
        values = cls._values_from_rectangle(**args)
        obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
        obj._values_method = "from_rectangle"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_random_uniform(
        cls,
        path,
        num: int,
        low: float = 0.0,
        high: float = 1.0,
        seed: int | list[int] | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        """
        Build a sequence from a uniform random number generator.
        """
        args = {"low": low, "high": high, "num": num, "seed": seed, **kwargs}
        values = cls._values_from_random_uniform(**args)
        obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
        obj._values_method = "from_random_uniform"
        obj._values_method_args = args
        return obj


class MultiPathSequence(_BaseSequence):
    """
    A sequence of values to be distributed across one or more paths.

    Notes
    -----
    This is useful when we would like to generate values for multiple input paths that
    have some interdependency, or when they must be generate together in one go.

    Parameters
    ----------
    paths:
        The paths to this multi-path sequence.
    values:
        The values in this multi-path sequence.
    nesting_order: int
        A nesting order for this multi-path sequence. Can be used to compose sequences
        together.
    label: str
        A label for this multi-path sequence.
    value_class_method: str
        Name of a method used to generate multi-path sequence values. Not normally used
        directly.
    """

    # TODO: add a `path_axis` argument with doc string like:
    # path_axis:
    #    The axis (as in a Numpy axis) along `values` to which the different paths
    #    correspond.

    def __init__(
        self,
        paths: Sequence[str],
        values: NDArray | Sequence[Sequence] | None,
        nesting_order: int | float | None = None,
        label: str | int | None = None,
        value_class_method: str | None = None,
    ):
        self.paths = list(paths)
        self.nesting_order = nesting_order
        self.label = label
        self.value_class_method = value_class_method

        self._sequences: list[ValueSequence] | None = None
        self._values: NDArray | Sequence[Sequence] | None = None

        if values is not None:
            if (len_paths := len(paths)) != (len_vals := len(values)):
                raise ValueError(
                    f"The number of values ({len_vals}) must be equal to the number of "
                    f"paths provided ({len_paths})."
                )
            self._values = values
            self._sequences = [
                self._app.ValueSequence(
                    path=path,
                    values=values[idx],
                    label=label,
                    nesting_order=nesting_order,
                    value_class_method=value_class_method,
                )
                for idx, path in enumerate(paths)
            ]

        # assigned by `_move_to_sequence_list` (invoked by first init of parent
        # `ElementSet`), corresponds to the sequence indices with the element set's
        # sequence list:
        self._sequence_indices: Sequence[int] | None = None

        self._element_set: ElementSet | None = None  # assigned by parent `ElementSet`

        self._values_method: str | None = None
        self._values_method_args: dict | None = None

    def __repr__(self):

        label_str = f"label={self.label!r}, " if self.label else ""
        val_cls_str = (
            f"value_class_method={self.value_class_method!r}, "
            if self.value_class_method
            else ""
        )
        return (
            f"{self.__class__.__name__}("
            f"paths={self.paths!r}, "
            f"{label_str}"
            f"nesting_order={self.nesting_order}, "
            f"{val_cls_str}"
            f"values={self.values}"
            f")"
        )

    def __deepcopy__(self, memo: dict[int, Any]):
        kwargs = self.to_dict()
        kwargs["values"] = kwargs.pop("_values")

        _sequences = kwargs.pop("_sequences", None)
        _sequence_indices = kwargs.pop("_sequence_indices", None)
        _values_method = kwargs.pop("_values_method", None)
        _values_method_args = kwargs.pop("_values_method_args", None)

        obj = self.__class__(**copy.deepcopy(kwargs, memo))

        obj._sequences = _sequences
        obj._sequence_indices = _sequence_indices
        obj._values_method = _values_method
        obj._values_method_args = _values_method_args

        obj._element_set = self._element_set

        return obj

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        dct = super()._postprocess_to_dict(d)
        del dct["_sequences"]
        return dct

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""

        # pop the keys we don't accept in `__init__`, and then assign after `__init__`:
        _sequence_indices = json_like.pop("_sequence_indices", None)

        _values_method = json_like.pop("_values_method", None)
        _values_method_args = json_like.pop("_values_method_args", None)
        if "_values" in json_like:
            json_like["values"] = json_like.pop("_values")

        obj = cls(**json_like)
        obj._sequence_indices = _sequence_indices
        obj._values_method = _values_method
        obj._values_method_args = _values_method_args
        return obj

    @property
    def sequence_indices(self) -> Sequence[int] | None:
        """
        The range indices (start and stop) to the parent element set's sequences list that
        correspond to the `ValueSequence`s generated by this multi-path sequence, if this
        object is bound to a parent element set.
        """
        return self._sequence_indices

    @property
    def sequences(self) -> Sequence[ValueSequence]:
        """
        The child value sequences, one for each path.
        """
        if self._sequence_indices:
            # they are stored in the parent `ElementSet`
            assert self._element_set
            return self._element_set.sequences[slice(*self._sequence_indices)]
        else:
            # not yet bound to a parent `ElementSet`
            assert self._sequences
            return self._sequences

    @property
    def values(self) -> list[Sequence[Any]]:
        values = []
        for seq_i in self.sequences:
            assert seq_i.values
            values.append(seq_i.values)
        return values

    def _move_to_sequence_list(self, sequences: list[ValueSequence]) -> None:
        """
        Move the individual value sequences to an external list of value sequences (i.e.,
        the parent `ElementSet`'s), and update the `sequence_indices` attribute so we can
        retrieve the sequences from that list at will.
        """
        len_ours = len(self.sequences)
        len_ext = len(sequences)
        sequences.extend(self.sequences)

        # child sequences are now stored externally, and values retrieved via those:
        self._sequences = None
        self._values = None
        self._sequence_indices = [len_ext, len_ext + len_ours]

    @classmethod
    def _values_from_latin_hypercube(
        cls,
        paths: Sequence[str],
        num_samples: int,
        *,
        bounds: dict[str, Sequence[float]] | None = None,
        scramble: bool = True,
        strength: int = 1,
        optimization: Literal["random-cd", "lloyd"] | None = None,
        rng=None,
    ) -> NDArray:

        num_paths = len(paths)
        kwargs = dict(
            d=num_paths,
            scramble=scramble,
            strength=strength,
            optimization=optimization,
            rng=rng,
        )

        bounds = bounds or {}

        parameter_ranges = np.array([bounds.get(path, [0, 1]) for path in paths]).T

        lower_bound = parameter_ranges[0]
        upper_bound = parameter_ranges[1]

        try:
            sampler = LatinHypercube(**kwargs)
        except TypeError:
            # `rng` was previously (<1.15.0) `seed`:
            kwargs["seed"] = kwargs.pop("rng")
            sampler = LatinHypercube(**kwargs)

        samples = scale(sampler.random(n=num_samples), lower_bound, upper_bound).T

        return samples

    @classmethod
    def from_latin_hypercube(
        cls,
        paths: Sequence[str],
        num_samples: int,
        *,
        bounds: dict[str, Sequence[float]] | None = None,
        scramble: bool = True,
        strength: int = 1,
        optimization: Literal["random-cd", "lloyd"] | None = None,
        rng=None,
        nesting_order: int | float | None = None,
        label: str | int | None = None,
    ) -> Self:
        """
        Generate values from SciPy's latin hypercube sampler: :class:`scipy.stats.qmc.LatinHypercube`.
        """
        kwargs = {
            "paths": paths,
            "num_samples": num_samples,
            "scramble": scramble,
            "strength": strength,
            "optimization": optimization,
            "rng": rng,
            "bounds": bounds,
        }
        values = cls._values_from_latin_hypercube(**kwargs)
        assert values is not None
        obj = cls(
            paths=paths,
            values=values,
            nesting_order=nesting_order,
            label=label,
        )
        obj._values_method = "from_latin_hypercube"
        obj._values_method_args = kwargs
        return obj


@dataclass
class AbstractInputValue(JSONLike):
    """Class to represent all sequence-able inputs to a task."""

    _workflow: Workflow | None = None
    _element_set: ElementSet | None = None
    _schema_input: SchemaInput | None = None
    _value: Any | None = None
    _value_group_idx: int | list[int] | None = None

    def __repr__(self) -> str:
        try:
            value_str = f", value={self.value}"
        except WorkflowParameterMissingError:
            value_str = ""

        return (
            f"{self.__class__.__name__}("
            f"_value_group_idx={self._value_group_idx}"
            f"{value_str}"
            f")"
        )

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        out = super()._postprocess_to_dict(d)
        out.pop("_workflow", None)
        out.pop("_schema_input", None)
        return out

    def make_persistent(
        self, workflow: Workflow, source: ParamSource
    ) -> tuple[str, list[int | list[int]], bool]:
        """Save value to a persistent workflow.

        Returns
        -------
        str
            Normalised path for this task input.
        list[int | list[int]]
            The index of the parameter data Zarr group where the data is stored.
        bool
            Whether this is newly persistent.
        """

        if self._value_group_idx is not None:
            data_ref = self._value_group_idx
            is_new = False
            if not workflow.check_parameters_exist(data_ref):
                raise RuntimeError(
                    f"{self.__class__.__name__} has a data reference "
                    f"({data_ref}), but does not exist in the workflow."
                )
            # TODO: log if already persistent.
        else:
            data_ref = workflow._add_parameter_data(self._value, source=source)
            self._value_group_idx = data_ref
            is_new = True
            self._value = None

        return (self.normalised_path, [data_ref], is_new)

    @property
    def normalised_path(self) -> str:
        """
        The normalised path, if known.
        """
        raise NotImplementedError

    @property
    def workflow(self) -> Workflow | None:
        """
        The workflow containing this input value.
        """
        if self._workflow:
            return self._workflow
        if self._element_set:
            if w_tmpl := self._element_set.task_template.workflow_template:
                return w_tmpl.workflow
        if self._schema_input:
            if t_tmpl := self._schema_input.task_schema.task_template:
                if w_tmpl := t_tmpl.workflow_template:
                    return w_tmpl.workflow
        return None

    @property
    def value(self) -> Any:
        """
        The value itself.
        """
        return self._value


@dataclass
class ValuePerturbation(AbstractInputValue):
    """
    A perturbation applied to a value.
    """

    #: The name of this perturbation.
    name: str = ""
    #: The path to the value(s) to perturb.
    path: Sequence[str | int | float] | None = None
    #: The multiplicative factor to apply.
    multiplicative_factor: Numeric | None = 1
    #: The additive factor to apply.
    additive_factor: Numeric | None = 0

    def __post_init__(self):
        assert self.name

    @classmethod
    def from_spec(cls, spec):
        """
        Construct an instance from a specification dictionary.
        """
        return cls(**spec)


@hydrate
class InputValue(AbstractInputValue):
    """
    An input value to a task.

    Parameters
    ----------
    parameter: Parameter | SchemaInput | str
        Parameter whose value is to be specified.
    label: str
        Optional identifier to be used where the associated `SchemaInput` accepts multiple
        parameters of the specified type. This will be cast to a string.
    value: Any
        The input parameter value.
    value_class_method: How to obtain the real value.
        A class method that can be invoked with the `value` attribute as keyword
        arguments.
    path: str
        Dot-delimited path within the parameter's nested data structure for which `value`
        should be set.

    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="parameter",
            class_name="Parameter",
            shared_data_primary_key="typ",
            shared_data_name="parameters",
        ),
    )

    def __init__(
        self,
        parameter: Parameter | SchemaInput | str,
        value: Any | None = None,
        label: str | int | None = None,
        value_class_method: str | None = None,
        path: str | None = None,
        _check_obj: bool = True,
    ):
        super().__init__()
        if isinstance(parameter, str):
            try:
                #: Parameter whose value is to be specified.
                self.parameter = self._app.parameters.get(parameter)
            except ValueError:
                self.parameter = self._app.Parameter(parameter)
        elif isinstance(parameter, SchemaInput):
            self.parameter = parameter.parameter
        else:
            self.parameter = parameter

        #: Identifier to be used where the associated `SchemaInput` accepts multiple
        #: parameters of the specified type.
        self.label = str(label) if label is not None else ""
        #: Dot-delimited path within the parameter's nested data structure for which
        #: `value` should be set.
        self.path = (path.strip(".") or None) if path else None
        #: A class method that can be invoked with the `value` attribute as keyword
        #: arguments.
        self.value_class_method = value_class_method
        self._value = _process_demo_data_strings(self._app, value)

        # record if a ParameterValue sub-class is passed for value, which allows us
        # to re-init the object on `.value`:
        self._value_is_obj = isinstance(value, ParameterValue)
        if _check_obj:
            self._check_dict_value_if_object()

    def _check_dict_value_if_object(self):
        """For non-persistent input values, check that, if a matching `ParameterValue`
        class exists and the specified value is not of that type, then the specified
        value is a dict, which can later be passed to the ParameterValue sub-class
        to initialise the object.
        """
        if (
            self._value_group_idx is None
            and not self.path
            and not self._value_is_obj
            and self.parameter._value_class
            and self._value is not None
            and not isinstance(self._value, dict)
        ):
            raise ValueError(
                f"{self.__class__.__name__} with specified value {self._value!r} is "
                f"associated with a ParameterValue subclass "
                f"({self.parameter._value_class!r}), but the value data type is not a "
                f"dict."
            )

    def __deepcopy__(self, memo: dict[int, Any]) -> Self:
        kwargs = self.to_dict()
        _value = kwargs.pop("_value")
        kwargs.pop("_schema_input", None)
        _value_group_idx = kwargs.pop("_value_group_idx")
        _value_is_obj = kwargs.pop("_value_is_obj")
        obj = self.__class__(**copy.deepcopy(kwargs, memo), _check_obj=False)
        obj._value = _value
        obj._value_group_idx = _value_group_idx
        obj._value_is_obj = _value_is_obj
        obj._element_set = self._element_set
        obj._schema_input = self._schema_input
        return obj

    def __repr__(self) -> str:
        val_grp_idx = ""
        if self._value_group_idx is not None:
            val_grp_idx = f", value_group_idx={self._value_group_idx}"

        path_str = ""
        if self.path is not None:
            path_str = f", path={self.path!r}"

        label_str = ""
        if self.label is not None:
            label_str = f", label={self.label!r}"

        try:
            value_str = f", value={self.value!r}"
        except WorkflowParameterMissingError:
            value_str = ""

        return (
            f"{self.__class__.__name__}("
            f"parameter={self.parameter.typ!r}{label_str}"
            f"{value_str}"
            f"{path_str}"
            f"{val_grp_idx}"
            f")"
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.to_dict() == other.to_dict()

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""

        _value_group_idx = json_like.pop("_value_group_idx", None)
        _value_is_obj = json_like.pop("_value_is_obj", None)
        if "_value" in json_like:
            json_like["value"] = json_like.pop("_value")

        obj = cls(**json_like, _check_obj=False)
        obj._value_group_idx = _value_group_idx
        obj._value_is_obj = _value_is_obj
        obj._check_dict_value_if_object()
        return obj

    @property
    def labelled_type(self) -> str:
        """
        The labelled type of this input value.
        """
        label = f"[{self.label}]" if self.label else ""
        return f"{self.parameter.typ}{label}"

    @property
    def normalised_inputs_path(self) -> str:
        """
        The normalised input path without the ``inputs.`` prefix.
        """
        return f"{self.labelled_type}{f'.{self.path}' if self.path else ''}"

    @property
    def normalised_path(self) -> str:
        """
        The full normalised input path.
        """
        return f"inputs.{self.normalised_inputs_path}"

    def make_persistent(
        self, workflow: Workflow, source: ParamSource
    ) -> tuple[str, list[int | list[int]], bool]:
        source = copy.deepcopy(source)
        if self.value_class_method is not None:
            source["value_class_method"] = self.value_class_method
        return super().make_persistent(workflow, source)

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        if "[" in json_like["parameter"]:
            # extract out the parameter label:
            param, label = split_param_label(json_like["parameter"])
            json_like["parameter"] = param
            json_like["label"] = label

        if "::" in json_like["parameter"]:
            # double-colon syntax indicates a `ParameterValue`-subclass class method
            # of the specified name should be used to construct the values:
            param, cls_method = json_like["parameter"].split("::")
            json_like["parameter"] = param
            json_like["value_class_method"] = cls_method

        if "path" not in json_like:
            # in the case this value corresponds to some sub-part of the parameter's
            # nested data structure:
            param, *path = json_like["parameter"].split(".")
            json_like["parameter"] = param
            json_like["path"] = ".".join(path)

        return super().from_json_like(json_like, shared_data)

    @property
    def is_sub_value(self) -> bool:
        """True if the value is for a sub part of the parameter (i.e. if `path` is set).
        Sub-values are not added to the base parameter data, but are interpreted as
        single-value sequences."""
        return bool(self.path)

    @property
    def value(self) -> Any:
        if self._value_group_idx is not None and self.workflow:
            val = self.workflow.get_parameter_data(cast("int", self._value_group_idx))
            if self._value_is_obj and self.parameter._value_class:
                return self.parameter._value_class(**val)
            return val
        else:
            return self._value


class ResourceSpec(JSONLike):
    """Class to represent specification of resource requirements for a (set of) actions.

    Notes
    -----
    `os_name` is used for retrieving a default shell name and for retrieving the correct
    `Shell` class; when using WSL, it should still be `nt` (i.e. Windows).

    Parameters
    ----------
    scope:
        Which scope does this apply to.
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

    #: The names of parameters that may be used when making an instance of this class.
    ALLOWED_PARAMETERS: ClassVar[set[str]] = {
        "scratch",
        "parallel_mode",
        "num_cores",
        "num_cores_per_node",
        "num_threads",
        "num_nodes",
        "scheduler",
        "shell",
        "use_job_array",
        "max_array_items",
        "write_app_logs",
        "combine_jobscript_std",
        "combine_scripts",
        "time_limit",
        "scheduler_args",
        "shell_args",
        "os_name",
        "environments",
        "resources_id",
        "skip_downstream_on_failure",
        "SGE_parallel_env",
        "SLURM_partition",
        "SLURM_num_tasks",
        "SLURM_num_tasks_per_node",
        "SLURM_num_nodes",
        "SLURM_num_cpus_per_task",
    }

    _resource_list: ResourceList | None = None

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="scope",
            class_name="ActionScope",
        ),
    )

    @staticmethod
    def __quoted(values: Iterable):
        return ", ".join(f'"{item}"' for item in values)

    @classmethod
    def _allowed_params_quoted(cls) -> str:
        """
        The string version of the list of allowed parameters.
        """
        return cls.__quoted(cls.ALLOWED_PARAMETERS)

    @staticmethod
    def __parse_thing(
        typ: type[ActionScope], val: ActionScope | str | None
    ) -> ActionScope | None:
        if isinstance(val, typ):
            return val
        elif val is None:
            return typ.any()
        else:
            return typ.from_json_like(cast("str", val))

    def __init__(
        self,
        scope: ActionScope | str | None = None,
        scratch: str | None = None,
        parallel_mode: str | ParallelMode | None = None,
        num_cores: int | None = None,
        num_cores_per_node: int | None = None,
        num_threads: int | None = None,
        num_nodes: int | None = None,
        scheduler: str | None = None,
        shell: str | None = None,
        use_job_array: bool | None = None,
        max_array_items: int | None = None,
        write_app_logs: bool | None = None,
        combine_jobscript_std: bool | None = None,
        combine_scripts: bool | None = None,
        time_limit: str | timedelta | None = None,
        scheduler_args: dict[str, Any] | None = None,
        shell_args: dict[str, Any] | None = None,
        os_name: str | None = None,
        environments: Mapping[str, Mapping[str, Any]] | None = None,
        resources_id: int | None = None,
        skip_downstream_on_failure: bool | None = None,
        SGE_parallel_env: str | None = None,
        SLURM_partition: str | None = None,
        SLURM_num_tasks: str | None = None,
        SLURM_num_tasks_per_node: str | None = None,
        SLURM_num_nodes: str | None = None,
        SLURM_num_cpus_per_task: str | None = None,
    ):
        #: Which scope does this apply to.
        self.scope = self.__parse_thing(self._app.ActionScope, scope)

        if isinstance(time_limit, timedelta):
            time_limit = timedelta_format(time_limit)

        # assigned by `make_persistent`
        self._workflow: Workflow | None = None
        self._value_group_idx: int | list[int] | None = None

        # user-specified resource parameters:
        self._scratch = scratch
        self._parallel_mode = get_enum_by_name_or_val(ParallelMode, parallel_mode)
        self._num_cores = num_cores
        self._num_threads = num_threads
        self._num_nodes = num_nodes
        self._num_cores_per_node = num_cores_per_node
        self._scheduler = self._process_string(scheduler)
        self._shell = self._process_string(shell)
        self._os_name = self._process_string(os_name)
        self._environments = environments
        self._resources_id = resources_id
        self._skip_downstream_on_failure = skip_downstream_on_failure
        self._use_job_array = use_job_array
        self._max_array_items = max_array_items
        self._write_app_logs = write_app_logs
        self._combine_jobscript_std = combine_jobscript_std
        self._combine_scripts = combine_scripts
        self._time_limit = time_limit
        self._scheduler_args = scheduler_args
        self._shell_args = shell_args

        # user-specified SGE-specific parameters:
        self._SGE_parallel_env = SGE_parallel_env

        # user-specified SLURM-specific parameters:
        self._SLURM_partition = SLURM_partition
        self._SLURM_num_tasks = SLURM_num_tasks
        self._SLURM_num_tasks_per_node = SLURM_num_tasks_per_node
        self._SLURM_num_nodes = SLURM_num_nodes
        self._SLURM_num_cpus_per_task = SLURM_num_cpus_per_task

    def __deepcopy__(self, memo: dict[int, Any]) -> Self:
        kwargs = copy.deepcopy(self.to_dict(), memo)
        _value_group_idx = kwargs.pop("value_group_idx", None)
        obj = self.__class__(**kwargs)
        obj._value_group_idx = _value_group_idx
        obj._resource_list = self._resource_list
        return obj

    def __repr__(self):
        param_strs = ""
        for param in self.ALLOWED_PARAMETERS:
            try:
                i_val = getattr(self, param)
            except WorkflowParameterMissingError:
                continue
            if i_val is not None:
                param_strs += f", {param}={i_val!r}"

        return f"{self.__class__.__name__}(scope={self.scope}{param_strs})"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.to_dict() == other.to_dict()

    @classmethod
    def _json_like_constructor(cls, json_like) -> Self:
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""

        _value_group_idx = json_like.pop("value_group_idx", None)
        try:
            obj = cls(**json_like)
        except TypeError:
            given_keys = set(k for k in json_like if k != "scope")
            bad_keys = cls.__quoted(given_keys - cls.ALLOWED_PARAMETERS)
            good_keys = cls._allowed_params_quoted()
            raise UnknownResourceSpecItemError(
                f"The following resource item names are unknown: {bad_keys}. "
                f"Allowed resource item names are: {good_keys}."
            )
        obj._value_group_idx = _value_group_idx

        return obj

    @property
    def normalised_resources_path(self) -> str:
        """
        Standard name of this resource spec.
        """
        scope = self.scope
        assert scope is not None
        return scope.to_string()

    @property
    def normalised_path(self) -> str:
        """
        Full name of this resource spec.
        """
        return f"resources.{self.normalised_resources_path}"

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        out = super()._postprocess_to_dict(d)
        out.pop("_workflow", None)

        if self._value_group_idx is not None:
            # only store pointer to persistent data:
            out = {k: v for k, v in out.items() if k in ["_value_group_idx", "scope"]}
        else:
            out = {k: v for k, v in out.items() if v is not None}

        out = {k.lstrip("_"): v for k, v in out.items()}
        return out

    def _get_members(self):
        out = self.to_dict()
        out.pop("scope")
        out.pop("value_group_idx", None)
        out = {k: v for k, v in out.items() if v is not None}
        return out

    @classmethod
    def __is_Workflow(cls, value) -> TypeIs[Workflow]:
        return isinstance(value, cls._app.Workflow)

    def make_persistent(
        self, workflow: ResourcePersistingWorkflow, source: ParamSource
    ) -> tuple[str, list[int | list[int]], bool]:
        """Save to a persistent workflow.

        Returns
        -------
        String is the data path for this task input and integer list
        contains the indices of the parameter data Zarr groups where the data is
        stored.

        Note
        ----
        May modify the internal state of this object.
        """

        if self._value_group_idx is not None:
            data_ref = self._value_group_idx
            is_new = False
            if not workflow.check_parameters_exist(data_ref):
                raise RuntimeError(
                    f"{self.__class__.__name__} has a parameter group index "
                    f"({data_ref}), but does not exist in the workflow."
                )
            # TODO: log if already persistent.
        else:
            data_ref = workflow._add_parameter_data(self._get_members(), source=source)
            is_new = True
            self._value_group_idx = data_ref
            if self.__is_Workflow(workflow):
                self._workflow = workflow

            self._num_cores = None
            self._scratch = None
            self._scheduler = None
            self._shell = None
            self._use_job_array = None
            self._max_array_items = None
            self._write_app_logs = None
            self._combine_jobscript_std = None
            self._combine_scripts = None
            self._time_limit = None
            self._scheduler_args = None
            self._shell_args = None
            self._os_name = None
            self._environments = None
            self._resources_id = None
            self._skip_downstream_on_failure = None

        return (self.normalised_path, [data_ref], is_new)

    def copy_non_persistent(self):
        """Make a non-persistent copy."""
        kwargs = {"scope": self.scope}
        for name in self.ALLOWED_PARAMETERS:
            kwargs[name] = getattr(self, name)
        return self.__class__(**kwargs)

    def _get_value(self, value_name: str | None = None):
        if self._value_group_idx is not None and self.workflow:
            val = self.workflow.get_parameter_data(cast("int", self._value_group_idx))
        else:
            val = self._get_members()
        if value_name is not None and val is not None:
            return val.get(value_name)

        return val

    @staticmethod
    def _process_string(value: str | None):
        return value.lower().strip() if value else value

    def _setter_persistent_check(self):
        if self._value_group_idx:
            raise ValueError(
                f"Cannot set attribute of a persistent {self.__class__.__name__!r}."
            )

    @property
    def scratch(self) -> str | None:
        """
        Which scratch space to use.

        Todo
        ----
        Currently unused, except in tests.
        """
        return self._get_value("scratch")

    @property
    def parallel_mode(self) -> ParallelMode | None:
        """
        Which parallel mode to use.
        """
        return self._get_value("parallel_mode")

    @property
    def num_cores(self) -> int | None:
        """
        How many cores to request.
        """
        return self._get_value("num_cores")

    @property
    def num_cores_per_node(self) -> int | None:
        """
        How many cores per compute node to request.
        """
        return self._get_value("num_cores_per_node")

    @property
    def num_nodes(self) -> int | None:
        """
        How many compute nodes to request.
        """
        return self._get_value("num_nodes")

    @property
    def num_threads(self) -> int | None:
        """
        How many threads to request.
        """
        return self._get_value("num_threads")

    @property
    def scheduler(self) -> str | None:
        """
        Which scheduler to use.
        """
        return self._get_value("scheduler")

    @scheduler.setter
    def scheduler(self, value: str | None):
        self._setter_persistent_check()
        self._scheduler = self._process_string(value)

    @property
    def shell(self) -> str | None:
        """
        Which system shell to use.
        """
        return self._get_value("shell")

    @shell.setter
    def shell(self, value: str | None):
        self._setter_persistent_check()
        self._shell = self._process_string(value)

    @property
    def use_job_array(self) -> bool:
        """
        Whether to use array jobs.
        """
        return self._get_value("use_job_array")

    @property
    def max_array_items(self) -> int | None:
        """
        If using array jobs, up to how many items should be in the job array.
        """
        return self._get_value("max_array_items")

    @property
    def write_app_logs(self) -> bool:
        return self._get_value("write_app_logs")

    @property
    def combine_jobscript_std(self) -> bool:
        return self._get_value("combine_jobscript_std")

    @property
    def combine_scripts(self) -> bool:
        return self._get_value("combine_scripts")

    @property
    def time_limit(self) -> str | None:
        """
        How long to run for.
        """
        return self._get_value("time_limit")

    @property
    def scheduler_args(self) -> Mapping:  # TODO: TypedDict
        """
        Additional arguments to pass to the scheduler.
        """
        return self._get_value("scheduler_args")

    @property
    def shell_args(self) -> Mapping | None:  # TODO: TypedDict
        """
        Additional arguments to pass to the shell.
        """
        return self._get_value("shell_args")

    @property
    def os_name(self) -> str:
        """
        Which OS to use.
        """
        return self._get_value("os_name")

    @os_name.setter
    def os_name(self, value: str):
        self._setter_persistent_check()
        self._os_name = self._process_string(value)

    @property
    def environments(self) -> Mapping | None:  # TODO: TypedDict
        """
        Which execution environments to use.
        """
        return self._get_value("environments")

    @property
    def resources_id(self) -> int:
        return self._get_value("resources_id")

    @property
    def skip_downstream_on_failure(self) -> bool:
        return self._get_value("skip_downstream_on_failure")

    @property
    def SGE_parallel_env(self) -> str | None:
        """
        Which SGE parallel environment to request.
        """
        return self._get_value("SGE_parallel_env")

    @property
    def SLURM_partition(self) -> str | None:
        """
        Which SLURM partition to request.
        """
        return self._get_value("SLURM_partition")

    @property
    def SLURM_num_tasks(self) -> int | None:
        """
        How many SLURM tasks to request.
        """
        return self._get_value("SLURM_num_tasks")

    @property
    def SLURM_num_tasks_per_node(self) -> int | None:
        """
        How many SLURM tasks per compute node to request.
        """
        return self._get_value("SLURM_num_tasks_per_node")

    @property
    def SLURM_num_nodes(self) -> int | None:
        """
        How many compute nodes to request.
        """
        return self._get_value("SLURM_num_nodes")

    @property
    def SLURM_num_cpus_per_task(self) -> int | None:
        """
        How many CPU cores to ask for per SLURM task.
        """
        return self._get_value("SLURM_num_cpus_per_task")

    @property
    def workflow(self) -> Workflow | None:
        """
        The workflow owning this resource spec.
        """
        if self._workflow:
            return self._workflow

        elif self.element_set:
            # element-set-level resources
            wt = self.element_set.task_template.workflow_template
            return wt.workflow if wt else None

        elif self.workflow_template:
            # template-level resources
            return self.workflow_template.workflow

        elif self._value_group_idx is not None:
            raise RuntimeError(
                f"`{self.__class__.__name__}._value_group_idx` is set but the `workflow` "
                f"attribute is not. This might be because we are in the process of "
                f"creating the workflow object."
            )

        return None

    @property
    def element_set(self) -> ElementSet | None:
        """
        The element set that will use this resource spec.
        """
        if not self._resource_list:
            return None
        return self._resource_list.element_set

    @property
    def workflow_template(self) -> WorkflowTemplate | None:
        """
        The workflow template that will use this resource spec.
        """
        if not self._resource_list:
            return None
        return self._resource_list.workflow_template


#: How to specify a selection rule.
Where: TypeAlias = "RuleArgs | Rule | Sequence[RuleArgs | Rule] | ElementFilter"


class InputSource(JSONLike):
    """
    An input source to a workflow task.

    Parameters
    ----------
    source_type: InputSourceType
        Type of the input source.
    import_ref:
        Where the input comes from when the type is `IMPORT`.
    task_ref:
        Which task is this an input for? Used when the type is `TASK`.
    task_source_type: TaskSourceType
        Type of task source.
    element_iters:
        Which element iterations does this apply to?
    path:
        Path to where this input goes.
    where: ~hpcflow.app.Rule | list[~hpcflow.app.Rule] | ~hpcflow.app.ElementFilter
        Filtering rules.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="source_type",
            json_like_name="type",
            class_name="InputSourceType",
            is_enum=True,
        ),
    )

    @classmethod
    def __is_ElementFilter(cls, value) -> TypeIs[ElementFilter]:
        return isinstance(value, cls._app.ElementFilter)

    @classmethod
    def __is_Rule(cls, value) -> TypeIs[Rule]:
        return isinstance(value, cls._app.Rule)

    def __init__(
        self,
        source_type: InputSourceType | str,
        import_ref: int | None = None,
        task_ref: int | None = None,
        task_source_type: TaskSourceType | str | None = None,
        element_iters: list[int] | None = None,
        path: str | None = None,
        where: Where | None = None,
    ):
        if where is None or self.__is_ElementFilter(where):
            #: Filtering rules.
            self.where: ElementFilter | None = where
        else:
            self.where = self._app.ElementFilter(
                rules=[
                    rule if self.__is_Rule(rule) else self._app.Rule(**rule)
                    for rule in (where if isinstance(where, Sequence) else [where])
                ]
            )

        #: Type of the input source.
        self.source_type = get_enum_by_name_or_val(InputSourceType, source_type)
        #: Where the input comes from when the type is `IMPORT`.
        self.import_ref = import_ref
        #: Which task is this an input for? Used when the type is `TASK`.
        self.task_ref = task_ref
        #: Type of task source.
        self.task_source_type = get_enum_by_name_or_val(TaskSourceType, task_source_type)
        #: Which element iterations does this apply to?
        self.element_iters = element_iters
        #: Path to where this input goes.
        self.path = path

        if self.source_type is InputSourceType.TASK:
            if self.task_ref is None:
                raise ValueError("Must specify `task_ref` if `source_type` is TASK.")
            if self.task_source_type is None:
                self.task_source_type = TaskSourceType.OUTPUT

        if self.source_type is InputSourceType.IMPORT and self.import_ref is None:
            raise ValueError("Must specify `import_ref` if `source_type` is IMPORT.")

    def __eq__(self, other: Any):
        if not isinstance(other, self.__class__):
            return False
        return (
            self.source_type == other.source_type
            and self.import_ref == other.import_ref
            and self.task_ref == other.task_ref
            and self.task_source_type == other.task_source_type
            and self.element_iters == other.element_iters
            and self.where == other.where
            and self.path == other.path
        )

    def __repr__(self) -> str:
        assert self.source_type
        cls_method_name = self.source_type.name.lower()

        args_lst: list[str] = []

        if self.source_type is InputSourceType.IMPORT:
            cls_method_name += "_"
            args_lst.append(f"import_ref={self.import_ref}")

        elif self.source_type is InputSourceType.TASK:
            assert self.task_source_type
            args_lst.append(f"task_ref={self.task_ref}")
            args_lst.append(
                f"task_source_type={self.task_source_type.name.lower()!r}",
            )

        if self.element_iters is not None:
            args_lst.append(f"element_iters={self.element_iters}")

        if self.where is not None:
            args_lst.append(f"where={self.where!r}")

        args = ", ".join(args_lst)
        out = f"{self.__class__.__name__}.{cls_method_name}({args})"

        return out

    def get_task(self, workflow: Workflow) -> WorkflowTask | None:
        """If source_type is task, then return the referenced task from the given
        workflow."""
        if self.source_type is InputSourceType.TASK:
            return next(
                (task for task in workflow.tasks if task.insert_ID == self.task_ref), None
            )
        return None

    def is_in(self, other_input_sources: Sequence[InputSource]) -> int | None:
        """Check if this input source is in a list of other input sources, without
        considering the `element_iters` and `where` attributes."""

        for idx, other in enumerate(other_input_sources):
            if (
                self.source_type == other.source_type
                and self.import_ref == other.import_ref
                and self.task_ref == other.task_ref
                and self.task_source_type == other.task_source_type
                and self.path == other.path
            ):
                return idx
        return None

    def to_string(self) -> str:
        """
        Render this input source as a string.
        """
        out = [self.source_type.name.lower()]
        if self.source_type is InputSourceType.TASK:
            assert self.task_source_type
            out.append(str(self.task_ref))
            out.append(self.task_source_type.name.lower())
            if self.element_iters is not None:
                out.append(f'[{",".join(map(str, self.element_iters))}]')
        elif self.source_type is InputSourceType.IMPORT:
            out.append(str(self.import_ref))
        return ".".join(out)

    @classmethod
    def _validate_task_source_type(cls, task_src_type) -> None | TaskSourceType:
        if task_src_type is None:
            return None
        if isinstance(task_src_type, TaskSourceType):
            return task_src_type
        try:
            task_source_type = getattr(cls._app.TaskSourceType, task_src_type.upper())
        except AttributeError:
            raise ValueError(
                f"InputSource `task_source_type` specified as {task_src_type!r}, but "
                f"must be one of: {TaskSourceType.names!r}."
            )
        return task_source_type

    @classmethod
    def from_string(cls, str_defn: str) -> Self:
        """Parse a dot-delimited string definition of an InputSource.

        Parameter
        ---------
        str_defn:
            The string to parse.

        Examples
        --------
            task.[task_ref].input
            task.[task_ref].output
            local
            default
            import.[import_ref]
        """
        return cls(**cls._parse_from_string(str_defn))

    @staticmethod
    def _parse_from_string(str_defn: str) -> dict[str, Any]:
        """Parse a dot-delimited string definition of an InputSource.

        Examples
        --------
            task.[task_ref].input
            task.[task_ref].output
            local
            default
            import.[import_ref]
        """
        parts = str_defn.split(".")
        source_type = get_enum_by_name_or_val(InputSourceType, parts[0])
        task_ref: int | None = None
        task_source_type: TaskSourceType | None = None
        import_ref: int | None = None
        if (
            (
                source_type in (InputSourceType.LOCAL, InputSourceType.DEFAULT)
                and len(parts) > 1
            )
            or (source_type is InputSourceType.TASK and len(parts) > 3)
            or (source_type is InputSourceType.IMPORT and len(parts) > 2)
        ):
            raise ValueError(f"InputSource string not understood: {str_defn!r}.")

        if source_type is InputSourceType.TASK:
            # TODO: does this include element_iters?
            try:
                task_ref = int(parts[1])
            except ValueError:
                pass
            try:
                task_source_type = get_enum_by_name_or_val(TaskSourceType, parts[2])
            except IndexError:
                task_source_type = TaskSourceType.OUTPUT
        elif source_type is InputSourceType.IMPORT:
            try:
                import_ref = int(parts[1])
            except ValueError:
                pass

        return {
            "source_type": source_type,
            "import_ref": import_ref,
            "task_ref": task_ref,
            "task_source_type": task_source_type,
        }

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        if isinstance(json_like, str):
            json_like = cls._parse_from_string(json_like)
        return super().from_json_like(json_like, shared_data)

    @classmethod
    def import_(
        cls,
        import_ref: int,
        element_iters: list[int] | None = None,
        where: Where | None = None,
    ) -> Self:
        """
        Make an instance of an input source that is an import.

        Parameters
        ----------
        import_ref:
            Import reference.
        element_iters:
            Originating element iterations.
        where:
            Filtering rule.
        """
        return cls(
            source_type=InputSourceType.IMPORT,
            import_ref=import_ref,
            element_iters=element_iters,
            where=where,
        )

    @classmethod
    def local(cls) -> Self:
        """
        Make an instance of an input source that is local.
        """
        return cls(source_type=InputSourceType.LOCAL)

    @classmethod
    def default(cls) -> Self:
        """
        Make an instance of an input source that is default.
        """
        return cls(source_type=InputSourceType.DEFAULT)

    @classmethod
    def task(
        cls,
        task_ref: int,
        task_source_type: TaskSourceType | str | None = None,
        element_iters: list[int] | None = None,
        where: Where | None = None,
    ) -> Self:
        """
        Make an instance of an input source that is a task.

        Parameters
        ----------
        task_ref:
            Source task reference.
        task_source_type:
            Type of task source.
        element_iters:
            Originating element iterations.
        where:
            Filtering rule.
        """
        return cls(
            source_type=InputSourceType.TASK,
            task_ref=task_ref,
            task_source_type=get_enum_by_name_or_val(
                TaskSourceType, task_source_type or TaskSourceType.OUTPUT
            ),
            where=where,
            element_iters=element_iters,
        )
