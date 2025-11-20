"""
Importing parameter data from other workflows.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.parameters import InputSource, Parameter
from hpcflow.sdk.core.workflow import Workflow
from hpcflow.sdk.core.input_sources import (
    get_available_task_sources,
    validate_specified_source,
)
from typing_extensions import override, Self


class ImportParameter(JSONLike):
    """A parameter to import from another workflow.

    Parameters
    ----------
    parameter: Parameter
        The parameter to import from the other workflow.
    as_: str
        The parameter type that should be used to represent the imported parameter in the
        current workflow.
    source: Inputsource
        From where in the workflow to retrieve the parameter to import.

    """

    _child_objects = (
        ChildObjectSpec(
            name="parameter",
            class_name="Parameter",
            shared_data_name="parameters",
            shared_data_primary_key="typ",
        ),
        ChildObjectSpec(
            name="as_",
            json_like_name="as",
            class_name="Parameter",
            shared_data_name="parameters",
            shared_data_primary_key="typ",
        ),
        ChildObjectSpec(
            name="source",
            class_name="InputSource",
        ),
    )

    def __init__(
        self,
        parameter: Parameter | str,
        as_: Parameter | str | None = None,
        source: InputSource | None = None,
    ):

        if isinstance(parameter, str):
            parameter = self._app.Parameter(typ=parameter)

        as_ = as_ or parameter.typ
        if isinstance(as_, str):
            as_ = self._app.Parameter(typ=as_)

        self.parameter = parameter
        self.as_ = as_
        self.original_source = source

        # the parent `Import` object, assigned in `Import.__init__`:
        self.__import: Import | None = None

        # assigned when `__import` is set:
        self.source: InputSource | None = None

    @classmethod
    def _json_like_constructor(cls, json_like) -> Self:
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""
        orig_src = json_like.pop("original_source", None)
        obj = cls(**json_like)
        obj.original_source = orig_src
        return obj

    def _ensure_source(self, source: InputSource | str | None) -> InputSource:
        """
        Ensure the input source is resolved.

        This is deferred until the source workflow has been resolved by the parent
        `Import` object.

        """

        # TODO: `self.parameter.typ` is not the correct key here. e.g. what about
        # labelled?

        avail_key = self.parameter.typ

        # get available sources
        available: dict[str, list[InputSource]] = {}

        # mutates `available`:
        get_available_task_sources(
            self._app,
            inputs_path=avail_key,
            source_tasks=sorted(self.workflow.tasks, key=lambda x: x.index, reverse=True),
            available=available,
        )

        if isinstance(source, str):
            source = self._app.InputSource.from_string(source)
        elif source and not isinstance(source, self._app.InputSource):
            raise ValueError(f"Import source not understood: {source!r}.")

        if source:
            return validate_specified_source(
                specified=source,
                available=available[avail_key],
                workflow=self.workflow,
                input_path=avail_key,
            )
        else:
            # first item is defined by default to take precedence in
            # `get_available_task_input_sources`:
            return available[avail_key][0]

    def __repr__(self):
        out = f"{self.__class__.__name__}(parameter={self.parameter}"
        if self.as_:
            out += f", as_={self.as_}"
        if self.source:
            out += f", source={self.source}"
        return out + ")"

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return (
            self.parameter == other.parameter
            and self.as_ == other.as_
            and self.source == other.source
        )

    @property
    def import_(self) -> Import | None:
        return self.__import

    @import_.setter
    def import_(self, value: Import):
        self.__import = value
        # workflow is now accessible, so validate the source:
        self.source = self._ensure_source(self.original_source)

    @property
    def workflow(self) -> Workflow:
        assert self.import_
        return self.import_.workflow

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        out = super()._postprocess_to_dict(d)
        del out[f"_{self.__class__.__name__}__import"]
        return out


class Import(JSONLike):

    _child_objects = (
        ChildObjectSpec(
            name="parameters",
            class_name="ImportParameter",
            is_multiple=True,
            dict_key_attr="parameter",
            dict_val_attr="as_",
            parent_ref="import_",
        ),
    )

    def __init__(
        self,
        label: str,
        workflow: int | str | Path | Workflow,
        parameters: list[ImportParameter] | None = None,
    ):

        self.label = label
        self.workflow = (
            workflow
            if isinstance(workflow, self._app.Workflow)
            else self._app.Workflow(
                self._app._resolve_workflow_reference(str(workflow), None)
            )
        )
        self.parameters = parameters or [
            self._app.ImportParameter(self._app.Parameter(self.label))
        ]

        self._set_parent_refs()

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"label={self.label}, "
            f"workflow={self.workflow!r}, "
            f"parameters={self.parameters}"
            f")"
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return (
            self.label == other.label
            and self.workflow.path == other.workflow.path
            and self.parameters == other.parameters
        )

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        out = super()._postprocess_to_dict(d)
        out["workflow"] = self.workflow.path
        return out
