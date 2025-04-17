"""
Schema management.
"""

from __future__ import annotations
from collections.abc import Mapping, Sequence
from typing import Any, Generic, Protocol, TypeVar
from valida import Schema as ValidaSchema  # type: ignore
from hpcflow.sdk.core.utils import open_text_resource

T = TypeVar("T")


class ValidatedData(Protocol, Generic[T]):
    """
    Typed profile of ``valida.ValidatedData``.
    """

    @property
    def is_valid(self) -> bool: ...

    def get_failures_string(self) -> str: ...

    cast_data: T


class PreparedConditionCallable(Protocol):
    """
    Typed profile of ``valida.PreparedConditionCallable``.
    """

    @property
    def name(self) -> str: ...

    @property
    def args(self) -> tuple[str, ...]: ...


class Condition(Protocol):
    """
    Typed profile of ``valida.Condition``.
    """

    @property
    def callable(self) -> PreparedConditionCallable: ...


class Rule(Protocol):
    """
    Typed profile of ``valida.Rule``.
    """

    @property
    def condition(self) -> Condition: ...

    @property
    def path(self) -> object: ...


class Schema(Protocol):
    """
    Typed profile of ``valida.Schema``.
    """

    def validate(self, data: T) -> ValidatedData[T]: ...

    @property
    def rules(self) -> Sequence[Rule]: ...

    def add_schema(self, schema: Schema, root_path: Any = None) -> None: ...

    def to_tree(self, **kwargs) -> Sequence[Mapping[str, str]]: ...


def get_schema(filename) -> Schema:
    """
    Get a valida `Schema` object from the embedded data directory.

    Parameter
    ---------
    schema: str
        The name of the schema file within the resources package
        (:py:mod:`hpcflow.sdk.data`).
    """
    with open_text_resource("hpcflow.sdk.data", filename) as fh:
        schema_dat = fh.read()
    return ValidaSchema.from_yaml(schema_dat)
