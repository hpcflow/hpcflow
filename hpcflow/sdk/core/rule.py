"""
Rules apply conditions to workflow elements or loops.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from valida.conditions import ConditionLike  # type: ignore
from valida import Rule as ValidaRule  # type: ignore

from hpcflow.sdk.core.json_like import JSONLike
from hpcflow.sdk.core.utils import get_in_container
from hpcflow.sdk.log import TimeIt

if TYPE_CHECKING:
    from typing import Any
    from typing_extensions import TypeIs
    from .actions import Action, ElementActionRun
    from .element import ElementIteration


class Rule(JSONLike):
    """
    Class to represent a testable condition on an element iteration or run.

    Exactly one of ``check_exists``, ``check_missing`` and ``condition`` must be provided.

    Parameters
    ----------
    check_exists: str
        If set, check this attribute exists.
    check_missing: str
        If set, check this attribute does *not* exist.
    path: str
        Where to look up the attribute to check.
        If not specified, determined by context.
    condition: ConditionLike
        A general condition to check (or kwargs used to generate one).
    cast: str
        If set, a cast to apply prior to running the general check.
    doc: str
        Optional descriptive text.
    """

    def __init__(
        self,
        check_exists: str | None = None,
        check_missing: str | None = None,
        path: str | None = None,
        condition: dict[str, Any] | ConditionLike | None = None,
        cast: str | None = None,
        doc: str | None = None,
    ):
        if sum(arg is not None for arg in (check_exists, check_missing, condition)) != 1:
            raise ValueError(
                "Specify either one of `check_exists`, `check_missing` or a `condition` "
                "(and optional `path`)"
            )

        if not isinstance(condition, dict):
            #: A general condition for this rule to check.
            self.condition = condition
        else:
            self.condition = ConditionLike.from_json_like(condition)

        #: If set, this rule checks this attribute exists.
        self.check_exists = check_exists
        #: If set, this rule checks this attribute does *not* exist.
        self.check_missing = check_missing
        #: Where to look up the attribute to check (if not determined by context).
        self.path = path
        #: If set, a cast to apply prior to running the general check.
        self.cast = cast
        #: Optional descriptive text.
        self.doc = doc

    def __repr__(self) -> str:
        out = f"{self.__class__.__name__}("
        if self.check_exists:
            out += f"check_exists={self.check_exists!r}"
        elif self.check_missing:
            out += f"check_missing={self.check_missing!r}"
        else:
            out += f"condition={self.condition!r}"
            if self.path:
                out += f", path={self.path!r}"
            if self.cast:
                out += f", cast={self.cast!r}"

        out += ")"
        return out

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return (
            self.check_exists == other.check_exists
            and self.check_missing == other.check_missing
            and self.path == other.path
            and self.condition == other.condition
            and self.cast == other.cast
            and self.doc == other.doc
        )

    @classmethod
    def __is_ElementIteration(cls, value) -> TypeIs[ElementIteration]:
        return isinstance(value, cls._app.ElementIteration)

    @TimeIt.decorator
    def test(
        self,
        element_like: ElementIteration | ElementActionRun,
        action: Action | None = None,
    ) -> bool:
        """Test if the rule evaluates to true or false for a given run, or element
        iteration and action combination."""

        task = element_like.task
        schema_data_idx = element_like.data_idx

        if check := self.check_exists or self.check_missing:
            if len(check.split(".")) > 2:
                # sub-parameter, so need to try to retrieve parameter data
                try:
                    task._get_merged_parameter_data(
                        schema_data_idx, raise_on_missing=True
                    )
                    return bool(self.check_exists)
                except ValueError:
                    return not self.check_exists
            else:
                if self.check_exists:
                    return self.check_exists in schema_data_idx
                elif self.check_missing:
                    return self.check_missing not in schema_data_idx
        else:
            if self.path and self.path.startswith("resources."):
                if self.__is_ElementIteration(element_like):
                    assert action is not None
                    elem_res = element_like.get_resources(
                        action=action, set_defaults=True
                    )
                else:
                    elem_res = element_like.get_resources()

                res_path = self.path.split(".")[1:]
                element_dat = get_in_container(
                    cont=elem_res, path=res_path, cast_indices=True
                )
            else:
                element_dat = element_like.get(
                    self.path,
                    raise_on_missing=True,
                    raise_on_unset=True,
                )
            # test the rule:
            return self._valida_check(element_dat)

        # Something bizarre was specified. Don't match it!
        return False

    def _valida_check(self, value: Any) -> bool:
        """
        Check this rule against the specific object, under the assumption that we need
        to use valida for the check. Does not do path tracing to select the object to
        pass; that is the caller's responsibility.
        """
        # note: Valida can't `rule.test` scalars yet, so wrap it in a list and set
        # path to first element (see: https://github.com/hpcflow/valida/issues/9):
        rule = ValidaRule(
            path=[0],
            condition=self.condition,
            cast=self.cast,
        )
        return rule.test([value]).is_valid
