"""Build-time workflow input references and arithmetic in template values."""

from __future__ import annotations

import ast
import copy
import math
import operator
import re
from collections.abc import Callable
from typing import Any, TYPE_CHECKING

from hpcflow.sdk.core.errors import TaskTemplateMultipleInputValues
from hpcflow.sdk.core.utils import get_in_container, split_param_label
from hpcflow.sdk.core.values import process_demo_data_strings

if TYPE_CHECKING:
    from hpcflow.sdk.core.parameters import InputValue


_INPUT = re.compile(r"<<input:([^<>]+)>>")
_CALC = re.compile(r"<<calc\((.*?)\)>>", re.DOTALL)
_BINARY: dict[type[ast.operator], Callable[[Any, Any], Any]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}


def _number(value: object) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(
            "Calculation operands and results must be numbers, not booleans."
        )
    if isinstance(value, int):
        if value.bit_length() > 4096:
            raise ValueError("Calculation integer exceeds the 4096-bit limit.")
    elif not math.isfinite(value):
        raise ValueError("Calculation operands and results must be finite.")
    return value


def _calculate(expression: str, inputs: dict[str, Any]) -> int | float:
    if len(expression) > 4096:
        raise ValueError("Calculation expression exceeds the 4096-character limit.")
    try:
        tree = ast.parse(expression.strip(), mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"Invalid calculation expression: {expression!r}.") from exc
    if sum(1 for _ in ast.walk(tree)) > 128:
        raise ValueError("Calculation expression is too complex (maximum 128 AST nodes).")

    def evaluate(node: ast.AST) -> int | float:
        if isinstance(node, ast.Constant):
            return _number(node.value)
        if isinstance(node, ast.Name) and node.id in inputs:
            return _number(inputs[node.id])
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            value = evaluate(node.operand)
            return value if isinstance(node.op, ast.UAdd) else -value
        if isinstance(node, ast.BinOp) and type(node.op) in _BINARY:
            left, right = evaluate(node.left), evaluate(node.right)
            if isinstance(node.op, ast.Pow):
                if abs(right) > 1000 or (
                    isinstance(left, int)
                    and isinstance(right, int)
                    and right > 0
                    and left.bit_length() * right > 4096
                ):
                    raise ValueError("Calculation exponent exceeds the supported limit.")
            try:
                return _number(_BINARY[type(node.op)](left, right))
            except ArithmeticError as exc:
                raise ValueError(f"Calculation failed: {expression!r}: {exc}.") from exc
        raise ValueError(
            "Calculations support only numeric literals, input references, parentheses, "
            "unary +/-, and +, -, *, /, //, %, ** operators."
        )

    return evaluate(tree.body)


def resolve_workflow_input_references(
    data: dict[str, Any],
    input_value_cls: type[InputValue] | None = None,
    calculations: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve static workflow inputs throughout a parsed template, without evaluating code.

    Schema-output multiplicity's plain input references remain per-element references.

    If `calculations` is given, it is updated with the result of each `<<calc(...)>>`
    expression, keyed by the expression text.
    """
    specs: dict[str, dict[str, Any]] = {}
    raw_inputs = data.get("inputs", {})
    if isinstance(raw_inputs, dict):
        input_specs = [
            {"parameter": key, "value": value} for key, value in raw_inputs.items()
        ]
    else:
        input_specs = raw_inputs
    for spec in input_specs:
        parameter, label = split_param_label(spec["parameter"].split("::")[0])
        if not parameter:
            raise ValueError(f"Invalid workflow input parameter: {spec['parameter']!r}.")
        parameter, _, path = parameter.partition(".")
        label = spec.get("label", label)
        path = spec.get("path", path)
        key = parameter + (f"[{label}]" if label is not None and str(label) else "")
        key += f".{path}" if path else ""
        if key in specs:
            raise TaskTemplateMultipleInputValues(
                f"Workflow inputs have multiple value definitions: {key!r}."
            )
        specs[key] = spec

    resolved: dict[str, Any] = {}
    resolving: list[str] = []
    materialised: dict[str, dict[str, Any]] = {}

    def lookup(name: str) -> Any:
        if name in resolved:
            return copy.deepcopy(resolved[name])
        if name in resolving:
            raise ValueError(
                f"Cyclic workflow input reference: {' -> '.join([*resolving, name])}."
            )
        if name not in specs:
            parent, separator, tail = name.rpartition(".")
            if separator:
                try:
                    return copy.deepcopy(
                        get_in_container(lookup(parent), [tail], cast_indices=True)
                    )
                except (KeyError, IndexError, TypeError, ValueError) as exc:
                    raise ValueError(
                        f"Workflow input {name!r} is not resolvable at build time: {exc}"
                    ) from exc
            raise ValueError(
                f"Workflow input {name!r} is not defined; references require a "
                "build-time workflow-level input, not a task input or output."
            )
        spec = specs[name]
        if spec.get("value_class_method") or spec.get("_value_group_idx") is not None:
            raise ValueError(f"Workflow input {name!r} is not resolvable at build time.")
        resolving.append(name)
        if "::" in spec["parameter"]:
            if input_value_cls is None:
                raise ValueError(
                    f"Workflow input {name!r} requires an input-value factory."
                )
            input_value = input_value_cls.from_json_like(
                walk(copy.deepcopy(spec), ("inputs", name))
            )
            if input_value.value_class_method is not None:
                raise ValueError(
                    f"Workflow input {name!r} uses a deferred value-class method "
                    "and cannot be resolved at build time."
                )
            value = input_value.value
            materialised[name] = {
                **input_value.to_dict(),
                "parameter": input_value.parameter.typ,
            }
        else:
            value = walk(spec.get("value", spec.get("_value")), ("inputs", name))
        resolving.pop()
        if input_value_cls is not None:
            value = process_demo_data_strings(input_value_cls._app, value)
        if has_placeholder(value):
            raise ValueError(
                f"Workflow input {name!r} contains a deferred reference and cannot be "
                "resolved at build time."
            )
        resolved[name] = value
        return copy.deepcopy(value)

    def has_placeholder(value: Any) -> bool:
        if isinstance(value, str):
            return "<<" in value and ">>" in value
        if isinstance(value, dict):
            return any(has_placeholder(item) for item in value.values())
        if isinstance(value, list):
            return any(has_placeholder(item) for item in value)
        return False

    def string(value: str) -> Any:
        def calculate(match: re.Match[str]) -> int | float:
            operands: dict[str, Any] = {}

            def operand(ref: re.Match[str]) -> str:
                key = f"_input_{len(operands)}"
                operands[key] = lookup(ref[1])
                return key

            result = _calculate(_INPUT.sub(operand, match[1]), operands)
            if calculations is not None:
                calculations[match[1].strip()] = result
            return result

        if (match := _CALC.search(value)) and match.span() == (0, len(value)):
            return calculate(match)
        value = _CALC.sub(lambda match: str(calculate(match)), value)
        if "<<calc(" in value:
            raise ValueError(f"Malformed calculation: {value!r}.")
        if match := _INPUT.fullmatch(value):
            return lookup(match[1])
        value = _INPUT.sub(lambda match: str(lookup(match[1])), value)
        if "<<input:" in value:
            raise ValueError(f"Malformed workflow input reference: {value!r}.")
        return value

    def walk(value: Any, path: tuple[str | int, ...]) -> Any:
        if isinstance(value, dict):
            return {key: walk(item, (*path, key)) for key, item in value.items()}
        if isinstance(value, list):
            return [walk(item, (*path, idx)) for idx, item in enumerate(value)]
        if isinstance(value, str):
            if (
                (
                    path[:2]
                    in (
                        ("template_components", "task_schemas"),
                        ("template_components", "meta_task_schemas"),
                    )
                    or "schema" in path
                )
                and len(path) >= 5
                and path[-3] == "outputs"
                and path[-1] == "multiplicity"
                and _INPUT.fullmatch(value)
            ):
                return value
            try:
                return string(value)
            except ValueError as exc:
                raise ValueError(f"Template value at {path!r}: {exc}") from exc
        return value

    result = {key: walk(value, (key,)) for key, value in data.items()}
    if materialised:
        result["inputs"] = [
            walk(materialised.get(name, spec), ("inputs", name))
            for name, spec in specs.items()
        ]
    return result
