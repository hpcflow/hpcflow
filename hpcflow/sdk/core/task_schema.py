"""
Abstract task, prior to instantiation.
"""

from __future__ import annotations
from contextlib import contextmanager
import copy
from dataclasses import dataclass
from importlib import import_module
from itertools import chain
from typing import TYPE_CHECKING
from html import escape

from rich import print as rich_print
from rich.table import Table
from rich.panel import Panel
from rich.markup import escape as rich_esc
from rich.text import Text

from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.enums import ParameterPropagationMode
from hpcflow.sdk.core.errors import EnvironmentPresetUnknownEnvironmentError
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.parameters import Parameter
from hpcflow.sdk.core.utils import check_valid_py_identifier

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence
    from typing import Any, ClassVar
    from typing_extensions import Self, TypeIs
    from .actions import Action
    from .object_list import ParametersList, TaskSchemasList
    from .parameters import InputValue, SchemaInput, SchemaOutput, SchemaParameter
    from .task import TaskTemplate
    from .types import ActParameterDependence
    from .workflow import Workflow
    from ..typing import ParamSource


@dataclass
@hydrate
class TaskObjective(JSONLike):
    """
    A thing that a task is attempting to achieve.

    Parameter
    ---------
    name: str
        The name of the objective. A valid Python identifier.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="name",
            is_single_attribute=True,
        ),
    )

    #: The name of the objective. A valid Python identifier.
    name: str

    def __post_init__(self):
        self.name = check_valid_py_identifier(self.name)

    @classmethod
    def _parse_from_string(cls, string):
        return string


class TaskSchema(JSONLike):
    """Class to represent the inputs, outputs and implementation mechanism of a given
    task.

    Parameters
    ----------
    objective:
        This is a string representing the objective of the task schema.
    actions:
        A list of Action objects whose commands are to be executed by the task.
    method:
        An optional string to label the task schema by its method.
    implementation:
        An optional string to label the task schema by its implementation.
    inputs:
        A list of SchemaInput objects that define the inputs to the task.
    outputs:
        A list of SchemaOutput objects that define the outputs of the task.
    version:
        The version of this task schema.
    parameter_class_modules:
        Where to find implementations of parameter value handlers.
    web_doc:
        True if this object should be included in the Sphinx documentation
        (normally only relevant for built-in task schemas). True by default.
    environment_presets:
        Information about default execution environments. Can be overridden in specific
        cases in the concrete tasks.
    """

    _validation_schema: ClassVar[str] = "task_schema_spec_schema.yaml"
    _hash_value = None
    _validate_actions = True

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(name="objective", class_name="TaskObjective"),
        ChildObjectSpec(
            name="inputs",
            class_name="SchemaInput",
            is_multiple=True,
            parent_ref="_task_schema",
        ),
        ChildObjectSpec(name="outputs", class_name="SchemaOutput", is_multiple=True),
        ChildObjectSpec(
            name="actions",
            class_name="Action",
            is_multiple=True,
            parent_ref="_task_schema",
        ),
    )

    @classmethod
    def __is_InputValue(cls, value) -> TypeIs[InputValue]:
        return isinstance(value, cls._app.InputValue)

    @classmethod
    def __is_Parameter(cls, value) -> TypeIs[Parameter]:
        return isinstance(value, cls._app.Parameter)

    @classmethod
    def __is_SchemaOutput(cls, value) -> TypeIs[SchemaOutput]:
        return isinstance(value, cls._app.SchemaOutput)

    def __init__(
        self,
        objective: TaskObjective | str,
        actions: list[Action] | None = None,
        method: str | None = None,
        implementation: str | None = None,
        inputs: list[Parameter | SchemaInput] | None = None,
        outputs: list[Parameter | SchemaParameter] | None = None,
        version: str | None = None,
        parameter_class_modules: list[str] | None = None,
        web_doc: bool | None = True,
        environment_presets: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None,
        doc: str = "",
        _hash_value: str | None = None,
    ):
        #: This is a string representing the objective of the task schema.
        self.objective = self.__coerce_objective(objective)
        #: A list of Action objects whose commands are to be executed by the task.
        self.actions = actions or []
        #: An optional string to label the task schema by its method.
        self.method = method
        #: An optional string to label the task schema by its implementation.
        self.implementation = implementation
        #: A list of SchemaInput objects that define the inputs to the task.
        self.inputs = self.__coerce_inputs(inputs or ())
        #: A list of SchemaOutput objects that define the outputs of the task.
        self.outputs = self.__coerce_outputs(outputs or ())
        #: Where to find implementations of parameter value handlers.
        self.parameter_class_modules = parameter_class_modules or []
        #: Whether this object should be included in the Sphinx documentation
        #: (normally only relevant for built-in task schemas).
        self.web_doc = web_doc
        #: Information about default execution environments.
        self.environment_presets = environment_presets
        #: Documentation information about the task schema.
        self.doc = doc
        self._hash_value = _hash_value

        self._set_parent_refs()

        # process `Action` script_data_in/out formats:
        for act in self.actions:
            act.process_script_data_formats()

        self._validate()
        self.actions = self.__expand_actions()
        #: The version of this task schema.
        self.version = version
        self._task_template: TaskTemplate | None = None  # assigned by parent Task

        self.__update_parameter_value_classes()

        if self.environment_presets:
            # validate against env names in actions:
            env_names = {act.get_environment_name() for act in self.actions}
            preset_envs = {
                preset_name
                for preset in self.environment_presets.values()
                for preset_name in preset
            }
            if bad_envs := preset_envs - env_names:
                raise EnvironmentPresetUnknownEnvironmentError(self.name, bad_envs)

        # if version is not None:  # TODO: this seems fragile
        #     self.assign_versions(
        #         version=version,
        #         app_data_obj_list=self._app.task_schemas
        #         if app.is_data_files_loaded
        #         else [],
        #     )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.objective.name!r})"

    @classmethod
    def __parameters(cls) -> ParametersList:
        # Workaround for a dumb mypy bug
        return cls._app.parameters

    @classmethod
    def __task_schemas(cls) -> TaskSchemasList:
        # Workaround for a dumb mypy bug
        return cls._app.task_schemas

    def __get_param_type_str(self, param: Parameter) -> str:
        type_fmt = "-"
        if param._validation:
            try:
                type_fmt = param._validation.to_tree()[0]["type_fmt"]
            except Exception:
                pass
        elif param._value_class:
            param_cls = param._value_class
            cls_url = (
                f"{self._app.docs_url}/reference/_autosummary/{param_cls.__module__}."
                f"{param_cls.__name__}"
            )
            type_fmt = f"[link={cls_url}]{param_cls.__name__}[/link]"
        return type_fmt

    def __format_parameter_type(self, param: Parameter) -> str:
        param_typ_fmt = param.typ
        if param.typ in self.__parameters().list_attrs():
            param_url = (
                f"{self._app.docs_url}/reference/template_components/"
                f"parameters.html#{param.url_slug}"
            )
            param_typ_fmt = f"[link={param_url}]{param_typ_fmt}[/link]"
        return param_typ_fmt

    def __get_info(self, include: Sequence[str] = ()):
        if not include:
            include = ("inputs", "outputs", "actions")

        tab = Table(show_header=False, box=None, padding=(0, 0), collapse_padding=True)
        tab.add_column(justify="right")
        tab.add_column()

        tab_ins_outs: Table | None = None
        if "inputs" in include or "outputs" in include:
            tab_ins_outs = Table(
                show_header=False,
                box=None,
                padding=(0, 1),
            )

            tab_ins_outs.add_column(justify="left")  # row heading ("Inputs" or "Outputs")
            tab_ins_outs.add_column()  # parameter name
            tab_ins_outs.add_column()  # type if available
            tab_ins_outs.add_column()  # default value (inputs only)
            tab_ins_outs.add_row()

        if "inputs" in include:
            assert tab_ins_outs
            if self.inputs:
                tab_ins_outs.add_row(
                    "",
                    Text("parameter", style="italic grey50"),
                    Text("type", style="italic grey50"),
                    Text("default", style="italic grey50"),
                )
            for inp_idx, inp in enumerate(self.inputs):
                def_str = "-"
                if not inp.multiple:
                    if self.__is_InputValue(inp.default_value):
                        if inp.default_value.value is None:
                            def_str = "None"
                        else:
                            def_str = f"{rich_esc(str(inp.default_value.value))!r}"
                tab_ins_outs.add_row(
                    "" if inp_idx > 0 else "[bold]Inputs[/bold]",
                    self.__format_parameter_type(inp.parameter),
                    self.__get_param_type_str(inp.parameter),
                    def_str,
                )

        if "outputs" in include:
            assert tab_ins_outs
            if "inputs" in include:
                tab_ins_outs.add_row()  # for spacing
            else:
                tab_ins_outs.add_row(
                    "",
                    Text("parameter", style="italic grey50"),
                    Text("type", style="italic grey50"),
                    "",
                )
            for out_idx, out in enumerate(self.outputs):
                tab_ins_outs.add_row(
                    "" if out_idx > 0 else "[bold]Outputs[/bold]",
                    self.__format_parameter_type(out.parameter),
                    self.__get_param_type_str(out.parameter),
                    "",
                )

        if tab_ins_outs:
            tab.add_row(tab_ins_outs)

        if "actions" in include:
            tab_acts = Table(
                show_header=False, box=None, padding=(1, 1), collapse_padding=True
            )
            tab_acts.add_column()
            tab_acts.add_row("[bold]Actions[/bold]")
            for act in self.actions:
                tab_cmds_i = Table(show_header=False, box=None)
                tab_cmds_i.add_column(justify="right")
                tab_cmds_i.add_column()
                if act.rules:
                    seen_rules = []  # bug: some rules seem to be repeated
                    for act_rule_j in act.rules:
                        if act_rule_j.rule in seen_rules:
                            continue
                        else:
                            seen_rules.append(act_rule_j.rule)
                        r_path = ""
                        if act_rule_j.rule.check_missing:
                            r_cond = f"check missing: {act_rule_j.rule.check_missing}"
                        elif act_rule_j.rule.check_exists:
                            r_cond = f"check exists: {act_rule_j.rule.check_exists}"
                        elif act_rule_j.rule.condition:
                            r_path = f"{act_rule_j.rule.path}: "
                            r_cond = str(act_rule_j.rule.condition.to_json_like())
                        else:
                            continue
                        tab_cmds_i.add_row(
                            "[italic]rule:[/italic]",
                            rich_esc(f"{r_path}{r_cond}"),
                        )
                tab_cmds_i.add_row(
                    "[italic]scope:[/italic]",
                    rich_esc(act.get_precise_scope().to_string()),
                )
                for cmd in act.commands:
                    cmd_str = "cmd" if cmd.command else "exe"
                    tab_cmds_i.add_row(
                        f"[italic]{cmd_str}:[/italic]",
                        rich_esc(cmd.command or cmd.executable or ""),
                    )
                    if cmd.stdout:
                        tab_cmds_i.add_row(
                            "[italic]out:[/italic]",
                            rich_esc(cmd.stdout),
                        )
                    if cmd.stderr:
                        tab_cmds_i.add_row(
                            "[italic]err:[/italic]",
                            rich_esc(cmd.stderr),
                        )

                tab_acts.add_row(tab_cmds_i)
            tab.add_row(tab_acts)
        else:
            tab.add_row()

        panel = Panel(tab, title=f"Task schema: {rich_esc(self.objective.name)!r}")
        return panel

    def basic_info(self) -> None:
        """Show inputs and outputs, formatted in a table."""
        rich_print(self.__get_info(include=("inputs", "outputs")))

    def info(self) -> None:
        """Show inputs, outputs, and actions, formatted in a table."""
        rich_print(self.__get_info(include=()))

    def get_info_html(self) -> str:
        """
        Describe the task schema as an HTML document.
        """

        def _format_parameter_type(param: Parameter) -> str:
            param_typ_fmt = param.typ
            if param.typ in param_types:
                param_url = (
                    f"{self._app.docs_url}/reference/template_components/"
                    f"parameters.html#{param.url_slug}"
                )
                param_typ_fmt = f'<a href="{param_url}">{param_typ_fmt}</a>'
            return param_typ_fmt

        def _get_param_type_str(param: Parameter) -> str:
            type_fmt = "-"
            if param._validation:
                try:
                    type_fmt = param._validation.to_tree()[0]["type_fmt"]
                except Exception:
                    pass
            elif param._value_class:
                param_cls = param._value_class
                cls_url = (
                    f"{self._app.docs_url}/reference/_autosummary/{param_cls.__module__}."
                    f"{param_cls.__name__}"
                )
                type_fmt = f'<a href="{cls_url}">{param_cls.__name__}</a>'
            return type_fmt

        def _prepare_script_data_format_table(
            script_data_grouped: Mapping[str, Mapping[str, Mapping[str, str]]],
        ) -> str:
            out = ""
            rows = ""
            for fmt, params in script_data_grouped.items():
                params_rows = "</tr><tr>".join(
                    f"<td><code>{k}</code></td><td><code>{v if v else ''}</code></td>"
                    for k, v in params.items()
                )
                rows += f'<tr><td rowspan="{len(params)}"><code>{fmt!r}</code></td>{params_rows}</tr>'
            if rows:
                out = f'<table class="script-data-format-table">{rows}</table>'

            return out

        param_types = self.__parameters().list_attrs()

        inputs_header_row = "<tr><th>parameter</th><th>type</th><th>default</th></tr>"
        input_rows = ""
        for inp in self.inputs:
            def_str = "-"
            if not inp.multiple:
                if self.__is_InputValue(inp.default_value):
                    if inp.default_value.value is None:
                        def_str = "None"
                    else:
                        def_str = f"{rich_esc(str(inp.default_value.value))!r}"

            param_str = _format_parameter_type(inp.parameter)
            type_str = _get_param_type_str(inp.parameter)
            input_rows += (
                f"<tr>"
                f"<td>{param_str}</td>"
                f"<td>{type_str}</td>"
                f"<td>{def_str}</td>"
                f"</tr>"
            )

        if input_rows:
            inputs_table = (
                f'<table class="schema-inputs-table">'
                f"{inputs_header_row}{input_rows}</table>"
            )
        else:
            inputs_table = (
                '<span class="schema-note-no-inputs">This task schema has no input '
                "parameters.</span>"
            )

        outputs_header_row = "<tr><th>parameter</th><th>type</th></tr>"
        output_rows = ""
        for out in self.outputs:
            param_str = _format_parameter_type(out.parameter)
            type_str = _get_param_type_str(out.parameter)
            output_rows += f"<tr>" f"<td>{param_str}</td>" f"<td>{type_str}</td>" f"</tr>"

        if output_rows:
            outputs_table = (
                f'<table class="schema-inputs-table">{outputs_header_row}{output_rows}'
                f"</table>"
            )

        else:
            outputs_table = (
                '<span class="schema-note-no-outputs">This task schema has no output '
                "parameters.</span>"
            )

        action_rows = ""
        for act_idx, act in enumerate(self.actions):
            act_i_rules = ""
            if act.rules:
                seen_rules = []  # bug: some rules seem to be repeated
                for act_rule_j in act.rules:
                    if act_rule_j.rule in seen_rules:
                        continue
                    else:
                        seen_rules.append(act_rule_j.rule)
                    r_path = ""
                    if act_rule_j.rule.check_missing:
                        r_cond = f"check missing: {act_rule_j.rule.check_missing!r}"
                    elif act_rule_j.rule.check_exists:
                        r_cond = f"check exists: {act_rule_j.rule.check_exists!r}"
                    elif act_rule_j.rule.condition:
                        r_path = f"{act_rule_j.rule.path}: "
                        r_cond = str(act_rule_j.rule.condition.to_json_like())
                    else:
                        continue
                    act_i_rules += f"<div><code>{r_path}{r_cond}</code></div>"

            act_i_script_rows = ""
            num_script_rows = 0
            if act.script:
                act_i_script_rows += (
                    f'<tr><td class="action-header-cell">script:</td>'
                    f"<td><code>{escape(act.script)}</code></td></tr>"
                )
                num_script_rows += 1
            if act.script_exe:
                act_i_script_rows += (
                    f'<tr><td class="action-header-cell">script exe:</td>'
                    f"<td><code>{escape(act.script_exe)}</code></td></tr>"
                )
                num_script_rows += 1
            if act.script_data_in_grouped:
                act_i_script_rows += (
                    f'<tr><td class="action-header-cell">script data-in:</td>'
                    f"<td>{_prepare_script_data_format_table(act.script_data_in_grouped)}"
                    f"</td></tr>"
                )
                num_script_rows += 1
            if act.script_data_out_grouped:
                act_i_script_rows += (
                    f'<tr><td class="action-header-cell">script data-out:</td>'
                    f"<td>{_prepare_script_data_format_table(act.script_data_out_grouped)}"
                    f"</td></tr>"
                )
                num_script_rows += 1

            inp_fg_rows = ""
            num_inp_fg_rows = 0
            if act.input_file_generators:
                inp_fg = act.input_file_generators[0]  # should be only one
                inps = ", ".join(f"<code>{in_.typ}</code>" for in_ in inp_fg.inputs)
                inp_fg_rows += (
                    f"<tr>"
                    f'<td class="action-header-cell">input file:</td>'
                    f"<td><code>{inp_fg.input_file.label}</code></td>"
                    f"</tr>"
                    f"<tr>"
                    f'<td class="action-header-cell">inputs:</td>'
                    f"<td>{inps}</td>"
                    f"</tr>"
                )
                num_inp_fg_rows += 2

            out_fp_rows = ""
            num_out_fp_rows = 0
            if act.output_file_parsers:
                out_fp = act.output_file_parsers[0]  # should be only one
                files = ", ".join(
                    f"<code>{of_.label}</code>" for of_ in out_fp.output_files
                )
                out_fp_rows += (
                    f"<tr>"
                    f'<td class="action-header-cell">output:</td>'
                    f"<td><code>{out_fp.output.typ if out_fp.output else ''}</code></td>"
                    f"</tr>"
                    f"<tr>"
                    f'<td class="action-header-cell">output files:</td>'
                    f"<td>{files}</td>"
                    f"</tr>"
                )
                num_out_fp_rows += 2

            act_i_cmds_tab_rows = ""
            for cmd_idx, cmd in enumerate(act.commands):
                cmd_j_tab_rows = (
                    f'<tr><td colspan="3" class="commands-table-top-spacer-cell"></td>'
                    f"</tr><tr>"
                    f'<td rowspan="{bool(cmd.stdout) + bool(cmd.stderr) + 1}">'
                    f'<span class="cmd-idx-numeral">{cmd_idx}</span></td>'
                    f'<td class="command-header-cell">{"cmd" if cmd.command else "exe"}:'
                    f"</td><td><code><pre>{escape(cmd.command or cmd.executable or '')}</pre>"
                    f"</code></td></tr>"
                )
                if cmd.stdout:
                    cmd_j_tab_rows += (
                        f'<tr><td class="command-header-cell">out:</td>'
                        f"<td><code>{escape(cmd.stdout)}</code></td></tr>"
                    )
                if cmd.stderr:
                    cmd_j_tab_rows += (
                        f'<tr><td class="command-header-cell">err:</td>'
                        f"<td><code>{escape(cmd.stderr)}</code></td></tr>"
                    )
                if cmd_idx < len(act.commands) - 1:
                    cmd_j_tab_rows += (
                        '<tr><td colspan="3" class="commands-table-bottom-spacer-cell">'
                        "</td></tr>"
                    )
                act_i_cmds_tab_rows += cmd_j_tab_rows

            act_i_cmds_tab = (
                f'<table class="actions-commands-table">{act_i_cmds_tab_rows}</table>'
            )

            idx_rowspan = 4 + num_script_rows + num_inp_fg_rows + num_out_fp_rows
            action_rows += (
                f'<tr><td colspan="3" class="action-table-top-spacer-cell"></td></tr>'
                f'<tr><td rowspan="{idx_rowspan}" class="act-idx-cell">'
                f'<span class="act-idx-numeral">{act_idx}</span></td>'
                f'<td class="action-header-cell">rules:</td><td>{act_i_rules or "-"}</td>'
                f'</tr><tr><td class="action-header-cell">scope:</td>'
                f"<td><code>{act.get_precise_scope().to_string()}</code></td></tr>"
                f'<tr><td class="action-header-cell">environment:</td>'
                f"<td><code>{act.get_environment_name()}</code></td></tr>"
                f"{inp_fg_rows}"
                f"{out_fp_rows}"
                f"{act_i_script_rows}"
                f'<tr class="action-commands-row">'
                f'<td class="action-header-cell" colspan="2">'
                f"commands:{act_i_cmds_tab}</td></tr>"
                f'<tr><td colspan="3" class="action-table-bottom-spacer-cell"></td></tr>'
            )

        if action_rows:
            action_table = f'<table class="action-table hidden">{action_rows}</table>'
            action_show_hide = (
                '<span class="actions-show-hide-toggle">[<span class="action-show-text">'
                'show ↓</span><span class="action-hide-text hidden">hide ↑</span>]'
                "</span>"
            )
            act_heading_class = ' class="actions-heading"'
        else:
            action_table = (
                '<span class="schema-note-no-actions">'
                "This task schema has no actions.</span>"
            )
            action_show_hide = ""
            act_heading_class = ""
        description = (
            f"<h3 class='task-desc'>Description</h3>{self.doc}" if self.doc else ""
        )
        return (
            f"{description}"
            f"<h3>Inputs</h3>{inputs_table}"
            f"<h3>Outputs</h3>{outputs_table}"
            # f"<h3>Examples</h3>examples here..." # TODO:
            f"<h3{act_heading_class}>Actions{action_show_hide}</h3>"
            f"{action_table}"
        )

    def __eq__(self, other: Any):
        if id(self) == id(other):
            return True
        if not isinstance(other, self.__class__):
            return False
        return (
            self.objective == other.objective
            and self.actions == other.actions
            and self.method == other.method
            and self.implementation == other.implementation
            and self.inputs == other.inputs
            and self.outputs == other.outputs
            and self.version == other.version
            and self._hash_value == other._hash_value
        )

    def __deepcopy__(self, memo: dict[int, Any]) -> Self:
        kwargs = self.to_dict()
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._task_template = self._task_template
        return obj

    @classmethod
    @contextmanager
    def ignore_invalid_actions(cls) -> Iterator[None]:
        """
        A context manager within which invalid actions will be ignored.
        """
        try:
            cls._validate_actions = False
            yield
        finally:
            cls._validate_actions = True

    @classmethod
    def __coerce_objective(cls, objective: TaskObjective | str) -> TaskObjective:
        if isinstance(objective, str):
            return cls._app.TaskObjective(objective)
        else:
            return objective

    @classmethod
    def __coerce_one_input(cls, inp: Parameter | SchemaInput) -> SchemaInput:
        return cls._app.SchemaInput(inp) if cls.__is_Parameter(inp) else inp

    @classmethod
    def __coerce_inputs(
        cls, inputs: Iterable[Parameter | SchemaInput]
    ) -> list[SchemaInput]:
        """coerce Parameters to SchemaInputs"""
        return [cls.__coerce_one_input(inp) for inp in inputs]

    @classmethod
    def __coerce_one_output(cls, out: Parameter | SchemaParameter) -> SchemaOutput:
        return (
            out
            if cls.__is_SchemaOutput(out)
            else cls._app.SchemaOutput(out if cls.__is_Parameter(out) else out.parameter)
        )

    @classmethod
    def __coerce_outputs(
        cls, outputs: Iterable[Parameter | SchemaParameter]
    ) -> list[SchemaOutput]:
        """coerce Parameters to SchemaOutputs"""
        return [cls.__coerce_one_output(out) for out in outputs]

    def _validate(self) -> None:
        if self.method:
            self.method = check_valid_py_identifier(self.method)
        if self.implementation:
            self.implementation = check_valid_py_identifier(self.implementation)

        # check action input/outputs
        if self._validate_actions:
            has_script = any(
                act.script
                and not act.input_file_generators
                and not act.output_file_parsers
                for act in self.actions
            )

            all_outs: set[str] = set()
            extra_ins = set(self.input_types)

            act_ins_lst = [act.get_input_types() for act in self.actions]
            act_outs_lst = [act.get_output_types() for act in self.actions]

            schema_outs = set(self.output_types)

            all_act_ins = set(chain.from_iterable(act_ins_lst))
            all_act_outs = set(chain.from_iterable(act_outs_lst))

            non_schema_act_ins = all_act_ins.difference(self.input_types)
            non_schema_act_outs = all_act_outs.difference(schema_outs)

            extra_act_outs = non_schema_act_outs
            seen_act_outs: set[str] = set()
            for act_idx in range(len(self.actions)):
                for act_in in act_ins_lst[act_idx]:
                    if act_in in non_schema_act_ins and act_in not in seen_act_outs:
                        raise ValueError(
                            f"Action {act_idx} input {act_in!r} of schema {self.name!r} "
                            f"is not a schema input, but nor is it an action output from "
                            f"a preceding action."
                        )
                seen_act_outs.update(act_outs_lst[act_idx])
                extra_act_outs.difference_update(act_ins_lst[act_idx])
                extra_ins.difference_update(act_ins_lst[act_idx])
                all_outs.update(act_outs_lst[act_idx])

            if extra_act_outs:
                raise ValueError(
                    f"The following action outputs of schema {self.name!r} are not schema"
                    f" outputs, but nor are they consumed by subsequent actions as "
                    f"action inputs: {tuple(extra_act_outs)!r}."
                )

            if extra_ins and not has_script:
                # TODO: bit of a hack, need to consider script ins/outs later
                # i.e. are all schema inputs "consumed" by an action?

                # consider OFP inputs:
                for act in self.actions:
                    for ofp in act.output_file_parsers:
                        extra_ins.difference_update(ofp.inputs or ())

                if self.actions and extra_ins:
                    # allow for no actions (e.g. defining inputs for downstream tasks)
                    raise ValueError(
                        f"Schema {self.name!r} inputs {tuple(extra_ins)!r} are not used "
                        f"by any actions."
                    )

            missing_outs = schema_outs - all_outs
            if missing_outs and not has_script:
                # TODO: bit of a hack, need to consider script ins/outs later
                raise ValueError(
                    f"Schema {self.name!r} outputs {tuple(missing_outs)!r} are not "
                    f"generated by any actions."
                )

    def __expand_actions(self) -> list[Action]:
        """Create new actions for input file generators and output parsers in existing
        actions."""
        return [new_act for act in self.actions for new_act in act.expand()]

    def __update_parameter_value_classes(self):
        # ensure any referenced parameter_class_modules are imported:
        for module in self.parameter_class_modules:
            import_module(module)

        # TODO: support specifying file paths in addition to (instead of?) importable
        # module paths

        for inp in self.inputs:
            inp.parameter._set_value_class()

        for out in self.outputs:
            out.parameter._set_value_class()

    def make_persistent(
        self, workflow: Workflow, source: ParamSource
    ) -> list[int | list[int]]:
        """
        Convert this task schema to persistent form within the context of the given
        workflow.
        """
        new_refs: list[int | list[int]] = []
        for input_i in self.inputs:
            for lab_info in input_i.labelled_info():
                if "default_value" in lab_info:
                    _, dat_ref, is_new = lab_info["default_value"].make_persistent(
                        workflow, source
                    )
                    new_refs.extend(dat_ref) if is_new else None
        return new_refs

    @property
    def name(self) -> str:
        """
        The name of this schema.
        """
        return (
            f"{self.objective.name}"
            f"{f'_{self.method}' if self.method else ''}"
            f"{f'_{self.implementation}' if self.implementation else ''}"
        )

    @property
    def input_types(self) -> list[str]:
        """
        The input types to the schema.
        """
        return [typ for inp in self.inputs for typ in inp.all_labelled_types]

    @property
    def output_types(self) -> list[str]:
        """
        The output types from the schema.
        """
        return [out.typ for out in self.outputs]

    @property
    def provides_parameters(self) -> Iterator[tuple[str, str]]:
        """
        The parameters that this schema provides.
        """
        for schema_inp in self.inputs:
            for label, prop_mode in schema_inp._simple_labelled_info:
                if prop_mode is not ParameterPropagationMode.NEVER:
                    yield (schema_inp.input_or_output, label)
        for schema_out in self.outputs:
            if schema_out.propagation_mode is not ParameterPropagationMode.NEVER:
                yield (schema_out.input_or_output, schema_out.typ)

    @property
    def task_template(self) -> TaskTemplate | None:
        """
        The template that this schema is contained in.
        """
        return self._task_template

    @classmethod
    def get_by_key(cls, key: str) -> TaskSchema:
        """Get a config-loaded task schema from a key."""
        return cls.__task_schemas().get(key)

    def get_parameter_dependence(
        self, parameter: SchemaParameter
    ) -> ActParameterDependence:
        """Find if/where a given parameter is used by the schema's actions."""
        out: ActParameterDependence = {"input_file_writers": [], "commands": []}
        for act_idx, action in enumerate(self.actions):
            deps = action.get_parameter_dependence(parameter)
            out["input_file_writers"].extend(
                (act_idx, ifw) for ifw in deps["input_file_writers"]
            )
            out["commands"].extend((act_idx, cmd) for cmd in deps["commands"])
        return out

    def get_key(self) -> tuple:
        """
        Get the hashable value that represents this schema.
        """
        return (str(self.objective), self.method, self.implementation)

    def _get_single_label_lookup(self, prefix: str = "") -> Mapping[str, str]:
        """
        Get a mapping between schema input types that have a single label (i.e.
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
        if prefix and not prefix.endswith("."):
            prefix += "."
        for sch_inp in self.inputs:
            if not sch_inp.multiple and sch_inp.single_label:
                labelled_type = sch_inp.single_labelled_type
                lookup[f"{prefix}{labelled_type}"] = f"{prefix}{sch_inp.typ}"
        return lookup

    @property
    def multi_input_types(self) -> list[str]:
        """Get a list of input types that have multiple labels."""
        return [inp.parameter.typ for inp in self.inputs if inp.multiple]


class MetaTaskSchema(TaskSchema):
    """Class to represent a task schema with no actions, that can be used to represent the
    effect of multiple task schemas.

    Parameters
    ----------
    objective:
        This is a string representing the objective of the task schema.
    method:
        An optional string to label the task schema by its method.
    implementation:
        An optional string to label the task schema by its implementation.
    inputs:
        A list of SchemaInput objects that define the inputs to the task.
    outputs:
        A list of SchemaOutput objects that define the outputs of the task.
    version:
        The version of this task schema.
    web_doc:
        True if this object should be included in the Sphinx documentation
        (normally only relevant for built-in task schemas). True by default.
    environment_presets:
        Information about default execution environments. Can be overridden in specific
        cases in the concrete tasks.
    """

    _validation_schema: ClassVar[str] = "task_schema_spec_schema.yaml"
    _hash_value = None
    _validate_actions = False

    _child_objects = (
        ChildObjectSpec(name="objective", class_name="TaskObjective"),
        ChildObjectSpec(
            name="inputs",
            class_name="SchemaInput",
            is_multiple=True,
            parent_ref="_task_schema",
        ),
        ChildObjectSpec(name="outputs", class_name="SchemaOutput", is_multiple=True),
    )

    def __init__(
        self,
        objective: TaskObjective | str,
        method: str | None = None,
        implementation: str | None = None,
        inputs: list[Parameter | SchemaInput] | None = None,
        outputs: list[Parameter | SchemaParameter] | None = None,
        version: str | None = None,
        web_doc: bool | None = True,
        environment_presets: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None,
        doc: str = "",
        _hash_value: str | None = None,
    ):
        super().__init__(
            objective=objective,
            method=method,
            implementation=implementation,
            inputs=inputs,
            outputs=outputs,
            version=version,
            web_doc=web_doc,
            environment_presets=environment_presets,
            doc=doc,
            _hash_value=_hash_value,
        )
