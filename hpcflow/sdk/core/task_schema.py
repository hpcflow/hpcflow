"""
Abstract task, prior to instantiation.
"""

from contextlib import contextmanager
import copy
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Dict, List, Optional, Tuple, Union
from html import escape

from rich import print as rich_print
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.markup import escape as rich_esc
from rich.text import Text

from hpcflow.sdk import app
from hpcflow.sdk.core.errors import EnvironmentPresetUnknownEnvironmentError
from hpcflow.sdk.core.parameters import Parameter
from .json_like import ChildObjectSpec, JSONLike
from .parameters import NullDefault, ParameterPropagationMode, SchemaInput
from .utils import check_valid_py_identifier


@dataclass
class TaskObjective(JSONLike):
    """
    A thing that a task is attempting to achieve.

    Parameter
    ---------
    name: str
        The name of the objective. A valid Python identifier.
    """

    _child_objects = (
        ChildObjectSpec(
            name="name",
            is_single_attribute=True,
        ),
    )

    #: The name of the objective. A valid Python identifier.
    name: str

    def __post_init__(self):
        self.name = check_valid_py_identifier(self.name)


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

    _validation_schema = "task_schema_spec_schema.yaml"
    _hash_value = None
    _validate_actions = True

    _child_objects = (
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

    def __init__(
        self,
        objective: Union[app.TaskObjective, str],
        actions: List[app.Action] = None,
        method: Optional[str] = None,
        implementation: Optional[str] = None,
        inputs: Optional[List[Union[app.Parameter, app.SchemaInput]]] = None,
        outputs: Optional[List[Union[app.Parameter, app.SchemaOutput]]] = None,
        version: Optional[str] = None,
        parameter_class_modules: Optional[List[str]] = None,
        web_doc: Optional[bool] = True,
        environment_presets: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,
        doc: str = "",
        _hash_value: Optional[str] = None,
    ):
        #: This is a string representing the objective of the task schema.
        self.objective = objective
        #: A list of Action objects whose commands are to be executed by the task.
        self.actions = actions or []
        #: An optional string to label the task schema by its method.
        self.method = method
        #: An optional string to label the task schema by its implementation.
        self.implementation = implementation
        #: A list of SchemaInput objects that define the inputs to the task.
        self.inputs = inputs or []
        #: A list of SchemaOutput objects that define the outputs of the task.
        self.outputs = outputs or []
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
        for i in self.actions:
            i.process_script_data_formats()

        self._validate()
        self.actions = self._expand_actions()
        #: The version of this task schema.
        self.version = version
        self._task_template = None  # assigned by parent Task

        self._update_parameter_value_classes()

        if self.environment_presets:
            # validate against env names in actions:
            env_names = {act.get_environment_name() for act in self.actions}
            preset_envs = {i for v in self.environment_presets.values() for i in v.keys()}
            bad_envs = preset_envs - env_names
            if bad_envs:
                raise EnvironmentPresetUnknownEnvironmentError(
                    f"Task schema {self.name} has environment presets that refer to one "
                    f"or more environments that are not referenced in any of the task "
                    f"schema's actions: {', '.join(f'{i!r}' for i in bad_envs)}."
                )

        # if version is not None:  # TODO: this seems fragile
        #     self.assign_versions(
        #         version=version,
        #         app_data_obj_list=self.app.task_schemas
        #         if app.is_data_files_loaded
        #         else [],
        #     )

    def __repr__(self):
        return f"{self.__class__.__name__}({self.objective.name!r})"

    def _get_info(self, include=None):
        def _get_param_type_str(parameter) -> str:
            type_fmt = "-"
            if parameter._validation:
                try:
                    type_fmt = parameter._validation.to_tree()[0]["type_fmt"]
                except Exception:
                    pass
            elif parameter._value_class:
                param_cls = parameter._value_class
                cls_url = (
                    f"{self.app.docs_url}/reference/_autosummary/{param_cls.__module__}."
                    f"{param_cls.__name__}"
                )
                type_fmt = f"[link={cls_url}]{param_cls.__name__}[/link]"
            return type_fmt

        def _format_parameter_type(param) -> str:
            param_typ_fmt = param.typ
            if param.typ in self.app.parameters.list_attrs():
                param_url = (
                    f"{self.app.docs_url}/reference/template_components/"
                    f"parameters.html#{param.url_slug}"
                )
                param_typ_fmt = f"[link={param_url}]{param_typ_fmt}[/link]"
            return param_typ_fmt

        if not include:
            include = ("inputs", "outputs", "actions")

        tab = Table(show_header=False, box=None, padding=(0, 0), collapse_padding=True)
        tab.add_column(justify="right")
        tab.add_column()

        from rich.table import box

        tab_ins_outs = None
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
                    if inp.default_value is not NullDefault.NULL:
                        if inp.default_value.value is None:
                            def_str = "None"
                        else:
                            def_str = f"{rich_esc(str(inp.default_value.value))!r}"
                tab_ins_outs.add_row(
                    "" if inp_idx > 0 else "[bold]Inputs[/bold]",
                    _format_parameter_type(inp.parameter),
                    _get_param_type_str(inp.parameter),
                    def_str,
                )

        if "outputs" in include:
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
                    _format_parameter_type(out.parameter),
                    _get_param_type_str(out.parameter),
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
                        rich_esc(cmd.command or cmd.executable),
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

    def _show_info(self, include=None):
        panel = self._get_info(include=include)
        rich_print(panel)

    @property
    def basic_info(self):
        """Show inputs and outputs, formatted in a table."""
        return self._show_info(include=("inputs", "outputs"))

    @property
    def info(self):
        """Show inputs, outputs, and actions, formatted in a table."""
        return self._show_info()

    def get_info_html(self) -> str:
        """
        Describe the task schema as an HTML document.
        """

        def _format_parameter_type(param):
            param_typ_fmt = param.typ
            if param.typ in param_types:
                param_url = (
                    f"{self.app.docs_url}/reference/template_components/"
                    f"parameters.html#{param.url_slug}"
                )
                param_typ_fmt = f'<a href="{param_url}">{param_typ_fmt}</a>'
            return param_typ_fmt

        def _get_param_type_str(param) -> str:
            type_fmt = "-"
            if param._validation:
                try:
                    type_fmt = param._validation.to_tree()[0]["type_fmt"]
                except Exception:
                    pass
            elif param._value_class:
                param_cls = param._value_class
                cls_url = (
                    f"{self.app.docs_url}/reference/_autosummary/{param_cls.__module__}."
                    f"{param_cls.__name__}"
                )
                type_fmt = f'<a href="{cls_url}">{param_cls.__name__}</a>'
            return type_fmt

        def _prepare_script_data_format_table(script_data_grouped):
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

        param_types = self.app.parameters.list_attrs()

        inputs_header_row = f"<tr><th>parameter</th><th>type</th><th>default</th></tr>"
        input_rows = ""
        for inp in self.inputs:
            def_str = "-"
            if not inp.multiple:
                if inp.default_value is not NullDefault.NULL:
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
                f'<span class="schema-note-no-inputs">This task schema has no input '
                f"parameters.</span>"
            )

        outputs_header_row = f"<tr><th>parameter</th><th>type</th></tr>"
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
                f'<span class="schema-note-no-outputs">This task schema has no output '
                f"parameters.</span>"
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
                inps = ", ".join(f"<code>{i.typ}</code>" for i in inp_fg.inputs)
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
                files = ", ".join(f"<code>{i.label}</code>" for i in out_fp.output_files)
                out_fp_rows += (
                    f"<tr>"
                    f'<td class="action-header-cell">output:</td>'
                    f"<td><code>{out_fp.output.typ}</code></td>"
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
                    f"</td><td><code><pre>{escape(cmd.command or cmd.executable)}</pre>"
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
                        f'<tr><td colspan="3" class="commands-table-bottom-spacer-cell">'
                        f"</td></tr>"
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
                f'<span class="actions-show-hide-toggle">[<span class="action-show-text">'
                f'show ↓</span><span class="action-hide-text hidden">hide ↑</span>]'
                f"</span>"
            )
            act_heading_class = ' class="actions-heading"'
        else:
            action_table = (
                f'<span class="schema-note-no-actions">'
                f"This task schema has no actions.</span>"
            )
            action_show_hide = ""
            act_heading_class = ""
        description = (
            f"<h3 class='task-desc'>Description</h3>{self.doc}" if self.doc else ""
        )
        out = (
            f"{description}"
            f"<h3>Inputs</h3>{inputs_table}"
            f"<h3>Outputs</h3>{outputs_table}"
            # f"<h3>Examples</h3>examples here..." # TODO:
            f"<h3{act_heading_class}>Actions{action_show_hide}</h3>"
            f"{action_table}"
        )
        return out

    def __eq__(self, other):
        if type(other) is not self.__class__:
            return False
        if (
            self.objective == other.objective
            and self.actions == other.actions
            and self.method == other.method
            and self.implementation == other.implementation
            and self.inputs == other.inputs
            and self.outputs == other.outputs
            and self.version == other.version
            and self._hash_value == other._hash_value
        ):
            return True
        return False

    def __deepcopy__(self, memo):
        kwargs = self.to_dict()
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._task_template = self._task_template
        return obj

    @classmethod
    @contextmanager
    def ignore_invalid_actions(cls):
        """
        A context manager within which invalid actions will be ignored.
        """
        try:
            cls._validate_actions = False
            yield
        finally:
            cls._validate_actions = True

    def _validate(self):
        if isinstance(self.objective, str):
            self.objective = self.app.TaskObjective(self.objective)

        if self.method:
            self.method = check_valid_py_identifier(self.method)
        if self.implementation:
            self.implementation = check_valid_py_identifier(self.implementation)

        # coerce Parameters to SchemaInputs
        for idx, i in enumerate(self.inputs):
            if isinstance(
                i, Parameter
            ):  # TODO: doc. that we should use the sdk class for type checking!
                self.inputs[idx] = self.app.SchemaInput(i)

        # coerce Parameters to SchemaOutputs
        for idx, i in enumerate(self.outputs):
            if isinstance(i, Parameter):
                self.outputs[idx] = self.app.SchemaOutput(i)
            elif isinstance(i, SchemaInput):
                self.outputs[idx] = self.app.SchemaOutput(i.parameter)

        # check action input/outputs
        if self._validate_actions:
            has_script = any(
                i.script and not i.input_file_generators and not i.output_file_parsers
                for i in self.actions
            )

            all_outs = []
            extra_ins = set(self.input_types)

            act_ins_lst = [act.get_input_types() for act in self.actions]
            act_outs_lst = [act.get_output_types() for act in self.actions]

            schema_ins = set(self.input_types)
            schema_outs = set(self.output_types)

            all_act_ins = set(j for i in act_ins_lst for j in i)
            all_act_outs = set(j for i in act_outs_lst for j in i)

            non_schema_act_ins = all_act_ins - schema_ins
            non_schema_act_outs = set(all_act_outs - schema_outs)

            extra_act_outs = non_schema_act_outs
            seen_act_outs = []
            for act_idx in range(len(self.actions)):
                for act_in in [
                    i for i in act_ins_lst[act_idx] if i in non_schema_act_ins
                ]:
                    if act_in not in seen_act_outs:
                        raise ValueError(
                            f"Action {act_idx} input {act_in!r} of schema {self.name!r} "
                            f"is not a schema input, but nor is it an action output from "
                            f"a preceding action."
                        )
                seen_act_outs += [
                    i for i in act_outs_lst[act_idx] if i not in seen_act_outs
                ]
                extra_act_outs = extra_act_outs - set(act_ins_lst[act_idx])
                act_inputs = set(act_ins_lst[act_idx])
                act_outputs = set(act_outs_lst[act_idx])
                extra_ins = extra_ins - act_inputs
                all_outs.extend(list(act_outputs))

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
                for act_i in self.actions:
                    for OFP_j in act_i.output_file_parsers:
                        extra_ins = extra_ins - set(OFP_j.inputs or [])

                if self.actions and extra_ins:
                    # allow for no actions (e.g. defining inputs for downstream tasks)
                    raise ValueError(
                        f"Schema {self.name!r} inputs {tuple(extra_ins)!r} are not used "
                        f"by any actions."
                    )

            missing_outs = set(self.output_types) - set(all_outs)
            if missing_outs and not has_script:
                # TODO: bit of a hack, need to consider script ins/outs later
                raise ValueError(
                    f"Schema {self.name!r} outputs {tuple(missing_outs)!r} are not "
                    f"generated by any actions."
                )

    def _expand_actions(self):
        """Create new actions for input file generators and output parsers in existing
        actions."""
        return [j for i in self.actions for j in i.expand()]

    def _update_parameter_value_classes(self):
        # ensure any referenced parameter_class_modules are imported:
        for module in self.parameter_class_modules:
            import_module(module)

        # TODO: support specifying file paths in addition to (instead of?) importable
        # module paths

        for inp in self.inputs:
            inp.parameter._set_value_class()

        for out in self.outputs:
            out.parameter._set_value_class()

    def make_persistent(self, workflow: app.Workflow, source: Dict) -> List[int]:
        """
        Convert this task schema to persistent form within the context of the given
        workflow.
        """
        new_refs = []
        for input_i in self.inputs:
            for lab_info in input_i.labelled_info():
                if "default_value" in lab_info:
                    _, dat_ref, is_new = lab_info["default_value"].make_persistent(
                        workflow, source
                    )
                    new_refs.extend(dat_ref) if is_new else None
        return new_refs

    @property
    def name(self):
        """
        The name of this schema.
        """
        out = (
            f"{self.objective.name}"
            f"{f'_{self.method}' if self.method else ''}"
            f"{f'_{self.implementation}' if self.implementation else ''}"
        )
        return out

    @property
    def input_types(self):
        """
        The input types to the schema.
        """
        return tuple(j for i in self.inputs for j in i.all_labelled_types)

    @property
    def output_types(self):
        """
        The output types from the schema.
        """
        return tuple(i.typ for i in self.outputs)

    @property
    def provides_parameters(self) -> Tuple[Tuple[str, str]]:
        """
        The parameters that this schema provides.
        """
        out = []
        for schema_inp in self.inputs:
            for labelled_info in schema_inp.labelled_info():
                prop_mode = labelled_info["propagation_mode"]
                if prop_mode is not ParameterPropagationMode.NEVER:
                    out.append(
                        (schema_inp.input_or_output, labelled_info["labelled_type"])
                    )
        for schema_out in self.outputs:
            if schema_out.propagation_mode is not ParameterPropagationMode.NEVER:
                out.append((schema_out.input_or_output, schema_out.typ))
        return tuple(out)

    @property
    def task_template(self):
        """
        The template that this schema is contained in.
        """
        return self._task_template

    @classmethod
    def get_by_key(cls, key):
        """Get a config-loaded task schema from a key."""
        return cls.app.task_schemas.get(key)

    def get_parameter_dependence(self, parameter: app.SchemaParameter):
        """Find if/where a given parameter is used by the schema's actions."""
        out = {"input_file_writers": [], "commands": []}
        for act_idx, action in enumerate(self.actions):
            deps = action.get_parameter_dependence(parameter)
            for key in out:
                out[key].extend((act_idx, i) for i in deps[key])
        return out

    def get_key(self):
        """
        Get the hashable value that represents this schema.
        """
        return (str(self.objective), self.method, self.implementation)

    def _get_single_label_lookup(self, prefix="") -> Dict[str, str]:
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
        lookup = {}
        if prefix and not prefix.endswith("."):
            prefix += "."
        for sch_inp in self.inputs:
            if not sch_inp.multiple and sch_inp.single_label:
                labelled_type = sch_inp.single_labelled_type
                lookup[f"{prefix}{labelled_type}"] = f"{prefix}{sch_inp.typ}"
        return lookup

    @property
    def multi_input_types(self) -> List[str]:
        """Get a list of input types that have multiple labels."""
        out = []
        for inp in self.inputs:
            if inp.multiple:
                out.append(inp.parameter.typ)
        return out
