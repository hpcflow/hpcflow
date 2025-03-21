"""
Model of a command run in an action.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
import re
from typing import Any, ClassVar, TYPE_CHECKING

import numpy as np

from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.element import ElementResources
from hpcflow.sdk.core.errors import NoCLIFormatMethodError
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.parameters import ParameterValue

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence
    from re import Pattern
    from .actions import ActionRule, Action
    from .element import ElementActionRun
    from .environment import Environment
    from ..submission.shells import Shell


@dataclass
@hydrate
class Command(JSONLike):
    """
    A command that may be run within a workflow action.

    Parameters
    ----------
    command: str
        The actual command.
    executable: str
        The executable to run,
        from the set of executable managed by the environment.
    arguments: list[str]
        The arguments to pass in.
    variables: dict[str, str]
        Values that may be substituted when preparing the arguments.
    stdout: str
        The name of a file to write standard output to.
    stderr: str
        The name of a file to write standard error to.
    stdin: str
        The name of a file to read standard input from.
    rules: list[~hpcflow.app.ActionRule]
        Rules that state whether this command is eligible to run.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="rules",
            class_name="ActionRule",
            is_multiple=True,
            parent_ref="command",
        ),
    )

    #: The actual command.
    #: Overrides :py:attr:`executable`.
    command: str | None = None
    #: The executable to run,
    #: from the set of executable managed by the environment.
    executable: str | None = None
    #: The arguments to pass in.
    arguments: list[str] | None = None
    #: Values that may be substituted when preparing the arguments.
    variables: dict[str, str] | None = None
    #: The name of a file to write standard output to.
    stdout: str | None = None
    #: The name of a file to write standard error to.
    stderr: str | None = None
    #: The name of a file to read standard input from.
    stdin: str | None = None
    #: Rules that state whether this command is eligible to run.
    rules: list[ActionRule] = field(default_factory=list)

    action: Action | None = None  # assigned by parent Action

    def __post_init__(self):
        self._set_parent_refs()

    def __repr__(self) -> str:
        out = []
        if self.command:
            out.append(f"command={self.command!r}")
        if self.executable:
            out.append(f"executable={self.executable!r}")
        if self.arguments:
            out.append(f"arguments={self.arguments!r}")
        if self.variables:
            out.append(f"variables={self.variables!r}")
        if self.stdout:
            out.append(f"stdout={self.stdout!r}")
        if self.stderr:
            out.append(f"stderr={self.stderr!r}")
        if self.stdin:
            out.append(f"stdin={self.stdin!r}")
        if self.rules:
            out.append(f"rules={self.rules!r}")

        return f"{self.__class__.__name__}({', '.join(out)})"

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (
            self.command == other.command
            and self.executable == other.executable
            and self.arguments == other.arguments
            and self.variables == other.variables
            and self.stdout == other.stdout
            and self.stderr == other.stderr
            and self.stdin == other.stdin
            and self.rules == other.rules
        )

    def __get_initial_command_line(self) -> str:
        if self.command:
            return self.command
        else:
            return self.executable or ""

    __EXE_SCRIPT_RE: ClassVar[Pattern] = re.compile(r"\<\<(executable|script):(.*?)\>\>")
    __ENV_SPEC_RE: ClassVar[Pattern] = re.compile(r"\<\<env:(.*?)\>\>")

    def get_command_line(
        self, EAR: ElementActionRun, shell: Shell, env: Environment
    ) -> tuple[str, list[tuple[str, ...]]]:
        """Return the resolved command line.

        This is ordinarily called at run-time by `Workflow.write_commands`.
        """

        self._app.persistence_logger.debug("Command.get_command_line")
        cmd_str = self.__get_initial_command_line()

        def _format_sum(iterable: Iterable) -> str:
            return str(sum(iterable))

        def _join(iterable: Iterable, delim: str) -> str:
            return delim.join(map(str, iterable))

        parse_types: dict[str, Callable[..., str]] = {
            "sum": _format_sum,
            "join": _join,
        }

        def exec_script_repl(match_obj: re.Match[str]) -> str:
            typ, val = match_obj.groups()
            if typ == "executable":
                executable = env.executables.get(val)
                filterable = ElementResources.get_env_instance_filterable_attributes()
                filter_exec = {attr: EAR.get_resources().get(attr) for attr in filterable}
                exec_cmd = executable.filter_instances(**filter_exec)[0].command
                return exec_cmd.replace("<<num_cores>>", str(EAR.resources.num_cores))
            elif typ == "script":
                # TODO: is this needed? we have <<script_name>> <<script_path>> etc as command variables
                return EAR.action.get_script_name(val)
            else:
                raise ValueError("impossible match occurred")

        def input_param_repl(match_obj: re.Match[str], inp_val) -> str:
            _, func, func_kwargs, method, method_kwargs = match_obj.groups()

            if isinstance(inp_val, ParameterValue):
                if not method:
                    method = "CLI_format"
                if not hasattr(inp_val, method):
                    raise NoCLIFormatMethodError(method, inp_val)
                kwargs = self.__prepare_kwargs_from_string(args_str=method_kwargs)
                inp_val = getattr(inp_val, method)(**kwargs)

            if func:
                kwargs = self.__prepare_kwargs_from_string(
                    args_str=func_kwargs,
                    doubled_quoted_args=["delim"],
                )
                inp_val = parse_types[func](inp_val, **kwargs)

            return str(inp_val)

        file_regex = r"(\<\<file:{}\>\>?)"

        # substitute executables:
        cmd_str = self.__EXE_SCRIPT_RE.sub(
            repl=exec_script_repl,
            string=cmd_str,
        )

        # executable command might itself contain variables defined in `variables`, and/or
        # an `<<args>>` variable::
        for var_key, var_val in (self.variables or {}).items():
            # substitute any `<<env:>>` specifiers
            var_val = self.__ENV_SPEC_RE.sub(
                repl=lambda match_obj: EAR.env_spec[match_obj[1]],
                string=var_val,
            )
            cmd_str = cmd_str.replace(f"<<{var_key}>>", var_val)
            if "<<args>>" in cmd_str:
                args_str = " ".join(self.arguments or ())
                ends_in_args = cmd_str.endswith("<<args>>")
                cmd_str = cmd_str.replace("<<args>>", args_str)
                if ends_in_args and not args_str:
                    cmd_str = cmd_str.rstrip()

        # remove any left over "<<args>>" and "<<script_name>>"s:
        cmd_str = (
            cmd_str.replace("<<args>>", "")
            .replace("<<script_name>>", "")
            .replace("<<script_path>>", "")
        )

        # substitute input parameters in command:
        types_pattern = "|".join(parse_types)
        pattern = (
            r"(\<\<(?:({types_pattern})(?:\[(.*)\])?\()?parameter:{name}(?:\.(\w+)"
            r"\((.*?)\))?\)?\>\>?)"
        )

        for cmd_inp_full in EAR.action.get_command_input_types(sub_parameters=True):
            # remove any CLI formatting method, which will be the final component and will
            # include parentheses:
            cmd_inp_parts = cmd_inp_full.split(".")
            if "(" in cmd_inp_parts[-1]:
                cmd_inp = ".".join(cmd_inp_parts[:-1])
            else:
                cmd_inp = cmd_inp_full
            inp_val = EAR.get(
                f"inputs.{cmd_inp}",
                raise_on_unset=True,
            )  # TODO: what if schema output?
            pattern_i = pattern.format(
                types_pattern=types_pattern,
                name=re.escape(cmd_inp),
            )
            cmd_str = re.sub(
                pattern=pattern_i,
                repl=partial(input_param_repl, inp_val=inp_val),
                string=cmd_str,
            )

        # substitute input/output files in command:
        for cmd_file in EAR.action.get_command_file_labels():
            file_path = EAR.get(
                f"input_files.{cmd_file}", raise_on_unset=True
            ) or EAR.get(f"output_files.{cmd_file}", raise_on_unset=True)
            # assuming we have copied this file to the EAR directory, then we just
            # need the file name:
            file_name = Path(file_path).name
            cmd_str = re.sub(
                pattern=file_regex.format(cmd_file),
                repl=file_name,
                string=cmd_str,
            )

        shell_vars: list[tuple[str, ...]] = []
        out_types = self.get_output_types()
        if out_types["stdout"]:
            # TODO: also map stderr/both if possible
            # assign stdout to a shell variable if required:
            param_name = f"outputs.{out_types['stdout']}"
            shell_var_name = f"parameter_{out_types['stdout']}"
            shell_vars.append((param_name, shell_var_name, "stdout"))
            cmd_str = shell.format_stream_assignment(
                shell_var_name=shell_var_name,
                command=cmd_str,
            )
        elif self.stdout:
            cmd_str += f" 1>> {self.stdout}"

        if self.stderr:
            cmd_str += f" 2>> {self.stderr}"

        return cmd_str, shell_vars

    # note: we use "parameter" rather than "output", because it could be a schema
    # output or schema input.
    __PARAM_RE: ClassVar[Pattern] = re.compile(
        r"(?:\<\<(?:\w+(?:\[(?:.*)\])?\()?parameter:(\w+)"
        r"(?:\.(?:\w+)\((?:.*?)\))?\)?\>\>?)"
    )

    def get_output_types(self) -> Mapping[str, str | None]:
        """
        Get whether stdout and stderr are workflow parameters.
        """
        out: dict[str, str | None] = {"stdout": None, "stderr": None}
        for i, label in zip((self.stdout, self.stderr), ("stdout", "stderr")):
            if i and (match := self.__PARAM_RE.search(i)):
                param_typ: str = match[1]
                if match.span(0) != (0, len(i)):
                    raise ValueError(
                        f"If specified as a parameter, `{label}` must not include"
                        f" any characters other than the parameter "
                        f"specification, but this was given: {i!r}."
                    )
                out[label] = param_typ
        return out

    @staticmethod
    def __prepare_kwargs_from_string(
        args_str: str | None, doubled_quoted_args: list[str] | None = None
    ) -> dict[str, str]:
        if args_str is None:
            return {}

        kwargs: dict[str, str] = {}
        # deal with specified double-quoted arguments first if it exists:
        for quote_arg in doubled_quoted_args or ():
            quote_pat = r'.*({quote_arg}="(.*)").*'.format(quote_arg=quote_arg)
            if match := re.match(quote_pat, args_str):
                quote_str, quote_contents = match.groups()
                args_str = args_str.replace(quote_str, "")
                kwargs[quote_arg] = quote_contents

        if args_str := args_str.strip().strip(","):
            for arg_part in args_str.split(","):
                name_i, value_i = map(str.strip, arg_part.split("="))
                kwargs[name_i] = value_i
        return kwargs

    def process_std_stream(self, name: str, value: str, stderr: bool) -> Any:
        """
        Process a description of a standard stread from a command to get how it becomes
        a workflow parameter for later actions.

        Parameters
        ---------
        name:
            The name of the output, describing how to process things.
        value:
            The actual value read from the stream.
        stderr:
            If true, this is handling the stderr stream. If false, the stdout stream.
        """

        def _parse_list(
            lst_str: str, item_type: str = "str", delim: str = " "
        ) -> list[Any]:
            return [parse_types[item_type](i) for i in lst_str.split(delim)]

        def _parse_array(
            arr_str: str, item_type: str = "float", delim: str = " "
        ) -> np.ndarray[Any, np.dtype[Any]]:
            return np.array(
                _parse_list(lst_str=arr_str, item_type=item_type, delim=delim)
            )

        def _parse_bool(bool_str: str) -> bool:
            bool_str = bool_str.lower()
            if bool_str in ("true", "1"):
                return True
            elif bool_str in ("false", "0"):
                return False
            else:
                raise ValueError(
                    f"Cannot parse value {bool_str!r} as a boolean in command "
                    f"{'stderr' if stderr else 'stdout'}: "
                    f"{self.stderr if stderr else self.stdout!r}."
                )

        parse_types: dict[str, Callable[[str], Any]] = {
            "str": str,
            "int": int,
            "float": float,
            "bool": _parse_bool,
            "list": _parse_list,
            "array": _parse_array,
        }
        types_pattern = "|".join(parse_types)

        # TODO: use str.removeprefix in 3.9 onwards
        out_name = name.replace("outputs.", "")
        pattern = (
            r"(\<\<(?:({types_pattern})(?:\[(.*)\])?\()?parameter:{name}(?:\.(\w+)"
            r"\((.*?)\))?\)?\>\>?)"
        )
        pattern = pattern.format(types_pattern=types_pattern, name=out_name)
        spec = self.stderr if stderr else self.stdout
        assert spec is not None
        self._app.submission_logger.info(
            f"processing shell standard stream according to spec: {spec!r}"
        )
        param = self._app.Parameter(out_name)
        if (match := re.match(pattern, spec)) is None:
            return value
        groups = match.groups()
        parse_type, parse_args_str = groups[1:3]
        parse_args = self.__prepare_kwargs_from_string(
            args_str=parse_args_str,
            doubled_quoted_args=["delim"],
        )
        if param._value_class:
            method, method_args_str = groups[3:5]
            method_args = self.__prepare_kwargs_from_string(
                args_str=method_args_str,
                doubled_quoted_args=["delim"],
            )
            method = method or "CLI_parse"
            value = getattr(param._value_class, method)(value, **method_args)
        if parse_type:
            value = parse_types[parse_type](value, **parse_args)

        return value

    __EXE_RE: ClassVar[Pattern] = re.compile(r"\<\<(?:executable):(.*?)\>\>")

    @classmethod
    def _extract_executable_labels(cls, cmd_str: str) -> Sequence[str]:
        return cls.__EXE_RE.findall(cmd_str)

    def get_required_executables(self) -> Sequence[str]:
        """Return executable labels required by this command."""
        # an executable label might appear in the `command` or `executable` attribute:
        return self._extract_executable_labels(self.__get_initial_command_line())
