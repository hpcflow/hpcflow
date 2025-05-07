"""
Shell models based on the GNU Bourne-Again Shell.
"""

from __future__ import annotations
from pathlib import Path
import subprocess
from textwrap import dedent, indent
from typing import TYPE_CHECKING
from typing_extensions import override
from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core import ABORT_EXIT_CODE
from hpcflow.sdk.submission.shells.base import Shell
from hpcflow.sdk.submission.shells.os_version import (
    get_OS_info_POSIX,
    get_OS_info_windows,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import Any, ClassVar
    from .base import VersionInfo, JobscriptHeaderArgs


@hydrate
class Bash(Shell):
    """
    Class to represent using bash on a POSIX OS to generate and submit a jobscript.
    """

    #: Default for executable name.
    DEFAULT_EXE: ClassVar[str] = "/bin/bash"

    #: File extension for jobscripts.
    JS_EXT: ClassVar[str] = ".sh"
    #: Basic indent.
    JS_INDENT: ClassVar[str] = "  "
    #: Indent for environment setup.
    JS_ENV_SETUP_INDENT: ClassVar[str] = 2 * JS_INDENT
    #: Template for the jobscript shebang line.
    JS_SHEBANG: ClassVar[str] = """#!{shebang}"""
    #: Template for the jobscript functions file.
    JS_FUNCS: ClassVar[str] = dedent(
        """\
        {workflow_app_alias} () {{
        (
        {env_setup}{app_invoc}\\
                --with-config log_file_path "${app_caps}_LOG_PATH"\\
                --config-dir "{config_dir}"\\
                --config-key "{config_invoc_key}"\\
                "$@"
        )
        }}
    """
    )
    #: Template for the common part of the jobscript header.
    JS_HEADER: ClassVar[str] = dedent(
        """\
        WK_PATH=`pwd`
        WK_PATH_ARG="$WK_PATH"
        SUB_IDX={sub_idx}
        JS_IDX={js_idx}
        APP_CAPS={app_caps}

        SUB_DIR="$WK_PATH/artifacts/submissions/${{SUB_IDX}}"
        JS_FUNCS_PATH="$SUB_DIR/{jobscript_functions_dir}/{jobscript_functions_name}"
        . "$JS_FUNCS_PATH"        
        
        EAR_ID_FILE="$WK_PATH/artifacts/submissions/${{SUB_IDX}}/{run_IDs_file_dir}/{run_IDs_file_name}"
        SUB_TMP_DIR="$SUB_DIR/{tmp_dir_name}"
        SUB_LOG_DIR="$SUB_DIR/{log_dir_name}"
        SUB_STD_DIR="$SUB_DIR/{app_std_dir_name}"
        SUB_SCRIPTS_DIR="$SUB_DIR/{scripts_dir_name}"

        export {app_caps}_WK_PATH=$WK_PATH
        export {app_caps}_WK_PATH_ARG=$WK_PATH_ARG
        export {app_caps}_SUB_IDX={sub_idx}
        export {app_caps}_SUB_SCRIPTS_DIR=$SUB_SCRIPTS_DIR
        export {app_caps}_SUB_TMP_DIR=$SUB_TMP_DIR
        export {app_caps}_SUB_LOG_DIR=$SUB_LOG_DIR
        export {app_caps}_SUB_STD_DIR=$SUB_STD_DIR
        export {app_caps}_LOG_PATH="$SUB_LOG_DIR/js_${{JS_IDX}}.log"
        export {app_caps}_JS_FUNCS_PATH=$JS_FUNCS_PATH
        export {app_caps}_JS_IDX={js_idx}
        export {app_caps}_RUN_ID_FILE=$EAR_ID_FILE
    """
    )
    #: Template for the jobscript header when scheduled.
    JS_SCHEDULER_HEADER: ClassVar[str] = dedent(
        """\
        {shebang}

        {scheduler_options}
        {header}
    """
    )
    #: Template for the jobscript header when directly executed.
    JS_DIRECT_HEADER: ClassVar[str] = dedent(
        """\
        {shebang}
        {header}
        {wait_command}
    """
    )
    #: Template for enabling writing of the app log.
    JS_RUN_LOG_PATH_ENABLE: ClassVar[str] = '"$SUB_LOG_DIR/{run_log_file_name}"'
    #: Template for disabling writing of the app log.
    JS_RUN_LOG_PATH_DISABLE: ClassVar[str] = '" "'
    #: Template for the run execution command.
    JS_RUN_CMD: ClassVar[str] = (
        '{workflow_app_alias} internal workflow "$WK_PATH_ARG" execute-run '
        "$SUB_IDX $JS_IDX $block_idx $block_act_idx $EAR_ID\n"
    )
    #: Template for the execution command for multiple combined runs.
    JS_RUN_CMD_COMBINED: ClassVar[str] = (
        '{workflow_app_alias} internal workflow "$WK_PATH_ARG" execute-combined-runs '
        "$SUB_IDX $JS_IDX\n"
    )
    #: Template for setting up run environment variables and executing the run.
    JS_RUN: ClassVar[str] = dedent(
        """\
        EAR_ID="$(cut -d'{EAR_files_delimiter}' -f $(($block_act_idx + 1)) <<< $elem_EAR_IDs)"
        if [ "$EAR_ID" = "-1" ]; then
            continue
        fi

        export {app_caps}_RUN_ID=$EAR_ID
        export {app_caps}_RUN_LOG_PATH={run_log_enable_disable}
        export {app_caps}_LOG_PATH="${app_caps}_RUN_LOG_PATH"
        export {app_caps}_RUN_STD_PATH="$SUB_STD_DIR/${app_caps}_RUN_ID.txt"
        export {app_caps}_BLOCK_ACT_IDX=$block_act_idx
                
        cd "$SUB_TMP_DIR"
        
        {run_cmd}
    """
    )
    #: Template for the action-run processing loop in a jobscript.
    JS_ACT_MULTI: ClassVar[str] = dedent(
        """\
        for ((block_act_idx=0;block_act_idx<{num_actions};block_act_idx++))
        do      
        {run_block}
        done  
        """
    )
    #: Template for the single-action-run execution in a jobscript.
    JS_ACT_SINGLE: ClassVar[str] = dedent(
        """\
        block_act_idx=0        
        {run_block}
        """
    )
    #: Template for setting up environment variables and running one or more action-runs.
    JS_MAIN: ClassVar[str] = dedent(
        """\
        block_elem_idx=$(( $JS_elem_idx - {block_start_elem_idx} ))
        elem_EAR_IDs=`sed "$((${{JS_elem_idx}} + 1))q;d" "$EAR_ID_FILE"`
        export {app_caps}_JS_ELEM_IDX=$JS_elem_idx
        export {app_caps}_BLOCK_ELEM_IDX=$block_elem_idx
        
        {action}
    """
    )
    #: Template for a jobscript-block header.
    JS_BLOCK_HEADER: ClassVar[str] = dedent(  # for single-block jobscripts only
        """\
        block_idx=0
        export {app_caps}_BLOCK_IDX=0
        """
    )
    #: Template for single-element execution.
    JS_ELEMENT_SINGLE: ClassVar[str] = dedent(
        """\
        JS_elem_idx={block_start_elem_idx}
        {main}
    """
    )
    #: Template for the element processing loop in a jobscript.
    JS_ELEMENT_MULTI_LOOP: ClassVar[str] = dedent(
        """\
        for ((JS_elem_idx={block_start_elem_idx};JS_elem_idx<$(({block_start_elem_idx} + {num_elements}));JS_elem_idx++))
        do
        {main}
        done
    """
    )
    #: Template for the array handling code in a jobscript.
    JS_ELEMENT_MULTI_ARRAY: ClassVar[str] = dedent(
        """\
        JS_elem_idx=$(({scheduler_array_item_var} - 1))
        {main}
    """
    )
    #: Template for the jobscript block loop in a jobscript.
    JS_BLOCK_LOOP: ClassVar[str] = dedent(
        """\
        num_elements={num_elements}
        num_actions={num_actions}
        block_start_elem_idx=0
        for ((block_idx=0;block_idx<{num_blocks};block_idx++))
        do
            export {app_caps}_BLOCK_IDX=$block_idx
        {element_loop}
            block_start_elem_idx=$(($block_start_elem_idx + ${{num_elements[$block_idx]}}))
        done
    """
    )
    #: Template for the jobscript footer.
    JS_FOOTER: ClassVar[str] = dedent(
        """\
        cd $WK_PATH
    """
    )

    @property
    def linux_release_file(self) -> str:
        """
        The name of the file describing the Linux version.
        """
        return self.os_args["linux_release_file"]

    def _get_OS_info_POSIX(self) -> Mapping[str, str]:
        return get_OS_info_POSIX(linux_release_file=self.linux_release_file)

    @override
    def get_version_info(self, exclude_os: bool = False) -> VersionInfo:
        """Get bash version information.

        Parameters
        ----------
        exclude_os
            If True, exclude operating system information.

        """

        bash_proc = subprocess.run(
            args=self.executable + ["--version"],
            stdout=subprocess.PIPE,
            text=True,
        )
        if bash_proc.returncode == 0:
            first_line = bash_proc.stdout.splitlines()[0]
            bash_version = first_line.split(" ")[3]
        else:
            raise RuntimeError("Failed to parse bash version information.")

        return {
            "shell_name": "bash",
            "shell_executable": self.executable,
            "shell_version": bash_version,
            **({} if exclude_os else self._get_OS_info_POSIX()),
        }

    @staticmethod
    def process_app_invoc_executable(app_invoc_exe: str) -> str:
        # escape spaces with a back slash:
        return app_invoc_exe.replace(" ", r"\ ")

    @override
    def format_env_var_get(self, var: str) -> str:
        """
        Format retrieval of a shell environment variable.
        """
        return f"${var}"

    @override
    def format_array(self, lst: list) -> str:
        """
        Format construction of a shell array.
        """
        return "(" + " ".join(str(i) for i in lst) + ")"

    @override
    def format_array_get_item(self, arr_name: str, index: int | str) -> str:
        """
        Format retrieval of a shell array item at a specified index.
        """
        return f"${{{arr_name}[{index}]}}"

    @override
    def format_stream_assignment(self, shell_var_name: str, command: str) -> str:
        """
        Produce code to assign the output of the command to a shell variable.
        """
        return f"{shell_var_name}=`{command}`"

    @override
    def format_source_functions_file(self, app_name: str, commands: str) -> str:
        """
        Format sourcing (i.e. invocation) of the jobscript functions file.
        """
        return dedent(
            """\
            . "${app_caps}_JS_FUNCS_PATH"

            """
        ).format(app_caps=app_name.upper())

    @override
    def format_commands_file(self, app_name: str, commands: str) -> str:
        """
        Format the commands file.
        """
        return self.format_source_functions_file(app_name, commands) + commands

    @override
    def format_save_parameter(
        self,
        workflow_app_alias: str,
        param_name: str,
        shell_var_name: str,
        cmd_idx: int,
        stderr: bool,
        app_name: str,
    ):
        """
        Produce code to save a parameter's value into the workflow persistent store.
        """
        # TODO: quote shell_var_name as well? e.g. if it's a white-space delimited list?
        #   and test.
        stderr_str = " --stderr" if stderr else ""
        app_caps = app_name.upper()
        return (
            f'{workflow_app_alias} --std-stream "${app_caps}_RUN_STD_PATH" '
            f'internal workflow "${app_caps}_WK_PATH_ARG" save-parameter {stderr_str}'
            f'"--" {param_name} ${shell_var_name} ${app_caps}_RUN_ID {cmd_idx}'
            f"\n"
        )


class WSLBash(Bash):
    """
    A variant of bash that handles running under WSL on Windows.
    """

    #: Default name of the WSL interface executable.
    DEFAULT_WSL_EXE: ClassVar[str] = "wsl"

    #: Template for the jobscript functions file.
    JS_FUNCS: ClassVar[str] = dedent(
        """\
        {{workflow_app_alias}} () {{{{
        (
        {log_path_block}
        {{env_setup}}{{app_invoc}}\\
                --with-config log_file_path "$LOG_FILE_PATH"\\
                --config-dir "{{config_dir}}"\\
                --config-key "{{config_invoc_key}}"\\
                "$@"
        )
        }}}}
    """
    ).format(
        log_path_block=indent(
            dedent(
                """\
                    if [ -z "${app_caps}_LOG_PATH" ] || [ "${app_caps}_LOG_PATH" = " " ]; then                    
                        LOG_FILE_PATH=" "
                    else
                        LOG_FILE_PATH="$(wslpath -m ${app_caps}_LOG_PATH)"
                    fi                    
                """
            ),
            prefix=Bash.JS_ENV_SETUP_INDENT,
        )
    )
    #: Template for the common part of the jobscript header.
    JS_HEADER: ClassVar[str] = Bash.JS_HEADER.replace(
        'WK_PATH_ARG="$WK_PATH"',
        'WK_PATH_ARG=`wslpath -m "$WK_PATH"`',
    )
    #: Template for the run execution command.
    JS_RUN_CMD: ClassVar[str] = (
        dedent(
            """\
        WSLENV=$WSLENV:${{APP_CAPS}}_WK_PATH
        WSLENV=$WSLENV:${{APP_CAPS}}_WK_PATH_ARG
        WSLENV=$WSLENV:${{APP_CAPS}}_JS_FUNCS_PATH
        WSLENV=$WSLENV:${{APP_CAPS}}_STD_STREAM_FILE
        WSLENV=$WSLENV:${{APP_CAPS}}_SUB_IDX
        WSLENV=$WSLENV:${{APP_CAPS}}_JS_IDX
        WSLENV=$WSLENV:${{APP_CAPS}}_RUN_ID
        WSLENV=$WSLENV:${{APP_CAPS}}_BLOCK_ACT_IDX
        WSLENV=$WSLENV:${{APP_CAPS}}_JS_ELEM_IDX
        WSLENV=$WSLENV:${{APP_CAPS}}_BLOCK_ELEM_IDX
        WSLENV=$WSLENV:${{APP_CAPS}}_BLOCK_IDX
        WSLENV=$WSLENV:${{APP_CAPS}}_LOG_PATH/p

    """
        )
        + Bash.JS_RUN_CMD
    )

    def __init__(
        self,
        WSL_executable: str | None = None,
        WSL_distribution: str | None = None,
        WSL_user: str | None = None,
        *args,
        **kwargs,
    ):
        #: The WSL executable wrapper.
        self.WSL_executable = WSL_executable or self.DEFAULT_WSL_EXE
        #: The WSL distribution to use, if any.
        self.WSL_distribution = WSL_distribution
        #: The WSL user to use, if any.
        self.WSL_user = WSL_user
        super().__init__(*args, **kwargs)

    def __eq__(self, other: Any) -> bool:
        return super().__eq__(other) and (
            self.WSL_executable == other.WSL_executable
            and self.WSL_distribution == other.WSL_distribution
            and self.WSL_user == other.WSL_user
        )

    def _get_WSL_command(self) -> list[str]:
        out = [self.WSL_executable]
        if self.WSL_distribution:
            out += ["--distribution", self.WSL_distribution]
        if self.WSL_user:
            out += ["--user", self.WSL_user]
        return out

    @property
    def executable(self) -> list[str]:
        return self._get_WSL_command() + super().executable

    @property
    def shebang_executable(self) -> list[str]:
        """
        The executable to use in a shebang line, overridden here to exclude the WSL
        command.
        """
        return super().executable

    def _get_OS_info_POSIX(self) -> Mapping[str, str]:
        return get_OS_info_POSIX(
            WSL_executable=self._get_WSL_command(),
            use_py=False,
            linux_release_file=self.linux_release_file,
        )

    @staticmethod
    def _convert_to_wsl_path(win_path: str | Path) -> str:
        win_path = Path(win_path)
        parts = list(win_path.parts)
        parts[0] = f"/mnt/{win_path.drive.lower().rstrip(':')}"
        return "/".join(parts)

    def process_JS_header_args(
        self, header_args: JobscriptHeaderArgs
    ) -> JobscriptHeaderArgs:
        # convert executable windows paths to posix style as expected by WSL:
        ai = header_args["app_invoc"]
        if isinstance(ai, list):
            ai[0] = self._convert_to_wsl_path(ai[0])
        return super().process_JS_header_args(header_args)

    def prepare_JS_path(self, js_path: Path) -> str:
        return self._convert_to_wsl_path(js_path)

    def prepare_element_run_dirs(self, run_dirs: list[list[Path]]) -> list[list[str]]:
        return [[str(path).replace("\\", "/") for path in i] for i in run_dirs]

    @override
    def get_version_info(self, exclude_os: bool = False) -> VersionInfo:
        """Get WSL and bash version information.

        Parameters
        ----------
        exclude_os
            If True, exclude operating system information.

        """
        vers_info = super().get_version_info(exclude_os=exclude_os)

        vers_info["shell_name"] = f"wsl+{vers_info['shell_name']}".lower()
        vers_info["WSL_executable"] = self.WSL_executable
        if self.WSL_distribution:
            vers_info["WSL_distribution"] = self.WSL_distribution
        if self.WSL_user:
            vers_info["WSL_user"] = self.WSL_user

        for key in tuple(vers_info):
            if key.startswith("OS_"):
                vers_info[f"WSL_{key}"] = vers_info.pop(key)

        if not exclude_os:
            vers_info.update(**get_OS_info_windows())

        return vers_info

    def get_command_file_launch_command(self, cmd_file_path: str) -> list[str]:
        """Get the command for launching the commands file for a given run."""
        return self.executable + [self._convert_to_wsl_path(cmd_file_path)]
