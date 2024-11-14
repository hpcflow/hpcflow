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
    JS_SHEBANG: ClassVar[str] = """#!{shebang_executable} {shebang_args}"""
    #: Template for the common part of the jobscript header.
    JS_HEADER: ClassVar[str] = dedent(
        """\
        {workflow_app_alias} () {{
        (
        {env_setup}{app_invoc}\\
                --with-config log_file_path "`pwd`/{run_log_file}"\\
                --config-dir "{config_dir}"\\
                --config-key "{config_invoc_key}"\\
                "$@"
        )
        }}

        WK_PATH=`pwd`
        WK_PATH_ARG="$WK_PATH"
        SUB_IDX={sub_idx}
        JS_IDX={js_idx}
        EAR_ID_FILE="$WK_PATH/artifacts/submissions/${{SUB_IDX}}/{EAR_file_name}"
        ELEM_RUN_DIR_FILE="$WK_PATH/artifacts/submissions/${{SUB_IDX}}/{element_run_dirs_file_path}"
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
    #: Template for the jobscript body.
    JS_MAIN: ClassVar[str] = dedent(
        """\
        elem_EAR_IDs=`sed "$((${{JS_elem_idx}} + 1))q;d" "$EAR_ID_FILE"`
        elem_run_dirs=`sed "$((${{JS_elem_idx}} + 1))q;d" "$ELEM_RUN_DIR_FILE"`

        for ((JS_act_idx=0;JS_act_idx<{num_actions};JS_act_idx++))
        do

          EAR_ID="$(cut -d'{EAR_files_delimiter}' -f $(($JS_act_idx + 1)) <<< $elem_EAR_IDs)"
          if [ "$EAR_ID" = "-1" ]; then
              continue
          fi

          run_dir="$(cut -d'{EAR_files_delimiter}' -f $(($JS_act_idx + 1)) <<< $elem_run_dirs)"
          cd "$WK_PATH/$run_dir"
          app_stream_file="`pwd`/{run_stream_file}"

          skip=`{workflow_app_alias} internal workflow "$WK_PATH_ARG" get-ear-skipped $EAR_ID 2>> "$app_stream_file"`
          exc_sk=$?

          if [ $exc_sk -eq 0 ]; then

              if [ "$skip" = "1" ]; then
                  continue
              fi

              {workflow_app_alias} internal workflow "$WK_PATH_ARG" write-commands $SUB_IDX $JS_IDX $JS_act_idx $EAR_ID >> "$app_stream_file" 2>&1
              exc_wc=$?

              {workflow_app_alias} internal workflow "$WK_PATH_ARG" set-ear-start $EAR_ID >> "$app_stream_file" 2>&1
              exc_se=$?

              if [ $exc_wc -eq 0 ] && [ $exc_se -eq 0 ]; then
                  . {commands_file_name}
                  exit_code=$?
              else
                  exit_code=$([ $exc_wc -ne 0 ] && echo "$exc_wc" || echo "$exc_se")
              fi

          else
              exit_code=$exc_sk
          fi

          {workflow_app_alias} internal workflow "$WK_PATH_ARG" set-ear-end $JS_IDX $JS_act_idx $EAR_ID "--" "$exit_code" >> "$app_stream_file" 2>&1

        done
    """
    )
    #: Template for the element processing loop in a jobscript.
    JS_ELEMENT_LOOP: ClassVar[str] = dedent(
        """\
        for ((JS_elem_idx=0;JS_elem_idx<{num_elements};JS_elem_idx++))
        do
        {main}
        done
        cd "$WK_PATH"
    """
    )
    #: Template for the array handling code in a jobscript.
    JS_ELEMENT_ARRAY: ClassVar[str] = dedent(
        """\
        JS_elem_idx=$(({scheduler_array_item_var} - 1))
        {main}
        cd "$WK_PATH"
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
    @staticmethod
    def format_stream_assignment(shell_var_name: str, command: str) -> str:
        """
        Produce code to assign the output of the command to a shell variable.
        """
        return f"{shell_var_name}=`{command}`"

    @override
    def format_save_parameter(
        self,
        workflow_app_alias: str,
        param_name: str,
        shell_var_name: str,
        EAR_ID: int,
        cmd_idx: int,
        stderr: bool,
    ):
        """
        Produce code to save a parameter's value into the workflow persistent store.
        """
        # TODO: quote shell_var_name as well? e.g. if it's a white-space delimited list?
        #   and test.
        stderr_str = " --stderr" if stderr else ""
        return (
            f"{workflow_app_alias} "
            f'internal workflow "$WK_PATH_ARG" save-parameter '
            f"{param_name} ${shell_var_name} {EAR_ID} {cmd_idx}{stderr_str} "
            f'>> "$app_stream_file" 2>&1'
            f"\n"
        )

    @override
    def format_loop_check(
        self, workflow_app_alias: str, loop_name: str, run_ID: int
    ) -> str:
        """
        Produce code to check the looping status of part of a workflow.
        """
        return (
            f"{workflow_app_alias} "
            f'internal workflow "$WK_PATH_ARG" check-loop '
            f"{loop_name} {run_ID} "
            f'>> "$app_stream_file" 2>&1'
            f"\n"
        )

    @override
    def wrap_in_subshell(self, commands: str, abortable: bool) -> str:
        """Format commands to run within a subshell.

        This assumes commands ends in a newline.

        """
        commands = indent(commands, self.JS_INDENT)
        if abortable:
            # run commands in the background, and poll a file to check for abort requests:
            return dedent(
                """\
                (
                {commands}) &

                pid=$!
                abort_file=$WK_PATH/artifacts/submissions/$SUB_IDX/abort_EARs.txt
                while true
                do
                    is_abort=`sed "$(($EAR_ID + 1))q;d" $abort_file`
                    ps -p $pid > /dev/null
                    if [ $? == 1 ]; then
                        wait $pid
                        exitcode=$?
                        break
                    elif [ "$is_abort" = "1" ]; then
                        echo "Abort instruction received; stopping commands..." >> "$app_stream_file"
                        kill $pid
                        wait $pid 2>/dev/null
                        exitcode={abort_exit_code}
                        break
                    else
                        sleep 1 # TODO: TEMP: increase for production
                    fi
                done
                return $exitcode
                """
            ).format(commands=commands, abort_exit_code=ABORT_EXIT_CODE)
        else:
            # run commands in "foreground":
            return dedent(
                """\
                (
                {commands})
            """
            ).format(commands=commands)


@hydrate
class WSLBash(Bash):
    """
    A variant of bash that handles running under WSL on Windows.
    """

    #: Default name of the WSL interface executable.
    DEFAULT_WSL_EXE: ClassVar[str] = "wsl"

    #: Template for the common part of the jobscript header.
    JS_HEADER: ClassVar[str] = Bash.JS_HEADER.replace(
        'WK_PATH_ARG="$WK_PATH"',
        'WK_PATH_ARG=`wslpath -m "$WK_PATH"`',
    ).replace(
        '--with-config log_file_path "`pwd`',
        '--with-config log_file_path "$(wslpath -m `pwd`)',
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
