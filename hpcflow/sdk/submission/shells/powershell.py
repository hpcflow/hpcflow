"""
Shell models based on Microsoft PowerShell.
"""

from __future__ import annotations
import subprocess
from textwrap import dedent, indent
from typing import TYPE_CHECKING
from typing_extensions import override
from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core import ABORT_EXIT_CODE
from hpcflow.sdk.submission.shells.base import Shell
from hpcflow.sdk.submission.shells.os_version import get_OS_info_windows

if TYPE_CHECKING:
    from typing import ClassVar
    from .base import VersionInfo


@hydrate
class WindowsPowerShell(Shell):
    """Class to represent using PowerShell on Windows to generate and submit a jobscript."""

    # TODO: add snippets that can be used in demo task schemas?

    #: Default for executable name.
    DEFAULT_EXE: ClassVar[str] = "powershell.exe"

    #: File extension for jobscripts.
    JS_EXT: ClassVar[str] = ".ps1"
    #: Basic indent.
    JS_INDENT: ClassVar[str] = "    "
    #: Indent for environment setup.
    JS_ENV_SETUP_INDENT: ClassVar[str] = 2 * JS_INDENT
    #: Template for the jobscript shebang line.
    JS_SHEBANG: ClassVar[str] = ""
    #: Template for the common part of the jobscript header.
    JS_HEADER: ClassVar[str] = dedent(
        """\
        function {workflow_app_alias} {{
            & {{
        {env_setup}{app_invoc} `
                    --with-config log_file_path "$pwd/{run_log_file}" `
                    --config-dir "{config_dir}" `
                    --config-key "{config_invoc_key}" `
                    $args
            }} @args
        }}

        function get_nth_line($file, $line) {{
            Get-Content $file | Select-Object -Skip $line -First 1
        }}

        function JoinMultiPath {{
            $numArgs = $args.Length
            $path = $args[0]
            for ($i = 1; $i -lt $numArgs; $i++) {{
                $path = Join-Path $path $args[$i]
            }}
            return $path
        }}

        function StartJobHere($block) {{
            $jobInitBlock = [scriptblock]::Create(@"
                Function wkflow_app {{ $function:wkflow_app }}
                Function get_nth_line {{ $function:get_nth_line }}
                Function JoinMultiPath {{ $function:JoinMultiPath }}
                Set-Location '$pwd'
        "@)
            Start-Job -InitializationScript $jobInitBlock -Script $block
        }}

        $WK_PATH = $(Get-Location)
        $WK_PATH_ARG = $WK_PATH
        $SUB_IDX = {sub_idx}
        $JS_IDX = {js_idx}
        $EAR_ID_FILE = JoinMultiPath $WK_PATH artifacts submissions $SUB_IDX {EAR_file_name}
        $ELEM_RUN_DIR_FILE = JoinMultiPath $WK_PATH artifacts submissions $SUB_IDX {element_run_dirs_file_path}
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
        $elem_EAR_IDs = get_nth_line $EAR_ID_FILE $JS_elem_idx
        $elem_run_dirs = get_nth_line $ELEM_RUN_DIR_FILE $JS_elem_idx

        for ($JS_act_idx = 0; $JS_act_idx -lt {num_actions}; $JS_act_idx += 1) {{

            $EAR_ID = ($elem_EAR_IDs -split "{EAR_files_delimiter}")[$JS_act_idx]
            if ($EAR_ID -eq -1) {{
                continue
            }}

            $run_dir = ($elem_run_dirs -split "{EAR_files_delimiter}")[$JS_act_idx]
            $run_dir_abs = "$WK_PATH\\$run_dir"
            Set-Location $run_dir_abs
            $app_stream_file = "$pwd/{run_stream_file}"

            $skip = {workflow_app_alias} internal workflow $WK_PATH get-ear-skipped $EAR_ID 2>> $app_stream_file
            $exc_sk = $LASTEXITCODE

            if ($exc_sk -eq 0) {{
                if ($skip -eq "1") {{
                    continue
                }}

                {workflow_app_alias} internal workflow $WK_PATH write-commands $SUB_IDX $JS_IDX $JS_act_idx $EAR_ID 2>&1 >> $app_stream_file
                $exc_wc = $LASTEXITCODE

                {workflow_app_alias} internal workflow $WK_PATH set-ear-start $EAR_ID 2>&1 >> $app_stream_file
                $exc_se = $LASTEXITCODE

                if (($exc_wc -eq 0) -and ($exc_se -eq 0)) {{
                    . (Join-Path $run_dir_abs "{commands_file_name}")
                    $exit_code = $LASTEXITCODE
                }}
                else {{
                    $exit_code = If ($exc_wc -ne 0) {{$exc_wc}} Else {{$exc_se}}
                }}
            }}
            else {{
                $exit_code = $exc_sk
            }}
            $global:LASTEXITCODE = $null
            {workflow_app_alias} internal workflow $WK_PATH set-ear-end $JS_IDX $JS_act_idx $EAR_ID "--" "$exit_code" 2>&1 >> $app_stream_file

        }}
    """
    )
    #: Template for the element processing loop in a jobscript.
    JS_ELEMENT_LOOP: ClassVar[str] = dedent(
        """\
        for ($JS_elem_idx = 0; $JS_elem_idx -lt {num_elements}; $JS_elem_idx += 1) {{
        {main}
        }}
        Set-Location $WK_PATH
    """
    )

    def get_direct_submit_command(self, js_path: str) -> list[str]:
        """Get the command for submitting a non-scheduled jobscript."""
        return [*self.executable, "-File", js_path]

    @override
    def get_version_info(self, exclude_os: bool = False) -> VersionInfo:
        """Get powershell version information.

        Parameters
        ----------
        exclude_os
            If True, exclude operating system information.

        """

        proc = subprocess.run(
            args=self.executable + ["$PSVersionTable.PSVersion.ToString()"],
            stdout=subprocess.PIPE,
            text=True,
        )
        if proc.returncode == 0:
            PS_version = proc.stdout.strip()
        else:
            raise RuntimeError("Failed to parse PowerShell version information.")

        osinfo = {} if exclude_os else get_OS_info_windows()
        return {
            "shell_name": "powershell",
            "shell_executable": self.executable,
            "shell_version": PS_version,
            **osinfo,
        }

    @staticmethod
    def process_app_invoc_executable(app_invoc_exe: str) -> str:
        if " " in app_invoc_exe:
            # use call operator and single-quote the executable path:
            app_invoc_exe = f"& '{app_invoc_exe}'"
        return app_invoc_exe

    @override
    def format_stream_assignment(self, shell_var_name: str, command: str) -> str:
        """
        Produce code to assign the output of the command to a shell variable.
        """
        return f"${shell_var_name} = {command}"

    @override
    def format_save_parameter(
        self,
        workflow_app_alias: str,
        param_name: str,
        shell_var_name: str,
        EAR_ID: int,
        cmd_idx: int,
        stderr: bool,
    ) -> str:
        """
        Produce code to save a parameter's value into the workflow persistent store.
        """
        # TODO: quote shell_var_name as well? e.g. if it's a white-space delimited list?
        #   and test.
        stderr_str = " --stderr" if stderr else ""
        return (
            f"{workflow_app_alias} "
            f"internal workflow $WK_PATH save-parameter "
            f"{param_name} ${shell_var_name} {EAR_ID} {cmd_idx}{stderr_str} "
            f"2>&1 >> $app_stream_file"
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
            f"internal workflow $WK_PATH check-loop "
            f"{loop_name} {run_ID} "
            f"2>&1 >> $app_stream_file"
            f"\n"
        )

    @override
    def wrap_in_subshell(self, commands: str, abortable: bool) -> str:
        """Format commands to run within a child scope.

        This assumes `commands` ends in a newline.

        """
        commands = indent(commands, self.JS_INDENT)
        if abortable:
            # run commands as a background job, and poll a file to check for abort
            # requests:
            return dedent(
                """\
                $job = StartJobHere {{
                    $WK_PATH = $using:WK_PATH
                    $SUB_IDX = $using:SUB_IDX
                    $JS_IDX = $using:JS_IDX
                    $EAR_ID = $using:EAR_ID
                    $app_stream_file= $using:app_stream_file

                {commands}
                    if ($LASTEXITCODE -ne 0) {{
                        throw
                    }}
                }}

                $is_abort = $null
                $abort_file = JoinMultiPath $WK_PATH artifacts submissions $SUB_IDX abort_EARs.txt
                while ($true) {{
                    $is_abort = get_nth_line $abort_file $EAR_ID
                    if ($job.State -ne "Running") {{
                        break
                    }}
                    elseif ($is_abort -eq "1") {{
                        Add-Content -Path $app_stream_file -Value "Abort instruction received; stopping commands..."
                        Stop-Job -Job $job
                        Wait-Job -Job $job
                        break
                    }}
                    else {{
                        Receive-Job -job $job | Write-Output
                        Start-Sleep 1 # TODO: TEMP: increase for production
                    }}
                }}
                Receive-Job -job $job | Write-Output
                if ($job.state -eq "Completed") {{
                    exit 0
                }}
                elseif ($is_abort -eq "1") {{
                    exit {abort_exit_code}
                }}
                else {{
                    exit 1
                }}
            """
            ).format(commands=commands, abort_exit_code=ABORT_EXIT_CODE)
        else:
            # run commands in "foreground":
            return dedent(
                """\
                & {{
                {commands}}}
            """
            ).format(commands=commands)
