import subprocess
from textwrap import dedent
from typing import Dict, List, Optional
from hpcflow.sdk.submission.shells import Shell
from hpcflow.sdk.submission.shells.os_version import get_OS_info_windows


class WindowsPowerShell(Shell):
    """Class to represent using PowerShell on Windows to generate and submit a jobscript."""

    # TODO: add snippets that can be used in demo task schemas?

    DEFAULT_EXE = "powershell.exe"

    JS_EXT = ".ps1"
    JS_INDENT = "    "
    JS_ENV_SETUP_INDENT = 2 * JS_INDENT
    JS_SHEBANG = ""
    JS_FUNCS = dedent(
        """\
        function {workflow_app_alias} {{
            & {{
        {env_setup}{app_invoc} `
                    --with-config log_file_path "$env:{app_caps}_LOG_PATH" `
                    --config-dir "{config_dir}" `
                    --config-key "{config_invoc_key}" `
                    $args
            }} @args
        }}

        function get_nth_line($file, $line) {{
            Get-Content $file | Select-Object -Skip $line -First 1
        }}
    """
    )
    JS_HEADER = dedent(
        """\
        $ErrorActionPreference = 'Stop'

        function JoinMultiPath {{
            $numArgs = $args.Length
            $path = $args[0]
            for ($i = 1; $i -lt $numArgs; $i++) {{
                $path = Join-Path $path $args[$i]
            }}
            return $path
        }}        

        $WK_PATH = $(Get-Location)
        $WK_PATH_ARG = $WK_PATH
        $SUB_IDX = {sub_idx}
        $JS_IDX = {js_idx}

        $SUB_DIR = JoinMultiPath $WK_PATH artifacts submissions $SUB_IDX
        $JS_FUNCS_PATH = JoinMultiPath $SUB_DIR {jobscript_functions_dir} {jobscript_functions_name}
        . $JS_FUNCS_PATH

        $EAR_ID_FILE = JoinMultiPath $SUB_DIR {run_IDs_file_dir} {run_IDs_file_name}
        $SUB_TMP_DIR = Join-Path $SUB_DIR {tmp_dir_name}
        $SUB_LOG_DIR = Join-Path $SUB_DIR {log_dir_name}
        $SUB_STD_DIR = Join-Path $SUB_DIR {app_std_dir_name}
        $SUB_SCRIPTS_DIR = Join-Path $SUB_DIR {scripts_dir_name}        

        $env:{app_caps}_WK_PATH = $WK_PATH
        $env:{app_caps}_WK_PATH_ARG = $WK_PATH_ARG
        $env:{app_caps}_SUB_IDX = {sub_idx}
        $env:{app_caps}_SUB_SCRIPTS_DIR = $SUB_SCRIPTS_DIR
        $env:{app_caps}_SUB_TMP_DIR = $SUB_TMP_DIR
        $env:{app_caps}_SUB_LOG_DIR = $SUB_LOG_DIR
        $env:{app_caps}_SUB_STD_DIR = $SUB_STD_DIR                
        $env:{app_caps}_LOG_PATH = Join-Path $SUB_LOG_DIR "js_$JS_IDX.log"
        $env:{app_caps}_JS_FUNCS_PATH = $JS_FUNCS_PATH
        $env:{app_caps}_JS_IDX = {js_idx}
        $env:{app_caps}_RUN_ID_FILE = $EAR_ID_FILE
    """
    )
    JS_DIRECT_HEADER = dedent(
        """\
        {shebang}
        {header}
        {wait_command}
    """
    )
    JS_RUN_LOG_PATH_ENABLE = 'Join-Path $SUB_LOG_DIR "{run_log_file_name}"'
    JS_RUN_LOG_PATH_DISABLE = '" "'
    JS_RUN_CMD = (
        "{workflow_app_alias} internal workflow $WK_PATH execute-run "
        "$SUB_IDX $JS_IDX $block_idx $block_act_idx $EAR_ID\n"
    )
    JS_RUN_CMD_COMBINED = (
        "{workflow_app_alias} internal workflow $WK_PATH execute-combined-runs "
        "$SUB_IDX $JS_IDX\n"
    )
    JS_RUN = dedent(
        """\
        $EAR_ID = ($elem_EAR_IDs -split "{EAR_files_delimiter}")[$block_act_idx]
        if ($EAR_ID -eq -1) {{
            continue
        }}

        $env:{app_caps}_RUN_ID = $EAR_ID
        $env:{app_caps}_RUN_LOG_PATH = {run_log_enable_disable}
        $env:{app_caps}_LOG_PATH = $env:{app_caps}_RUN_LOG_PATH
        $env:{app_caps}_RUN_STD_PATH = Join-Path $SUB_STD_DIR "$env:{app_caps}_RUN_ID.txt"
        $env:{app_caps}_BLOCK_ACT_IDX = $block_act_idx            

        Set-Location $SUB_TMP_DIR
        
        {run_cmd}
        """
    )
    JS_ACT_MULTI = dedent(
        """\
        for ($block_act_idx = 0; $block_act_idx -lt {num_actions}; $block_act_idx += 1) {{        
        {run_block}
        }}
        """
    )
    JS_ACT_SINGLE = dedent(
        """\
        $block_act_idx = 0        
        {run_block}
        """
    )
    JS_MAIN = dedent(
        """\
        $block_elem_idx = ($JS_elem_idx - {block_start_elem_idx})
        $elem_EAR_IDs = get_nth_line $EAR_ID_FILE $JS_elem_idx
        $env:{app_caps}_JS_ELEM_IDX = $JS_elem_idx
        $env:{app_caps}_BLOCK_ELEM_IDX = $block_elem_idx

        {action}
    """
    )
    JS_BLOCK_HEADER = dedent(  # for single-block jobscripts only
        """\
        $block_idx = 0
        $env:{app_caps}_BLOCK_IDX = 0
        """
    )
    JS_ELEMENT_SINGLE = dedent(
        """\
        $JS_elem_idx = {block_start_elem_idx}
        {main}
    """
    )
    JS_ELEMENT_MULTI_LOOP = dedent(
        """\
        for ($JS_elem_idx = {block_start_elem_idx}; $JS_elem_idx -lt ({block_start_elem_idx} + {num_elements}); $JS_elem_idx += 1) {{            
        {main}
        }}
    """
    )
    JS_ELEMENT_MULTI_ARRAY = None  # not implemented # TODO: add to Shell class
    JS_BLOCK_LOOP = dedent(
        """\
        $num_elements = {num_elements}
        $num_actions = {num_actions}
        $block_start_elem_idx = 0
        for ($block_idx = 0; $block_idx -lt {num_blocks}; $block_idx += 1 ) {{
            $env:{app_caps}_BLOCK_IDX = $block_idx
        {element_loop}
            $block_start_elem_idx += $num_elements[$block_idx]
        }}
    """
    )
    JS_FOOTER = dedent(
        """\
        Set-Location $WK_PATH
    """
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_direct_submit_command(self, js_path: str) -> List[str]:
        """Get the command for submitting a non-scheduled jobscript."""
        return self.executable + ["-File", js_path]

    def get_command_file_launch_command(self, cmd_file_path: str) -> List[str]:
        """Get the command for launching the commands file for a given run."""
        # note the "-File" argument is required for the correct exit code to be recorded.
        return self.executable + ["-File", cmd_file_path]

    def get_version_info(self, exclude_os: Optional[bool] = False) -> Dict:
        """Get powershell version information.

        Parameters
        ----------
        exclude_os
            If True, exclude operating system information.

        """

        proc = subprocess.run(
            args=self.executable + ["-Command", "$PSVersionTable.PSVersion.ToString()"],
            stdout=subprocess.PIPE,
            text=True,
        )
        if proc.returncode == 0:
            PS_version = proc.stdout.strip()
        else:
            raise RuntimeError("Failed to parse PowerShell version information.")

        out = {
            "shell_name": "powershell",
            "shell_executable": self.executable,
            "shell_version": PS_version,
        }

        if not exclude_os:
            out.update(**get_OS_info_windows())

        return out

    @staticmethod
    def process_app_invoc_executable(app_invoc_exe):
        if " " in app_invoc_exe:
            # use call operator and single-quote the executable path:
            app_invoc_exe = f"& '{app_invoc_exe}'"
        return app_invoc_exe

    def format_env_var_get(self, var: str):
        return f"$env:{var}"

    def format_array(self, lst: List) -> str:
        return "@(" + ", ".join(str(i) for i in lst) + ")"

    def format_array_get_item(self, arr_name, index) -> str:
        return f"${arr_name}[{index}]"

    def format_stream_assignment(self, shell_var_name, command):
        return f"${shell_var_name} = {command}"

    def format_source_functions_file(self, app_name, commands):
        app_caps = app_name.upper()
        out = dedent(
            """\
            . $env:{app_name}_JS_FUNCS_PATH

            """
        ).format(app_name=app_caps)

        var_strings = (
            f"{app_caps}_WK_PATH",
            f"{app_caps}_SUB_SCRIPTS_DIR",
            f"{app_caps}_JS_IDX",
            f"{app_caps}_BLOCK_IDX",
            f"{app_caps}_BLOCK_ACT_IDX",
            f"{app_caps}_RUN_ID",
            f"{app_caps}_RUN_STD_PATH",
            f"{app_caps}_RUN_SCRIPT_NAME",
            f"{app_caps}_RUN_SCRIPT_NAME_NO_EXT",
            f"{app_caps}_RUN_SCRIPT_DIR",
            f"{app_caps}_RUN_SCRIPT_PATH",
        )
        add = False
        for i in var_strings:
            if i in commands:
                add = True
                out += f"${i} = $env:{i}\n"

        if add:
            out += "\n"

        return out

    def format_commands_file(self, app_name, commands):
        return (
            self.format_source_functions_file(app_name, commands)
            + commands
            + "\nexit $LASTEXITCODE\n"
        )

    def format_save_parameter(
        self,
        workflow_app_alias: str,
        param_name: str,
        shell_var_name: str,
        cmd_idx: int,
        stderr: bool,
        app_name: str,
    ):
        # TODO: quote shell_var_name as well? e.g. if it's a white-space delimited list?
        #   and test.
        stderr_str = " --stderr" if stderr else ""
        app_caps = app_name.upper()
        return (
            f'{workflow_app_alias} --std-stream "${app_caps}_RUN_STD_PATH" '
            f'internal workflow "${app_caps}_WK_PATH" save-parameter {stderr_str}'
            f'"--" {param_name} ${shell_var_name} ${app_caps}_RUN_ID {cmd_idx}'
            f"\n"
        )
