from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional

from hpcflow.sdk.utils.hashing import get_hash


class Shell(ABC):
    """Class to represent a shell and templates for jobscript composition.

    This class represents a combination of a shell and an OS. For example, running
    bash on a POSIX OS, and provides snippets that are used to compose a jobscript for
    that combination.

    """

    def __init__(self, executable=None, os_args=None):
        self._executable = executable or self.DEFAULT_EXE
        self.os_args = os_args

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if self._executable == other._executable and self.os_args == other.os_args:
            return True
        return False

    def __hash__(self):
        return get_hash((self._executable, self.os_args))

    @property
    def executable(self) -> List[str]:
        return [self._executable]

    @property
    def shebang_executable(self) -> List[str]:
        return self.executable

    def get_direct_submit_command(self, js_path) -> List[str]:
        """Get the command for submitting a non-scheduled jobscript."""
        return self.executable + [js_path]

    def get_command_file_launch_command(self, cmd_file_path: str) -> List[str]:
        """Get the command for launching the commands file for a given run."""
        return self.executable + [cmd_file_path]

    @abstractmethod
    def get_version_info(self, exclude_os: Optional[bool] = False) -> Dict:
        """Get shell and operating system information."""

    def get_wait_command(self, workflow_app_alias: str, sub_idx: int, deps: Dict):
        if deps:
            return (
                f'{workflow_app_alias} workflow $WK_PATH_ARG wait --jobscripts "{sub_idx}:'
                + ",".join(str(i) for i in deps.keys())
                + '"'
            )
        else:
            return ""

    @staticmethod
    def process_app_invoc_executable(app_invoc_exe):
        return app_invoc_exe

    def process_JS_header_args(self, header_args: Dict) -> Dict:
        app_invoc = self.process_app_invoc_executable(header_args["app_invoc"][0])
        if len(header_args["app_invoc"]) > 1:
            app_invoc += ' "' + header_args["app_invoc"][1] + '"'

        header_args["app_invoc"] = app_invoc
        return header_args

    def prepare_JS_path(self, js_path: Path) -> str:
        return str(js_path)

    def prepare_element_run_dirs(self, run_dirs: List[List[Path]]) -> List[List[str]]:
        return [[str(j) for j in i] for i in run_dirs]
