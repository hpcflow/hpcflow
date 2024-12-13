"""
Base model of a shell.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING
from hpcflow.sdk.typing import hydrate

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path
    from typing import Any, ClassVar
    from ..types import JobscriptHeaderArgs, VersionInfo


@hydrate
class Shell(ABC):
    """Class to represent a shell and templates for jobscript composition.

    This class represents a combination of a shell and an OS. For example, running
    bash on a POSIX OS, and provides snippets that are used to compose a jobscript for
    that combination.

    Parameters
    ----------
    executable: str
        Which executable implements the shell.
    os_args:
        Arguments to pass to the shell.
    """

    #: File extension for jobscripts.
    JS_EXT: ClassVar[str]
    #: Default for executable name.
    DEFAULT_EXE: ClassVar[str]
    #: Indent for environment setup.
    JS_ENV_SETUP_INDENT: ClassVar[str]
    #: Template for the jobscript shebang line.
    JS_SHEBANG: ClassVar[str]
    #: Template for the common part of the jobscript header.
    JS_HEADER: ClassVar[str]
    #: Template for the jobscript header when scheduled.
    JS_SCHEDULER_HEADER: ClassVar[str]
    #: Template for the jobscript header when directly executed.
    JS_DIRECT_HEADER: ClassVar[str]
    #: Template for the jobscript body.
    JS_MAIN: ClassVar[str]
    #: Template for the array handling code in a jobscript.
    JS_ELEMENT_ARRAY: ClassVar[str]
    #: Template for the element processing loop in a jobscript.
    JS_ELEMENT_LOOP: ClassVar[str]
    #: Basic indent.
    JS_INDENT: ClassVar[str]
    __slots__ = ("_executable", "os_args")

    def __init__(
        self, executable: str | None = None, os_args: dict[str, str] | None = None
    ):
        #: Which executable implements the shell.
        self._executable = executable or self.DEFAULT_EXE
        #: Arguments to pass to the shell.
        self.os_args = os_args or {}

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self._executable == other._executable and self.os_args == other.os_args

    @property
    def executable(self) -> list[str]:
        """
        The executable to use plus any mandatory arguments.
        """
        return [self._executable]

    @property
    def shebang_executable(self) -> list[str]:
        """
        The executable to use in a shebang line.
        """
        return self.executable

    def get_direct_submit_command(self, js_path: str) -> list[str]:
        """Get the command for submitting a non-scheduled jobscript."""
        return self.executable + [js_path]

    @abstractmethod
    def get_version_info(self, exclude_os: bool = False) -> VersionInfo:
        """Get shell and operating system information."""

    def get_wait_command(
        self, workflow_app_alias: str, sub_idx: int, deps: Mapping[int, Any]
    ):
        """
        Get the command to wait for a workflow.
        """
        if not deps:
            return ""
        return (
            f"{workflow_app_alias} workflow $WK_PATH_ARG wait --jobscripts "
            f'"{sub_idx}:{",".join(str(i) for i in deps)}"'
        )

    @staticmethod
    def process_app_invoc_executable(app_invoc_exe: str) -> str:
        """
        Perform any post-processing of an application invocation command name.
        """
        return app_invoc_exe

    def process_JS_header_args(
        self, header_args: JobscriptHeaderArgs
    ) -> JobscriptHeaderArgs:
        """
        Process the application invocation key in the jobscript header arguments.
        """
        app_invoc_ = header_args["app_invoc"]
        if not isinstance(app_invoc_, str):
            app_invoc = self.process_app_invoc_executable(app_invoc_[0])
            for item in app_invoc_[1:]:
                app_invoc += f' "{item}"'
            header_args["app_invoc"] = app_invoc
        return header_args

    def prepare_JS_path(self, js_path: Path) -> str:
        """
        Prepare the jobscript path for use.
        """
        return str(js_path)

    def prepare_element_run_dirs(self, run_dirs: list[list[Path]]) -> list[list[str]]:
        """
        Prepare the element run directory names for use.
        """
        return [[str(path) for path in i] for i in run_dirs]

    @abstractmethod
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
        Format instructions to save a parameter.
        """

    @abstractmethod
    def wrap_in_subshell(self, commands: str, abortable: bool) -> str:
        """
        Format commands to run within a child scope.

        This assumes `commands` ends in a newline.
        """

    @abstractmethod
    def format_loop_check(
        self, workflow_app_alias: str, loop_name: str, run_ID: int
    ) -> str:
        """
        Format a loop check.
        """

    @abstractmethod
    def format_stream_assignment(self, shell_var_name: str, command: str) -> str:
        """
        Format a stream assignment.
        """
