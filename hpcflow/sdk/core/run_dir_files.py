"""
Model of files in the run directory.
"""

from __future__ import annotations
import re
from typing import Any, TYPE_CHECKING
from hpcflow.sdk.core.app_aware import AppAware
from hpcflow.sdk.core.utils import JSONLikeDirSnapShot

if TYPE_CHECKING:
    from re import Pattern
    from typing_extensions import ClassVar
    from ..submission.shells.base import Shell


class RunDirAppFiles(AppAware):
    """A class to encapsulate the naming/recognition of app-created files within run
    directories."""

    __CMD_FILES_RE_PATTERN: ClassVar[Pattern] = re.compile(r"js_\d+_act_\d+\.?\w*")

    @classmethod
    def get_log_file_name(cls) -> str:
        """File name for the app log file."""
        return f"{cls._app.package_name}.log"

    @classmethod
    def get_std_file_name(cls) -> str:
        """File name for stdout and stderr streams from the app."""
        return f"{cls._app.package_name}_std.txt"

    @staticmethod
    def get_run_file_prefix(js_idx: int | str, js_action_idx: int | str) -> str:
        """
        Get the common prefix for files associated with a run.
        """
        return f"js_{js_idx}_act_{js_action_idx}"

    @classmethod
    def get_commands_file_name(
        cls, js_idx: int | str, js_action_idx: int | str, shell: Shell
    ) -> str:
        """
        Get the name of the file containing commands.
        """
        return cls.get_run_file_prefix(js_idx, js_action_idx) + shell.JS_EXT

    @classmethod
    def get_run_param_dump_file_prefix(
        cls, js_idx: int | str, js_action_idx: int | str
    ) -> str:
        """Get the prefix to a file in the run directory that the app will dump parameter
        data to."""
        return cls.get_run_file_prefix(js_idx, js_action_idx) + "_inputs"

    @classmethod
    def get_run_param_load_file_prefix(
        cls, js_idx: int | str, js_action_idx: int | str
    ) -> str:
        """Get the prefix to a file in the run directory that the app will load parameter
        data from."""
        return cls.get_run_file_prefix(js_idx, js_action_idx) + "_outputs"

    @classmethod
    def take_snapshot(cls) -> dict[str, Any]:
        """
        Take a :py:class:`JSONLikeDirSnapShot`, and process to ignore files created by
        the app.

        This includes command files that are invoked by jobscripts, the app log file, and
        the app standard out/error file.
        """
        snapshot = JSONLikeDirSnapShot()
        snapshot.take(".")
        ss_js = snapshot.to_json_like()
        ss_js.pop("root_path")  # always the current working directory of the run
        excluded = {cls.get_log_file_name(), cls.get_std_file_name()}
        data: dict[str, Any] = ss_js["data"]
        for filename in tuple(data):
            if filename in excluded or cls.__CMD_FILES_RE_PATTERN.match(filename):
                data.pop(filename)

        return ss_js
