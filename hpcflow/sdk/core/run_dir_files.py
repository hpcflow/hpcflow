"""
Model of files in the run directory.
"""

from __future__ import annotations
from typing import Any, TYPE_CHECKING
from hpcflow.sdk.core.app_aware import AppAware
from hpcflow.sdk.core.utils import JSONLikeDirSnapShot

if TYPE_CHECKING:
    from ..submission.shells.base import Shell
    from .types import BlockActionKey


class RunDirAppFiles(AppAware):
    """A class to encapsulate the naming/recognition of app-created files within run
    directories."""

    @staticmethod
    def get_run_file_prefix(block_act_key: BlockActionKey) -> str:
        """
        Get the common prefix for files associated with a run.
        """
        return f"js_{block_act_key[0]}_block_{block_act_key[1]}_act_{block_act_key[2]}"

    @classmethod
    def get_commands_file_name(cls, block_act_key: BlockActionKey, shell: Shell) -> str:
        """
        Get the name of the file containing commands.
        """
        return cls.get_run_file_prefix(block_act_key) + shell.JS_EXT

    @classmethod
    def get_run_param_dump_file_prefix(cls, block_act_key: BlockActionKey) -> str:
        """Get the prefix to a file in the run directory that the app will dump parameter
        data to."""
        return cls.get_run_file_prefix(block_act_key) + "_inputs"

    @classmethod
    def get_run_param_load_file_prefix(cls, block_act_key: BlockActionKey) -> str:
        """Get the prefix to a file in the run directory that the app will load parameter
        data from."""
        return cls.get_run_file_prefix(block_act_key) + "_outputs"

    @classmethod
    def take_snapshot(cls, root_path=None) -> dict[str, Any]:
        """
        Take a :py:class:`JSONLikeDirSnapShot`.

        This includes command files that are invoked by jobscripts, the app log file, and
        the app standard out/error file.
        """
        snapshot = JSONLikeDirSnapShot()
        snapshot.take(root_path or ".")
        ss_js = snapshot.to_json_like(use_strings=True)
        ss_js.pop("root_path")  # always the current working directory of the run
        return ss_js
