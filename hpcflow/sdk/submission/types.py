"""
Types for the submission subsystem.
"""

from __future__ import annotations
from typing import Any, TYPE_CHECKING
from typing_extensions import NotRequired, TypeAlias, TypedDict

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime
    from numpy.typing import NDArray
    from ..core.element import ElementResources


class JobScriptDescriptor(TypedDict):
    """
    Descriptor for a jobscript.
    """

    #: Resources required by the jobscript.
    resources: Any
    #: Elements handled by the jobscript.
    elements: dict[int, list[int]]
    #: Dependencies of the jobscript.
    dependencies: NotRequired[dict[int, ResolvedJobscriptBlockDependencies]]
    #: Hash of resources.
    resource_hash: NotRequired[str]


class ResolvedJobscriptBlockDependencies(TypedDict):
    """
    The resolution of a jobscript block dependency. This represents the dependency of one
    jobscript block on another.
    """

    #: Mapping of jobscript elements.
    js_element_mapping: dict[int, list[int]]
    #: Whether this is an array mapping.
    is_array: NotRequired[bool]


class JobScriptCreationArguments(TypedDict):
    """
    Arguments to pass to create a :class:`Jobscript`.
    """

    # TODO: this currently represents a mix of arguments for both jobscripts and jobscript
    # blocks; need to separate

    #: The task insertion IDs.
    task_insert_IDs: list[int]
    #: The actions of the tasks.
    task_actions: list[tuple[int, int, int]]
    #: The elements of the tasks.
    task_elements: dict[int, list[int]]
    #: Element action run information.
    EAR_ID: NDArray
    #: Resources to use.
    resources: NotRequired[ElementResources]
    #: Description of what loops are in play.
    task_loop_idx: list[dict[str, int]]
    #: Description of dependencies.
    dependencies: dict[int | tuple[int, int], ResolvedJobscriptBlockDependencies]
    #: Whether this is an array jobscript.
    is_array: NotRequired[bool]
    #: When the jobscript was submitted, if known.
    submit_time: NotRequired[datetime]
    #: Where the jobscript was submitted, if known.
    submit_hostname: NotRequired[str]
    #: Description of what the jobscript was submitted to, if known.
    submit_machine: NotRequired[str]
    #: The command line used to do the commit, if known.
    submit_cmdline: NotRequired[list[str]]
    #: The job ID from the scheduler, if known.
    scheduler_job_ID: NotRequired[str]
    #: The process ID of the subprocess, if known.
    process_ID: NotRequired[int]
    #: Version info about the target system.
    version_info: NotRequired[dict[str, str | list[str]]]
    #: The name of the OS.
    os_name: NotRequired[str]
    #: The name of the shell.
    shell_name: NotRequired[str]
    #: The scheduler used.
    scheduler_name: NotRequired[str]
    #: Whether the jobscript is currently running.
    running: NotRequired[bool]
    #: Do not supply!
    resource_hash: NotRequired[str]
    #: Do not supply!
    elements: NotRequired[dict[int, list[int]]]


class SchedulerRef(TypedDict):
    """
    Scheduler reference descriptor.
    """

    #: Jobscript references.
    js_refs: list  # Internal type is horrible and variable
    #: Number of jobscript elements.
    num_js_elements: int


class SubmissionPart(TypedDict):
    """
    A part of a submission.
    """

    #: Timestamp for when this part was submitted.
    submit_time: datetime
    #: The jobscripts involved in this submission.
    jobscripts: list[int]


# This needs PEP 728 for a better type, alas
#: Version data.
VersionInfo: TypeAlias = "dict[str, str | list[str]]"


# TODO: This really doesn't belong here?!
class JobscriptHeaderArgs(TypedDict):
    """
    Keyword arguments to use when creating a job script from a
    :class:`Jobscript`.
    """

    #: Application invocation. (Arguments, etc.)
    app_invoc: str | Sequence[str]
    #: Workflow application alias.
    workflow_app_alias: NotRequired[str]
    #: Environment setup.
    env_setup: NotRequired[str]
    #: Application name in CAPS
    app_caps: NotRequired[str]
    #: Configuration directory.
    config_dir: NotRequired[str]
    #: Configuration key.
    config_invoc_key: NotRequired[Any]
