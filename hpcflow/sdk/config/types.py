"""
Types used in configuration.
"""

from __future__ import annotations
from typing import TYPE_CHECKING
from typing_extensions import TypeAlias, TypedDict, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path
    from typing import Any
    from typing_extensions import NotRequired
    from .config import Config
    from ..core.validation import Schema


T = TypeVar("T")
#: Type of a getter callback.
GetterCallback: TypeAlias = "Callable[[Config, T], T]"
#: Type of a setter callback.
SetterCallback: TypeAlias = "Callable[[Config, T], Any]"
#: Type of a unsetter callback.
UnsetterCallback: TypeAlias = "Callable[[Config], None]"


class SGEParallelEnvsDescriptor(TypedDict):
    """
    SGE parallel environment descriptor.
    """

    #: Number of supported cores.
    num_cores: list[int]


class SLURMPartitionsDescriptor(TypedDict):
    """
    SLURM partition descriptor.
    """

    #: Number of supported cores.
    num_cores: NotRequired[list[int]]
    #: Number of cores per node.
    num_cores_per_node: NotRequired[list[int]]
    #: Number of supported nodes.
    num_nodes: NotRequired[list[int]]
    #: Possible parallel modes.
    parallel_modes: NotRequired[list[str]]


class SchedulerConfigDescriptor(TypedDict):
    """
    Scheduler configuration descriptor.
    """

    #: Default values.
    defaults: dict[str, Any]
    #: SGE parallel environments, by name.
    parallel_environments: NotRequired[dict[str, SGEParallelEnvsDescriptor]]
    #: SLURM partitions, by name.
    partitions: NotRequired[dict[str, SLURMPartitionsDescriptor]]


class ShellConfigDescriptor(TypedDict):
    """
    Shell configuration descriptor.
    """

    #: Default values.
    defaults: dict[str, Any]


class ConfigDescriptor(TypedDict):
    """
    Configuration descriptor.
    """

    #: Execution machine.
    machine: NotRequired[str]
    #: Where to log.
    log_file_path: NotRequired[str]
    #: Where to find execution environments.
    environment_sources: NotRequired[list[str]]
    #: Where to find task schemas.
    task_schema_sources: NotRequired[list[str]]
    #: Where to find command files.
    command_file_sources: NotRequired[list[str]]
    #: Where to find parameter implementations.
    parameter_sources: NotRequired[list[str]]
    #: Default scheduler.
    default_scheduler: NotRequired[str]
    #: Default shell.
    default_shell: NotRequired[str]
    #: Supported schedulers.
    schedulers: NotRequired[dict[str, SchedulerConfigDescriptor]]
    #: Supported shells.
    shells: NotRequired[dict[str, ShellConfigDescriptor]]


class InvocationDescriptor(TypedDict):
    """
    Invocation descriptor.
    """

    #: Used to set up the environment.
    environment_setup: str | None
    #: setting to apply if matched.
    match: dict[str, str | list[str]]


class DefaultConfiguration(TypedDict):
    """
    The default configuration.
    """

    #: Default invocation.
    invocation: InvocationDescriptor
    #: Default configuration.
    config: ConfigDescriptor


#: A configuration dictionary.
ConfigDict: TypeAlias = "dict[str, dict[str, DefaultConfiguration]]"


class ConfigMetadata(TypedDict):
    """
    Metadata supported by the :class:`Config` class.
    """

    #: Location of directory containing the config file.
    config_directory: Path
    #: Name of the config file.
    config_file_name: str
    #: Full path to the config file.
    config_file_path: Path
    #: The contents of the config file.
    config_file_contents: str
    #: The key identifying the config section within the config file.
    config_key: str
    #: Schemas that apply to the config.
    config_schemas: Sequence[Schema]
    #: The user that invoked things.
    invoking_user_id: str
    #: The user hosting things.
    host_user_id: str
    #: Path to file holding description of :attr:``host_user_id``.
    host_user_id_file_path: Path
