"""
Model of an execution environment.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.errors import DuplicateExecutableError
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.object_list import ExecutablesList
from hpcflow.sdk.core.utils import check_valid_py_identifier, get_duplicate_items

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from typing import Any, ClassVar


@dataclass
class NumCores(JSONLike):
    """
    A range of cores supported by an executable instance.

    Parameters
    ----------
    start:
        The minimum number of cores supported.
    stop:
        The maximum number of cores supported.
    step: int
        The step in the number of cores supported. Defaults to 1.
    """

    #: The minimum number of cores supported.
    start: int
    #: The maximum number of cores supported.
    stop: int
    #: The step in the number of cores supported. Normally 1.
    step: int = 1

    def __contains__(self, x: int) -> bool:
        return x in range(self.start, self.stop + 1, self.step)


@dataclass
@hydrate
class ExecutableInstance(JSONLike):
    """
    A particular instance of an executable that can support some mode of operation.

    Parameters
    ----------
    parallel_mode:
        What parallel mode is supported by this executable instance.
    num_cores: NumCores | int | dict[str, int]
        The number of cores supported by this executable instance.
    command:
        The actual command to use for this executable instance.
    """

    #: What parallel mode is supported by this executable instance.
    parallel_mode: str | None
    #: The number of cores supported by this executable instance.
    num_cores: NumCores
    #: The actual command to use for this executable instance.
    command: str

    def __init__(
        self, parallel_mode: str | None, num_cores: NumCores | int | dict, command: str
    ):
        self.parallel_mode = parallel_mode
        self.command = command
        if isinstance(num_cores, NumCores):
            self.num_cores = num_cores
        elif isinstance(num_cores, int):
            self.num_cores = NumCores(num_cores, num_cores)
        else:
            self.num_cores = NumCores(**num_cores)

    @classmethod
    def from_spec(cls, spec: dict[str, Any]) -> ExecutableInstance:
        """
        Construct an instance from a specification dictionary.
        """
        return cls(**spec)


class Executable(JSONLike):
    """
    A program managed by the environment.

    Parameters
    ----------
    label:
        The abstract name of the program.
    instances: list[ExecutableInstance]
        The concrete instances of the application that may be present.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="instances",
            class_name="ExecutableInstance",
            is_multiple=True,
        ),
    )

    def __init__(self, label: str, instances: list[ExecutableInstance]):
        #: The abstract name of the program.
        self.label = check_valid_py_identifier(label)
        #: The concrete instances of the application that may be present.
        self.instances = instances

        self._executables_list: ExecutablesList | None = None  # assigned by parent

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"label={self.label}, "
            f"instances={self.instances!r}"
            f")"
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.label == other.label
            and self.instances == other.instances
            and (
                (
                    self.environment
                    and other.environment
                    and self.environment.name == other.environment.name
                )
                or (not self.environment and not other.environment)
            )
        )

    @property
    def environment(self) -> Environment | None:
        """
        The environment that the executable is going to run in.
        """
        return None if (el := self._executables_list) is None else el.environment

    def filter_instances(
        self, parallel_mode: str | None = None, num_cores: int | None = None
    ) -> list[ExecutableInstance]:
        """
        Select the instances of the executable that are compatible with the given
        requirements.

        Parameters
        ----------
        parallel_mode: str
            If given, the parallel mode to require.
        num_cores:  int
            If given, the number of cores desired.

        Returns
        -------
        list[ExecutableInstance]:
            The known executable instances that match the requirements.
        """
        return [
            inst
            for inst in self.instances
            if (parallel_mode is None or inst.parallel_mode == parallel_mode)
            and (num_cores is None or num_cores in inst.num_cores)
        ]


class Environment(JSONLike):
    """
    An execution environment that contains a number of executables.

    Parameters
    ----------
    name: str
        The name of the environment.
    setup: list[str]
        Commands to run to enter the environment.
    specifiers: dict[str, str]
        Dictionary of attributes that may be used to supply additional key/value pairs to
        look up an environment by.
    executables: list[Executable]
        List of abstract executables in the environment.
    """

    _validation_schema: ClassVar[str] = "environments_spec_schema.yaml"
    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="executables",
            class_name="ExecutablesList",
            parent_ref="environment",
        ),
    )

    def __init__(
        self,
        name: str,
        setup: Sequence[str] | None = None,
        specifiers: Mapping[str, str] | None = None,
        executables: ExecutablesList | Sequence[Executable] | None = None,
        doc: str = "",
        _hash_value: str | None = None,
    ):
        #: The name of the environment.
        self.name = name
        #: Documentation for the environment.
        self.doc = doc
        #: Dictionary of attributes that may be used to supply additional key/value pairs
        #: to look up an environment by.
        self.specifiers: Mapping[str, str] = specifiers or {}
        #: List of abstract executables in the environment.
        self.executables = (
            executables
            if isinstance(executables, ExecutablesList)
            else self._app.ExecutablesList(executables or ())
        )
        self._hash_value = _hash_value
        #: Commands to run to enter the environment.
        self.setup: tuple[str, ...] | None
        if not setup:
            self.setup = None
        elif isinstance(setup, str):
            self.setup = tuple(cmd.strip() for cmd in setup.strip().split("\n"))
        else:
            self.setup = tuple(setup)
        self._set_parent_refs()
        self._validate()

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.setup == other.setup
            and self.executables == other.executables
            and self.specifiers == other.specifiers
        )

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name!r})"

    def _validate(self):
        if dup_labels := get_duplicate_items(exe.label for exe in self.executables):
            raise DuplicateExecutableError(dup_labels)

    @property
    def documentation(self) -> str:
        if self.doc:
            import markupsafe

            return markupsafe.Markup(self.doc)
        return repr(self)
