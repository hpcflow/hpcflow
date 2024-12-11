"""
Model of an execution environment.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Any

from textwrap import dedent

from hpcflow.sdk import app
from hpcflow.sdk.core.errors import DuplicateExecutableError
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.object_list import ExecutablesList
from hpcflow.sdk.core.utils import check_valid_py_identifier, get_duplicate_items


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
    step: int = None

    def __post_init__(self):
        if self.step is None:
            self.step = 1

    def __contains__(self, x):
        if x in range(self.start, self.stop + 1, self.step):
            return True
        else:
            return False

    def __eq__(self, other):
        if (
            type(self) == type(other)
            and self.start == other.start
            and self.stop == other.stop
            and self.step == other.step
        ):
            return True
        return False


@dataclass
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
    parallel_mode: str
    #: The number of cores supported by this executable instance.
    num_cores: Any
    #: The actual command to use for this executable instance.
    command: str

    def __post_init__(self):
        if not isinstance(self.num_cores, dict):
            self.num_cores = {"start": self.num_cores, "stop": self.num_cores}
        if not isinstance(self.num_cores, NumCores):
            self.num_cores = self.app.NumCores(**self.num_cores)

    def __eq__(self, other):
        if (
            type(self) == type(other)
            and self.parallel_mode == other.parallel_mode
            and self.num_cores == other.num_cores
            and self.command == other.command
        ):
            return True
        return False

    @classmethod
    def from_spec(cls, spec):
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

    _child_objects = (
        ChildObjectSpec(
            name="instances",
            class_name="ExecutableInstance",
            is_multiple=True,
        ),
    )

    def __init__(self, label: str, instances: List[app.ExecutableInstance]):
        #: The abstract name of the program.
        self.label = check_valid_py_identifier(label)
        #: The concrete instances of the application that may be present.
        self.instances = instances

        self._executables_list = None  # assigned by parent

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"label={self.label}, "
            f"instances={self.instances!r}"
            f")"
        )

    def __eq__(self, other):
        if (
            type(self) == type(other)
            and self.label == other.label
            and self.instances == other.instances
            and self.environment.name == other.environment.name
        ):
            return True
        return False

    @property
    def environment(self):
        """
        The environment that the executable is going to run in.
        """
        return self._executables_list.environment

    def filter_instances(self, parallel_mode=None, num_cores=None):
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
        out = []
        for i in self.instances:
            if parallel_mode is None or i.parallel_mode == parallel_mode:
                if num_cores is None or num_cores in i.num_cores:
                    out.append(i)
        return out


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
        Dictionary of attributes that may be used to supply addional key/value pairs to
        look up an environment by.
    executables: list[Executable]
        List of abstract executables in the environment.
    """

    _hash_value = None
    _validation_schema = "environments_spec_schema.yaml"
    _child_objects = (
        ChildObjectSpec(
            name="executables",
            class_name="ExecutablesList",
            parent_ref="environment",
        ),
    )

    def __init__(
        self,
        name,
        setup=None,
        specifiers=None,
        executables=None,
        doc="",
        _hash_value=None,
    ):
        #: The name of the environment.
        self.name = name
        #: Documentation for the environment.
        self.doc = doc
        #: Commands to run to enter the environment.
        self.setup = setup
        #: Dictionary of attributes that may be used to supply addional key/value pairs
        #: to look up an environment by.
        self.specifiers = specifiers or {}
        #: List of abstract executables in the environment.
        self.executables = (
            executables
            if isinstance(executables, ExecutablesList)
            else self.app.ExecutablesList(executables or [])
        )
        self._hash_value = _hash_value
        if self.setup:
            if isinstance(self.setup, str):
                self.setup = tuple(
                    i.strip() for i in dedent(self.setup).strip().split("\n")
                )
            elif not isinstance(self.setup, tuple):
                self.setup = tuple(self.setup)
        self._set_parent_refs()
        self._validate()

    def __eq__(self, other):
        if (
            type(self) == type(other)
            and self.setup == other.setup
            and self.executables == other.executables
            and self.specifiers == other.specifiers
        ):
            return True
        return False

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name!r})"

    def _validate(self):
        dup_labels = get_duplicate_items(i.label for i in self.executables)
        if dup_labels:
            raise DuplicateExecutableError(
                f"Executables must have unique `label`s within each environment, but "
                f"found label(s) multiple times: {dup_labels!r}"
            )

    @property
    def documentation(self) -> str:
        if self.doc:
            import markupsafe

            return markupsafe.Markup(self.doc)
        return repr(self)
