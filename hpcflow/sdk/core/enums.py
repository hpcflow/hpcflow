"""
Core enumeration types.
"""

from __future__ import annotations
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum


class ActionScopeType(Enum):
    """
    Types of action scope.
    """

    #: Scope that applies to anything.
    ANY = 0
    #: Scope that only applies to main scripts.
    MAIN = 1
    #: Scope that applies to processing steps.
    PROCESSING = 2
    #: Scope that applies to input file generators.
    INPUT_FILE_GENERATOR = 3
    #: Scope that applies to output file parsers.
    OUTPUT_FILE_PARSER = 4


@dataclass(frozen=True)
class _ReportableStateData:
    """
    Model of the state of something that is renderable using a symbol and colour.

    Notes
    -----
    This class is used as the value in the the enumeration subclasses
    :py:class:`EARStatus` and :py:class:`JobscriptElementState`.

    """

    #: ID that distinguishes the state.
    id: int
    #: Symbol to use when rendering a state.
    symbol: str
    #: Colour to use when rendering a state.
    colour: str
    #: Documentation of the meaning of the state.
    __doc__: str = ""


class _ReportableStateEnum(Enum):
    """Enumeration superclass for reportable state subclasses with some shared methods."""

    @property
    def id(self) -> int:
        """
        The integer ID associated with this state.
        """
        return self.value.id

    @property
    def colour(self) -> str:
        """
        The colour associated with this state.
        """
        return self.value.colour

    @property
    def symbol(self) -> str:
        """
        The symbol associated with this state.
        """
        return self.value.symbol

    @property
    def rich_repr(self) -> str:
        """
        Rich representation of this enumeration element.
        """
        return f"[{self.colour}]{self.symbol}[/{self.colour}]"


class EARStatus(_ReportableStateEnum):
    """Enumeration of all possible EAR statuses, and their associated status colour."""

    #: Not yet associated with a submission.
    pending = _ReportableStateData(
        0,
        ".",
        "grey46",
        "Not yet associated with a submission.",
    )
    #: Associated with a prepared submission that is not yet submitted.
    prepared = _ReportableStateData(
        1,
        ".",
        "grey46",
        "Associated with a prepared submission that is not yet submitted.",
    )
    #: Submitted for execution.
    submitted = _ReportableStateData(
        2,
        ".",
        "grey46",
        "Submitted for execution.",
    )
    #: Executing now.
    running = _ReportableStateData(
        3,
        "●",
        "dodger_blue1",
        "Executing now.",
    )
    #: Not attempted due to a failure of an upstream action on which this depends,
    #: or a loop termination condition being satisfied.
    skipped = _ReportableStateData(
        4,
        "s",
        "dark_orange",
        (
            "Not attempted due to a failure of an upstream action on which this depends, "
            "or a loop termination condition being satisfied."
        ),
    )
    #: Aborted by the user; downstream actions will be attempted.
    aborted = _ReportableStateData(
        5,
        "A",
        "deep_pink4",
        "Aborted by the user; downstream actions will be attempted.",
    )
    #: Probably exited successfully.
    success = _ReportableStateData(
        6,
        "■",
        "green3",
        "Probably exited successfully.",
    )
    #: Probably failed.
    error = _ReportableStateData(
        7,
        "E",
        "red3",
        "Probably failed.",
    )

    @classmethod
    def get_non_running_submitted_states(cls) -> frozenset[EARStatus]:
        """Return the set of all non-running states, excluding those before submission."""
        return frozenset(
            {
                cls.skipped,
                cls.aborted,
                cls.success,
                cls.error,
            }
        )


class InputSourceType(Enum):
    """
    The types of input sources.
    """

    #: Input source is an import.
    IMPORT = 0
    #: Input source is local.
    LOCAL = 1
    #: Input source is a default.
    DEFAULT = 2
    #: Input source is a task.
    TASK = 3


class ParallelMode(Enum):
    """
    Potential parallel modes.
    """

    #: Use distributed-memory parallelism (e.g. MPI).
    DISTRIBUTED = 0
    #: Use shared-memory parallelism (e.g. OpenMP).
    SHARED = 1
    #: Use both distributed- and shared-memory parallelism.
    #:
    #: Note
    #: ----
    #: This is not yet implemented in any meaningful way!
    HYBRID = 2


class ParameterPropagationMode(Enum):
    """
    How a parameter is propagated.
    """

    #: Parameter is propagated implicitly.
    IMPLICIT = 0
    #: Parameter is propagated explicitly.
    EXPLICIT = 1
    #: Parameter is never propagated.
    NEVER = 2


class TaskSourceType(Enum):
    """
    The types of task-based input sources.
    """

    #: Input source is a task input.
    INPUT = 0
    #: Input source is a task output.
    OUTPUT = 1
    #: Input source is unspecified.
    ANY = 2

    @classmethod
    def names(cls) -> Sequence[str]:
        """
        Get the names of the task source types.
        """
        return cls._member_names_
