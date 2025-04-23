"""
Submission enumeration types.
"""

from __future__ import annotations
from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True)
class _JES:
    """
    Model of the state of a JobscriptElementState
    """

    _value: int
    #: The symbol used to render the state.
    symbol: str
    #: The colour used to render the state.
    colour: str
    __doc__: str = ""


class JobscriptElementState(_JES, Enum):
    """Enumeration to convey a particular jobscript element state as reported by the
    scheduler."""

    #: Waiting for resource allocation.
    pending = (
        0,
        "○",
        "yellow",
        "Waiting for resource allocation.",
    )
    #: Waiting for one or more dependencies to finish.
    waiting = (
        1,
        "◊",
        "grey46",
        "Waiting for one or more dependencies to finish.",
    )
    #: Executing now.
    running = (
        2,
        "●",
        "dodger_blue1",
        "Executing now.",
    )
    #: Previously submitted but is no longer active.
    finished = (
        3,
        "■",
        "grey46",
        "Previously submitted but is no longer active.",
    )
    #: Cancelled by the user.
    cancelled = (
        4,
        "C",
        "red3",
        "Cancelled by the user.",
    )
    #: The scheduler reports an error state.
    errored = (
        5,
        "E",
        "red3",
        "The scheduler reports an error state.",
    )

    @property
    def value(self) -> int:
        """
        The numerical value of this state.
        """
        return self._value

    @property
    def rich_repr(self) -> str:
        """
        Rich representation of this enumeration element.
        """
        return f"[{self.colour}]{self.symbol}[/{self.colour}]"


class SubmissionStatus(Enum):
    """
    The overall status of a submission.
    """

    #: Not yet submitted.
    PENDING = 0
    #: All jobscripts submitted successfully.
    SUBMITTED = 1
    #: Some jobscripts submitted successfully.
    PARTIALLY_SUBMITTED = 2
