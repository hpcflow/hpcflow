"""
Submission enumeration types.
"""

from __future__ import annotations
from dataclasses import dataclass
from enum import Enum

from hpcflow.sdk.core.enums import _ReportableStateData, _ReportableStateEnum


class JobscriptElementState(_ReportableStateEnum):
    """Enumeration to convey a particular jobscript element state as reported by the
    scheduler."""

    #: Waiting for resource allocation.
    pending = _ReportableStateData(
        0,
        "○",
        "yellow",
        "Waiting for resource allocation.",
    )
    #: Waiting for one or more dependencies to finish.
    waiting = _ReportableStateData(
        1,
        "◊",
        "grey46",
        "Waiting for one or more dependencies to finish.",
    )
    #: Executing now.
    running = _ReportableStateData(
        2,
        "●",
        "dodger_blue1",
        "Executing now.",
    )
    #: Previously submitted but is no longer active.
    finished = _ReportableStateData(
        3,
        "■",
        "grey46",
        "Previously submitted but is no longer active.",
    )
    #: Cancelled by the user.
    cancelled = _ReportableStateData(
        4,
        "C",
        "red3",
        "Cancelled by the user.",
    )
    #: The scheduler reports an error state.
    errored = _ReportableStateData(
        5,
        "E",
        "red3",
        "The scheduler reports an error state.",
    )


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
