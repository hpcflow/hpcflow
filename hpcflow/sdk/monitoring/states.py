"""Action display states, including distinct symbols for known skip reasons."""

from dataclasses import dataclass

from ..core.enums import EARStatus
from ..core.skip_reason import SkipReason
from .models import ActionRunState


@dataclass(frozen=True)
class ActionDisplayState:
    """A named display state with its icon, colour, sort order, and legend description."""

    name: str
    symbol: str
    colour: str
    order: tuple[int, int]
    description: str


ACTION_STATES = {
    state.name: ActionDisplayState(
        state.name, state.symbol, state.colour, (state.id, 0), state.doc
    )
    for state in EARStatus
}
ACTION_STATES["skipped"] = ActionDisplayState(
    "skipped",
    EARStatus.skipped.symbol,
    EARStatus.skipped.colour,
    (EARStatus.skipped.id, 0),
    "Not attempted due to an upstream failure or an unspecified skip reason.",
)
ACTION_STATES["loop_skipped"] = ActionDisplayState(
    "loop_skipped",
    "L",
    EARStatus.pending.colour,
    (EARStatus.skipped.id, 1),
    "Not attempted because a loop terminated.",
)
ACTION_STATES["condition_skipped"] = ActionDisplayState(
    "condition_skipped",
    "C",
    EARStatus.pending.colour,
    (EARStatus.skipped.id, 2),
    "Not attempted because a task condition was not met.",
)
ACTION_STATES["interrupted"] = ActionDisplayState(
    "interrupted",
    "I",
    "dark_orange",
    (EARStatus.aborted.id, 1),
    "Last recorded as running, but the submission is inactive; exit outcome unknown.",
)
ACTION_STATES["unknown"] = ActionDisplayState(
    "unknown",
    "?",
    EARStatus.pending.colour,
    (EARStatus.running.id, 1),
    "Last recorded as running, but submission activity cannot be confirmed.",
)


def display_state_name(entry: ActionRunState) -> str:
    """Map an engine state to its display name, distinguishing known skip reasons.

    Loop termination and unmet task conditions have separate symbols; unknown or
    upstream-related skips keep the general skipped state.
    """
    if entry.state is EARStatus.skipped:
        if entry.skip_reason is SkipReason.LOOP_TERMINATION:
            return "loop_skipped"
        if entry.skip_reason is SkipReason.TASK_CONDITION_NOT_MET:
            return "condition_skipped"
    return entry.state.name
