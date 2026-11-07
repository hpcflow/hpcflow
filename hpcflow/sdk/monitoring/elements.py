"""Roll up action states into one state per task element."""

from collections import Counter, defaultdict
from collections.abc import Sequence

from .models import ActionSummary, ElementGroup, IterationActionState

# Failure-related states remain visible even while other actions are outstanding.
_STATE_PRIORITY = (
    "error",
    "aborted",
    "interrupted",
    "skipped",
    "running",
    "unknown",
    "submitted",
    "scheduler_pending",
    "prepared",
    "pending",
    "condition_skipped",
    "loop_skipped",
    "success",
)


def overall_element_state(pattern: Sequence[IterationActionState]) -> str:
    """Reduce the latest action attempts across all displayed iterations to one state.

    Failures precede running and waiting states; task-condition skips precede success.
    Ignore loop-skipped future iterations when other states exist, but preserve a
    wholly loop-skipped element. An empty pattern is pending.
    """
    latest: dict[tuple[int, int], IterationActionState] = {}
    for step in pattern:
        key = step.iteration, step.action
        if key not in latest or step.attempt > latest[key].attempt:
            latest[key] = step
    states = {step.state for step in latest.values()}
    # Terminated future iterations do not make an otherwise completed element
    # loop-skipped; wholly skipped elements still have their own L state.
    if len(states) > 1:
        states.discard("loop_skipped")
    return next((state for state in _STATE_PRIORITY if state in states), "pending")


def element_summary(summary: ActionSummary) -> ActionSummary:
    """Aggregate an action summary into counts and groups of overall element states.

    Count each element once across its iterations and group equal states per task.
    Use a synthetic single-action pattern so existing action renderers can display
    the result. Apply loop-skip visibility before calling this function.
    """
    counts: Counter[str] = Counter()
    grouped: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    for group in summary.groups:
        if not group.pattern:
            continue
        state = overall_element_state(group.pattern)
        counts[state] += len(group.elements)
        grouped[group.task][state].extend(group.elements)
    groups = [
        ElementGroup(task, (IterationActionState(0, 0, 0, state),), sorted(elements))
        for task, states in grouped.items()
        for state, elements in states.items()
    ]
    return ActionSummary(
        counts=dict(counts),
        display_counts=dict(counts),
        groups=groups,
        observed_at=summary.observed_at,
        complete=summary.complete,
    )
