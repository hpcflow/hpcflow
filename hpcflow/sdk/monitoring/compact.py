"""Bounded task summaries for large iterative action layouts.

Aggregate existing element groups with their multiplicity rather than expanding
every element. Progress uses the latest attempt of each action, while run counts
retain all attempts, matching the detailed Actions view.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from itertools import groupby

from rich.text import Text

from .models import ActionSummary, IterationActionState
from .states import ACTION_STATES

PROBLEM_STATES = frozenset({"error", "aborted", "interrupted", "skipped"})
_PROBLEM_PRIORITY = {
    name: index
    for index, name in enumerate(("error", "aborted", "interrupted", "skipped"))
}
MAX_PROBLEM_GROUPS = 3


@dataclass(slots=True)
class CountRange:
    """Minimum and maximum counts across groups of task elements."""

    minimum: int | None = None
    maximum: int = 0

    def add(self, value: int) -> None:
        """Include one group's per-element count, including zero."""
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = max(self.maximum, value)

    @property
    def label(self) -> str:
        """Format counts, not iteration indices; zero completed iterations is valid."""
        low = self.minimum or 0
        return str(low) if low == self.maximum else f"{low}\u2013{self.maximum}"


@dataclass(frozen=True, slots=True)
class ProblemGroup:
    """Elements sharing a problem state at the same iterations and action index."""

    state: str
    action: int
    elements: tuple[int, ...]
    iterations: tuple[int, ...]


@dataclass(slots=True)
class TaskProgress:
    """One task's progress ranges, run totals, and bounded diagnostic references."""

    name: str
    elements: set[int] = field(default_factory=set)
    counts: Counter[str] = field(default_factory=Counter)
    completed: CountRange = field(default_factory=CountRange)
    visible: CountRange = field(default_factory=CountRange)
    condition_skipped: CountRange = field(default_factory=CountRange)
    problems: list[ProblemGroup] = field(default_factory=list)
    problem_group_count: int = 0


@dataclass(slots=True)
class TaskElementStates:
    """Overall element-state counts for one task, across all its iterations."""

    name: str
    counts: Counter[str] = field(default_factory=Counter)


def compact_element_text(summary: ActionSummary) -> Text:
    """Render per-task totals from an already aggregated element summary.

    Each synthetic pattern contains one overall state per element. Do not count
    iterations or action attempts here; their rollup belongs to element_summary.
    Names wrap separately from icons, as in the Actions task view.
    """
    tasks: dict[str, TaskElementStates] = {}
    for group in summary.groups:
        if group.pattern:
            task = tasks.setdefault(group.task, TaskElementStates(group.task))
            task.counts[group.pattern[0].state] += len(group.elements)
    text = Text("Task element summary\n", overflow="fold")
    for index, task in enumerate(tasks.values()):
        if index:
            text.append("\n")
        count = sum(task.counts.values())
        text.append(task.name, style="bold")
        text.append(
            f"  ({count} {'element' if count == 1 else 'elements'})\n  elements: "
        )
        for position, name in enumerate(
            sorted(task.counts, key=lambda name: ACTION_STATES[name].order)
        ):
            if position:
                text.append(" | ")
            state = ACTION_STATES[name]
            text.append(f"{state.symbol}:{task.counts[name]}", style=state.colour)
    text.append("\nExact ranges: --view detail", style="dim")
    return text


def task_progress(summary: ActionSummary) -> list[TaskProgress]:
    """Aggregate tasks without multiplying iteration patterns by element count.

    An iteration is complete only when all known task actions have a successful
    latest attempt. Missing actions in a partial terminal cache are not successes.
    Condition-skipped iterations are reported separately. Sparse iteration indices
    are counted directly rather than interpreted as contiguous progress.
    """
    tasks: dict[str, TaskProgress] = {}
    actions: dict[str, set[int]] = defaultdict(set)
    for group in summary.groups:
        actions[group.task].update(step.action for step in group.pattern)
    for group in summary.groups:
        if not group.elements or not group.pattern:
            continue
        task = tasks.setdefault(group.task, TaskProgress(group.task))
        task.elements.update(group.elements)
        weight = len(group.elements)
        latest: dict[tuple[int, int], IterationActionState] = {}
        for step in group.pattern:
            task.counts[step.state] += weight
            key = step.iteration, step.action
            if key not in latest or step.attempt > latest[key].attempt:
                latest[key] = step
        iterations: dict[int, list[IterationActionState]] = defaultdict(list)
        problems: dict[tuple[str, int], list[int]] = defaultdict(list)
        for step in latest.values():
            iterations[step.iteration].append(step)
            if step.state in PROBLEM_STATES:
                problems[step.state, step.action].append(step.iteration)
        completed = condition_skipped = 0
        for steps in iterations.values():
            if {step.action for step in steps} != actions[group.task]:
                continue
            states = {step.state for step in steps}
            if states == {"success"}:
                completed += 1
            elif states <= {"success", "condition_skipped"}:
                condition_skipped += 1
        task.completed.add(completed)
        task.visible.add(len(iterations))
        task.condition_skipped.add(condition_skipped)
        for (state, action), indices in problems.items():
            task.problem_group_count += 1
            task.problems.append(
                ProblemGroup(state, action, tuple(group.elements), tuple(sorted(indices)))
            )
            task.problems.sort(key=lambda problem: _PROBLEM_PRIORITY[problem.state])
            del task.problems[MAX_PROBLEM_GROUPS:]
    # Collected groups already follow workflow task order; retain it for every state.
    return list(tasks.values())


def _ranges(indices: tuple[int, ...], brackets: str) -> str:
    """Bound a diagnostic index label without implying omitted indices are included."""
    parts = []
    for _, values in groupby(
        enumerate(sorted(set(indices))), lambda pair: pair[1] - pair[0]
    ):
        run = [value for _, value in values]
        parts.append(str(run[0]) if len(run) == 1 else f"{run[0]}\u2013{run[-1]}")
    label = ",".join(parts)
    if len(label) > 40:
        return f"{len(set(indices))} indices (details omitted)"
    return brackets[0] + label + brackets[1]


def compact_action_text(summary: ActionSummary) -> Text:
    """Render wrapping task blocks with progress, icons, and bounded problem details.

    Keep state icons on their own lines so long task names and element ranges
    cannot consume their display columns. The output size depends on task count,
    not the number of distinct histories or iteration ranges.
    """
    text = Text(overflow="fold")
    text.append("Task summary\n", style="dim")
    for index, task in enumerate(task_progress(summary)):
        if index:
            text.append("\n")
        text.append(task.name, style="bold")
        text.append(
            f"  ({len(task.elements)} {'element' if len(task.elements) == 1 else 'elements'})\n"
        )
        text.append("  iters/element", style="italic")
        text.append(f": {task.completed.label} done / {task.visible.label} visible")
        if task.condition_skipped.maximum:
            text.append(
                f"; C {task.condition_skipped.label}",
                style=ACTION_STATES["condition_skipped"].colour,
            )
        text.append("\n  runs: ")
        for position, name in enumerate(
            sorted(task.counts, key=lambda name: ACTION_STATES[name].order)
        ):
            if position:
                text.append(" | ")
            state = ACTION_STATES[name]
            text.append(f"{state.symbol}:{task.counts[name]}", style=state.colour)
        for problem in task.problems:
            state = ACTION_STATES[problem.state]
            text.append(f"\n  {state.symbol} ", style=state.colour)
            text.append(f"elements {_ranges(problem.elements, '[]')}; ")
            text.append("iters", style="italic")
            text.append(f" {_ranges(problem.iterations, '{}')}; action {problem.action}")
        if remaining := task.problem_group_count - len(task.problems):
            text.append(f"\n  +{remaining} more problem groups", style="dim")
    text.append("\nExact ranges: --view detail", style="dim")
    return text
