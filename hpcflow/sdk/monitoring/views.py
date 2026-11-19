"""Resolve explicit show views and legacy detail/grouping options."""

from typing import Literal, cast

ShowView = Literal["counts", "tasks", "detail", "auto"]
SHOW_VIEWS = ("counts", "tasks", "detail", "auto")


def resolve_view(
    view: str | None = None,
    full: bool = False,
    group_elements: bool = True,
    group_iterations: bool = True,
) -> ShowView:
    """Prefer an explicit view; otherwise preserve counts, --full, and ungrouping.

    Ungrouping requests exact detail and is incompatible with explicit aggregate
    views. Auto respects ungrouping by retaining detailed patterns.
    """
    if view is not None and view not in SHOW_VIEWS:
        raise ValueError(
            f"Unknown show view {view!r}; choose from: {', '.join(SHOW_VIEWS)}"
        )
    ungrouped = not group_elements or not group_iterations
    if view in ("counts", "tasks") and ungrouped:
        raise ValueError(
            f"The {view!r} view cannot be combined with disabled grouping; use 'detail' or 'auto'"
        )
    return cast(
        ShowView, view or ("auto" if full else "detail" if ungrouped else "counts")
    )
