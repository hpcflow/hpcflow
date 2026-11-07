"""List registered submissions without loading workflows or querying schedulers."""

from __future__ import annotations

from typing import TYPE_CHECKING
from collections.abc import Sequence

from .references import resolve_records
from .availability import existing_records
from .filtering import filter_templates
from .models import SubmissionRecord
from .registry import SubmissionRegistry, recent_key, sort_timestamp

if TYPE_CHECKING:
    from ..app import BaseApp


def list_registered_submissions(
    app: BaseApp,
    workflow_ref=None,
    ref_type=None,
    max_recent: int | None = None,
    template_names: Sequence[str] | str | None = None,
    include_missing: bool = False,
) -> list[SubmissionRecord]:
    """Return registered active/unknown records followed by recent inactive history.

    Resolve an optional workflow reference and exact template-name filter before
    applying ``max_recent`` to inactive records; None lists all retained history.
    Activity is the last recorded observation, not a live scheduler query. Missing
    paths are excluded before history limits unless ``include_missing`` is True;
    checking existence does not change or remove registry entries.

    Never prune or write the registry, query schedulers, or construct workflow objects.
    A template filter may read metadata headers to recover names absent from older
    records; saved names remain usable when workflow data is missing.
    """
    if max_recent is not None and (type(max_recent) is not int or max_recent < 0):
        raise ValueError("max_recent must be a non-negative integer")
    records = resolve_records(
        SubmissionRegistry(app).read().records, workflow_ref, ref_type
    )
    records = existing_records(records, include_missing)
    records = filter_templates(records, template_names)
    active = sorted(
        (record for record in records if record.activity != "inactive"),
        key=lambda record: (
            record.activity == "active",
            sort_timestamp(record.submitted_at),
        ),
        reverse=True,
    )
    inactive = sorted(
        (record for record in records if record.activity == "inactive"),
        key=recent_key,
        reverse=True,
    )
    if max_recent is not None:
        inactive = inactive[:max_recent]
    return active + inactive
