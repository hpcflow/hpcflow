"""Filter submissions by exact workflow template names before expensive queries."""

from __future__ import annotations

from collections.abc import Sequence

from .models import SubmissionRecord, WorkflowKey
from .headers import read_header
from ..log import TimeIt


def template_names(value: Sequence[str] | str | None) -> tuple[str, ...] | None:
    """Normalize a template filter while preserving name order.

    Accept a comma-separated string or a sequence of names. Whitespace is stripped
    and duplicates are removed; matching remains case-sensitive. ``None`` disables
    filtering. An empty selection or an empty name raises ``ValueError``.
    """
    if value is None:
        return None
    names = value.split(",") if isinstance(value, str) else value
    if not names or any(not isinstance(name, str) or not name.strip() for name in names):
        raise ValueError("Template names must be non-empty, comma-separated names")
    return tuple(dict.fromkeys(name.strip() for name in names))


def metadata_template_name(metadata: dict) -> str | None:
    """Return the original template name, or None if the header has no string name."""
    template = metadata.get("template")
    name = template.get("name") if isinstance(template, dict) else None
    return name if isinstance(name, str) else None


@TimeIt.decorator
def _read_template_name(record: SubmissionRecord) -> str | None:
    """Recover an older record's template name from a supported metadata header.

    Require the header's workflow ID to match the registration. Missing, replaced,
    malformed, or unreadable workflows return None rather than failing the listing.
    No workflow object is created and no files are written.
    """
    try:
        metadata, _ = read_header(record.workflow_path)
        if metadata is not None and metadata["creation_info"]["id"] == record.workflow_id:
            return metadata_template_name(metadata)
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        pass
    return None


@TimeIt.decorator
def filter_templates(
    records: list[SubmissionRecord],
    names: Sequence[str] | str | None,
    *,
    cached_only: bool = False,
) -> list[SubmissionRecord]:
    """Select records whose original template name exactly matches any supplied name.

    Saved names work even when workflows are inaccessible. For older records with
    no saved name, read each workflow header at most once during this call and set
    ``template_name`` on the supplied records in memory. This function does not
    persist those changes. Unknown names do not match; None returns all records.
    Apply this filter before selecting recent history or observing scheduler jobs.
    ``cached_only`` disables metadata recovery for registry-only cached views;
    records with unknown saved template names do not match an explicit filter.
    """
    selected_names = template_names(names)
    if selected_names is None:
        return records
    wanted = set(selected_names)
    recovered: dict[WorkflowKey, str | None] = {}
    selected = []
    for record in records:
        if record.template_name is None and not cached_only:
            key = WorkflowKey(record.workflow_path, record.workflow_id)
            if key not in recovered:
                recovered[key] = _read_template_name(record)
            record.template_name = recovered[key]
        if record.template_name in wanted:
            selected.append(record)
    return selected
