"""Resolve workflow references without opening workflow stores."""

from pathlib import Path
from collections.abc import Sequence
from typing import cast

from .models import SubmissionRecord

from .naming import registered_workflow_name
from .registry import sort_timestamp


def parse_submission_ids(value: int | str | Sequence[int]) -> list[int]:
    """Validate and deduplicate explicit IDs, allowing -1 as the latest alias."""
    if isinstance(value, str):
        parts = value.split(",")
        if any(not part.strip() for part in parts):
            raise ValueError("Submission ID lists must not contain empty entries.")
        try:
            ids = [int(part.strip()) for part in parts]
        except ValueError:
            raise ValueError(
                "Submission IDs must be integers separated by commas."
            ) from None
    elif type(value) is int:
        ids = [value]
    elif isinstance(value, Sequence):
        ids = list(cast(Sequence[int], value))
    else:
        raise ValueError("Submission IDs must be integers or a comma-separated list.")
    if not ids or any(type(local_id) is not int or local_id < -1 for local_id in ids):
        raise ValueError(
            "Submission IDs must be non-negative integers, or -1 for latest."
        )
    return list(dict.fromkeys(ids))


def resolve_submission_id(
    records: list[SubmissionRecord], local_id: int
) -> SubmissionRecord | None:
    """Find a saved ID, or the latest submitted record for the special ID ``-1``.

    Use submission timestamps, with registry insertion order breaking ties. Local
    IDs may be reused and activity/finish times do not determine submission order.
    Reject an empty registry for ``-1`` rather than interpreting it as a path.
    """
    if local_id == -1:
        if not records:
            raise ValueError("No registered submissions are available for reference -1.")
        return max(
            enumerate(records),
            key=lambda item: (sort_timestamp(item[1].submitted_at), item[0]),
        )[1]
    return next((record for record in records if record.local_id == local_id), None)


def is_path_reference(reference) -> bool:
    """Recognize explicit path syntax without checking whether the path exists."""
    text = str(reference)
    return (
        isinstance(reference, Path)
        or Path(text).is_absolute()
        or text in (".", "..")
        or "/" in text
        or "\\" in text
    )


def _named_records(records: list[SubmissionRecord], name: str) -> list[SubmissionRecord]:
    """Resolve an exact saved workflow name and return all parts of its workflow.

    Reject names shared by different workflow IDs, reporting candidate local IDs
    and paths so the caller can select an unambiguous reference.
    """
    matches = [record for record in records if registered_workflow_name(record) == name]
    identities: dict[str, SubmissionRecord] = {}
    for record in matches:
        identities.setdefault(record.workflow_id, record)
    if len(identities) > 1:
        candidates = "; ".join(
            f"ID {record.local_id} ({record.workflow_path})"
            for record in identities.values()
        )
        raise ValueError(
            f"Ambiguous workflow name {name!r}: {candidates}. Use a local ID or an explicit path."
        )
    if not matches:
        return []
    workflow_id = matches[0].workflow_id
    return [record for record in records if record.workflow_id == workflow_id]


def resolve_records(
    records: list[SubmissionRecord],
    reference,
    ref_type=None,
    *,
    allow_multiple: bool = False,
) -> list[SubmissionRecord]:
    """Resolve a registry reference without constructing workflow objects.

    None selects all records. In automatic mode, a valid local ID takes precedence
    over an exact workflow name, followed by a path. Return all submission parts
    for an ID/name match, or all records at the resolved path. Reject ambiguous
    names and name/path collisions; explicit reference types bypass interpretation.
    An unmatched path returns an empty list for callers that support store fallback.
    ``-1`` aliases the latest submitted record's local ID, selecting all registered
    parts of its workflow just like a normal ID. Explicit names/paths stay literal.
    ``allow_multiple`` enables strict comma-separated ID lists for show/cancel.
    Validate the entire list before returning any matches and deduplicate workflows.
    """
    if reference is None:
        return records
    if ref_type not in (None, "assume-id", "id", "name", "path"):
        raise ValueError(f"Unknown workflow reference type: {ref_type}")
    if (
        allow_multiple
        and isinstance(reference, str)
        and "," in reference
        and (
            ref_type == "id"
            or (ref_type in (None, "assume-id") and not is_path_reference(reference))
        )
    ):
        workflow_ids = set()
        try:
            requested = parse_submission_ids(reference)
        except ValueError:
            if ref_type in (None, "assume-id") and any(
                registered_workflow_name(record) == reference for record in records
            ):
                return resolve_records(records, reference, ref_type)
            raise
        for local_id in requested:
            match = resolve_submission_id(records, local_id)
            if match is None:
                raise ValueError(f"Unknown submission ID: {local_id}")
            workflow_ids.add(match.workflow_id)
        return [record for record in records if record.workflow_id in workflow_ids]
    if ref_type == "id" or (
        ref_type in (None, "assume-id") and not is_path_reference(reference)
    ):
        try:
            local_id = int(reference)
        except (TypeError, ValueError):
            if ref_type == "id":
                raise ValueError(f"Invalid submission ID: {reference}") from None
        else:
            match = resolve_submission_id(records, local_id)
            if match:
                return [r for r in records if r.workflow_id == match.workflow_id]
            if ref_type == "id":
                raise ValueError(f"Unknown submission ID: {reference}")
    if ref_type == "name" or (
        ref_type in (None, "assume-id") and not is_path_reference(reference)
    ):
        matches = _named_records(records, str(reference))
        if matches:
            path = Path(str(reference)).resolve()
            if (
                ref_type != "name"
                and path.exists()
                and any(
                    Path(record.workflow_path).resolve() != path for record in matches
                )
            ):
                raise ValueError(
                    f"Workflow reference {str(reference)!r} matches a registered name and "
                    "a different existing path. Use --ref-type name or --ref-type path."
                )
            return matches
        if ref_type == "name":
            raise ValueError(f"Unknown registered workflow name: {str(reference)!r}")
    path = Path(str(reference)).resolve()
    return [r for r in records if Path(r.workflow_path).resolve() == path]


def resolve_workflow_path(
    records: list[SubmissionRecord], reference, ref_type=None
) -> Path:
    """Resolve a registry ID/name or an existing path for workflow operations."""
    if ref_type == "path":
        return Path(reference).resolve()
    if ref_type in (None, "assume-id", "id") and not is_path_reference(reference):
        try:
            local_id = int(reference)
        except (TypeError, ValueError):
            pass
        else:
            match = resolve_submission_id(records, local_id)
            if match:
                return Path(match.workflow_path).resolve()
    matches = resolve_records(records, reference, ref_type)
    if matches:
        paths = {Path(record.workflow_path).resolve() for record in matches}
        if len(paths) != 1:
            raise ValueError(
                f"Workflow reference {str(reference)!r} has multiple registered paths. "
                "Use a local ID or an explicit path."
            )
        return paths.pop()
    path = Path(str(reference)).resolve()
    if path.exists():
        return path
    raise ValueError(
        f"Workflow reference is neither a registered ID, a registered name, nor an "
        f"existing path: {str(reference)!r}"
    )
