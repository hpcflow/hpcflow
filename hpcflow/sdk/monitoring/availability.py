"""Filter missing workflow paths without discarding their registered job references."""

from pathlib import Path

from .models import SubmissionRecord
from ..log import TimeIt


@TimeIt.decorator
def existing_records(
    records: list[SubmissionRecord], include_missing: bool = False
) -> list[SubmissionRecord]:
    """Exclude missing paths before history limits and scheduler observations.

    Check each distinct path once per invocation, so restored workflows are visible
    even if an older observation marked them missing. Permission errors are not
    evidence of deletion. Retain all registry entries for later inspection/cancel.
    """
    exists = {}
    selected = []
    for record in records:
        path = record.workflow_path
        if path not in exists:
            try:
                Path(path).stat()
                exists[path] = True
            except FileNotFoundError:
                exists[path] = False
            except OSError:
                exists[path] = True
        if exists[path]:
            selected.append(record)
        else:
            record.accessibility = "missing"
            record.detail_error = "Workflow path does not exist"
            if include_missing:
                selected.append(record)
    return selected
