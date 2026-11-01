"""Workflow display names for registry records, including timestamp subdirectories."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import SubmissionRecord

_TIMESTAMP_DIRECTORY = re.compile(r"\d{4}-\d{2}-\d{2}_\d{6}")


def workflow_name_from_path(path: str) -> str:
    """Recover the default name/timestamp layout without opening a workflow."""
    workflow_path = Path(path)
    name = workflow_path.name
    if workflow_path.parent.name and _TIMESTAMP_DIRECTORY.fullmatch(name):
        try:
            datetime.strptime(name, "%Y-%m-%d_%H%M%S")
        except ValueError:
            pass
        else:
            return f"{workflow_path.parent.name}_{name}"
    return name


def registered_workflow_name(record: SubmissionRecord) -> str:
    """Prefer a saved workflow name, repairing old basename-only records."""
    name = record.workflow_name
    if name and (
        record.workflow_name_from_metadata or name != Path(record.workflow_path).name
    ):
        return name
    return workflow_name_from_path(record.workflow_path)
