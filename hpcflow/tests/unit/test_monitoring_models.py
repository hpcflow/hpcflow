"""Compatibility of monitoring dataclasses with existing registry/cache files."""

from copy import deepcopy
from dataclasses import is_dataclass
import json

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.skip_reason import SkipReason
from hpcflow.sdk.monitoring.cache import cache_path, read_cache
from hpcflow.sdk.monitoring.models import (
    ActionRunState,
    IterationActionState,
    JobRecord,
    ProcessReference,
    RegistryData,
    SubmissionRecord,
    TerminalStateCache,
    to_json,
)
from hpcflow.sdk.monitoring.registry import SubmissionRegistry, atomic_json


@pytest.fixture
def existing_registry_json():
    return {
        "schema_version": 1,
        "next_local_id": 8,
        "records": [
            {
                "local_id": 7,
                "workflow_id": "workflow-id",
                "workflow_path": "example/workflow",
                "workflow_name": "example",
                "submission_index": 2,
                "submission_part_id": "2026-11-01 12:00:00.000000",
                "submitted_at": "2026-11-01 12:00:00.000000",
                "activity": "active",
                "accessibility": "readable",
                "submitting_owner": [123, 1234.5],
                "workflow_header_token": [".zattrs", 1, 2, 3, 4, 5],
                "jobs": [
                    {
                        "jobscript_index": 0,
                        "scheduler_type": "direct",
                        "scheduler_reference": [321, ["python", "run.py"]],
                        "run_ids": [10],
                    }
                ],
                "last_action_summary": {
                    "counts": {"success": 1},
                    "complete": True,
                    "groups": [
                        {
                            "task": "analysis",
                            "elements": [0, 1],
                            "pattern": [[3, 0, 0, "success"]],
                        }
                    ],
                },
            }
        ],
    }


def test_existing_registry_sequences_decode_into_named_models(existing_registry_json):
    data = RegistryData.from_json(existing_registry_json)
    record = data.records[0]
    assert isinstance(record, SubmissionRecord)
    assert isinstance(record.jobs[0], JobRecord)
    assert record.jobs[0].scheduler_reference == ProcessReference(
        321, ("python", "run.py")
    )
    assert record.submitting_owner.pid == 123
    assert record.workflow_header_token.filename == ".zattrs"
    assert record.last_action_summary.groups[0].pattern == (
        IterationActionState(iteration=3, action=0, attempt=0, state="success"),
    )
    encoded = to_json(data)
    # Historical sequence representations stay intact at the file boundary.
    assert encoded["records"][0]["jobs"][0]["scheduler_reference"] == [
        321,
        ["python", "run.py"],
    ]
    assert encoded["records"][0]["submitting_owner"] == [123, 1234.5]
    assert encoded["records"][0]["workflow_header_token"] == [".zattrs", 1, 2, 3, 4, 5]
    assert encoded["records"][0]["last_action_summary"]["groups"][0]["pattern"] == [
        [3, 0, 0, "success"]
    ]
    assert RegistryData.from_json(json.loads(json.dumps(encoded))) == data


@pytest.mark.parametrize(
    "reason",
    [
        SkipReason.UPSTREAM_FAILURE,
        SkipReason.LOOP_TERMINATION,
        SkipReason.TASK_CONDITION_NOT_MET,
    ],
)
def test_terminal_cache_keeps_enum_names_and_numeric_skip_reasons(tmp_path, reason):
    record = SubmissionRecord("unused", "workflow-id", "workflow", 2, "part", "part")
    record.workflow_path = str(tmp_path)
    state = ActionRunState(
        0, "analysis", 1, 3, 0, 0, EARStatus.skipped, skip_reason=reason
    )
    cache = TerminalStateCache(
        record.workflow_id, record.submission_index, {10: state}, "observed"
    )
    atomic_json(cache_path(record).with_suffix(""), to_json(cache))
    raw = json.loads(cache_path(record).with_suffix("").read_text())
    assert raw["schema_version"] == 1
    assert raw["terminal_states"]["10"]["state"] == "skipped"
    assert raw["terminal_states"]["10"]["skip_reason"] == reason.value
    assert read_cache(record) == cache
    assert read_cache(record).terminal_states[10].state is EARStatus.skipped


@pytest.mark.parametrize(
    "problem",
    [
        "schema",
        "duplicate_id",
        "id_type",
        "reference_type",
        "run_id_type",
        "workflow_name_type",
    ],
)
def test_registry_rejects_invalid_persisted_data(existing_registry_json, problem):
    raw = deepcopy(existing_registry_json)
    if problem == "schema":
        raw["schema_version"] = 99
    elif problem == "duplicate_id":
        raw["records"].append(deepcopy(raw["records"][0]))
    elif problem == "id_type":
        raw["records"][0]["local_id"] = "7"
    elif problem == "reference_type":
        raw["records"][0]["jobs"][0]["scheduler_reference"] = 123
    elif problem == "run_id_type":
        raw["records"][0]["jobs"][0]["run_ids"] = [True]
    else:
        raw["records"][0]["workflow_name"] = 17
    with pytest.raises(ValueError):
        RegistryData.from_json(raw)


def test_public_sdk_snapshots_still_contain_plain_json_data(tmp_path, monkeypatch):
    monkeypatch.setattr(hf, "_user_data_hostname_dir", tmp_path / "history")
    workflow_path = tmp_path / "workflow"
    atomic_json(
        workflow_path / ".zattrs",
        {"creation_info": {"id": "workflow-id"}, "name": "workflow"},
    )
    record = SubmissionRecord(
        str(workflow_path),
        "workflow-id",
        "workflow",
        0,
        "part",
        "part",
        local_id=0,
        activity="inactive",
        jobs=[JobRecord(jobscript_index=0, run_ids=[10])],
    )
    atomic_json(
        cache_path(record).with_suffix(""),
        to_json(
            TerminalStateCache(
                record.workflow_id,
                0,
                {10: ActionRunState(0, "analysis", 0, 0, 0, 0, EARStatus.success)},
                "observed",
            )
        ),
    )
    registry = SubmissionRegistry(hf)
    with registry.transaction() as data:
        data.records.append(record)
        data.next_local_id = 1
    before = registry.path.read_bytes()
    snapshots = hf.get_known_submissions(no_update=True)
    assert isinstance(snapshots[0], dict)
    assert not is_dataclass(snapshots[0])
    assert snapshots[0]["last_action_summary"]["counts"] == {"success": 1}
    json.dumps(snapshots)
    assert registry.path.read_bytes() == before
