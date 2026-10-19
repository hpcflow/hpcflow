from __future__ import annotations
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.config.types import SchedulerConfigDescriptor, SLURMPartitionsDescriptor
from hpcflow.sdk.core.enums import ParallelMode
from hpcflow.sdk.core.errors import (
    IncompatibleSLURMPartitionError,
    UnknownSLURMPartitionError,
)
from hpcflow.sdk.submission.schedulers.slurm import SlurmPosix


def test_partition_selection_formats_directive_with_defaults() -> None:
    config: SchedulerConfigDescriptor = {
        "defaults": {
            "directives": {
                "--account": "UKAEA-AP002-CPU",
                "--time": "00:10:00",
            }
        },
        "partitions": {"ukaea-spr": {"num_cores": [1, 1, 1]}},
    }
    resources = hf.ElementResources(scheduler="slurm")

    SlurmPosix.process_resources(resources, config)
    directives = SlurmPosix(**config["defaults"]).format_directives(
        resources, num_elements=1, is_array=False, sub_idx=0, js_idx=0
    )

    assert resources.SLURM_partition == "ukaea-spr"
    assert "#SBATCH --partition ukaea-spr" in directives.splitlines()
    assert "#SBATCH --account UKAEA-AP002-CPU" in directives.splitlines()
    assert "#SBATCH --time 00:10:00" in directives.splitlines()


def test_partition_directive_overrides_selected_partition() -> None:
    resources = hf.ElementResources(scheduler="slurm")
    SlurmPosix.process_resources(
        resources,
        {
            "defaults": {},
            "partitions": {"selected-by-resources": {"num_cores": [1, 1, 1]}},
        },
    )
    directives = SlurmPosix(directives={"--partition": "specified-in-directives"})
    jobscript = directives.format_directives(
        resources, num_elements=1, is_array=False, sub_idx=0, js_idx=0
    )

    partition_lines = [line for line in jobscript.splitlines() if "--partition" in line]
    assert partition_lines == ["#SBATCH --partition specified-in-directives"]


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize(
    "partition",
    [
        {},
        {"num_cores": [4, 1, 4]},
        {"num_cores_per_node": [2, 1, 2]},
        {"num_nodes": [2, 1, 2]},
        {"parallel_modes": ["distributed"]},
        {
            "num_cores": [4, 1, 4],
            "num_cores_per_node": [2, 1, 2],
            "num_nodes": [2, 1, 2],
            "parallel_modes": ["distributed"],
        },
        {
            "num_cores": [],
            "num_cores_per_node": [],
            "num_nodes": [],
            "parallel_modes": [],
        },
    ],
)
def test_partition_optional_constraints(
    partition: SLURMPartitionsDescriptor, explicit: bool
) -> None:
    resources = hf.ElementResources(
        num_cores=4,
        num_cores_per_node=2,
        num_nodes=2,
        SLURM_partition="matching" if explicit else None,
    )
    SlurmPosix.process_resources(
        resources, {"defaults": {}, "partitions": {"matching": partition}}
    )

    assert resources.parallel_mode is ParallelMode.DISTRIBUTED
    assert resources.SLURM_partition == "matching"


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize(
    "constraint",
    [
        {"num_cores": [1, 2, 5]},
        {"num_cores_per_node": [4, 1, 8]},
        {"num_nodes": [3, 1, 4]},
        {"parallel_modes": ["shared"]},
    ],
)
def test_partition_incompatible_constraints(
    constraint: SLURMPartitionsDescriptor, explicit: bool
) -> None:
    partition: SLURMPartitionsDescriptor = {
        "num_cores": [1, 1, 8],
        "num_cores_per_node": [1, 1, 8],
        "num_nodes": [1, 1, 4],
        "parallel_modes": ["distributed"],
    }
    partition.update(constraint)
    resources = hf.ElementResources(
        num_cores=4,
        num_cores_per_node=2,
        num_nodes=2,
        SLURM_partition="incompatible" if explicit else None,
    )
    config: SchedulerConfigDescriptor = {
        "defaults": {},
        "partitions": {"incompatible": partition, "matching": {}},
    }
    if explicit:
        with pytest.raises(IncompatibleSLURMPartitionError):
            SlurmPosix.process_resources(resources, config)
    else:
        SlurmPosix.process_resources(resources, config)
        assert resources.SLURM_partition == "matching"


@pytest.mark.parametrize("explicit", [False, True])
def test_partition_selection_preserves_order_or_explicit_choice(explicit: bool) -> None:
    resources = hf.ElementResources(SLURM_partition="second" if explicit else None)
    SlurmPosix.process_resources(
        resources,
        {"defaults": {}, "partitions": {"first": {}, "second": {}}},
    )

    assert resources.SLURM_partition == ("second" if explicit else "first")


@pytest.mark.parametrize("explicit", [False, True])
def test_partition_selection_with_unspecified_resource_counts(explicit: bool) -> None:
    resources = hf.ElementResources(
        num_cores=4, SLURM_partition="matching" if explicit else None
    )
    SlurmPosix.process_resources(
        resources,
        {
            "defaults": {},
            "partitions": {
                "matching": {
                    "num_cores": [1, 1, 8],
                    "num_cores_per_node": [2, 1, 8],
                    "num_nodes": [2, 1, 4],
                }
            },
        },
    )

    assert resources.SLURM_partition == "matching"


@pytest.mark.parametrize("partitions", [{}, {"parallel": {"num_cores": [2, 1, 8]}}])
def test_partition_selection_without_match(partitions) -> None:
    resources = hf.ElementResources()
    SlurmPosix.process_resources(resources, {"defaults": {}, "partitions": partitions})

    assert resources.SLURM_partition is None


def test_partition_selection_unknown_explicit_partition() -> None:
    resources = hf.ElementResources(SLURM_partition="unknown")
    with pytest.raises(UnknownSLURMPartitionError):
        SlurmPosix.process_resources(resources, {"defaults": {}})


def test_parse_job_ID_simple() -> None:
    assert SlurmPosix._parse_job_IDs("123") == ("123", None)


def test_parse_job_ID_simple_array_item() -> None:
    assert SlurmPosix._parse_job_IDs("123_10") == ("123", [9])


def test_parse_job_ID_array_simple_range() -> None:
    assert SlurmPosix._parse_job_IDs("3397752_[9-11]") == ("3397752", [8, 9, 10])


def test_parse_job_ID_array_simple_multiple_range() -> None:
    assert SlurmPosix._parse_job_IDs("49203_[3-5,9-11]") == (
        "49203",
        [2, 3, 4, 8, 9, 10],
    )


def test_parse_job_ID_array_simple_mixed_range() -> None:
    assert SlurmPosix._parse_job_IDs("30627658_[5,8-10]") == (
        "30627658",
        [4, 7, 8, 9],
    )


def test_parse_job_ID_array_simple_range_with_max_concurrent() -> None:
    assert SlurmPosix._parse_job_IDs("3397752_[9-11%2]") == ("3397752", [8, 9, 10])


def test_parse_job_ID_array_simple_multiple_range_max_concurrent() -> None:
    assert SlurmPosix._parse_job_IDs("49203_[3-5%1,9-11%2]") == (
        "49203",
        [2, 3, 4, 8, 9, 10],
    )
