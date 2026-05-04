from __future__ import annotations
from hpcflow.sdk.submission.schedulers.slurm import SlurmPosix


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
