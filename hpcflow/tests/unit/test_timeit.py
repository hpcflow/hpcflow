"""Profiling preserves nested calls while monitoring workers overlap."""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, local

import pytest

from hpcflow.sdk.log import TimeIt


@pytest.fixture
def profiling(monkeypatch):
    monkeypatch.setattr(TimeIt, "active", True)
    monkeypatch.setattr(TimeIt, "timers", defaultdict(list))
    for name in ("trace", "trace_idx", "trace_prev", "trace_idx_prev"):
        monkeypatch.setattr(TimeIt, name, [])
    monkeypatch.setattr(TimeIt, "_worker_traces", local())


def test_overlapping_workers_have_independent_nested_timings(profiling):
    barrier = Barrier(4)

    @TimeIt.decorator
    def leaf():
        return 42

    @TimeIt.decorator
    def worker(index):
        with TimeIt(f"worker-{index}"):
            barrier.wait(timeout=10)
            return leaf()

    with TimeIt("main"):
        with ThreadPoolExecutor(max_workers=4) as executor:
            assert list(executor.map(worker, range(4))) == [42] * 4
        assert TimeIt.trace == ["main"]

    assert TimeIt.trace == []
    assert TimeIt.trace_idx == []
    assert len(TimeIt.timers[(worker.__qualname__,)]) == 4
    assert len(TimeIt.timers[("main",)]) == 1
    for index in range(4):
        key = (worker.__qualname__, f"worker-{index}")
        assert len(TimeIt.timers[key]) == 1
        assert len(TimeIt.timers[(*key, leaf.__qualname__)]) == 1
    assert len(TimeIt.timers) == 10


def test_exception_restores_context_and_decorated_nesting(profiling):
    @TimeIt.decorator
    def failing():
        with TimeIt("disk read"):
            raise OSError("unreadable")

    @TimeIt.decorator
    def successful():
        return 1

    with TimeIt("parent"):
        with pytest.raises(OSError, match="unreadable"):
            failing()
        assert TimeIt.trace == ["parent"]
        assert successful() == 1
    assert set(TimeIt.timers) == {
        ("parent",),
        ("parent", failing.__qualname__),
        ("parent", failing.__qualname__, "disk read"),
        ("parent", successful.__qualname__),
    }
    assert TimeIt.trace == []
    assert TimeIt.trace_idx == []


def test_disabled_profiling_does_not_collect_timings(profiling, monkeypatch):
    monkeypatch.setattr(TimeIt, "active", False)

    @TimeIt.decorator
    def read():
        with TimeIt("disk read"):
            return 1

    assert read() == 1
    assert not TimeIt.timers
    assert not TimeIt.trace
