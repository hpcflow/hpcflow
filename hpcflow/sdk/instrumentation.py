"""
Interface to instrumentation.
"""

from __future__ import annotations

import contextlib
import statistics
import time
from collections import defaultdict
from collections.abc import Callable, Iterator, Sequence
from contextvars import ContextVar, Token
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Literal, ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


@dataclass
class _Summary:
    """Summary of a particular node's execution time."""

    number: int
    mean: float
    stddev: float
    min: float
    max: float
    sum: float
    children: dict[tuple[str, ...], _Summary]


_current_timeit: ContextVar[TimeIt | None] = ContextVar(
    "current_timeit",
    default=None,
)


class TimeIt:
    """Instrumentation session.

    A ``TimeIt`` instance owns the timing state for one instrumentation scope,
    such as a CLI/jobscript invocation or an individual run.

    Instances may be nested. ``TimeIt.decorator`` and named ``TimeIt`` context
    managers record against the currently active instance.
    """

    def __init__(
        self,
        name: str | None = None,
        *,
        title: str | None = None,
        file_path: str | Path | None = None,
        file_mode: Literal["w", "a"] = "w",
        file_preamble: str | None = None,
        prepend: bool = False,
    ):
        self.name = name

        self.title = title
        self.file_path = file_path
        self.file_mode = file_mode
        self.file_preamble = file_preamble
        self.prepend = prepend

        self.timers: dict[tuple[str, ...], list[float]] = defaultdict(list)
        self.trace: list[str] = []
        self.trace_idx: list[int] = []
        self.trace_prev: list[str] = []
        self.trace_idx_prev: list[int] = []

        # overall/process-level timing metadata; these are optional because a run-level
        # TimeIt session generally does not have CLI timings:
        self.CLI_start: float | None = None
        self.CLI_end: float | None = None
        self.app_launch_time: float | None = None

        # command/child timing metadata for this particular session:
        self.run_command_time: float | None = None
        self.child_orchestration_time: float = 0.0
        self.child_work_time: float = 0.0

        self._tic: float | None = None
        self._trace_key: tuple[str, ...] | None = None
        self._activation_token: Token[TimeIt | None] | None = None

    @classmethod
    def is_active(cls) -> bool:
        """Return whether an instrumentation session is currently active."""
        return cls.current() is not None

    @classmethod
    def current(cls) -> TimeIt | None:
        """Return the currently active instrumentation session."""
        return _current_timeit.get()

    @classmethod
    def is_active(cls) -> bool:
        """Return whether an instrumentation session is currently active."""
        return cls.current() is not None

    @contextlib.contextmanager
    def activate(self) -> Iterator[TimeIt]:
        """Make this the active instrumentation session for this context.

        Nested activation is supported. On exit, the previously active session is
        restored.
        """
        token = _current_timeit.set(self)
        try:
            yield self
        finally:
            _current_timeit.reset(token)

    # ------------------------------------------------------------------
    # Context-manager API
    # ------------------------------------------------------------------

    def __enter__(self):
        # ``with TimeIt():`` creates/activates a complete session; a convenient top-level
        # API.
        if self.name is None:
            self._activation_token = _current_timeit.set(self)
            return self

        # ``with TimeIt("name"):`` is a named span against the currently active session.
        session = self.current()
        if session is None:
            return self

        session._start_span(self.name)
        self._tic = time.perf_counter()
        self._trace_key = tuple(session.trace)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.name is None:
            try:
                self.summarise_string()
            finally:
                if self._activation_token is not None:
                    _current_timeit.reset(self._activation_token)
                    self._activation_token = None
            return

        if self._tic is None or self._trace_key is None:
            return

        session = self.current()
        if session is None:
            return

        try:
            elapsed = time.perf_counter() - self._tic
            session.timers[self._trace_key].append(elapsed)
        finally:
            session._finish_span()

    def _start_span(self, name: str) -> tuple[str, ...]:
        self.trace.append(name)
        trace_key = tuple(self.trace)

        if self.trace_prev == self.trace:
            new_trace_idx = self.trace_idx_prev[-1] + 1
        else:
            new_trace_idx = 0

        self.trace_idx.append(new_trace_idx)
        return trace_key

    def _finish_span(self) -> None:
        self.trace_prev = list(self.trace)
        self.trace_idx_prev = list(self.trace_idx)

        self.trace.pop()
        self.trace_idx.pop()

    # ------------------------------------------------------------------
    # Decorator API
    # ------------------------------------------------------------------

    @classmethod
    def decorator(cls, func: Callable[P, T]) -> Callable[P, T]:
        """Instrument a function against the currently active session."""

        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            session = cls.current()
            if session is None:
                return func(*args, **kwargs)

            trace_key = session._start_span(func.__qualname__)
            tic = time.perf_counter()

            try:
                return func(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - tic
                session.timers[trace_key].append(elapsed)
                session._finish_span()

        return wrapper

    # ------------------------------------------------------------------
    # Summary calculations
    # ------------------------------------------------------------------

    def _summarise(self) -> dict[tuple[str, ...], _Summary]:
        """Produce machine-readable timing statistics."""

        stats = {
            key: _Summary(
                number=len(values),
                mean=statistics.mean(values),
                stddev=statistics.pstdev(values),
                min=min(values),
                max=max(values),
                sum=sum(values),
                children={},
            )
            for key, values in self.timers.items()
        }

        for key in sorted(stats, key=lambda x: len(x), reverse=True):
            if len(key) == 1:
                continue

            value = stats.pop(key)
            parent_key = key[:-1]

            if parent_key in stats:
                stats[parent_key].children[key] = value

        return stats

    def get_orchestration_time(self) -> float | None:
        """Return orchestration time for this process, excluding child processes."""
        if self.CLI_start is None or self.CLI_end is None:
            return None

        if self.app_launch_time is None:
            return None

        cli_time = self.CLI_end - self.CLI_start
        orchestration_time = self.app_launch_time + cli_time

        if self.run_command_time is not None:
            orchestration_time -= self.run_command_time

        return orchestration_time

    def get_total_orchestration_time(self) -> float | None:
        """Return orchestration time including child processes."""
        orchestration_time = self.get_orchestration_time()

        if orchestration_time is None:
            return None

        return orchestration_time + self.child_orchestration_time

    def get_command_work_time(self) -> float | None:
        """Return reported user/application work within the command."""
        if not self.child_work_time:
            return None

        return self.child_work_time

    def get_command_overhead_time(self) -> float | None:
        """Return command time not classified as work or child orchestration."""
        if self.run_command_time is None:
            return None

        if not self.child_work_time:
            return None

        return (
            self.run_command_time - self.child_orchestration_time - self.child_work_time
        )

    def get_total_time(self) -> float | None:
        """Return total wall time for this instrumentation session."""
        if self.CLI_start is None or self.CLI_end is None:
            return None

        if self.app_launch_time is None:
            return None

        return self.app_launch_time + (self.CLI_end - self.CLI_start)

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def summarise_string(self) -> None:
        """Write a human-readable summary of this instrumentation session."""

        out: list[str] = []

        def _format_nodes(
            node: dict[tuple[str, ...], _Summary],
            depth: int = 0,
            depth_final: Sequence[bool] = (),
        ) -> None:
            unit = 1e-3  # ms

            for idx, (key, value) in enumerate(node.items()):
                is_final_child = idx == len(node) - 1

                angle = "└ " if is_final_child else "├ "
                bars = ""

                if depth > 0:
                    bars = "".join("  " if is_final else "│ " for is_final in depth_final)

                key_str = bars + (angle if depth > 0 else "") + f"{key[depth]}"

                min_str = (
                    f"{value.min / unit:10.3f}" if value.number > 1 else f"{'-':^12s}"
                )
                max_str = (
                    f"{value.max / unit:10.3f}" if value.number > 1 else f"{'-':^12s}"
                )
                stddev_str = (
                    f"({value.stddev / unit:8.3f})" if value.number > 1 else f"{' ':^10s}"
                )

                out.append(
                    f"{key_str:.<80s} "
                    f"{value.sum / unit:12.3f} "
                    f"{value.mean / unit:10.3f} "
                    f"{stddev_str} "
                    f"{value.number:8d} "
                    f"{min_str} "
                    f"{max_str} "
                )

                depth_final_next = list(depth_final)
                if depth > 0:
                    depth_final_next.append(is_final_child)

                _format_nodes(
                    value.children,
                    depth + 1,
                    depth_final_next,
                )

        timing_summary = self._summarise()

        out.append(
            f"{'function':^80s} "
            f"{'sum /ms':^12s} "
            f"{'mean (stddev) /ms':^20s} "
            f"{'N':^8s} "
            f"{'min /ms':^12s} "
            f"{'max /ms':^12s}"
        )
        _format_nodes(timing_summary)

        out_str = "\n".join(out) + "\n"

        cli_time = None
        if self.CLI_start is not None and self.CLI_end is not None:
            cli_time = self.CLI_end - self.CLI_start

        command_work_time = self.get_command_work_time()
        command_overhead_time = self.get_command_overhead_time()
        orchestration_time = self.get_orchestration_time()
        total_orchestration_time = self.get_total_orchestration_time()
        total_time = self.get_total_time()

        summary_lines: list[str] = []

        if self.title:
            summary_lines.append(f"{self.title}\n{'=' * len(self.title)}")

        if self.app_launch_time is not None:
            summary_lines.append(
                f"App launch time:          {self.app_launch_time:.6f} s"
            )

        if cli_time is not None:
            summary_lines.append(f"CLI time:                 {cli_time:.6f} s")

        if self.run_command_time is not None:
            summary_lines.append(
                f"Command time:             {self.run_command_time:.6f} s"
            )

        if command_work_time is not None:
            summary_lines.append(f"Command work time:        {command_work_time:.6f} s")

        if command_overhead_time is not None:
            summary_lines.append(
                f"Command overhead time:    {command_overhead_time:.6f} s"
            )

        if orchestration_time is not None:
            summary_lines.append(f"Orchestration time:       {orchestration_time:.6f} s")

        if self.child_orchestration_time:
            summary_lines.append(
                "Child orchestration time: " f"{self.child_orchestration_time:.6f} s"
            )

        if total_orchestration_time is not None:
            summary_lines.append(
                "Total orchestration time: " f"{total_orchestration_time:.6f} s"
            )

        if total_time is not None:
            summary_lines.append(f"Total time:               {total_time:.6f} s")

        if summary_lines:
            out_str = "\n".join(summary_lines) + "\n\n" + out_str

        self._write_summary(out_str)

    def _write_summary(self, out_str: str) -> None:
        if self.file_path is None:
            print(out_str)
            return

        path = Path(self.file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        write_preamble = self.file_preamble and (
            not path.exists() or path.stat().st_size == 0
        )
        preamble = self.file_preamble if write_preamble else ""

        if self.prepend:
            # outer process timings should precede child timings while preserving
            # the run preamble at the very top.

            existing = path.read_text(encoding="utf-8") if path.exists() else ""
            if self.file_preamble:
                if existing.startswith(self.file_preamble):
                    existing = existing[len(self.file_preamble) :]

                content = self.file_preamble + out_str + "\n" + existing
            else:
                content = out_str + "\n" + existing

            path.write_text(content, encoding="utf-8")
            return

        with path.open(self.file_mode, encoding="utf-8") as fh:
            if preamble:
                fh.write(preamble)

            fh.write(out_str)

    def reset(self) -> None:
        """Reset collected timing data for this session."""
        self.timers = defaultdict(list)
        self.trace = []
        self.trace_idx = []
        self.trace_prev = []
        self.trace_idx_prev = []

        self.CLI_start = None
        self.CLI_end = None
        self.run_command_time = None
        self.app_launch_time = None
        self.child_orchestration_time = 0.0
        self.child_work_time = 0.0
