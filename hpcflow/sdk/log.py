"""
Interface to the standard logger, and performance logging utility.
"""

from __future__ import annotations
from functools import wraps
import logging
import logging.handlers
from pathlib import Path
import time
from collections import defaultdict
from collections.abc import Callable, Sequence
import statistics
from dataclasses import dataclass
from typing import ClassVar, Literal, ParamSpec, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from .app import BaseApp


P = ParamSpec("P")
T = TypeVar("T")


@dataclass
class _Summary:
    """
    Summary of a particular node's execution time.
    """

    number: int
    mean: float
    stddev: float
    min: float
    max: float
    sum: float
    children: dict[tuple[str, ...], _Summary]


class TimeIt:
    """
    Method execution time instrumentation.
    """

    #: Whether the instrumentation is active.
    active: ClassVar = False
    #: Title to be printed with the summary.
    title: ClassVar[str | None] = None
    #: Where to log to.
    file_path: ClassVar[str | Path | None] = None
    #: The details be tracked.
    timers: ClassVar[dict[tuple[str, ...], list[float]]] = defaultdict(list)
    #: Traces of the stack.
    trace: ClassVar[list[str]] = []
    #: Trace indices.
    trace_idx: ClassVar[list[int]] = []
    #: Preceding traces.
    trace_prev: ClassVar[list[str]] = []
    #: Preceding trace indices.
    trace_idx_prev: ClassVar[list[int]] = []
    #: File mode for when summarising to a file
    file_mode: ClassVar[Literal["w", "a"]] = "w"
    #: ``time.perf_counter`` assigned at the CLI entry point if active.
    CLI_start: ClassVar[float | None] = None
    #: ``time.perf_counter`` assigned at the CLI exit point if active.
    CLI_end: ClassVar[float | None] = None
    #: Time spent executing the run command, assigned in ``Workflow.execute_run`` if
    #: active.
    run_command_time: ClassVar[float | None] = None
    #: Time spent to launch the app, if set, and if instrumentation is active. This is
    #: measured from just before the app CLI is invoked in a shell, to the top-level Click
    #: command call.
    app_launch_time: ClassVar[float | None] = None
    #: Time spent by child app processes on orchestration overhead.
    child_orchestration_time: ClassVar[float] = 0.0
    #: Time spent by child app processes on doing work (i.e. script evaluation)
    child_work_time: ClassVar[float] = 0.0
    #: Preamble to write to the summary file, if requested.
    file_preamble: ClassVar[str | None] = None

    def __init__(self, name: str | None = None):
        self.name = name
        self._tic: float | None = None
        self._trace_key: tuple[str, ...] | None = None

    def __enter__(self):
        cls = self.__class__

        # `with TimeIt():` starts the profiling session.
        if self.name is None:
            cls.active = True
            return self

        # Named spans do nothing when profiling isn't active.
        if not cls.active:
            return self

        cls.trace.append(self.name)
        self._trace_key = tuple(cls.trace)

        if cls.trace_prev == cls.trace:
            new_trace_idx = cls.trace_idx_prev[-1] + 1
        else:
            new_trace_idx = 0

        cls.trace_idx.append(new_trace_idx)
        self._tic = time.perf_counter()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        cls = self.__class__

        # top-level profiling session.
        if self.name is None:
            try:
                cls.summarise_string()
            finally:
                cls.reset()
                cls.active = False
            return

        # named span while instrumentation wasn't active.
        if self._tic is None:
            return

        try:
            elapsed = time.perf_counter() - self._tic
            cls.timers[self._trace_key].append(elapsed)
        finally:
            cls.trace_prev = list(cls.trace)
            cls.trace_idx_prev = list(cls.trace_idx)

            cls.trace.pop()
            cls.trace_idx.pop()

    @classmethod
    def decorator(cls, func: Callable[P, T]) -> Callable[P, T]:
        """
        Decorator for a method that is to have its execution time monitored.
        """

        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            if not cls.active:
                return func(*args, **kwargs)

            cls.trace.append(func.__qualname__)
            trace_key = tuple(cls.trace)

            if cls.trace_prev == cls.trace:
                new_trace_idx = cls.trace_idx_prev[-1] + 1
            else:
                new_trace_idx = 0
            cls.trace_idx.append(new_trace_idx)

            tic = time.perf_counter()

            try:
                return func(*args, **kwargs)
            finally:
                toc = time.perf_counter()
                elapsed = toc - tic

                cls.timers[trace_key].append(elapsed)

                cls.trace_prev = list(cls.trace)
                cls.trace_idx_prev = list(cls.trace_idx)

                cls.trace.pop()
                cls.trace_idx.pop()

        return wrapper

    @classmethod
    def _summarise(cls) -> dict[tuple[str, ...], _Summary]:
        """
        Produce a machine-readable summary of method execution time statistics.
        """
        stats = {
            k: _Summary(
                len(v),
                statistics.mean(v),
                statistics.pstdev(v),
                min(v),
                max(v),
                sum(v),
                {},
            )
            for k, v in cls.timers.items()
        }

        # make a graph
        for key in sorted(stats, key=lambda x: len(x), reverse=True):
            if len(key) == 1:
                continue
            value = stats.pop(key)
            parent_key = key[:-1]
            if parent_key in stats:
                stats[parent_key].children[key] = value

        return stats

    @classmethod
    def get_orchestration_time(cls):
        """Return orchestration time for this process, excluding child processes."""
        if cls.CLI_start is None or cls.CLI_end is None:
            return None

        CLI_time = cls.CLI_end - cls.CLI_start

        if cls.app_launch_time is None:
            return None

        orchestration_time = cls.app_launch_time + CLI_time

        if cls.run_command_time is not None:
            orchestration_time -= cls.run_command_time

        return orchestration_time

    @classmethod
    def get_total_orchestration_time(cls):
        """Return orchestration time including child hpcflow processes."""
        orchestration_time = cls.get_orchestration_time()

        if orchestration_time is None:
            return None

        return orchestration_time + cls.child_orchestration_time

    @classmethod
    def get_command_work_time(cls):
        """Return reported user/application work within the command."""
        if not cls.child_work_time:
            return None

        return cls.child_work_time

    @classmethod
    def get_command_overhead_time(cls):
        """Return command time not classified as work or child orchestration."""
        if cls.run_command_time is None:
            return None

        if not cls.child_work_time:
            return None

        return cls.run_command_time - cls.child_orchestration_time - cls.child_work_time

    @classmethod
    def get_total_time(cls):
        """Return total wall time for this hpcflow process."""
        if cls.CLI_start is None or cls.CLI_end is None:
            return None

        if cls.app_launch_time is None:
            return None

        return cls.app_launch_time + (cls.CLI_end - cls.CLI_start)

    @classmethod
    def summarise_string(cls) -> None:
        """
        Produce a human-readable summary of execution timing statistics.

        The summary may contain two sections:

        1. Overall process timings
        These describe the wall-clock time associated with launching and running
        the CLI command:

        ``App launch time``
            Time elapsed before the CLI callback starts. This includes Python
            interpreter start-up, module imports, and other application
            initialisation performed before ``CLI_start`` is recorded.

        ``CLI time``
            Time between entering the CLI callback and completion of the invoked
            command. This includes app orchestration as well as execution
            of the command itself.

        ``Command time``
            Wall-clock time spent executing the external command launched by
            this process.

        ``Command work time``
            Time explicitly reported by the child process as application/user
            work. For generated Python scripts this is the time spent executing
            the user-defined function, excluding framework setup, input
            preparation, output handling, and imports.

        ``Command overhead time``
            The part of ``Command time`` not accounted for by reported user work
            or child orchestration:

                Command overhead
                    = Command time
                    - Child orchestration time
                    - Command work time

            This can include process and interpreter start-up, shell/environment
            activation, process shutdown, and other execution overhead outside
            the instrumented child process. For example, activation of a conda
            environment.

        ``Orchestration time``
            App orchestration performed by this process, excluding the external command
            execution itself.

        ``Child orchestration time``
            Framework/script orchestration explicitly reported by child
            processes. For generated Python scripts this includes imports,
            framework setup, input preparation, and output handling, but
            excludes the timed user work.

        ``Total orchestration time``
            Orchestration performed by this process and its instrumented child
            processes:

                Total orchestration
                    = Orchestration time
                    + Child orchestration time

        ``Total time``
            Total wall-clock time from application launch to completion of the
            CLI command:

                Total time
                    = App launch time + CLI time

            When child work timing is available, this can also be decomposed as:

                Total time
                    = Total orchestration time
                    + Command work time
                    + Command overhead time

        2. Instrumented timing tree
        The table reports timings collected by ``TimeIt`` decorators and context
        managers. ``sum`` is the accumulated inclusive time for a named timer;
        ``mean``, ``stddev``, ``min``, and ``max`` describe its individual
        invocations, and ``N`` is the number of invocations.

        Tree indentation represents dynamic nesting of timers. Parent timings
        are inclusive of their child timings, so values in the tree should not
        generally be added together. Time spent directly in a parent can be
        estimated by subtracting its child timings from the parent timing.

        Some fields are omitted when the information required to calculate them is
        unavailable. In particular, command work and command overhead require timing
        information reported by an instrumented child process.
        """

        def _format_nodes(
            node: dict[tuple[str, ...], _Summary],
            depth: int = 0,
            depth_final: Sequence[bool] = (),
        ):
            unit = 1e-3  # ms
            for idx, (k, v) in enumerate(node.items()):
                is_final_child = idx == len(node) - 1
                angle = "└ " if is_final_child else "├ "
                bars = ""
                if depth > 0:
                    bars = "".join(f"{'│ ' if not i else '  '}" for i in depth_final)
                k_str = bars + (angle if depth > 0 else "") + f"{k[depth]}"
                min_str = f"{v.min/unit:10.3f}" if v.number > 1 else f"{f'-':^12s}"
                max_str = f"{v.max/unit:10.3f}" if v.number > 1 else f"{f'-':^12s}"
                stddev_str = f"({v.stddev/unit:8.3f})" if v.number > 1 else f"{f' ':^10s}"
                out.append(
                    f"{k_str:.<80s} {v.sum/unit:12.3f} "
                    f"{v.mean/unit:10.3f} {stddev_str} {v.number:8d} "
                    f"{min_str} {max_str} "
                )
                depth_final_next = list(depth_final)
                if depth > 0:
                    depth_final_next.append(is_final_child)
                _format_nodes(v.children, depth + 1, depth_final_next)

        timing_summary = cls._summarise()

        out = [
            f"{'function':^80s} {'sum /ms':^12s} {'mean (stddev) /ms':^20s} {'N':^8s} "
            f"{'min /ms':^12s} {'max /ms':^12s}"
        ]
        _format_nodes(timing_summary)
        out_str = "\n".join(out) + "\n"

        CLI_time = None
        if cls.CLI_start is not None and cls.CLI_end is not None:
            CLI_time = cls.CLI_end - cls.CLI_start

        command_work_time = cls.get_command_work_time()
        command_overhead_time = cls.get_command_overhead_time()
        orchestration_time = cls.get_orchestration_time()
        total_orchestration_time = cls.get_total_orchestration_time()
        total_time = cls.get_total_time()

        summary_lines: list[str] = []
        if cls.title:
            summary_lines.append(f"{cls.title}\n{'=' * len(cls.title)}")

        if cls.app_launch_time is not None:
            summary_lines.append(f"App launch time:          {cls.app_launch_time:.6f} s")

        if CLI_time is not None:
            summary_lines.append(f"CLI time:                 {CLI_time:.6f} s")

        if cls.run_command_time is not None:
            summary_lines.append(
                f"Command time:             {cls.run_command_time:.6f} s"
            )

        if command_work_time is not None:
            summary_lines.append(f"Command work time:        {command_work_time:.6f} s")

        if command_overhead_time is not None:
            summary_lines.append(
                f"Command overhead time:    {command_overhead_time:.6f} s"
            )

        if orchestration_time is not None:
            summary_lines.append(f"Orchestration time:       {orchestration_time:.6f} s")

        if cls.child_orchestration_time:
            summary_lines.append(
                f"Child orchestration time: {cls.child_orchestration_time:.6f} s"
            )

        if total_orchestration_time is not None:
            summary_lines.append(
                f"Total orchestration time: {total_orchestration_time:.6f} s"
            )

        if total_time is not None:
            summary_lines.append(f"Total time:               {total_time:.6f} s")

        if summary_lines:
            out_str = "\n".join(summary_lines) + "\n\n" + out_str

        if cls.file_path:
            path = Path(cls.file_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            write_preamble = cls.file_preamble and (
                not path.exists() or path.stat().st_size == 0
            )
            preamble = cls.file_preamble if write_preamble else ""

            if cls.child_orchestration_time:
                # want outer process timings before child timings, but preserve
                # the run preamble at the very top.
                existing = path.read_text(encoding="utf-8") if path.exists() else ""

                if cls.file_preamble:
                    if existing.startswith(cls.file_preamble):
                        existing = existing[len(cls.file_preamble) :]

                    content = cls.file_preamble + out_str + "\n" + existing
                else:
                    content = out_str + "\n" + existing

                path.write_text(content, encoding="utf-8")

            else:
                with path.open(cls.file_mode, encoding="utf-8") as fh:
                    if preamble:
                        fh.write(preamble)
                    fh.write(out_str)
        else:
            print(out_str)

    @classmethod
    def reset(cls):
        cls.timers = defaultdict(list)
        cls.trace = []
        cls.trace_idx = []
        cls.trace_prev = []
        cls.trace_idx_prev = []


class AppLog:
    """
    Application log control.
    """

    #: Default logging level for the console.
    DEFAULT_LOG_CONSOLE_LEVEL: ClassVar = "WARNING"
    #: Default logging level for log files.
    DEFAULT_LOG_FILE_LEVEL: ClassVar = "WARNING"

    def __init__(self, app: BaseApp, log_console_level: str | None = None) -> None:
        #: The application context.
        self._app = app
        #: The base logger for the application.
        self.logger = logging.getLogger(app.package_name)
        self.logger.setLevel(logging.WARNING)
        #: The handler for directing logging messages to the console.
        self.console_handler = self.__add_console_logger(
            level=log_console_level or AppLog.DEFAULT_LOG_CONSOLE_LEVEL
        )
        self.file_handler: logging.FileHandler | None = None

    def _ensure_logger_level(self):
        """Ensure the logger's level is set to a level that triggers the handlers.

        Notes
        -----
        Previously, we fixed the logger to DEBUG, but we found other Python packages
        could then trigger debug logs in hpcflow even though the handlers were set to e.g.
        ERROR.

        """
        min_level = min((handler.level for handler in self.logger.handlers), default=0)
        if self.logger.level != min_level:
            self.logger.setLevel(min_level)

    def __add_console_logger(self, level: str, fmt: str | None = None) -> logging.Handler:
        fmt = fmt or "%(levelname)s %(name)s: %(message)s"
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(fmt))
        handler.setLevel(level)
        self.logger.addHandler(handler)
        self._ensure_logger_level()
        return handler

    def update_console_level(self, new_level: str | None = None) -> None:
        """
        Set the logging level for console messages.
        """
        new_level = new_level or AppLog.DEFAULT_LOG_CONSOLE_LEVEL
        self.console_handler.setLevel(new_level.upper())
        self._ensure_logger_level()

    def update_file_level(self, new_level: str | None = None) -> None:
        if self.file_handler:
            new_level = new_level or AppLog.DEFAULT_LOG_FILE_LEVEL
            self.file_handler.setLevel(new_level.upper())
            self._ensure_logger_level()

    def add_file_logger(
        self,
        path: str | Path,
        level: str | None = None,
        fmt: str | None = None,
        max_bytes: int | None = None,
        backup_count: int = 4,
    ) -> None:
        """
        Add a log file.
        """
        path = Path(path)
        fmt = fmt or "%(asctime)s %(levelname)s %(name)s: %(message)s"
        level = level or AppLog.DEFAULT_LOG_FILE_LEVEL
        max_bytes = max_bytes or int(50e6)

        if not path.parent.is_dir():
            self.logger.info(f"Generating log file parent directory: {path.parent!r}")
            path.parent.mkdir(exist_ok=True, parents=True)

        handler = logging.handlers.RotatingFileHandler(
            filename=path,
            maxBytes=max_bytes,
            backupCount=backup_count,
        )
        handler.setFormatter(logging.Formatter(fmt))
        handler.setLevel(level.upper())
        self.logger.addHandler(handler)
        self.file_handler = handler
        self._ensure_logger_level()

    def remove_file_handler(self) -> None:
        """Remove the file handler."""
        if self.file_handler:
            self.logger.debug(
                f"Removing file handler from the AppLog: {self.file_handler!r}."
            )
            self.logger.removeHandler(self.file_handler)
            self.file_handler = None
            self._ensure_logger_level()
