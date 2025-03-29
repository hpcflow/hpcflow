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
from typing import ClassVar, TypeVar, TYPE_CHECKING
from typing_extensions import ParamSpec

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
    #: Where to log to.
    file_path: ClassVar[str | None] = None
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

    def __enter__(self):
        self.__class__.active = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.__class__.summarise_string()
        finally:
            self.__class__.reset()
            self.__class__.active = False

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

            if cls.trace_prev == cls.trace:
                new_trace_idx = cls.trace_idx_prev[-1] + 1
            else:
                new_trace_idx = 0
            cls.trace_idx.append(new_trace_idx)

            tic = time.perf_counter()
            out = func(*args, **kwargs)
            toc = time.perf_counter()
            elapsed = toc - tic

            cls.timers[tuple(cls.trace)].append(elapsed)

            cls.trace_prev = list(cls.trace)
            cls.trace_idx_prev = list(cls.trace_idx)

            cls.trace.pop()
            cls.trace_idx.pop()

            return out

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
    def summarise_string(cls) -> None:
        """
        Produce a human-readable summary of method execution time statistics.
        """

        def _format_nodes(
            node: dict[tuple[str, ...], _Summary],
            depth: int = 0,
            depth_final: Sequence[bool] = (),
        ):
            for idx, (k, v) in enumerate(node.items()):
                is_final_child = idx == len(node) - 1
                angle = "└ " if is_final_child else "├ "
                bars = ""
                if depth > 0:
                    bars = "".join(f"{'│ ' if not i else '  '}" for i in depth_final)
                k_str = bars + (angle if depth > 0 else "") + f"{k[depth]}"
                min_str = f"{v.min:10.6f}" if v.number > 1 else f"{f'-':^12s}"
                max_str = f"{v.max:10.6f}" if v.number > 1 else f"{f'-':^12s}"
                stddev_str = f"({v.stddev:8.6f})" if v.number > 1 else f"{f' ':^10s}"
                out.append(
                    f"{k_str:.<80s} {v.sum:12.6f} "
                    f"{v.mean:10.6f} {stddev_str} {v.number:8d} "
                    f"{min_str} {max_str} "
                )
                depth_final_next = list(depth_final)
                if depth > 0:
                    depth_final_next.append(is_final_child)
                _format_nodes(v.children, depth + 1, depth_final_next)

        summary = cls._summarise()

        out = [
            f"{'function':^80s} {'sum /s':^12s} {'mean (stddev) /s':^20s} {'N':^8s} "
            f"{'min /s':^12s} {'max /s':^12s}"
        ]
        _format_nodes(summary)
        out_str = "\n".join(out)
        if cls.file_path:
            Path(cls.file_path).write_text(out_str, encoding="utf-8")
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
        self.logger.setLevel(logging.DEBUG)
        #: The handler for directing logging messages to the console.
        self.console_handler = self.__add_console_logger(
            level=log_console_level or AppLog.DEFAULT_LOG_CONSOLE_LEVEL
        )
        self.file_handler: logging.FileHandler | None = None

    def __add_console_logger(self, level: str, fmt: str | None = None) -> logging.Handler:
        fmt = fmt or "%(levelname)s %(name)s: %(message)s"
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(fmt))
        handler.setLevel(level)
        self.logger.addHandler(handler)
        return handler

    def update_console_level(self, new_level: str | None = None) -> None:
        """
        Set the logging level for console messages.
        """
        new_level = new_level or AppLog.DEFAULT_LOG_CONSOLE_LEVEL
        self.console_handler.setLevel(new_level.upper())

    def update_file_level(self, new_level: str | None = None) -> None:
        if self.file_handler:
            new_level = new_level or AppLog.DEFAULT_LOG_FILE_LEVEL
            self.file_handler.setLevel(new_level.upper())

    def add_file_logger(
        self,
        path: str | Path,
        level: str | None = None,
        fmt: str | None = None,
        max_bytes: int | None = None,
    ) -> None:
        """
        Add a log file.
        """
        path = Path(path)
        fmt = fmt or "%(asctime)s %(levelname)s %(name)s: %(message)s"
        level = level or AppLog.DEFAULT_LOG_FILE_LEVEL
        max_bytes = max_bytes or int(10e6)

        if not path.parent.is_dir():
            self.logger.info(f"Generating log file parent directory: {path.parent!r}")
            path.parent.mkdir(exist_ok=True, parents=True)

        handler = logging.handlers.RotatingFileHandler(filename=path, maxBytes=max_bytes)
        handler.setFormatter(logging.Formatter(fmt))
        handler.setLevel(level.upper())
        self.logger.addHandler(handler)
        self.file_handler = handler

    def remove_file_handler(self) -> None:
        """Remove the file handler."""
        if self.file_handler:
            self.logger.debug(
                f"Removing file handler from the AppLog: {self.file_handler!r}."
            )
            self.logger.removeHandler(self.file_handler)
            self.file_handler = None
