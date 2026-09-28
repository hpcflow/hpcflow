"""
Interface to the standard logger, and performance logging utility.
"""

from __future__ import annotations
import logging
import logging.handlers
from pathlib import Path
from typing import ClassVar, TYPE_CHECKING
import contextlib
from collections.abc import Iterator

if TYPE_CHECKING:
    from .app import BaseApp


class LoggerLevelFilter(logging.Filter):
    """Filter log records according to per-logger logging levels.

    Parameters
    ----------
    default_level
        Level to use for loggers that do not have an explicit override.
    logger_levels
        Mapping from fully-qualified logger names to logging levels.

    Notes
    -----
    Logger-level overrides also apply to descendant loggers. For example, an override for
    ``hpcflow.execution`` also applies to ``hpcflow.execution.executor``.

    """

    def __init__(self, default_level: int, logger_levels: dict[str, int]) -> None:
        super().__init__()
        self.default_level = default_level
        self.logger_levels = logger_levels

    def filter(self, record: logging.LogRecord) -> bool:
        level = self.default_level

        # Prefer the most-specific matching logger. This matters if, for example, both
        # "execution" and "execution.executor" have overrides.
        matching_loggers = (
            logger_name
            for logger_name in self.logger_levels
            if (record.name == logger_name or record.name.startswith(f"{logger_name}."))
        )

        try:
            logger_name = max(matching_loggers, key=len)
        except ValueError:
            pass
        else:
            level = self.logger_levels[logger_name]

        return record.levelno >= level


class AppLog:
    """Application log control."""

    #: Default logging level for the console.
    DEFAULT_LOG_CONSOLE_LEVEL: ClassVar = "WARNING"

    #: Default logging level for log files.
    DEFAULT_LOG_FILE_LEVEL: ClassVar = "WARNING"

    def __init__(
        self,
        app: BaseApp,
        log_console_level: str | None = None,
    ) -> None:
        #: The application context.
        self._app = app

        #: The base logger for the application.
        self.logger = logging.getLogger(app.package_name)
        self.logger.propagate = False
        self.logger.setLevel(logging.WARNING)

        #: Default level for records written to the file handler.
        self._file_level = self.DEFAULT_LOG_FILE_LEVEL

        # The handler for directing logging messages to the console.
        self.console_handler: logging.Handler | None = None

        # The handler for directing logging messages to a file.
        self.file_handler: logging.FileHandler | None = None

        self.console_handler = self.__add_console_logger(
            level=(log_console_level or self.DEFAULT_LOG_CONSOLE_LEVEL)
        )

        #: Per-logger overrides for the file handler. Keys are relative to the
        #: application's base logger, e.g. ``"execution"`` or ``"execution.executor"``.
        self._file_logger_levels: dict[str, str] = {}

    @staticmethod
    def _get_level(level: str) -> int:
        """Convert a logging level name to its integer value."""
        level = level.upper()
        try:
            level_number = logging.getLevelNamesMapping().get(level)
        except AttributeError:
            # TODO: remove fallback when minimum Python version is >= 3.11.
            level_number = logging.getLevelName(level)
            if not isinstance(level_number, int):
                level_number = None
        if level_number is None:
            raise ValueError(f"Invalid logging level {level!r}.")
        return level_number

    def _ensure_logger_level(self) -> None:
        """Ensure the base logger allows records required by AppLog handlers."""
        levels = []
        if self.console_handler is not None:
            levels.append(self.console_handler.level)
        if self.file_handler is not None:
            levels.append(self.file_handler.level)
        self.logger.setLevel(min(levels, default=logging.WARNING))

    def __add_console_logger(self, level: str, fmt: str | None = None) -> logging.Handler:
        """Add the console logging handler."""
        fmt = fmt or "%(levelname)s %(name)s: %(message)s"
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(fmt))
        handler.setLevel(level.upper())

        self.logger.addHandler(handler)
        self._ensure_logger_level()
        return handler

    def update_console_level(self, new_level: str | None = None) -> None:
        """Set the logging level for console messages."""
        new_level = new_level or self.DEFAULT_LOG_CONSOLE_LEVEL
        self.console_handler.setLevel(new_level.upper())
        self._ensure_logger_level()

    def _configure_file_handler(self) -> None:
        if self.file_handler is None:
            return

        default_level = self._get_level(self._file_level)

        logger_levels = {
            f"{self._app.package_name}.{logger_name}": self._get_level(level)
            for logger_name, level in self._file_logger_levels.items()
        }

        handler_level = min([default_level, *logger_levels.values()])

        # physical handler must accept the most permissive logger-specific level.
        self.file_handler.setLevel(handler_level)

        for filter_ in list(self.file_handler.filters):
            if isinstance(filter_, LoggerLevelFilter):
                self.file_handler.removeFilter(filter_)

        self.file_handler.addFilter(
            LoggerLevelFilter(
                default_level=default_level,
                logger_levels=logger_levels,
            )
        )

        # parent application logger must also accept that level.
        self._ensure_logger_level()

    def update_file_level(
        self,
        new_level: str | None = None,
    ) -> None:
        """Set the default logging level for file messages."""
        self._file_level = (new_level or self.DEFAULT_LOG_FILE_LEVEL).upper()

        # validate even when no file handler currently exists. This means invalid
        # configuration fails when it is supplied rather than later when a file handler
        # happens to be created.
        self._get_level(self._file_level)

        self._configure_file_handler()

    def update_file_logger_levels(
        self,
        levels: dict[str, str] | None = None,
    ) -> None:
        """Set logger-specific levels for the file handler.

        Parameters
        ----------
        levels
            Mapping from logger names relative to the application logger to
            their desired file logging levels. For example::

                {
                    "execution": "DEBUG",
                    "persistence": "INFO",
                }

            An empty mapping or ``None`` removes all overrides.
        """
        levels = levels or {}
        normalised_levels = {
            logger_name: level.upper() for logger_name, level in levels.items()
        }

        # validate all levels before changing state.
        for level in normalised_levels.values():
            self._get_level(level)

        self._file_logger_levels = normalised_levels
        self._configure_file_handler()

    def add_file_logger(
        self,
        path: str | Path,
        level: str | None = None,
        fmt: str | None = None,
        max_bytes: int | None = None,
        backup_count: int = 4,
    ) -> None:
        """Add a log file."""
        path = Path(path)
        fmt = fmt or "%(asctime)s %(levelname)s %(name)s: %(message)s"
        level = (level or self._file_level).upper()
        max_bytes = max_bytes or int(50e6)

        # validate before creating the handler
        self._get_level(level)

        if not path.parent.is_dir():
            self.logger.info("Generating log file parent directory: " f"{path.parent!r}")
            path.parent.mkdir(
                exist_ok=True,
                parents=True,
            )

        handler = logging.handlers.RotatingFileHandler(
            filename=path,
            maxBytes=max_bytes,
            backupCount=backup_count,
        )
        handler.setFormatter(logging.Formatter(fmt))

        self.logger.addHandler(handler)
        self.file_handler = handler
        self._file_level = level
        self._configure_file_handler()

    def remove_file_handler(self) -> None:
        """Remove the file handler."""
        if self.file_handler is None:
            return

        self.logger.debug(
            "Removing file handler from the AppLog: " f"{self.file_handler!r}."
        )

        self.logger.removeHandler(self.file_handler)
        self.file_handler.close()
        self.file_handler = None

        self._ensure_logger_level()

    @contextlib.contextmanager
    def temporary_file_logger(
        self,
        path: str | Path | None,
    ) -> Iterator[None]:
        old_handler = self.file_handler

        if old_handler is not None:
            self.logger.removeHandler(old_handler)

        self.file_handler = None
        self._ensure_logger_level()

        try:
            if path is not None:
                self.add_file_logger(path, level=self._file_level)
            yield

        finally:
            if self.file_handler is not None:
                self.logger.removeHandler(self.file_handler)
                self.file_handler.close()

            self.file_handler = old_handler

            if old_handler is not None:
                self.logger.addHandler(old_handler)

            self._ensure_logger_level()

    def flush_all(self) -> None:
        for handler in self.logger.handlers:
            handler.flush()
