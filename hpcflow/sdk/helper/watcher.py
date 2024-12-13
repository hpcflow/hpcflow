"""
File-system watcher classes.
"""

from __future__ import annotations
from collections.abc import Callable
from datetime import timedelta
from logging import Logger
from pathlib import Path
from watchdog.observers.polling import PollingObserver
from watchdog.events import (
    FileSystemEvent,
    FileSystemEventHandler,
    PatternMatchingEventHandler,
)


class _PMEHDelegate(PatternMatchingEventHandler):
    def __init__(self, pattern: str, on_modified: Callable[[FileSystemEvent], None]):
        super().__init__([pattern])
        self.__on_modified = on_modified

    def on_modified(self, event: FileSystemEvent) -> None:
        self.__on_modified(event)


class _FSEHDelegate(FileSystemEventHandler):
    def __init__(self, on_modified: Callable[[FileSystemEvent], None]) -> None:
        self.__on_modified = on_modified

    def on_modified(self, event: FileSystemEvent) -> None:
        self.__on_modified(event)


class MonitorController:
    """
    Controller for tracking watch files.
    """

    def __init__(
        self,
        workflow_dirs_file_path: str | Path,
        watch_interval: float | timedelta,
        logger: Logger,
    ):
        if isinstance(watch_interval, timedelta):
            self.watch_interval = int(watch_interval.total_seconds())
        else:
            self.watch_interval = int(watch_interval)
        self.workflow_dirs_file_path = Path(workflow_dirs_file_path).absolute()
        self.logger = logger

        if not self.workflow_dirs_file_path.exists():
            self.logger.info(
                f"Watch file does not exist; creating {str(self.workflow_dirs_file_path)}."
            )
            with self.workflow_dirs_file_path.open("wt") as fp:
                fp.write("\n")

        self.logger.info(f"Watching file: {str(self.workflow_dirs_file_path)}")

        self.event_handler = _PMEHDelegate("watch_workflows.txt", self.on_modified)

        self.observer = PollingObserver(timeout=self.watch_interval)
        self.observer.schedule(
            self.event_handler,
            path=self.workflow_dirs_file_path.parent,
            recursive=False,
        )

        self.observer.start()

        workflow_paths = self.parse_watch_workflows_file(
            self.workflow_dirs_file_path, logger=self.logger
        )
        self.workflow_monitor = WorkflowMonitor(
            workflow_paths,
            watch_interval=self.watch_interval,
            logger=self.logger,
        )

    @staticmethod
    def parse_watch_workflows_file(
        path: str | Path, logger: Logger
    ) -> list[dict[str, Path]]:
        """
        Parse the file describing what workflows to watch.
        """
        # TODO: and parse element IDs as well; and record which are set/unset.
        with Path(path).open("rt") as fp:
            lns = fp.readlines()

        wks: list[dict[str, Path]] = []
        for ln in lns:
            ln_s = ln.strip()
            if not ln_s:
                continue
            wk_path = Path(ln_s).absolute()
            if not wk_path.is_dir():
                logger.warning(f"{str(wk_path)} is not a workflow")
                continue

            wks.append(
                {
                    "path": wk_path,
                }
            )

        return wks

    def on_modified(self, event: FileSystemEvent):
        """
        Callback when files are modified.
        """
        self.logger.info(f"Watch file modified: {event.src_path}")
        wks = self.parse_watch_workflows_file(event.src_path, logger=self.logger)
        self.workflow_monitor.update_workflow_paths(wks)

    def join(self) -> None:
        """
        Join the worker thread.
        """
        self.observer.join()

    def stop(self) -> None:
        """
        Stop this monitor.
        """
        self.observer.stop()
        self.observer.join()  # wait for it to stop!
        self.workflow_monitor.stop()


class WorkflowMonitor:
    """
    Workflow monitor.
    """

    def __init__(
        self,
        workflow_paths: list[dict[str, Path]],
        watch_interval: float | timedelta,
        logger: Logger,
    ):
        if isinstance(watch_interval, timedelta):
            self.watch_interval = int(watch_interval.total_seconds())
        else:
            self.watch_interval = int(watch_interval)

        self.event_handler = _FSEHDelegate(self.on_modified)
        self.workflow_paths = workflow_paths
        self.logger = logger

        self._monitor_workflow_paths()

    def _monitor_workflow_paths(self) -> None:
        observer = PollingObserver(timeout=self.watch_interval)
        self.observer: PollingObserver | None = observer
        for i in self.workflow_paths:
            observer.schedule(self.event_handler, path=i["path"], recursive=False)
            self.logger.info(f"Watching workflow: {i['path'].name}")

        observer.start()

    def on_modified(self, event: FileSystemEvent):
        """
        Triggered on a workflow being modified.
        """
        self.logger.info(f"Workflow modified: {event.src_path}")

    def update_workflow_paths(self, new_paths: list[dict[str, Path]]):
        """
        Change the set of paths to monitored workflows.
        """
        self.logger.info("Updating watched workflows.")
        self.stop()
        self.workflow_paths = new_paths
        self._monitor_workflow_paths()

    def stop(self) -> None:
        """
        Stop this monitor.
        """
        if self.observer:
            self.observer.stop()
            self.observer.join()  # wait for it to stop!
            self.observer = None
