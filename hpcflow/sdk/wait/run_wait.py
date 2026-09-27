from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from hpcflow.sdk.wait.utils import remove_superseded_directory


class RunWaitEvent(StrEnum):
    START = "start"
    END = "end"


@dataclass(frozen=True)
class RunWaitState:
    submissions_path: Path
    submission_idx: int
    jobscript_idx: int

    @property
    def state_path(self) -> Path:
        return (
            self.submissions_path
            / str(self.submission_idx)
            / "js_state"
            / str(self.jobscript_idx)
        )

    def get_event_path(self, run_id: int, event: RunWaitEvent) -> Path:
        return self.state_path / "runs" / event / str(run_id)

    def get_completion_path(self, run_id: int, event: RunWaitEvent) -> Path:
        event_path = self.get_event_path(run_id, event)
        return event_path.with_name(f"{event_path.name}_complete")

    def get_waiters_path(self, run_id: int, event: RunWaitEvent) -> Path:
        return self.get_event_path(run_id, event) / "waiters"

    def is_complete(self, run_id: int, event: RunWaitEvent) -> bool:
        return self.get_completion_path(run_id, event).exists()

    def _register_waiter(self, run_id: int, event: RunWaitEvent, waiter_id: str) -> Path:
        waiter_path = self.get_waiters_path(run_id, event) / waiter_id
        waiter_path.parent.mkdir(parents=True, exist_ok=True)
        waiter_path.touch()
        return waiter_path

    def register(self, run_id: int, event: RunWaitEvent, waiter_id: str) -> bool:
        """Register a waiter for a run event.

        Returns
        -------
        bool
            True if the event had already completed, otherwise False.
        """
        if (completion_path := self.get_completion_path(run_id, event)).exists():
            return True

        waiter_path = self._register_waiter(run_id, event, waiter_id)

        # the event may have completed between the first check and registration:
        if completion_path.exists():
            waiter_path.unlink(missing_ok=True)
            return True

        return False

    def unregister(self, run_id: int, event: RunWaitEvent, waiter_id: str) -> None:
        waiter_path = self.get_waiters_path(run_id, event) / waiter_id
        waiter_path.unlink(missing_ok=True)

    def get_waiter_ids(self, run_id: int, event: RunWaitEvent) -> tuple[str, ...]:

        if not (waiters_path := self.get_waiters_path(run_id, event)).exists():
            return ()

        return tuple(path.name for path in waiters_path.iterdir() if path.is_file())

    def complete(self, run_id: int, event: RunWaitEvent) -> tuple[str, ...]:
        """Mark a run event complete and return the registered waiter IDs."""

        event_path = self.get_event_path(run_id, event)

        # nobody has registered an interest in this event.
        if not event_path.exists():
            return ()

        waiter_ids = self.get_waiter_ids(run_id, event)
        completion_path = self.get_completion_path(run_id, event)

        # establish the stronger state before removing the registration state:
        completion_path.parent.mkdir(parents=True, exist_ok=True)
        completion_path.touch()
        remove_superseded_directory(event_path)

        return waiter_ids
