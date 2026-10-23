"""Workflow wait notification protocol."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


class InvalidWaitNotificationError(ValueError):
    pass


@dataclass(frozen=True)
class WaitWakeup:
    submission_idx: int
    jobscript_idx: int

    def __post_init__(self) -> None:
        if type(self.submission_idx) is not int:
            raise InvalidWaitNotificationError("'submission_idx' must be an integer.")
        if type(self.jobscript_idx) is not int:
            raise InvalidWaitNotificationError("'jobscript_idx' must be an integer.")

    @classmethod
    def from_dict(cls, data: Any) -> WaitWakeup:
        if not isinstance(data, dict):
            raise InvalidWaitNotificationError("Wait notification must be a JSON object.")

        try:
            return cls(**data)
        except TypeError as exc:
            raise InvalidWaitNotificationError(
                f"Invalid wait notification: {exc}"
            ) from exc

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
