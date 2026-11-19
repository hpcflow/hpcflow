"""Persist one scheduler snapshot per query target and bound the query frequency."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
import time
from typing import Any, TYPE_CHECKING

import zstandard as zstd

from ..log import TimeIt
from ..submission.enums import JobscriptElementState
from .models import JobObservation, to_json
from .registry import atomic_bytes, file_lock

if TYPE_CHECKING:
    from ..app import BaseApp


@dataclass(slots=True)
class SchedulerSnapshot:
    """Queue states, covered references, and the actual query time, including failures.

    References not covered by a snapshot are unknown, rather than evidence that
    newly submitted jobs have finished. Cache hits never advance ``queried_at``.
    """

    queried_at: float
    references: set[str]
    states: dict[str, JobObservation]
    error: str | None = None
    cached: bool = False

    @property
    def observed_at(self) -> str:
        """Return the scheduler observation time in the registry timestamp format."""
        return datetime.fromtimestamp(self.queried_at, timezone.utc).isoformat()


def reference_key(reference: Any) -> str:
    """Include direct-process command lines when matching snapshot coverage."""
    return json.dumps(to_json(reference), sort_keys=True)


@TimeIt.decorator
def _read(path: Path) -> SchedulerSnapshot | None:
    """Ignore missing, corrupt, or unsupported snapshots and allow a fresh query."""
    try:
        data = json.loads(zstd.ZstdDecompressor().decompress(path.read_bytes()))
        queried_at = data["queried_at"]
        if (
            data["schema_version"] != 1
            or type(queried_at) not in (int, float)
            or not math.isfinite(queried_at)
            or not isinstance(data["references"], list)
            or not all(isinstance(ref, str) for ref in data["references"])
            or not isinstance(data["states"], dict)
            or (data.get("error") is not None and not isinstance(data["error"], str))
        ):
            return None
        states = {}
        names = JobscriptElementState.__members__
        for key, value in data["states"].items():
            state = value.get("state")
            elements = value.get("elements", {})
            if (state is not None and state not in names) or not isinstance(
                elements, dict
            ):
                return None
            if any(
                name not in names or int(index) < 0 for index, name in elements.items()
            ):
                return None
            states[key] = JobObservation(state, {int(i): s for i, s in elements.items()})
        return SchedulerSnapshot(
            queried_at, set(data["references"]), states, data.get("error")
        )
    except (OSError, ValueError, KeyError, TypeError, AttributeError, zstd.ZstdError):
        return None


@TimeIt.decorator
def query_snapshot(
    app: BaseApp,
    context: str,
    references: Sequence[Any],
    fetch: Callable[
        [], Mapping[str, JobscriptElementState | Mapping[int, JobscriptElementState]]
    ],
    min_interval: int,
    ignore_interval: bool = False,
    progress: Callable[[str], None] | None = None,
    scheduler_name: str = "",
    no_update: bool = False,
) -> SchedulerSnapshot:
    """Query at most once per interval, including across concurrent show processes.

    Cache failures too, so repeated invocations cannot hammer an unavailable
    scheduler. A per-target OS lock covers the cache check and scheduler call.
    The override bypasses the age check, while zero disables the cache entirely.
    ``no_update`` reuses existing snapshots without writing new ones or lock files.
    A live query in that mode cannot refresh the shared rate-limit timestamp.
    """
    keys = {reference_key(ref) for ref in references}

    def query() -> SchedulerSnapshot:
        if progress:
            progress(f"Querying {scheduler_name} scheduler ({len(references)} jobs)")
        states = {}
        error = None
        try:
            for key, observed in fetch().items():
                states[str(key)] = (
                    JobObservation(
                        elements={int(i): state.name for i, state in observed.items()}
                    )
                    if isinstance(observed, Mapping)
                    else JobObservation(state=observed.name)
                )
        except Exception as exc:
            error = str(exc)
        return SchedulerSnapshot(time.time(), keys, states, error)

    if min_interval == 0:
        result = query()
    else:
        digest = sha256(context.encode()).hexdigest()
        path = (
            Path(app.user_data_hostname_dir) / "scheduler_states" / f"{digest}.json.zst"
        )

        def recent() -> SchedulerSnapshot | None:
            if ignore_interval:
                return None
            cached = _read(path)
            age = time.time() - cached.queried_at if cached else None
            if cached is not None and age is not None and 0 <= age < min_interval:
                if progress:
                    progress(
                        f"Using cached {scheduler_name} scheduler states ({age:.0f}s old)"
                    )
                return replace(cached, cached=True)
            return None

        if no_update:
            result = recent() or query()
        else:
            with file_lock(path.with_suffix(".lock")):
                cached_result = recent()
                result = cached_result or query()
                if cached_result is None:
                    try:
                        atomic_bytes(
                            path,
                            zstd.ZstdCompressor().compress(
                                json.dumps(
                                    {
                                        "schema_version": 1,
                                        "queried_at": result.queried_at,
                                        "references": sorted(result.references),
                                        "states": to_json(result.states),
                                        "error": result.error,
                                    }
                                ).encode()
                            ),
                        )
                    except (OSError, zstd.ZstdError) as error:
                        app.submission_logger.info(
                            "Cannot cache scheduler observation: %s", error
                        )
    if result.error is not None:
        prefix = "Cached scheduler query failure: " if result.cached else ""
        raise RuntimeError(prefix + result.error)
    return result
