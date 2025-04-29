"""
Base persistence models.

Store* classes represent the element-metadata in the store, in a store-agnostic way.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
import contextlib
import copy
from dataclasses import dataclass, field
import enum
from logging import Logger
from functools import wraps
import os
from pathlib import Path
import shutil
import socket
import time
from typing import Generic, TypeVar, cast, overload, TYPE_CHECKING

import numpy as np

from hpcflow.sdk.core.utils import (
    flatten,
    get_in_container,
    get_relative_path,
    remap,
    reshape,
    set_in_container,
    normalise_timestamp,
    parse_timestamp,
    current_timestamp,
)
from hpcflow.sdk.core.errors import ParametersMetadataReadOnlyError
from hpcflow.sdk.submission.submission import (
    JOBSCRIPT_SUBMIT_TIME_KEYS,
    SUBMISSION_SUBMIT_TIME_KEYS,
)
from hpcflow.sdk.utils.strings import shorten_list_str
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.persistence.pending import PendingChanges
from hpcflow.sdk.persistence.types import (
    AnySTask,
    AnySElement,
    AnySElementIter,
    AnySEAR,
    AnySParameter,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
    from contextlib import AbstractContextManager
    from datetime import datetime
    from typing import Any, ClassVar, Final, Literal
    from typing_extensions import Self, TypeIs
    from fsspec import AbstractFileSystem  # type: ignore
    from numpy.typing import NDArray
    from .pending import CommitResourceMap
    from .store_resource import StoreResource
    from .types import (
        EncodedStoreParameter,
        File,
        FileDescriptor,
        LoopDescriptor,
        Metadata,
        ParameterTypes,
        PersistenceCache,
        StoreCreationInfo,
        TemplateMeta,
        TypeLookup,
        IterableParam,
    )
    from .zarr import ZarrAttrsDict
    from ..app import BaseApp
    from ..typing import DataIndex, PathLike, ParamSource
    from ..core.json_like import JSONed, JSONDocument
    from ..core.parameters import ParameterValue
    from ..core.workflow import Workflow
    from ..submission.types import VersionInfo, ResolvedJobscriptBlockDependencies

T = TypeVar("T")
#: Type of the serialized form.
SerFormT = TypeVar("SerFormT")
#: Type of the encoding and decoding context.
ContextT = TypeVar("ContextT")

PRIMITIVES = (
    int,
    float,
    str,
    type(None),
)

TEMPLATE_COMP_TYPES = (
    "parameters",
    "command_files",
    "environments",
    "task_schemas",
)

PARAM_DATA_NOT_SET: Final[int] = 0


def update_param_source_dict(source: ParamSource, update: ParamSource) -> ParamSource:
    """
    Combine two dicts into a new dict that is ordered on its keys.
    """
    return cast("ParamSource", dict(sorted({**source, **update}.items())))


def writes_parameter_data(func: Callable):
    """Decorator function that should wrap `PersistentStore` methods that write
    parameter-associated data.

    Notes
    -----
    This decorator checks that the parameters-metadata cache is not in use, which should
    not be used during writing of parameter-associated data.
    """

    @wraps(func)
    def inner(self, *args, **kwargs):
        if self._use_parameters_metadata_cache:
            raise ParametersMetadataReadOnlyError(
                "Cannot use the `parameters_metadata_cache` when writing parameter-"
                "associated data!"
            )
        return func(self, *args, **kwargs)

    return inner


@dataclass
class PersistentStoreFeatures:
    """
    Represents the features provided by a persistent store.

    Parameters
    ----------
    create:
        If True, a new workflow can be created using this store.
    edit:
        If True, the workflow can be modified.
    jobscript_parallelism:
        If True, the store supports workflows running multiple independent jobscripts
        simultaneously.
    EAR_parallelism:
        If True, the store supports workflows running multiple EARs simultaneously.
    schedulers:
        If True, the store supports submitting workflows to a scheduler.
    submission:
        If True, the store supports submission. If False, the store can be considered to
        be an archive, which would need transforming to another store type before
        submission.
    """

    #: Whether a new workflow can be created using this store.
    create: bool = False
    #: Whether the workflow can be modified.
    edit: bool = False
    #: Whetherthe store supports workflows running multiple independent jobscripts
    #: simultaneously.
    jobscript_parallelism: bool = False
    #: Whether the store supports workflows running multiple EARs simultaneously.
    EAR_parallelism: bool = False
    #: Whether the store supports submitting workflows to a scheduler.
    schedulers: bool = False
    #: Whether the store supports submission. If not, the store can be considered to
    #: be an archive, which would need transforming to another store type before
    #: submission.
    submission: bool = False


@dataclass
class StoreTask(Generic[SerFormT]):
    """
    Represents a task in a persistent store.

    Parameters
    ----------
    id_:
        The ID of the task.
    index:
        The index of the task within its workflow.
    is_pending:
        Whether the task has changes not yet persisted.
    element_IDs:
        The IDs of elements in the task.
    task_template:
        Description of the template for the task.
    """

    # This would be in the docstring except it renders really wrongly!
    # Type Parameters
    # ---------------
    # SerFormT
    #     Type of the serialized form.

    #: The ID of the task.
    id_: int
    #: The index of the task within its workflow.
    index: int
    #: Whether the task has changes not yet persisted.
    is_pending: bool
    #: The IDs of elements in the task.
    element_IDs: list[int]
    #: Description of the template for the task.
    task_template: Mapping[str, Any] | None = None

    @abstractmethod
    def encode(self) -> tuple[int, SerFormT, dict[str, Any]]:
        """Prepare store task data for the persistent store."""

    @classmethod
    @abstractmethod
    def decode(cls, task_dat: SerFormT) -> Self:
        """Initialise a `StoreTask` from store task data

        Note: the `task_template` is only needed for encoding because it is retrieved as
        part of the `WorkflowTemplate` so we don't need to load it when decoding.

        """

    @TimeIt.decorator
    def append_element_IDs(self, pend_IDs: list[int]) -> Self:
        """Return a copy, with additional element IDs."""
        return self.__class__(
            id_=self.id_,
            index=self.index,
            is_pending=self.is_pending,
            element_IDs=[*self.element_IDs, *pend_IDs],
            task_template=self.task_template,
        )


@dataclass
class StoreElement(Generic[SerFormT, ContextT]):
    """
    Represents an element in a persistent store.

    Parameters
    ----------
    id_:
        The ID of the element.
    is_pending:
        Whether the element has changes not yet persisted.
    index:
        Index of the element within its parent task.
    es_idx:
        Index of the element set containing this element.
    seq_idx:
        Value sequence index map.
    src_idx:
        Data source index map.
    task_ID:
        ID of the task that contains this element.
    iteration_IDs:
        IDs of element-iterations that belong to this element.
    """

    # These would be in the docstring except they render really wrongly!
    # Type Parameters
    # ---------------
    # SerFormT
    #     Type of the serialized form.
    # ContextT
    #     Type of the encoding and decoding context.

    #: The ID of the element.
    id_: int
    #: Whether the element has changes not yet persisted.
    is_pending: bool
    #: Index of the element within its parent task.
    index: int
    #: Index of the element set containing this element.
    es_idx: int
    #: Value sequence index map.
    seq_idx: dict[str, int]
    #: Data source index map.
    src_idx: dict[str, int]
    #: ID of the task that contains this element.
    task_ID: int
    #: IDs of element-iterations that belong to this element.
    iteration_IDs: list[int]

    @abstractmethod
    def encode(self, context: ContextT) -> SerFormT:
        """Prepare store element data for the persistent store."""

    @classmethod
    @abstractmethod
    def decode(cls, elem_dat: SerFormT, context: ContextT) -> Self:
        """Initialise a `StoreElement` from store element data"""

    def to_dict(self, iters) -> dict[str, Any]:
        """Prepare data for the user-facing `Element` object."""
        return {
            "id_": self.id_,
            "is_pending": self.is_pending,
            "index": self.index,
            "es_idx": self.es_idx,
            "seq_idx": self.seq_idx,
            "src_idx": self.src_idx,
            "iteration_IDs": self.iteration_IDs,
            "task_ID": self.task_ID,
            "iterations": iters,
        }

    @TimeIt.decorator
    def append_iteration_IDs(self, pend_IDs: Iterable[int]) -> Self:
        """Return a copy, with additional iteration IDs."""
        iter_IDs = [*self.iteration_IDs, *pend_IDs]
        return self.__class__(
            id_=self.id_,
            is_pending=self.is_pending,
            index=self.index,
            es_idx=self.es_idx,
            seq_idx=self.seq_idx,
            src_idx=self.src_idx,
            task_ID=self.task_ID,
            iteration_IDs=iter_IDs,
        )


@dataclass
class StoreElementIter(Generic[SerFormT, ContextT]):
    """
    Represents an element iteration in a persistent store.

    Parameters
    ----------
    id_:
        The ID of this element iteration.
    is_pending:
        Whether the element iteration has changes not yet persisted.
    element_ID:
        Which element is an iteration for.
    EARs_initialised:
        Whether EARs have been initialised for this element iteration.
    EAR_IDs:
        Maps task schema action indices to EARs by ID.
    data_idx:
        Overall data index for the element-iteration, which maps parameter names to
        parameter data indices.
    schema_parameters:
        List of parameters defined by the associated task schema.
    loop_idx:
        What loops are being handled here and where they're up to.
    """

    # These would be in the docstring except they render really wrongly!
    # Type Parameters
    # ---------------
    # SerFormT
    #     Type of the serialized form.
    # ContextT
    #     Type of the encoding and decoding context.

    #: The ID of this element iteration.
    id_: int
    #: Whether the element iteration has changes not yet persisted.
    is_pending: bool
    #: Which element is an iteration for.
    element_ID: int
    #: Whether EARs have been initialised for this element iteration.
    EARs_initialised: bool
    #: Maps task schema action indices to EARs by ID.
    EAR_IDs: dict[int, list[int]] | None
    #: Overall data index for the element-iteration, which maps parameter names to
    #: parameter data indices.
    data_idx: DataIndex
    #: List of parameters defined by the associated task schema.
    schema_parameters: list[str]
    #: What loops are being handled here and where they're up to.
    loop_idx: Mapping[str, int] = field(default_factory=dict)

    @abstractmethod
    def encode(self, context: ContextT) -> SerFormT:
        """Prepare store element iteration data for the persistent store."""

    @classmethod
    @abstractmethod
    def decode(cls, iter_dat: SerFormT, context: ContextT) -> Self:
        """Initialise a `StoreElementIter` from persistent store element iteration data"""

    def to_dict(self, EARs: dict[int, dict[str, Any]] | None) -> dict[str, Any]:
        """Prepare data for the user-facing `ElementIteration` object."""
        return {
            "id_": self.id_,
            "is_pending": self.is_pending,
            "element_ID": self.element_ID,
            "EAR_IDs": self.EAR_IDs,
            "data_idx": self.data_idx,
            "schema_parameters": self.schema_parameters,
            "EARs": EARs,
            "EARs_initialised": self.EARs_initialised,
            "loop_idx": dict(self.loop_idx),
        }

    @TimeIt.decorator
    def append_EAR_IDs(self, pend_IDs: Mapping[int, Sequence[int]]) -> Self:
        """Return a copy, with additional EAR IDs."""

        EAR_IDs = copy.deepcopy(self.EAR_IDs) or {}
        for act_idx, IDs_i in pend_IDs.items():
            EAR_IDs.setdefault(act_idx, []).extend(IDs_i)

        return self.__class__(
            id_=self.id_,
            is_pending=self.is_pending,
            element_ID=self.element_ID,
            EAR_IDs=EAR_IDs,
            data_idx=self.data_idx,
            schema_parameters=self.schema_parameters,
            loop_idx=self.loop_idx,
            EARs_initialised=self.EARs_initialised,
        )

    @TimeIt.decorator
    def update_loop_idx(self, loop_idx: Mapping[str, int]) -> Self:
        """Return a copy, with the loop index updated."""
        loop_idx_new = dict(self.loop_idx)
        loop_idx_new.update(loop_idx)
        return self.__class__(
            id_=self.id_,
            is_pending=self.is_pending,
            element_ID=self.element_ID,
            EAR_IDs=self.EAR_IDs,
            data_idx=self.data_idx,
            schema_parameters=self.schema_parameters,
            EARs_initialised=self.EARs_initialised,
            loop_idx=loop_idx_new,
        )

    @TimeIt.decorator
    def set_EARs_initialised(self) -> Self:
        """Return a copy with `EARs_initialised` set to `True`."""
        return self.__class__(
            id_=self.id_,
            is_pending=self.is_pending,
            element_ID=self.element_ID,
            EAR_IDs=self.EAR_IDs,
            data_idx=self.data_idx,
            schema_parameters=self.schema_parameters,
            loop_idx=self.loop_idx,
            EARs_initialised=True,
        )

    @TimeIt.decorator
    def update_data_idx(self: AnySElementIter, data_idx: DataIndex) -> AnySElementIter:
        """Return a copy with an updated `data_idx`.

        The existing data index is updated, not overwritten.

        """
        new_data_idx = copy.deepcopy(self.data_idx)
        new_data_idx.update(data_idx)
        return self.__class__(
            id_=self.id_,
            is_pending=self.is_pending,
            element_ID=self.element_ID,
            EAR_IDs=self.EAR_IDs,
            data_idx=new_data_idx,
            schema_parameters=self.schema_parameters,
            loop_idx=self.loop_idx,
            EARs_initialised=self.EARs_initialised,
        )


@dataclass
class StoreEAR(Generic[SerFormT, ContextT]):
    """
    Represents an element action run in a persistent store.

    Parameters
    ----------
    id_:
        The ID of this element action run.
    is_pending:
        Whether the element action run has changes not yet persisted.
    elem_iter_ID:
        What element iteration owns this EAR.
    action_idx:
        The task schema action associated with this EAR.
    commands_idx:
        The indices of the commands in the EAR.
    data_idx:
        Maps parameter names within this EAR to parameter data indices.
    submission_idx:
        Which submission contained this EAR, if known.
    skip:
        Whether to skip this EAR.
    success:
        Whether this EAR was successful, if known.
    start_time:
        When this EAR started, if known.
    end_time:
        When this EAR finished, if known.
    snapshot_start:
        Snapshot of files at EAR start, if recorded.
    snapshot_end:
        Snapshot of files at EAR end, if recorded.
    exit_code:
        The exit code of the underlying executable, if known.
    metadata:
        Metadata concerning e.g. the state of the EAR.
    run_hostname:
        Where this EAR was submitted to run, if known.
    """

    # These would be in the docstring except they render really wrongly!
    # Type Parameters
    # ---------------
    # SerFormT
    #     Type of the serialized form.
    # ContextT
    #     Type of the encoding and decoding context.

    #: The ID of this element action run.
    id_: int
    #: Whether the element action run has changes not yet persisted.
    is_pending: bool
    #: What element iteration owns this EAR.
    elem_iter_ID: int
    #: The task schema action associated with this EAR.
    action_idx: int
    #: The indices of the commands in the EAR.
    commands_idx: list[int]
    #: Maps parameter names within this EAR to parameter data indices.
    data_idx: DataIndex
    #: Which submission contained this EAR, if known.
    submission_idx: int | None = None
    #: Run ID whose commands can be used for this run (may be this run's ID).
    commands_file_ID: int | None = None
    #: Whether to skip this EAR.
    skip: int = 0
    #: Whether this EAR was successful, if known.
    success: bool | None = None
    #: When this EAR started, if known.
    start_time: datetime | None = None
    #: When this EAR finished, if known.
    end_time: datetime | None = None
    #: Snapshot of files at EAR start, if recorded.
    snapshot_start: dict[str, Any] | None = None
    #: Snapshot of files at EAR end, if recorded.
    snapshot_end: dict[str, Any] | None = None
    #: The exit code of the underlying executable, if known.
    exit_code: int | None = None
    #: Metadata concerning e.g. the state of the EAR.
    metadata: Metadata | None = None
    #: Where this EAR was submitted to run, if known.
    run_hostname: str | None = None
    port_number: int | None = None

    @staticmethod
    def _encode_datetime(dt: datetime | None, ts_fmt: str) -> str | None:
        return dt.strftime(ts_fmt) if dt else None

    @staticmethod
    def _decode_datetime(dt_str: str | None, ts_fmt: str) -> datetime | None:
        return parse_timestamp(dt_str, ts_fmt) if dt_str else None

    @abstractmethod
    def encode(self, ts_fmt: str, context: ContextT) -> SerFormT:
        """Prepare store EAR data for the persistent store."""

    @classmethod
    @abstractmethod
    def decode(cls, EAR_dat: SerFormT, ts_fmt: str, context: ContextT) -> Self:
        """Initialise a `StoreEAR` from persistent store EAR data"""

    def to_dict(self) -> dict[str, Any]:
        """Prepare data for the user-facing `ElementActionRun` object."""

        def _process_datetime(dt: datetime | None) -> datetime | None:
            """We store datetime objects implicitly in UTC, so we need to first make
            that explicit, and then convert to the local time zone."""
            return normalise_timestamp(dt) if dt else None

        return {
            "id_": self.id_,
            "is_pending": self.is_pending,
            "elem_iter_ID": self.elem_iter_ID,
            "action_idx": self.action_idx,
            "commands_idx": self.commands_idx,
            "data_idx": self.data_idx,
            "submission_idx": self.submission_idx,
            "commands_file_ID": self.commands_file_ID,
            "success": self.success,
            "skip": self.skip,
            "start_time": _process_datetime(self.start_time),
            "end_time": _process_datetime(self.end_time),
            "snapshot_start": self.snapshot_start,
            "snapshot_end": self.snapshot_end,
            "exit_code": self.exit_code,
            "metadata": self.metadata,
            "run_hostname": self.run_hostname,
            "port_number": self.port_number,
        }

    @TimeIt.decorator
    def update(
        self,
        submission_idx: int | None = None,
        commands_file_ID: int | None = None,
        skip: int | None = None,
        success: bool | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        snapshot_start: dict[str, Any] | None = None,
        snapshot_end: dict[str, Any] | None = None,
        exit_code: int | None = None,
        run_hostname: str | None = None,
        port_number: int | None = None,
        data_idx: DataIndex | None = None,
    ) -> Self:
        """Return a shallow copy, with specified data updated."""

        sub_idx = submission_idx if submission_idx is not None else self.submission_idx
        skip = skip if skip is not None else self.skip
        success = success if success is not None else self.success
        start_time = start_time if start_time is not None else self.start_time
        end_time = end_time if end_time is not None else self.end_time
        snap_s = snapshot_start if snapshot_start is not None else self.snapshot_start
        snap_e = snapshot_end if snapshot_end is not None else self.snapshot_end
        exit_code = exit_code if exit_code is not None else self.exit_code
        run_hn = run_hostname if run_hostname is not None else self.run_hostname
        port_num = port_number if port_number is not None else self.port_number
        cmd_file = (
            commands_file_ID if commands_file_ID is not None else self.commands_file_ID
        )
        if data_idx is not None:
            new_data_idx = copy.deepcopy(self.data_idx)
            new_data_idx.update(data_idx)
            data_idx = new_data_idx
        else:
            data_idx = self.data_idx

        return self.__class__(
            id_=self.id_,
            is_pending=self.is_pending,
            elem_iter_ID=self.elem_iter_ID,
            action_idx=self.action_idx,
            commands_idx=self.commands_idx,
            data_idx=data_idx,
            metadata=self.metadata,
            submission_idx=sub_idx,
            commands_file_ID=cmd_file,
            skip=skip,
            success=success,
            start_time=start_time,
            end_time=end_time,
            snapshot_start=snap_s,
            snapshot_end=snap_e,
            exit_code=exit_code,
            run_hostname=run_hn,
            port_number=port_num,
        )


@dataclass
@hydrate
class StoreParameter:
    """
    Represents a parameter in a persistent store.

    Parameters
    ----------
    id_:
        The ID of this parameter.
    is_pending:
        Whether the parameter has changes not yet persisted.
    is_set:
        Whether the parameter is set.
    data:
        Description of the value of the parameter.
    file:
        Description of the file this parameter represents.
    source:
        Description of where this parameter originated.
    """

    #: The ID of this parameter.
    id_: int
    #: Whether the parameter has changes not yet persisted.
    is_pending: bool
    #: Whether the parameter is set.
    is_set: bool
    #: Description of the value of the parameter.
    data: ParameterTypes
    #: Description of the file this parameter represents.
    file: File | None
    #: Description of where this parameter originated.
    source: ParamSource

    _encoders: ClassVar[dict[type, Callable]] = {}
    _decoders: ClassVar[dict[str, Callable]] = {}
    _MAX_DEPTH: ClassVar[int] = 50

    def encode(self, **kwargs) -> dict[str, Any] | int:
        """Prepare store parameter data for the persistent store."""
        if self.is_set:
            if self.file:
                return {"file": self.file}
            else:
                return cast("dict", self._encode(obj=self.data, **kwargs))
        else:
            return PARAM_DATA_NOT_SET

    @staticmethod
    def __is_ParameterValue(value) -> TypeIs[ParameterValue]:
        # avoid circular import of `ParameterValue` until needed...
        from ..core.parameters import ParameterValue as PV

        return isinstance(value, PV)

    def _init_type_lookup(self) -> TypeLookup:
        return cast(
            "TypeLookup",
            {
                "tuples": [],
                "sets": [],
                **{k: [] for k in self._decoders},
            },
        )

    def _encode(
        self,
        obj: ParameterTypes,
        path: list[int] | None = None,
        type_lookup: TypeLookup | None = None,
        **kwargs,
    ) -> EncodedStoreParameter:
        """Recursive encoder."""

        path = path or []
        if type_lookup is None:
            type_lookup = self._init_type_lookup()

        if len(path) > self._MAX_DEPTH:
            raise RuntimeError("I'm in too deep!")

        if self.__is_ParameterValue(obj):
            encoded = self._encode(
                obj=obj.to_dict(),
                path=path,
                type_lookup=type_lookup,
                **kwargs,
            )
            data, type_lookup = encoded["data"], encoded["type_lookup"]

        elif isinstance(obj, (list, tuple, set)):
            data = []
            for idx, item in enumerate(obj):
                encoded = self._encode(
                    obj=item,
                    path=[*path, idx],
                    type_lookup=type_lookup,
                    **kwargs,
                )
                item, type_lookup = encoded["data"], encoded["type_lookup"]
                assert type_lookup is not None
                data.append(item)

            if isinstance(obj, tuple):
                type_lookup["tuples"].append(path)

            elif isinstance(obj, set):
                type_lookup["sets"].append(path)

        elif isinstance(obj, dict):
            assert type_lookup is not None
            data = {}
            for dct_key, dct_val in obj.items():
                encoded = self._encode(
                    obj=dct_val,
                    path=[*path, dct_key],
                    type_lookup=type_lookup,
                    **kwargs,
                )
                dct_val, type_lookup = encoded["data"], encoded["type_lookup"]
                assert type_lookup is not None
                data[dct_key] = dct_val

        elif isinstance(obj, PRIMITIVES):
            data = obj

        elif type(obj) in self._encoders:
            assert type_lookup is not None
            data = self._encoders[type(obj)](
                obj=obj,
                path=path,
                type_lookup=type_lookup,
                **kwargs,
            )

        elif isinstance(obj, enum.Enum):
            data = obj.value

        else:
            raise ValueError(
                f"Parameter data with type {type(obj)} cannot be serialised into a "
                f"{self.__class__.__name__}: {obj}."
            )

        return {"data": data, "type_lookup": type_lookup}

    @classmethod
    def decode(
        cls,
        id_: int,
        data: dict[str, Any] | Literal[0] | None,
        source: ParamSource,
        *,
        path: list[str] | None = None,
        **kwargs,
    ) -> Self:
        """Initialise from persistent store parameter data."""
        if data and "file" in data:
            return cls(
                id_=id_,
                data=None,
                file=cast("File", data["file"]),
                is_set=True,
                source=source,
                is_pending=False,
            )
        elif not isinstance(data, dict):
            # parameter is not set
            return cls(
                id_=id_,
                data=None,
                file=None,
                is_set=False,
                source=source,
                is_pending=False,
            )

        data_ = cast("EncodedStoreParameter", data)
        path = path or []

        obj = get_in_container(data_["data"], path)

        for tuple_path in data_["type_lookup"]["tuples"]:
            try:
                rel_path = get_relative_path(tuple_path, path)
            except ValueError:
                continue
            if rel_path:
                set_in_container(obj, rel_path, tuple(get_in_container(obj, rel_path)))
            else:
                obj = tuple(obj)

        for set_path in data_["type_lookup"]["sets"]:
            try:
                rel_path = get_relative_path(set_path, path)
            except ValueError:
                continue
            if rel_path:
                set_in_container(obj, rel_path, set(get_in_container(obj, rel_path)))
            else:
                obj = set(obj)

        for data_type in cls._decoders:
            obj = cls._decoders[data_type](
                obj=obj,
                type_lookup=data_["type_lookup"],
                path=path,
                **kwargs,
            )

        return cls(
            id_=id_,
            data=obj,
            file=None,
            is_set=True,
            source=source,
            is_pending=False,
        )

    def set_data(self, value: Any) -> Self:
        """Return a copy, with data set."""
        if self.is_set:
            raise RuntimeError(f"Parameter ID {self.id_!r} is already set!")
        return self.__class__(
            id_=self.id_,
            is_set=True,
            is_pending=self.is_pending,
            data=value,
            file=None,
            source=self.source,
        )

    def set_file(self, value: File) -> Self:
        """Return a copy, with file set."""
        if self.is_set:
            raise RuntimeError(f"Parameter ID {self.id_!r} is already set!")
        return self.__class__(
            id_=self.id_,
            is_set=True,
            is_pending=self.is_pending,
            data=None,
            file=value,
            source=self.source,
        )

    def update_source(self, src: ParamSource) -> Self:
        """Return a copy, with updated source."""
        return self.__class__(
            id_=self.id_,
            is_set=self.is_set,
            is_pending=self.is_pending,
            data=self.data,
            file=self.file,
            source=update_param_source_dict(self.source, src),
        )


class PersistentStore(
    ABC, Generic[AnySTask, AnySElement, AnySElementIter, AnySEAR, AnySParameter]
):
    """
    An abstract class representing a persistent workflow store.

    Parameters
    ----------
    app: App
        The main hpcflow core.
    workflow: ~hpcflow.app.Workflow
        The workflow being persisted.
    path: pathlib.Path
        Where to hold the store.
    fs: fsspec.AbstractFileSystem
        Optionally, information about how to access the store.
    """

    # These would be in the docstring except they render really wrongly!
    # Type Parameters
    # ---------------
    # AnySTask: StoreTask
    #     The type of stored tasks.
    # AnySElement: StoreElement
    #     The type of stored elements.
    # AnySElementIter: StoreElementIter
    #     The type of stored element iterations.
    # AnySEAR: StoreEAR
    #     The type of stored EARs.
    # AnySParameter: StoreParameter
    #     The type of stored parameters.

    _name: ClassVar[str]

    @classmethod
    @abstractmethod
    def _store_task_cls(cls) -> type[AnySTask]: ...

    @classmethod
    @abstractmethod
    def _store_elem_cls(cls) -> type[AnySElement]: ...

    @classmethod
    @abstractmethod
    def _store_iter_cls(cls) -> type[AnySElementIter]: ...

    @classmethod
    @abstractmethod
    def _store_EAR_cls(cls) -> type[AnySEAR]: ...

    @classmethod
    @abstractmethod
    def _store_param_cls(cls) -> type[AnySParameter]: ...

    _resources: dict[str, StoreResource]
    _features: ClassVar[PersistentStoreFeatures]
    _res_map: ClassVar[CommitResourceMap]

    def __init__(
        self,
        app: BaseApp,
        workflow: Workflow | None,
        path: Path | str,
        fs: AbstractFileSystem | None = None,
    ):
        self._app = app
        self.__workflow = workflow
        self.path = str(path)
        self.fs = fs

        self._pending: PendingChanges[
            AnySTask, AnySElement, AnySElementIter, AnySEAR, AnySParameter
        ] = PendingChanges(app=app, store=self, resource_map=self._res_map)

        self._resources_in_use: set[tuple[str, str]] = set()
        self._in_batch_mode = False

        self._use_cache = False
        self._reset_cache()

        self._use_parameters_metadata_cache: bool = False  # subclass-specific cache

    @abstractmethod
    def cached_load(self) -> contextlib.AbstractContextManager[None]:
        """
        Perform a load with cache enabled while the ``with``-wrapped code runs.
        """

    @abstractmethod
    def get_name(self) -> str:
        """
        Get the workflow name.
        """

    @abstractmethod
    def get_creation_info(self) -> StoreCreationInfo:
        """
        Get the workflow creation data.
        """

    @abstractmethod
    def get_ts_fmt(self) -> str:
        """
        Get the timestamp format.
        """

    @abstractmethod
    def get_ts_name_fmt(self) -> str:
        """
        Get the timestamp format for names.
        """

    @abstractmethod
    def remove_replaced_dir(self) -> None:
        """
        Remove a replaced directory.
        """

    @abstractmethod
    def reinstate_replaced_dir(self) -> None:
        """
        Reinstate a replaced directory.
        """

    @abstractmethod
    def zip(
        self,
        path: str = ".",
        log: str | None = None,
        overwrite=False,
        include_execute=False,
        include_rechunk_backups=False,
    ) -> str:
        """
        Convert this store into archival form.
        """

    @abstractmethod
    def unzip(self, path: str = ".", log: str | None = None) -> str:
        """
        Convert this store into expanded form.
        """

    @abstractmethod
    def rechunk_parameter_base(
        self,
        chunk_size: int | None = None,
        backup: bool = True,
        status: bool = True,
    ) -> Any: ...

    @abstractmethod
    def rechunk_runs(
        self,
        chunk_size: int | None = None,
        backup: bool = True,
        status: bool = True,
    ) -> Any: ...

    @abstractmethod
    def get_dirs_array(self) -> NDArray:
        """
        Retrieve the run directories array.
        """

    @classmethod
    @abstractmethod
    def write_empty_workflow(
        cls,
        app: BaseApp,
        *,
        template_js: TemplateMeta,
        template_components_js: dict[str, Any],
        wk_path: str,
        fs: AbstractFileSystem,
        name: str,
        replaced_wk: str | None,
        creation_info: StoreCreationInfo,
        ts_fmt: str,
        ts_name_fmt: str,
    ) -> None:
        """
        Write an empty workflow.
        """

    @property
    def workflow(self) -> Workflow:
        """
        The workflow this relates to.
        """
        assert self.__workflow is not None
        return self.__workflow

    @property
    def logger(self) -> Logger:
        """
        The logger to use.
        """
        return self._app.persistence_logger

    @property
    def ts_fmt(self) -> str:
        """
        The format for timestamps.
        """
        return self.workflow.ts_fmt

    @property
    def has_pending(self) -> bool:
        """
        Whether there are any pending changes.
        """
        return bool(self._pending)

    @property
    def is_submittable(self) -> bool:
        """Does this store support workflow submission?"""
        return self.fs.__class__.__name__ == "LocalFileSystem"

    @property
    def use_cache(self) -> bool:
        """
        Whether to use a cache.
        """
        return self._use_cache

    @property
    def task_cache(self) -> dict[int, AnySTask]:
        """Cache for persistent tasks."""
        return self._cache["tasks"]

    @property
    def element_cache(self) -> dict[int, AnySElement]:
        """Cache for persistent elements."""
        return self._cache["elements"]

    @property
    def element_iter_cache(self) -> dict[int, AnySElementIter]:
        """Cache for persistent element iterations."""
        return self._cache["element_iters"]

    @property
    def EAR_cache(self) -> dict[int, AnySEAR]:
        """Cache for persistent EARs."""
        return self._cache["EARs"]

    @property
    def num_tasks_cache(self) -> int | None:
        """Cache for number of persistent tasks."""
        return self._cache["num_tasks"]

    @num_tasks_cache.setter
    def num_tasks_cache(self, value: int | None):
        self._cache["num_tasks"] = value

    @property
    def num_EARs_cache(self) -> int | None:
        """Cache for total number of persistent EARs."""
        return self._cache["num_EARs"]

    @num_EARs_cache.setter
    def num_EARs_cache(self, value: int | None):
        self._cache["num_EARs"] = value

    @property
    def num_params_cache(self) -> int | None:
        return self._cache["num_params"]

    @num_params_cache.setter
    def num_params_cache(self, value: int | None):
        self._cache["num_params"] = value

    @property
    def param_sources_cache(self) -> dict[int, ParamSource]:
        """Cache for persistent parameter sources."""
        return self._cache["param_sources"]

    @property
    def parameter_cache(self) -> dict[int, AnySParameter]:
        """Cache for persistent parameters."""
        return self._cache["parameters"]

    def _reset_cache(self) -> None:
        self._cache: PersistenceCache[
            AnySTask, AnySElement, AnySElementIter, AnySEAR, AnySParameter
        ] = {
            "tasks": {},
            "elements": {},
            "element_iters": {},
            "EARs": {},
            "param_sources": {},
            "num_tasks": None,
            "parameters": {},
            "num_EARs": None,
            "num_params": None,
        }

    @contextlib.contextmanager
    def cache_ctx(self) -> Iterator[None]:
        """Context manager for using the persistent element/iteration/run cache."""
        self._use_cache = True
        try:
            yield
        finally:
            self._use_cache = False
            self._reset_cache()

    @contextlib.contextmanager
    def parameters_metadata_cache(self):
        """Context manager for using the parameters-metadata cache.

        Notes
        -----
        This method can be overridden by a subclass to provide an implementation-specific
        cache of metadata associated with parameters, or even parameter data itself.

        Using this cache precludes writing/setting parameter data.

        """
        yield

    @staticmethod
    def prepare_test_store_from_spec(
        task_spec: Sequence[
            Mapping[str, Sequence[Mapping[str, Sequence[Mapping[str, Sequence]]]]]
        ],
    ) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
        """Generate a valid store from a specification in terms of nested
        elements/iterations/EARs.

        """
        tasks: list[dict] = []
        elements: list[dict] = []
        elem_iters: list[dict] = []
        EARs: list[dict] = []

        for task_idx, task_i in enumerate(task_spec):
            elems_i = task_i.get("elements", [])
            elem_IDs = list(range(len(elements), len(elements) + len(elems_i)))

            for elem_idx, elem_j in enumerate(elems_i):
                iters_j = elem_j.get("iterations", [])
                iter_IDs = list(range(len(elem_iters), len(elem_iters) + len(iters_j)))

                for iter_k in iters_j:
                    EARs_k = iter_k.get("EARs", [])
                    EAR_IDs = list(range(len(EARs), len(EARs) + len(EARs_k)))
                    EAR_IDs_dct = {0: EAR_IDs} if EAR_IDs else {}

                    for _ in EARs_k:
                        EARs.append(
                            {
                                "id_": len(EARs),
                                "is_pending": False,
                                "elem_iter_ID": len(elem_iters),
                                "action_idx": 0,
                                "data_idx": {},
                                "metadata": {},
                            }
                        )

                    elem_iters.append(
                        {
                            "id_": len(elem_iters),
                            "is_pending": False,
                            "element_ID": len(elements),
                            "EAR_IDs": EAR_IDs_dct,
                            "data_idx": {},
                            "schema_parameters": [],
                        }
                    )
                elements.append(
                    {
                        "id_": len(elements),
                        "is_pending": False,
                        "element_idx": elem_idx,
                        "seq_idx": {},
                        "src_idx": {},
                        "task_ID": task_idx,
                        "iteration_IDs": iter_IDs,
                    }
                )
            tasks.append(
                {
                    "id_": len(tasks),
                    "is_pending": False,
                    "element_IDs": elem_IDs,
                }
            )
        return (tasks, elements, elem_iters, EARs)

    def remove_path(self, path: str | Path) -> None:
        """Try very hard to delete a directory or file.

        Dropbox (on Windows, at least) seems to try to re-sync files if the parent directory
        is deleted soon after creation, which is the case on a failed workflow creation (e.g.
        missing inputs), so in addition to catching PermissionErrors generated when
        Dropbox has a lock on files, we repeatedly try deleting the directory tree.

        """

        fs = self.fs
        assert fs is not None

        @self._app.perm_error_retry()
        def _remove_path(_path: str) -> None:
            self.logger.debug(f"_remove_path: path={_path}")
            while fs.exists(_path):
                fs.rm(_path, recursive=True)
                time.sleep(0.5)

        return _remove_path(str(path))

    def rename_path(self, replaced: str, original: str | Path) -> None:
        """Revert the replaced workflow path to its original name.

        This happens when new workflow creation fails and there is an existing workflow
        with the same name; the original workflow which was renamed, must be reverted."""

        fs = self.fs
        assert fs is not None

        @self._app.perm_error_retry()
        def _rename_path(_replaced: str, _original: str) -> None:
            self.logger.debug(f"_rename_path: {_replaced!r} --> {_original!r}.")
            try:
                fs.rename(
                    _replaced, _original, recursive=True
                )  # TODO: why need recursive?
            except TypeError:
                # `SFTPFileSystem.rename` has no  `recursive` argument:
                fs.rename(_replaced, _original)

        return _rename_path(str(replaced), str(original))

    @abstractmethod
    def _get_num_persistent_tasks(self) -> int: ...

    def _get_num_total_tasks(self) -> int:
        """Get the total number of persistent and pending tasks."""
        return self._get_num_persistent_tasks() + len(self._pending.add_tasks)

    @abstractmethod
    def _get_num_persistent_loops(self) -> int: ...

    def _get_num_total_loops(self) -> int:
        """Get the total number of persistent and pending loops."""
        return self._get_num_persistent_loops() + len(self._pending.add_loops)

    @abstractmethod
    def _get_num_persistent_submissions(self) -> int: ...

    def _get_num_total_submissions(self) -> int:
        """Get the total number of persistent and pending submissions."""
        return self._get_num_persistent_submissions() + len(self._pending.add_submissions)

    @abstractmethod
    def _get_num_persistent_elements(self) -> int: ...

    def _get_num_total_elements(self) -> int:
        """Get the total number of persistent and pending elements."""
        return self._get_num_persistent_elements() + len(self._pending.add_elements)

    @abstractmethod
    def _get_num_persistent_elem_iters(self) -> int: ...

    def _get_num_total_elem_iters(self) -> int:
        """Get the total number of persistent and pending element iterations."""
        return self._get_num_persistent_elem_iters() + len(self._pending.add_elem_iters)

    @abstractmethod
    def _get_num_persistent_EARs(self) -> int: ...

    @TimeIt.decorator
    def _get_num_total_EARs(self) -> int:
        """Get the total number of persistent and pending EARs."""
        return self._get_num_persistent_EARs() + len(self._pending.add_EARs)

    def _get_task_total_num_elements(self, task_ID: int) -> int:
        """Get the total number of persistent and pending elements of a given task."""
        return len(self.get_task(task_ID).element_IDs)

    @abstractmethod
    def _get_num_persistent_parameters(self) -> int: ...

    def _get_num_total_parameters(self) -> int:
        """Get the total number of persistent and pending parameters."""
        return self._get_num_persistent_parameters() + len(self._pending.add_parameters)

    def _get_num_total_input_files(self) -> int:
        """Get the total number of persistent and pending user-supplied input files."""
        return self._get_num_persistent_input_files() + sum(
            fd["is_input"] for fd in self._pending.add_files
        )

    @abstractmethod
    def _get_num_persistent_added_tasks(self) -> int: ...

    def _get_num_total_added_tasks(self) -> int:
        """Get the total number of tasks ever added to the workflow."""
        return self._get_num_persistent_added_tasks() + len(self._pending.add_tasks)

    def _get_num_persistent_input_files(self) -> int:
        return sum(1 for _ in self.workflow.input_files_path.glob("*"))

    def save(self) -> None:
        """Commit pending changes to disk, if not in batch-update mode."""
        if not self.workflow._in_batch_mode:
            self._pending.commit_all()

    def add_template_components(
        self, temp_comps: Mapping[str, dict], save: bool = True
    ) -> None:
        """
        Add template components to the workflow.
        """
        all_tc = self.get_template_components()
        for name, dat in temp_comps.items():
            if name in all_tc:
                for hash_i, dat_i in dat.items():
                    if hash_i not in all_tc[name]:
                        self._pending.add_template_components[name][hash_i] = dat_i
            else:
                self._pending.add_template_components[name] = dat

        if save:
            self.save()

    def add_task(self, idx: int, task_template: Mapping, save: bool = True):
        """Add a new task to the workflow."""
        self.logger.debug("Adding store task.")
        new_ID = self._get_num_total_added_tasks()
        self._pending.add_tasks[new_ID] = self._store_task_cls()(
            id_=new_ID,
            index=idx,
            task_template=task_template,
            is_pending=True,
            element_IDs=[],
        )
        if save:
            self.save()
        return new_ID

    def add_loop(
        self,
        loop_template: Mapping[str, Any],
        iterable_parameters: Mapping[str, IterableParam],
        output_parameters: Mapping[str, int],
        parents: Sequence[str],
        num_added_iterations: Mapping[tuple[int, ...], int],
        iter_IDs: Iterable[int],
        save: bool = True,
    ):
        """Add a new loop to the workflow."""
        self.logger.debug("Adding store loop.")
        new_idx = self._get_num_total_loops()
        added_iters: list[list[list[int] | int]] = [
            [list(k), v] for k, v in num_added_iterations.items()
        ]
        self._pending.add_loops[new_idx] = {
            "loop_template": dict(loop_template),
            "iterable_parameters": cast("dict", iterable_parameters),
            "output_parameters": cast("dict", output_parameters),
            "parents": list(parents),
            "num_added_iterations": added_iters,
        }

        for i in iter_IDs:
            self._pending.update_loop_indices[i][loop_template["name"]] = 0

        if save:
            self.save()

    @TimeIt.decorator
    def add_submission(
        self, sub_idx: int, sub_js: Mapping[str, JSONed], save: bool = True
    ):
        """Add a new submission."""
        self.logger.debug("Adding store submission.")
        self._pending.add_submissions[sub_idx] = sub_js
        if save:
            self.save()

    def add_element_set(self, task_id: int, es_js: Mapping, save: bool = True):
        """
        Add an element set to a task.
        """
        self._pending.add_element_sets[task_id].append(es_js)
        if save:
            self.save()

    def add_element(
        self,
        task_ID: int,
        es_idx: int,
        seq_idx: dict[str, int],
        src_idx: dict[str, int],
        save: bool = True,
    ) -> int:
        """Add a new element to a task."""
        self.logger.debug("Adding store element.")
        new_ID = self._get_num_total_elements()
        new_elem_idx = self._get_task_total_num_elements(task_ID)
        self._pending.add_elements[new_ID] = self._store_elem_cls()(
            id_=new_ID,
            is_pending=True,
            index=new_elem_idx,
            es_idx=es_idx,
            seq_idx=seq_idx,
            src_idx=src_idx,
            task_ID=task_ID,
            iteration_IDs=[],
        )
        self._pending.add_elem_IDs[task_ID].append(new_ID)
        if save:
            self.save()
        return new_ID

    def add_element_iteration(
        self,
        element_ID: int,
        data_idx: DataIndex,
        schema_parameters: list[str],
        loop_idx: Mapping[str, int] | None = None,
        save: bool = True,
    ) -> int:
        """Add a new iteration to an element."""
        self.logger.debug("Adding store element-iteration.")
        new_ID = self._get_num_total_elem_iters()
        self._pending.add_elem_iters[new_ID] = self._store_iter_cls()(
            id_=new_ID,
            element_ID=element_ID,
            is_pending=True,
            EARs_initialised=False,
            EAR_IDs=None,
            data_idx=data_idx,
            schema_parameters=schema_parameters,
            loop_idx=loop_idx or {},
        )
        self._pending.add_elem_iter_IDs[element_ID].append(new_ID)
        if save:
            self.save()
        return new_ID

    @TimeIt.decorator
    def add_EAR(
        self,
        elem_iter_ID: int,
        action_idx: int,
        commands_idx: list[int],
        data_idx: DataIndex,
        metadata: Metadata | None = None,
        save: bool = True,
    ) -> int:
        """Add a new EAR to an element iteration."""
        self.logger.debug("Adding store EAR.")
        new_ID = self._get_num_total_EARs()
        self._pending.add_EARs[new_ID] = self._store_EAR_cls()(
            id_=new_ID,
            is_pending=True,
            elem_iter_ID=elem_iter_ID,
            action_idx=action_idx,
            commands_idx=commands_idx,
            data_idx=data_idx,
            metadata=metadata or {},
        )
        self._pending.add_elem_iter_EAR_IDs[elem_iter_ID][action_idx].append(new_ID)
        if save:
            self.save()
        return new_ID

    @TimeIt.decorator
    def set_run_dirs(
        self, run_dir_indices: np.ndarray, run_idx: np.ndarray, save: bool = True
    ):
        self.logger.debug(f"Setting {run_idx.size} run directory indices.")
        self._pending.set_run_dirs.append((run_dir_indices, run_idx))
        if save:
            self.save()

    def update_at_submit_metadata(
        self, sub_idx: int, submission_parts: dict[str, list[int]], save: bool = True
    ):
        """
        Update metadata that is set at submit-time.
        """
        if submission_parts:
            self._pending.update_at_submit_metadata[sub_idx][
                "submission_parts"
            ] = submission_parts
        if save:
            self.save()

    @TimeIt.decorator
    def set_run_submission_data(
        self, EAR_ID: int, cmds_ID: int | None, sub_idx: int, save: bool = True
    ) -> None:
        """
        Set the run submission data, like the submission index for an element action run.
        """
        self._pending.set_EAR_submission_data[EAR_ID] = (sub_idx, cmds_ID)
        if save:
            self.save()

    def set_EAR_start(
        self,
        EAR_ID: int,
        run_dir: Path | None,
        port_number: int | None,
        save: bool = True,
    ) -> datetime:
        """
        Mark an element action run as started.
        """
        dt = current_timestamp()
        ss_js = self._app.RunDirAppFiles.take_snapshot() if run_dir else None
        run_hostname = socket.gethostname()
        self._pending.set_EAR_starts[EAR_ID] = (dt, ss_js, run_hostname, port_number)
        if save:
            self.save()
        return dt

    def set_multi_run_starts(
        self,
        run_ids: list[int],
        run_dirs: list[Path | None],
        port_number: int,
        save: bool = True,
    ) -> datetime:
        dt = current_timestamp()
        run_hostname = socket.gethostname()
        run_start_data: dict[int, tuple] = {}
        for id_i, dir_i in zip(run_ids, run_dirs):
            ss_js_i = self._app.RunDirAppFiles.take_snapshot(dir_i) if dir_i else None
            run_start_data[id_i] = (dt, ss_js_i, run_hostname, port_number)

        self._pending.set_EAR_starts.update(run_start_data)
        if save:
            self.save()
        return dt

    def set_EAR_end(
        self,
        EAR_ID: int,
        exit_code: int,
        success: bool,
        snapshot: bool,
        save: bool = True,
    ) -> datetime:
        """
        Mark an element action run as finished.
        """
        # TODO: save output files
        dt = current_timestamp()
        ss_js = self._app.RunDirAppFiles.take_snapshot() if snapshot else None
        self._pending.set_EAR_ends[EAR_ID] = (dt, ss_js, exit_code, success)
        if save:
            self.save()
        return dt

    def set_multi_run_ends(
        self,
        run_ids: list[int],
        run_dirs: list[Path | None],
        exit_codes: list[int],
        successes: list[bool],
        save: bool = True,
    ) -> datetime:
        self.logger.info("PersistentStore.set_multi_run_ends.")
        dt = current_timestamp()
        run_end_data: dict[int, tuple] = {}
        for id_i, dir_i, ex_i, sc_i in zip(run_ids, run_dirs, exit_codes, successes):
            ss_js_i = self._app.RunDirAppFiles.take_snapshot(dir_i) if dir_i else None
            run_end_data[id_i] = (dt, ss_js_i, ex_i, sc_i)

        self._pending.set_EAR_ends.update(run_end_data)
        if save:
            self.save()
        self.logger.info("PersistentStore.set_multi_run_ends finished.")
        return dt

    def set_EAR_skip(self, skip_reasons: dict[int, int], save: bool = True) -> None:
        """
        Mark element action runs as skipped for the specified reasons.
        """
        self._pending.set_EAR_skips.update(skip_reasons)
        if save:
            self.save()

    def set_EARs_initialised(self, iter_ID: int, save: bool = True) -> None:
        """
        Mark an element action run as initialised.
        """
        self._pending.set_EARs_initialised.append(iter_ID)
        if save:
            self.save()

    def set_jobscript_metadata(
        self,
        sub_idx: int,
        js_idx: int,
        version_info: VersionInfo | None = None,
        submit_time: str | None = None,
        submit_hostname: str | None = None,
        submit_machine: str | None = None,
        shell_idx: int | None = None,
        submit_cmdline: list[str] | None = None,
        os_name: str | None = None,
        shell_name: str | None = None,
        scheduler_name: str | None = None,
        scheduler_job_ID: str | None = None,
        process_ID: int | None = None,
        save: bool = True,
    ):
        """
        Set the metadata for a job script.
        """
        entry = self._pending.set_js_metadata[sub_idx][js_idx]
        if version_info:
            entry["version_info"] = version_info
        if submit_time:
            entry["submit_time"] = submit_time
        if submit_hostname:
            entry["submit_hostname"] = submit_hostname
        if submit_machine:
            entry["submit_machine"] = submit_machine
        if shell_idx is not None:
            entry["shell_idx"] = shell_idx
        if submit_cmdline:
            entry["submit_cmdline"] = submit_cmdline
        if os_name:
            entry["os_name"] = os_name
        if shell_name:
            entry["shell_name"] = shell_name
        if scheduler_name:
            entry["scheduler_name"] = scheduler_name
        if scheduler_job_ID or process_ID:
            entry["scheduler_job_ID"] = scheduler_job_ID
        if process_ID or scheduler_job_ID:
            entry["process_ID"] = process_ID
        if save:
            self.save()

    @writes_parameter_data
    def _add_parameter(
        self,
        is_set: bool,
        source: ParamSource,
        data: (
            ParameterValue | list | tuple | set | dict | int | float | str | None | Any
        ) = None,
        file: File | None = None,
        save: bool = True,
    ) -> int:
        self.logger.debug(f"Adding store parameter{f' (unset)' if not is_set else ''}.")
        new_idx = self._get_num_total_parameters()
        self._pending.add_parameters[new_idx] = self._store_param_cls()(
            id_=new_idx,
            is_pending=True,
            is_set=is_set,
            data=PARAM_DATA_NOT_SET if not is_set else data,
            file=file,
            source=source,
        )
        if save:
            self.save()
        return new_idx

    def _prepare_set_file(
        self,
        store_contents: bool,
        is_input: bool,
        path: Path | str,
        contents: str | None = None,
        filename: str | None = None,
        clean_up: bool = False,
    ) -> File:
        if filename is None:
            filename = Path(path).name

        if store_contents:
            if is_input:
                new_idx = self._get_num_total_input_files()
                dst_dir = Path(self.workflow.input_files_path, str(new_idx))
                dst_path = dst_dir / filename
            else:
                # assume path is inside the EAR execution directory; transform that to the
                # equivalent artifacts directory:
                exec_sub_path = Path(path).relative_to(self.path)
                dst_path = Path(
                    self.workflow.task_artifacts_path, *exec_sub_path.parts[1:]
                )
            if dst_path.is_file():
                dst_path = dst_path.with_suffix(dst_path.suffix + "_2")  # TODO: better!
        else:
            dst_path = Path(path)

        file_param_dat: File = {
            "store_contents": store_contents,
            "path": str(dst_path.relative_to(self.path)),
        }
        self._pending.add_files.append(
            {
                "store_contents": store_contents,
                "is_input": is_input,
                "dst_path": str(dst_path),
                "path": str(path),
                "contents": contents or "",
                "clean_up": clean_up,
            }
        )

        return file_param_dat

    def set_file(
        self,
        store_contents: bool,
        is_input: bool,
        param_id: int | None,
        path: Path | str,
        contents: str | None = None,
        filename: str | None = None,
        clean_up: bool = False,
        save: bool = True,
    ):
        """
        Set details of a file, including whether it is associated with a parameter.
        """
        self.logger.debug("Setting new file")
        file_param_dat = self._prepare_set_file(
            store_contents=store_contents,
            is_input=is_input,
            path=path,
            contents=contents,
            filename=filename,
            clean_up=clean_up,
        )
        if param_id is not None:
            self.set_parameter_value(
                param_id, value=file_param_dat, is_file=True, save=save
            )
        if save:
            self.save()

    def add_file(
        self,
        store_contents: bool,
        is_input: bool,
        source: ParamSource,
        path: Path | str,
        contents: str | None = None,
        filename: str | None = None,
        save: bool = True,
    ):
        """
        Add a file that will be associated with a parameter.
        """
        self.logger.debug("Adding new file")
        file_param_dat = self._prepare_set_file(
            store_contents=store_contents,
            is_input=is_input,
            path=path,
            contents=contents,
            filename=filename,
        )
        p_id = self._add_parameter(
            file=file_param_dat,
            is_set=True,
            source=source,
            save=save,
        )
        if save:
            self.save()
        return p_id

    def _append_files(self, files: list[FileDescriptor]):
        """Add new files to the files or artifacts directories."""
        for dat in files:
            if dat["store_contents"]:
                dst_path = Path(dat["dst_path"])
                dst_path.parent.mkdir(parents=True, exist_ok=True)
                if dat["path"] is not None:
                    # copy from source path to destination:
                    shutil.copy(dat["path"], dst_path)
                    if dat["clean_up"]:
                        self.logger.info(f"deleting file {dat['path']}")
                        os.remove(dat["path"])
                else:
                    # write out text file:
                    with dst_path.open("wt") as fp:
                        fp.write(dat["contents"])

    @writes_parameter_data
    def add_set_parameter(
        self,
        data: ParameterValue | list | tuple | set | dict | int | float | str | Any,
        source: ParamSource,
        save: bool = True,
    ) -> int:
        """
        Add a parameter that is set to a value.
        """
        return self._add_parameter(data=data, is_set=True, source=source, save=save)

    @writes_parameter_data
    def add_unset_parameter(self, source: ParamSource, save: bool = True) -> int:
        """
        Add a parameter that is not set to any value.
        """
        return self._add_parameter(data=None, is_set=False, source=source, save=save)

    @abstractmethod
    def _set_parameter_values(self, set_parameters: dict[int, tuple[Any, bool]]): ...

    @writes_parameter_data
    def set_parameter_value(
        self, param_id: int, value: Any, is_file: bool = False, save: bool = True
    ):
        """
        Set the value of a parameter.
        """
        self.logger.debug(
            f"Setting store parameter ID {param_id} value with type: {type(value)!r})."
        )
        self._pending.set_parameters[param_id] = (value, is_file)
        if save:
            self.save()

    @writes_parameter_data
    def set_parameter_values(self, values: dict[int, Any], save: bool = True):
        """Set multiple non-file parameter values by parameter IDs."""
        param_ids = values.keys()
        self.logger.debug(f"Setting multiple store parameter IDs {param_ids!r}.")
        self._pending.set_parameters.update({k: (v, False) for k, v in values.items()})
        if save:
            self.save()

    @TimeIt.decorator
    @writes_parameter_data
    def update_param_source(
        self, param_sources: Mapping[int, ParamSource], save: bool = True
    ) -> None:
        """
        Set the source of a parameter.
        """
        self.logger.debug(f"Updating parameter sources with {param_sources!r}.")
        self._pending.update_param_sources.update(param_sources)
        if save:
            self.save()

    def update_loop_num_iters(
        self,
        index: int,
        num_added_iters: Mapping[tuple[int, ...], int],
        save: bool = True,
    ) -> None:
        """
        Add iterations to a loop.
        """
        self.logger.debug(
            f"Updating loop {index!r} num added iterations to {num_added_iters!r}."
        )
        self._pending.update_loop_num_iters[index] = [
            [list(k), v] for k, v in num_added_iters.items()
        ]
        if save:
            self.save()

    def update_loop_parents(
        self,
        index: int,
        num_added_iters: Mapping[tuple[int, ...], int],
        parents: Sequence[str],
        save: bool = True,
    ) -> None:
        """
        Set the parents of a loop.
        """
        self.logger.debug(
            f"Updating loop {index!r} parents to {parents!r}, and num added iterations "
            f"to {num_added_iters}."
        )
        self._pending.update_loop_num_iters[index] = [
            [list(k), v] for k, v in num_added_iters.items()
        ]
        self._pending.update_loop_parents[index] = list(parents)
        if save:
            self.save()

    def update_iter_data_indices(self, data_indices: dict[int, DataIndex]):
        """Update data indices of one or more iterations."""
        for k, v in data_indices.items():
            self._pending.update_iter_data_idx[k].update(v)

    def update_run_data_indices(self, data_indices: dict[int, DataIndex]):
        """Update data indices of one or more runs."""
        for k, v in data_indices.items():
            self._pending.update_run_data_idx[k].update(v)

    def get_template_components(self) -> dict[str, Any]:
        """Get all template components, including pending."""
        tc = copy.deepcopy(self._get_persistent_template_components())
        for typ in TEMPLATE_COMP_TYPES:
            for hash_i, dat_i in self._pending.add_template_components.get(
                typ, {}
            ).items():
                tc.setdefault(typ, {})[hash_i] = dat_i

        return tc

    @abstractmethod
    def _get_persistent_template_components(self) -> dict[str, Any]: ...

    def get_template(self) -> dict[str, JSONed]:
        """
        Get the workflow template.
        """
        return self._get_persistent_template()

    @abstractmethod
    def _get_persistent_template(self) -> dict[str, JSONed]: ...

    def _get_task_id_to_idx_map(self) -> dict[int, int]:
        return {task.id_: task.index for task in self.get_tasks()}

    @TimeIt.decorator
    def get_task(self, task_idx: int) -> AnySTask:
        """
        Get a task.
        """
        return self.get_tasks()[task_idx]

    def __process_retrieved_tasks(self, tasks: Iterable[AnySTask]) -> list[AnySTask]:
        """Add pending data to retrieved tasks."""
        tasks_new: list[AnySTask] = []
        for task in tasks:
            # consider pending element IDs:
            if pend_elems := self._pending.add_elem_IDs.get(task.id_):
                task = task.append_element_IDs(pend_elems)
            tasks_new.append(task)
        return tasks_new

    def __process_retrieved_loops(
        self, loops: Iterable[tuple[int, LoopDescriptor]]
    ) -> dict[int, LoopDescriptor]:
        """Add pending data to retrieved loops."""
        loops_new: dict[int, LoopDescriptor] = {}
        for id_, loop_i in loops:
            if "num_added_iterations" not in loop_i:
                loop_i["num_added_iterations"] = 1
            # consider pending changes to num added iterations:
            if pend_num_iters := self._pending.update_loop_num_iters.get(id_):
                loop_i["num_added_iterations"] = pend_num_iters
            # consider pending change to parents:
            if pend_parents := self._pending.update_loop_parents.get(id_):
                loop_i["parents"] = pend_parents

            loops_new[id_] = loop_i
        return loops_new

    @staticmethod
    def __split_pending(
        ids: Iterable[int], all_pending: Mapping[int, Any]
    ) -> tuple[tuple[int, ...], set[int], set[int]]:
        id_all = tuple(ids)
        id_set = set(id_all)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)
        return id_all, id_pers, id_pend

    @abstractmethod
    def _get_persistent_tasks(self, id_lst: Iterable[int]) -> dict[int, AnySTask]: ...

    def get_tasks_by_IDs(self, ids: Iterable[int]) -> Sequence[AnySTask]:
        """
        Get tasks with the given IDs.
        """
        # separate pending and persistent IDs:

        ids, id_pers, id_pend = self.__split_pending(ids, self._pending.add_tasks)
        tasks = self._get_persistent_tasks(id_pers) if id_pers else {}
        tasks.update((id_, self._pending.add_tasks[id_]) for id_ in id_pend)

        # order as requested:
        return self.__process_retrieved_tasks(tasks[id_] for id_ in ids)

    @TimeIt.decorator
    def get_tasks(self) -> list[AnySTask]:
        """Retrieve all tasks, including pending."""
        tasks = self._get_persistent_tasks(range(self._get_num_persistent_tasks()))
        tasks.update(self._pending.add_tasks)

        # order by index:
        return self.__process_retrieved_tasks(
            sorted(tasks.values(), key=lambda x: x.index)
        )

    @abstractmethod
    def _get_persistent_loops(
        self, id_lst: Iterable[int] | None = None
    ) -> dict[int, LoopDescriptor]: ...

    def get_loops_by_IDs(self, ids: Iterable[int]) -> dict[int, LoopDescriptor]:
        """Retrieve loops by index (ID), including pending."""

        # separate pending and persistent IDs:
        ids, id_pers, id_pend = self.__split_pending(ids, self._pending.add_loops)

        loops = self._get_persistent_loops(id_pers) if id_pers else {}
        loops.update((id_, self._pending.add_loops[id_]) for id_ in id_pend)

        # order as requested:
        return self.__process_retrieved_loops((id_, loops[id_]) for id_ in ids)

    def get_loops(self) -> dict[int, LoopDescriptor]:
        """Retrieve all loops, including pending."""

        loops = self._get_persistent_loops()
        loops.update(self._pending.add_loops)

        # order by index/ID:
        return self.__process_retrieved_loops(sorted(loops.items()))

    @abstractmethod
    def _get_persistent_submissions(
        self, id_lst: Iterable[int] | None = None
    ) -> dict[int, Mapping[str, JSONed]]: ...

    @TimeIt.decorator
    def get_submissions(self) -> dict[int, Mapping[str, JSONed]]:
        """Retrieve all submissions, including pending."""

        subs = self._get_persistent_submissions()
        subs.update(self._pending.add_submissions)

        # order by index/ID
        return dict(sorted(subs.items()))

    @TimeIt.decorator
    def get_submission_at_submit_metadata(
        self, sub_idx: int, metadata_attr: dict[str, Any] | None
    ) -> dict[str, Any]:
        """Retrieve the values of submission attributes that are stored at submit-time.

        Notes
        -----
        This method may need to be overridden if these attributes are stored separately
        from the remainder of the submission attributes.

        """
        return metadata_attr or {i: None for i in SUBMISSION_SUBMIT_TIME_KEYS}

    @TimeIt.decorator
    def get_jobscript_at_submit_metadata(
        self,
        sub_idx: int,
        js_idx: int,
        metadata_attr: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """For the specified jobscript, retrieve the values of jobscript-submit-time
        attributes.

        Notes
        -----
        This method may need to be overridden if these jobscript-submit-time attributes
        are stored separately from the remainder of the jobscript attributes.

        """
        return metadata_attr or {i: None for i in JOBSCRIPT_SUBMIT_TIME_KEYS}

    @TimeIt.decorator
    def get_jobscript_block_run_ID_array(
        self, sub_idx: int, js_idx: int, blk_idx: int, run_ID_arr: NDArray | None
    ) -> NDArray:
        """For the specified jobscript-block, retrieve the run ID array.

        Notes
        -----
        This method may need to be overridden if these attributes are stored separately
        from the remainder of the submission attributes.

        """
        assert run_ID_arr is not None
        return np.asarray(run_ID_arr)

    @TimeIt.decorator
    def get_jobscript_block_task_elements_map(
        self,
        sub_idx: int,
        js_idx: int,
        blk_idx: int,
        task_elems_map: dict[int, list[int]] | None,
    ) -> dict[int, list[int]]:
        """For the specified jobscript-block, retrieve the task-elements mapping.

        Notes
        -----
        This method may need to be overridden if these attributes are stored separately
        from the remainder of the submission attributes.

        """
        assert task_elems_map is not None
        return task_elems_map

    @TimeIt.decorator
    def get_jobscript_block_task_actions_array(
        self,
        sub_idx: int,
        js_idx: int,
        blk_idx: int,
        task_actions_arr: NDArray | list[tuple[int, int, int]] | None,
    ) -> NDArray:
        """For the specified jobscript-block, retrieve the task-actions array.

        Notes
        -----
        This method may need to be overridden if these attributes are stored separately
        from the remainder of the submission attributes.

        """
        assert task_actions_arr is not None
        return np.asarray(task_actions_arr)

    @TimeIt.decorator
    def get_jobscript_block_dependencies(
        self,
        sub_idx: int,
        js_idx: int,
        blk_idx: int,
        js_dependencies: dict[tuple[int, int], ResolvedJobscriptBlockDependencies] | None,
    ) -> dict[tuple[int, int], ResolvedJobscriptBlockDependencies]:
        """For the specified jobscript-block, retrieve the dependencies.

        Notes
        -----
        This method may need to be overridden if these attributes are stored separately
        from the remainder of the submission attributes.

        """
        assert js_dependencies is not None
        return js_dependencies

    @TimeIt.decorator
    def get_submissions_by_ID(
        self, ids: Iterable[int]
    ) -> dict[int, Mapping[str, JSONed]]:
        """
        Get submissions with the given IDs.
        """
        # separate pending and persistent IDs:
        _, id_pers, id_pend = self.__split_pending(ids, self._pending.add_submissions)
        subs = self._get_persistent_submissions(id_pers) if id_pers else {}
        subs.update((id_, self._pending.add_submissions[id_]) for id_ in id_pend)

        # order by index/ID
        return dict(sorted(subs.items()))

    @abstractmethod
    def _get_persistent_elements(
        self, id_lst: Iterable[int]
    ) -> dict[int, AnySElement]: ...

    @TimeIt.decorator
    def get_elements(self, ids: Iterable[int]) -> Sequence[AnySElement]:
        """
        Get elements with the given IDs.
        """
        # separate pending and persistent IDs:
        ids, id_pers, id_pend = self.__split_pending(ids, self._pending.add_elements)
        self.logger.debug(
            f"PersistentStore.get_elements: {len(ids)} elements: "
            f"{shorten_list_str(ids)}."
        )
        elems = self._get_persistent_elements(id_pers) if id_pers else {}
        elems.update((id_, self._pending.add_elements[id_]) for id_ in id_pend)

        elems_new: list[AnySElement] = []
        # order as requested:
        for elem_i in (elems[id_] for id_ in ids):
            # consider pending iteration IDs:
            # TODO: does this consider pending iterations from new loop iterations?
            if pend_iters := self._pending.add_elem_iter_IDs.get(elem_i.id_):
                elem_i = elem_i.append_iteration_IDs(pend_iters)
            elems_new.append(elem_i)

        return elems_new

    @abstractmethod
    def _get_persistent_element_iters(
        self, id_lst: Iterable[int]
    ) -> dict[int, AnySElementIter]: ...

    @TimeIt.decorator
    def get_element_iterations(self, ids: Iterable[int]) -> Sequence[AnySElementIter]:
        """
        Get element iterations with the given IDs.
        """
        # separate pending and persistent IDs:
        ids, id_pers, id_pend = self.__split_pending(ids, self._pending.add_elem_iters)
        self.logger.debug(
            f"PersistentStore.get_element_iterations: {len(ids)} iterations: "
            f"{shorten_list_str(ids)}."
        )
        iters = self._get_persistent_element_iters(id_pers) if id_pers else {}
        iters.update((id_, self._pending.add_elem_iters[id_]) for id_ in id_pend)

        iters_new: list[AnySElementIter] = []
        # order as requested:
        for iter_i in (iters[id_] for id_ in ids):
            # consider pending EAR IDs:
            if pend_EARs := self._pending.add_elem_iter_EAR_IDs.get(iter_i.id_):
                iter_i = iter_i.append_EAR_IDs(pend_EARs)

            # consider pending loop idx
            if pend_loop_idx := self._pending.update_loop_indices.get(iter_i.id_):
                iter_i = iter_i.update_loop_idx(pend_loop_idx)

            # consider pending `EARs_initialised`:
            if iter_i.id_ in self._pending.set_EARs_initialised:
                iter_i = iter_i.set_EARs_initialised()

            iters_new.append(iter_i)

        return iters_new

    @abstractmethod
    def _get_persistent_EARs(self, id_lst: Iterable[int]) -> dict[int, AnySEAR]: ...

    @TimeIt.decorator
    def get_EARs(self, ids: Iterable[int]) -> Sequence[AnySEAR]:
        """
        Get element action runs with the given IDs.
        """
        # separate pending and persistent IDs:
        ids, id_pers, id_pend = self.__split_pending(ids, self._pending.add_EARs)
        self.logger.debug(
            f"PersistentStore.get_EARs: {len(ids)} EARs: {shorten_list_str(ids)}."
        )
        EARs = self._get_persistent_EARs(id_pers) if id_pers else {}
        EARs.update((id_, self._pending.add_EARs[id_]) for id_ in id_pend)

        EARs_new: list[AnySEAR] = []
        # order as requested:
        for EAR_i in (EARs[id_] for id_ in ids):
            # consider updates:
            updates: dict[str, Any] = {}
            if EAR_i.id_ in self._pending.set_EAR_skips:
                updates["skip"] = True
            (
                updates["submission_idx"],
                updates["commands_file_ID"],
            ) = self._pending.set_EAR_submission_data.get(EAR_i.id_, (None, None))
            (
                updates["start_time"],
                updates["snapshot_start"],
                updates["run_hostname"],
                updates["port_number"],
            ) = self._pending.set_EAR_starts.get(EAR_i.id_, (None, None, None, None))
            (
                updates["end_time"],
                updates["snapshot_end"],
                updates["exit_code"],
                updates["success"],
            ) = self._pending.set_EAR_ends.get(EAR_i.id_, (None, None, None, None))
            if any(i is not None for i in updates.values()):
                EAR_i = EAR_i.update(**updates)

            EARs_new.append(EAR_i)

        return EARs_new

    @TimeIt.decorator
    def __get_cached_persistent_items(
        self, id_lst: Iterable[int], cache: dict[int, T]
    ) -> tuple[dict[int, T], list[int]]:
        """How to get things out of the cache. Caller says which cache."""
        if self.use_cache:
            id_cached = set(id_lst)
            id_non_cached = sorted(id_cached.difference(cache))
            id_cached.intersection_update(cache)
            items = {id_: cache[id_] for id_ in sorted(id_cached)}
        else:
            items = {}
            id_non_cached = list(id_lst)
        return items, id_non_cached

    def _get_cached_persistent_EARs(
        self, id_lst: Iterable[int]
    ) -> tuple[dict[int, AnySEAR], list[int]]:
        return self.__get_cached_persistent_items(id_lst, self.EAR_cache)

    def _get_cached_persistent_element_iters(
        self, id_lst: Iterable[int]
    ) -> tuple[dict[int, AnySElementIter], list[int]]:
        return self.__get_cached_persistent_items(id_lst, self.element_iter_cache)

    def _get_cached_persistent_elements(
        self, id_lst: Iterable[int]
    ) -> tuple[dict[int, AnySElement], list[int]]:
        return self.__get_cached_persistent_items(id_lst, self.element_cache)

    def _get_cached_persistent_tasks(
        self, id_lst: Iterable[int]
    ) -> tuple[dict[int, AnySTask], list[int]]:
        return self.__get_cached_persistent_items(id_lst, self.task_cache)

    def _get_cached_persistent_param_sources(
        self, id_lst: Iterable[int]
    ) -> tuple[dict[int, ParamSource], list[int]]:
        return self.__get_cached_persistent_items(id_lst, self.param_sources_cache)

    def _get_cached_persistent_parameters(
        self, id_lst: Iterable[int]
    ) -> tuple[dict[int, AnySParameter], list[int]]:
        return self.__get_cached_persistent_items(id_lst, self.parameter_cache)

    def get_EAR_skipped(self, EAR_ID: int) -> int:
        """
        Whether the element action run with the given ID was skipped.
        """
        self.logger.debug(f"PersistentStore.get_EAR_skipped: EAR_ID={EAR_ID!r}")
        return self.get_EARs((EAR_ID,))[0].skip

    @TimeIt.decorator
    def get_parameters(self, ids: Iterable[int], **kwargs) -> list[AnySParameter]:
        """
        Get parameters with the given IDs.

        Parameters
        ----------
        ids:
            The IDs of the parameters to get.

        Keyword Arguments
        -----------------
        dataset_copy: bool
            For Zarr stores only. If True, copy arrays as NumPy arrays.
        """
        # separate pending and persistent IDs:
        ids, id_pers, id_pend = self.__split_pending(ids, self._pending.add_parameters)
        params = (
            dict(self._get_persistent_parameters(id_pers, **kwargs)) if id_pers else {}
        )
        params.update((id_, self._pending.add_parameters[id_]) for id_ in id_pend)

        # order as requested:
        return [params[id_] for id_ in ids]

    @abstractmethod
    def _get_persistent_parameters(
        self, id_lst: Iterable[int], **kwargs
    ) -> Mapping[int, AnySParameter]: ...

    @TimeIt.decorator
    def get_parameter_set_statuses(self, ids: Iterable[int]) -> list[bool]:
        """
        Get whether the parameters with the given IDs are set.
        """
        # separate pending and persistent IDs:
        ids, id_pers, id_pend = self.__split_pending(ids, self._pending.add_parameters)
        set_status = self._get_persistent_parameter_set_status(id_pers) if id_pers else {}
        set_status.update(
            (id_, self._pending.add_parameters[id_].is_set) for id_ in id_pend
        )

        # order as requested:
        return [set_status[id_] for id_ in ids]

    @abstractmethod
    def _get_persistent_parameter_set_status(
        self, id_lst: Iterable[int]
    ) -> dict[int, bool]: ...

    @TimeIt.decorator
    def get_parameter_sources(self, ids: Iterable[int]) -> list[ParamSource]:
        """
        Get the sources of the parameters with the given IDs.
        """
        # separate pending and persistent IDs:
        ids, id_pers, id_pend = self.__split_pending(ids, self._pending.add_parameters)
        src = self._get_persistent_param_sources(id_pers) if id_pers else {}
        src.update((id_, self._pending.add_parameters[id_].source) for id_ in id_pend)

        # order as requested, and consider pending source updates:
        return [
            self.__merge_param_source(
                src[id_i], self._pending.update_param_sources.get(id_i)
            )
            for id_i in ids
        ]

    @staticmethod
    def __merge_param_source(
        src_i: ParamSource, pend_src: ParamSource | None
    ) -> ParamSource:
        """
        Helper to merge a second dict in if it is provided.
        """
        return {**src_i, **pend_src} if pend_src else src_i

    @abstractmethod
    def _get_persistent_param_sources(
        self, id_lst: Iterable[int]
    ) -> dict[int, ParamSource]: ...

    @TimeIt.decorator
    def get_task_elements(
        self,
        task_id: int,
        idx_lst: Iterable[int] | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """
        Get element data by an indices within a given task.

        Element iterations and EARs belonging to the elements are included.
        """

        all_elem_IDs = self.get_task(task_id).element_IDs
        store_elements = self.get_elements(
            all_elem_IDs if idx_lst is None else (all_elem_IDs[idx] for idx in idx_lst)
        )
        iter_IDs_flat, iter_IDs_lens = flatten(
            [el.iteration_IDs for el in store_elements]
        )
        store_iters = self.get_element_iterations(iter_IDs_flat)

        # retrieve EARs:
        EARs_dcts = remap(
            [list((elit.EAR_IDs or {}).values()) for elit in store_iters],
            lambda ears: [ear.to_dict() for ear in self.get_EARs(ears)],
        )

        # add EARs to iterations:
        iters: list[dict[str, Any]] = []
        for idx, i in enumerate(store_iters):
            EARs: dict[int, dict[str, Any]] | None = None
            if i.EAR_IDs is not None:
                EARs = dict(zip(i.EAR_IDs, cast("Any", EARs_dcts[idx])))
            iters.append(i.to_dict(EARs))

        # reshape iterations:
        iters_rs = reshape(iters, iter_IDs_lens)

        # add iterations to elements:
        for idx, element in enumerate(store_elements):
            yield element.to_dict(iters_rs[idx])

    @abstractmethod
    def _get_persistent_parameter_IDs(self) -> Iterable[int]: ...

    def check_parameters_exist(self, ids: Sequence[int]) -> Iterator[bool]:
        """
        For each parameter ID, return True if it exists, else False.
        """
        id_miss = set()
        if id_not_pend := set(ids).difference(self._pending.add_parameters):
            id_miss = id_not_pend.difference(self._get_persistent_parameter_IDs())
        return (id_ not in id_miss for id_ in ids)

    @abstractmethod
    def _append_tasks(self, tasks: Iterable[AnySTask]) -> None: ...

    @abstractmethod
    def _append_loops(self, loops: dict[int, LoopDescriptor]) -> None: ...

    @abstractmethod
    def _append_submissions(self, subs: dict[int, Mapping[str, JSONed]]) -> None: ...

    @abstractmethod
    def _update_at_submit_metadata(
        self, at_submit_metadata: dict[int, dict[str, Any]]
    ) -> None: ...

    @abstractmethod
    def _append_elements(self, elems: Sequence[AnySElement]) -> None: ...

    @abstractmethod
    def _append_element_sets(self, task_id: int, es_js: Sequence[Mapping]) -> None: ...

    @abstractmethod
    def _append_elem_iter_IDs(self, elem_ID: int, iter_IDs: Iterable[int]) -> None: ...

    @abstractmethod
    def _append_elem_iters(self, iters: Sequence[AnySElementIter]) -> None: ...

    @abstractmethod
    def _append_elem_iter_EAR_IDs(
        self, iter_ID: int, act_idx: int, EAR_IDs: Sequence[int]
    ) -> None: ...

    @abstractmethod
    def _append_EARs(self, EARs: Sequence[AnySEAR]) -> None: ...

    @abstractmethod
    def _update_elem_iter_EARs_initialised(self, iter_ID: int) -> None: ...

    @abstractmethod
    def _update_EAR_submission_data(
        self, sub_data: Mapping[int, tuple[int, int | None]]
    ): ...

    @abstractmethod
    def _update_EAR_start(
        self,
        run_starts: dict[int, tuple[datetime, dict[str, Any] | None, str, int | None]],
    ) -> None: ...

    @abstractmethod
    def _update_EAR_end(
        self, run_ends: dict[int, tuple[datetime, dict[str, Any] | None, int, bool]]
    ) -> None: ...

    @abstractmethod
    def _update_EAR_skip(self, skips: dict[int, int]) -> None: ...

    @abstractmethod
    def _update_js_metadata(
        self, js_meta: dict[int, dict[int, dict[str, Any]]]
    ) -> None: ...

    @abstractmethod
    def _append_parameters(self, params: Sequence[AnySParameter]) -> None: ...

    @abstractmethod
    def _update_template_components(self, tc: dict[str, Any]) -> None: ...

    @abstractmethod
    def _update_parameter_sources(self, sources: Mapping[int, ParamSource]) -> None: ...

    @abstractmethod
    def _update_loop_index(self, loop_indices: dict[int, dict[str, int]]) -> None: ...

    @abstractmethod
    def _update_loop_num_iters(
        self, index: int, num_iters: list[list[list[int] | int]]
    ) -> None: ...

    @abstractmethod
    def _update_loop_parents(self, index: int, parents: list[str]) -> None: ...

    @overload
    def using_resource(
        self, res_label: Literal["metadata"], action: str
    ) -> AbstractContextManager[Metadata]: ...

    @overload
    def using_resource(
        self, res_label: Literal["submissions"], action: str
    ) -> AbstractContextManager[list[dict[str, JSONed]]]: ...

    @overload
    def using_resource(
        self, res_label: Literal["parameters"], action: str
    ) -> AbstractContextManager[dict[str, dict[str, Any]]]: ...

    @overload
    def using_resource(
        self, res_label: Literal["runs"], action: str
    ) -> AbstractContextManager[dict[str, Any]]: ...

    @overload
    def using_resource(
        self, res_label: Literal["attrs"], action: str
    ) -> AbstractContextManager[ZarrAttrsDict]: ...

    @contextlib.contextmanager
    def using_resource(
        self,
        res_label: Literal["metadata", "submissions", "parameters", "attrs", "runs"],
        action: str,
    ) -> Iterator[Any]:
        """Context manager for managing `StoreResource` objects associated with the store."""

        try:
            res = self._resources[res_label]
        except KeyError:
            raise RuntimeError(
                f"{self.__class__.__name__!r} has no resource named {res_label!r}."
            ) from None

        key = (res_label, action)
        if key in self._resources_in_use:
            # retrieve existing data for this action:
            yield res.data[action]

        else:
            try:
                # "open" the resource, which assigns data for this action, which we yield:
                res.open(action)
                self._resources_in_use.add(key)
                yield res.data[action]

            except Exception as exc:
                self._resources_in_use.remove(key)
                raise exc

            else:
                # "close" the resource, clearing cached data for this action:
                res.close(action)
                self._resources_in_use.remove(key)

    def copy(self, path: PathLike = None) -> Path:
        """Copy the workflow store.

        This does not work on remote filesystems.

        """
        assert self.fs is not None
        if path is None:
            _path = Path(self.path)
            path = _path.parent / Path(_path.stem + "_copy" + _path.suffix)

        if self.fs.exists(str(path)):
            raise ValueError(f"Path already exists: {path}.")
        else:
            path = str(path)

        self.fs.copy(self.path, path)

        return Path(self.workflow._store.path).replace(path)

    def delete(self) -> None:
        """Delete the persistent workflow."""
        confirm = input(
            f"Permanently delete the workflow at path {self.path!r}; [y]es or [n]o?"
        )
        if confirm.strip().lower() == "y":
            self.delete_no_confirm()

    def delete_no_confirm(self) -> None:
        """Permanently delete the workflow data with no confirmation."""

        fs = self.fs
        assert fs is not None

        @self._app.perm_error_retry()
        def _delete_no_confirm() -> None:
            self.logger.debug(f"_delete_no_confirm: {self.path!r}.")
            fs.rm(self.path, recursive=True)

        return _delete_no_confirm()

    def get_text_file(self, path: str | Path) -> str:
        """Retrieve the contents of a text file stored within the workflow.

        Parameters
        ----------
        path
            The path to a text file stored within the workflow. This can either be an
            absolute path or a path that is relative to the workflow root.
        """
        path = Path(path)
        if not path.is_absolute():
            path = Path(self.path).joinpath(path)
        if not path.is_file():
            raise FileNotFoundError(f"File at location {path!r} does not exist.")
        return path.read_text()

    @abstractmethod
    def _append_task_element_IDs(self, task_ID: int, elem_IDs: list[int]):
        raise NotImplementedError

    @abstractmethod
    def _set_run_dirs(self, run_dir_arr: np.ndarray, run_idx: np.ndarray) -> None: ...

    @abstractmethod
    def _update_iter_data_indices(
        self, iter_data_indices: dict[int, DataIndex]
    ) -> None: ...

    @abstractmethod
    def _update_run_data_indices(
        self, run_data_indices: dict[int, DataIndex]
    ) -> None: ...
