# Store* classes represent the element-metadata in the store, in a store-agnostic way
from __future__ import annotations
from abc import ABC

import contextlib
import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
import enum
from functools import wraps
import os
from pathlib import Path
import shutil
import socket
import time
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, TypeVar, Union

import numpy as np

from hpcflow.sdk.core.utils import (
    flatten,
    get_in_container,
    get_relative_path,
    reshape,
    set_in_container,
)
from hpcflow.sdk.core.errors import ParametersMetadataReadOnlyError
from hpcflow.sdk.submission.submission import JOBSCRIPT_SUBMIT_TIME_KEYS
from hpcflow.sdk.utils.strings import shorten_list_str
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.persistence.pending import PendingChanges

AnySTask = TypeVar("AnySTask", bound="StoreTask")
AnySElement = TypeVar("AnySElement", bound="StoreElement")
AnySElementIter = TypeVar("AnySElementIter", bound="StoreElementIter")
AnySEAR = TypeVar("AnySEAR", bound="StoreEAR")
AnySParameter = TypeVar("AnySParameter", bound="StoreParameter")

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

PARAM_DATA_NOT_SET = 0


def update_param_source_dict(source, update):
    return dict(sorted({**source, **update}.items()))


def writes_parameter_data(func):
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
    """Class to represent the features provided by a persistent store.

    Parameters
    ----------
    create
        If True, a new workflow can be created using this store.
    edit
        If True, the workflow can be modified.
    jobscript_parallelism
        If True, the store supports workflows running multiple independent jobscripts
        simultaneously.
    EAR_parallelism
        If True, the store supports workflows running multiple EARs simultaneously.
    schedulers
        If True, the store supports submitting workflows to a scheduler
    submission
        If True, the store supports submission. If False, the store can be considered to
        be an archive, which would need transforming to another store type before
        submission.
    """

    create: bool = False
    edit: bool = False
    jobscript_parallelism: bool = False
    EAR_parallelism: bool = False
    schedulers: bool = False
    submission: bool = False


@dataclass
class StoreTask:
    id_: int
    index: int
    is_pending: bool
    element_IDs: List[int]
    task_template: Optional[Dict] = None

    def encode(self) -> Tuple[int, Dict, Dict]:
        """Prepare store task data for the persistent store."""
        wk_task = {"id_": self.id_, "element_IDs": self.element_IDs}
        task = {"id_": self.id_, **self.task_template}
        return self.index, wk_task, task

    @classmethod
    def decode(cls, task_dat: Dict) -> StoreTask:
        """Initialise a `StoreTask` from store task data

        Note: the `task_template` is only needed for encoding because it is retrieved as
        part of the `WorkflowTemplate` so we don't need to load it when decoding.

        """
        return cls(is_pending=False, **task_dat)

    @TimeIt.decorator
    def append_element_IDs(self: AnySTask, pend_IDs: List[int]) -> AnySTask:
        """Return a copy, with additional element IDs."""
        elem_IDs = self.element_IDs[:] + pend_IDs
        return self.__class__(
            id_=self.id_,
            index=self.index,
            is_pending=self.is_pending,
            element_IDs=elem_IDs,
            task_template=self.task_template,
        )


@dataclass
class StoreElement:
    """
    Parameters
    ----------
    index
        Index of the element within its parent task.
    iteration_IDs
        IDs of element-iterations that belong to this element.
    """

    id_: int
    is_pending: bool
    index: int
    es_idx: int
    seq_idx: Dict[str, int]
    src_idx: Dict[str, int]
    task_ID: int
    iteration_IDs: List[int]

    def encode(self) -> Dict:
        """Prepare store element data for the persistent store."""
        dct = self.__dict__
        del dct["is_pending"]
        return dct

    @classmethod
    def decode(cls, elem_dat: Dict) -> StoreElement:
        """Initialise a `StoreElement` from store element data"""
        return cls(is_pending=False, **elem_dat)

    def to_dict(self, iters):
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
    def append_iteration_IDs(self: AnySElement, pend_IDs: List[int]) -> AnySElement:
        """Return a copy, with additional iteration IDs."""
        iter_IDs = self.iteration_IDs[:] + pend_IDs
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
class StoreElementIter:
    """
    Parameters
    ----------
    data_idx
        Overall data index for the element-iteration, which maps parameter names to
        parameter data indices.
    EAR_IDs
        Maps task schema action indices to EARs by ID.
    schema_parameters
        List of parameters defined by the associated task schema.
    """

    id_: int
    is_pending: bool
    element_ID: int
    EARs_initialised: bool
    EAR_IDs: Dict[int, List[int]]
    data_idx: Dict[str, int]
    schema_parameters: List[str]
    loop_idx: Dict[str, int] = field(default_factory=dict)

    def encode(self) -> Dict:
        """Prepare store element iteration data for the persistent store."""
        dct = self.__dict__
        del dct["is_pending"]
        return dct

    @classmethod
    def decode(cls, iter_dat: Dict) -> StoreElementIter:
        """Initialise a `StoreElementIter` from persistent store element iteration data"""

        iter_dat = copy.deepcopy(iter_dat)  # to avoid mutating; can we avoid this?

        # cast JSON string keys to integers:
        for act_idx in list((iter_dat["EAR_IDs"] or {}).keys()):
            iter_dat["EAR_IDs"][int(act_idx)] = iter_dat["EAR_IDs"].pop(act_idx)

        return cls(is_pending=False, **iter_dat)

    def to_dict(self, EARs):
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
            "loop_idx": self.loop_idx,
        }

    @TimeIt.decorator
    def append_EAR_IDs(
        self: AnySElementIter, pend_IDs: Dict[int, List[int]]
    ) -> AnySElementIter:
        """Return a copy, with additional EAR IDs."""

        EAR_IDs = copy.deepcopy(self.EAR_IDs) or {}
        for act_idx, IDs_i in pend_IDs.items():
            if act_idx not in EAR_IDs:
                EAR_IDs[act_idx] = []
            EAR_IDs[act_idx].extend(IDs_i)

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
    def update_loop_idx(
        self: AnySElementIter, loop_idx: Dict[str, int]
    ) -> AnySElementIter:
        """Return a copy, with the loop index updated."""
        loop_idx_new = copy.deepcopy(self.loop_idx)
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
    def set_EARs_initialised(self: AnySElementIter) -> AnySElementIter:
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
    def update_data_idx(
        self: AnySElementIter, data_idx: Dict[str, int]
    ) -> AnySElementIter:
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
class StoreEAR:
    """
    Parameters
    ----------
    data_idx
        Maps parameter names within this EAR to parameter data indices.
    metadata
        Metadata concerning e.g. the state of the EAR.
    action_idx
        The task schema action associated with this EAR.
    """

    id_: int
    is_pending: bool
    elem_iter_ID: int
    action_idx: int
    commands_idx: List[int]
    data_idx: Dict[str, int]
    submission_idx: Optional[int] = None
    commands_file_ID: Optional[int] = None
    skip: Optional[int] = 0
    success: Optional[bool] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    snapshot_start: Optional[Dict] = None
    snapshot_end: Optional[Dict] = None
    exit_code: Optional[int] = None
    metadata: Dict[str, Any] = None
    run_hostname: Optional[str] = None
    port_number: Optional[int] = None

    @staticmethod
    def _encode_datetime(dt: Union[datetime, None], ts_fmt: str) -> str:
        return dt.strftime(ts_fmt) if dt else None

    @staticmethod
    def _decode_datetime(dt_str: Union[str, None], ts_fmt: str) -> datetime:
        return datetime.strptime(dt_str, ts_fmt) if dt_str else None

    def encode(self, ts_fmt: str) -> Dict:
        """Prepare store EAR data for the persistent store."""
        return {
            "id_": self.id_,
            "elem_iter_ID": self.elem_iter_ID,
            "action_idx": self.action_idx,
            "commands_idx": self.commands_idx,
            "data_idx": self.data_idx,
            "submission_idx": self.submission_idx,
            "commands_file_ID": self.commands_file_ID,
            "success": self.success,
            "skip": self.skip,
            "start_time": self._encode_datetime(self.start_time, ts_fmt),
            "end_time": self._encode_datetime(self.end_time, ts_fmt),
            "snapshot_start": self.snapshot_start,
            "snapshot_end": self.snapshot_end,
            "exit_code": self.exit_code,
            "metadata": self.metadata,
            "run_hostname": self.run_hostname,
            "port_number": self.port_number,
        }

    @classmethod
    def decode(cls, EAR_dat: Dict, ts_fmt: str) -> StoreEAR:
        """Initialise a `StoreEAR` from persistent store EAR data"""
        # don't want to mutate EAR_dat:
        EAR_dat = copy.deepcopy(EAR_dat)
        EAR_dat["start_time"] = cls._decode_datetime(EAR_dat["start_time"], ts_fmt)
        EAR_dat["end_time"] = cls._decode_datetime(EAR_dat["end_time"], ts_fmt)
        return cls(is_pending=False, **EAR_dat)

    def to_dict(self) -> Dict:
        """Prepare data for the user-facing `ElementActionRun` object."""

        def _process_datetime(dt: datetime) -> datetime:
            """We store datetime objects implicitly in UTC, so we need to first make
            that explicit, and then convert to the local time zone."""
            return dt.replace(tzinfo=timezone.utc).astimezone() if dt else None

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
        submission_idx: Optional[int] = None,
        commands_file_ID: Optional[int] = None,
        skip: Optional[int] = None,
        success: Optional[bool] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        snapshot_start: Optional[Dict] = None,
        snapshot_end: Optional[Dict] = None,
        exit_code: Optional[int] = None,
        run_hostname: Optional[str] = None,
        port_number: Optional[int] = None,
        data_idx: Optional[Dict[str, int]] = None,
    ) -> AnySEAR:
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
class StoreParameter:
    id_: int
    is_pending: bool
    is_set: bool
    data: Any
    file: Dict
    source: Dict

    _encoders = {}
    _decoders = {}

    def encode(self, **kwargs) -> Dict:
        """Prepare store parameter data for the persistent store."""
        if self.is_set:
            if self.file:
                return {"file": self.file}
            else:
                return self._encode(obj=self.data, **kwargs)
        else:
            return PARAM_DATA_NOT_SET

    def _encode(
        self,
        obj: Any,
        path: Optional[List] = None,
        type_lookup: Optional[Dict] = None,
        **kwargs,
    ) -> Dict:
        """Recursive encoder."""

        path = path or []
        if type_lookup is None:
            type_lookup = {
                "tuples": [],
                "sets": [],
                **{k: [] for k in self._decoders.keys()},
            }

        if len(path) > 50:
            raise RuntimeError("I'm in too deep!")

        if any("ParameterValue" in i.__name__ for i in obj.__class__.__mro__):
            # TODO: not nice; did this to avoid circular import of `ParameterValue`
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
                    path=path + [idx],
                    type_lookup=type_lookup,
                    **kwargs,
                )
                item, type_lookup = encoded["data"], encoded["type_lookup"]
                data.append(item)

            if isinstance(obj, tuple):
                type_lookup["tuples"].append(path)

            elif isinstance(obj, set):
                type_lookup["sets"].append(path)

        elif isinstance(obj, dict):
            data = {}
            for dct_key, dct_val in obj.items():
                encoded = self._encode(
                    obj=dct_val,
                    path=path + [dct_key],
                    type_lookup=type_lookup,
                    **kwargs,
                )
                dct_val, type_lookup = encoded["data"], encoded["type_lookup"]
                data[dct_key] = dct_val

        elif isinstance(obj, PRIMITIVES):
            data = obj

        elif type(obj) in self._encoders:
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
        data: Union[None, Dict],
        source: Dict,
        path: Optional[List[str]] = None,
        **kwargs,
    ) -> Any:
        """Initialise from persistent store parameter data."""
        if data and "file" in data:
            return cls(
                id_=id_,
                data=None,
                file=data["file"],
                is_set=True,
                source=source,
                is_pending=False,
            )
        elif data == PARAM_DATA_NOT_SET:
            # parameter is not set
            return cls(
                id_=id_,
                data=None,
                file=None,
                is_set=False,
                source=source,
                is_pending=False,
            )

        path = path or []

        obj = get_in_container(data["data"], path)

        for tuple_path in data["type_lookup"]["tuples"]:
            try:
                rel_path = get_relative_path(tuple_path, path)
            except ValueError:
                continue
            if rel_path:
                set_in_container(obj, rel_path, tuple(get_in_container(obj, rel_path)))
            else:
                obj = tuple(obj)

        for set_path in data["type_lookup"]["sets"]:
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
                type_lookup=data["type_lookup"],
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

    def set_data(self, value: Any) -> None:
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

    def set_file(self, value: Any) -> None:
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

    def update_source(self, src: Dict) -> None:
        """Return a copy, with updated source."""
        new_src = update_param_source_dict(self.source, src)
        return self.__class__(
            id_=self.id_,
            is_set=self.is_set,
            is_pending=self.is_pending,
            data=self.data,
            file=self.file,
            source=new_src,
        )


class PersistentStore(ABC):
    _store_task_cls = StoreTask
    _store_elem_cls = StoreElement
    _store_iter_cls = StoreElementIter
    _store_EAR_cls = StoreEAR
    _store_param_cls = StoreParameter

    _resources = {}

    def __init__(self, app, workflow, path, fs=None) -> None:
        self.app = app
        self.workflow = workflow
        self.path = path
        self.fs = fs

        self._pending = PendingChanges(app=app, store=self, resource_map=self._res_map)

        self._resources_in_use = set()
        self._in_batch_mode = False

        self._use_cache = False
        self._cache = None
        self._reset_cache()

        self._use_parameters_metadata_cache = None  # subclass-specific cache

    @property
    def logger(self):
        return self.app.persistence_logger

    @property
    def ts_fmt(self) -> str:
        return self.workflow.ts_fmt

    @property
    def has_pending(self):
        return bool(self._pending)

    @property
    def is_submittable(self):
        """Does this store support workflow submission?"""
        return self.fs.__class__.__name__ == "LocalFileSystem"

    @property
    def use_cache(self):
        return self._use_cache

    @property
    def task_cache(self):
        """Cache for persistent tasks."""
        return self._cache["tasks"]

    @property
    def element_cache(self):
        """Cache for persistent elements."""
        return self._cache["elements"]

    @property
    def element_iter_cache(self):
        """Cache for persistent element iterations."""
        return self._cache["element_iters"]

    @property
    def EAR_cache(self):
        """Cache for persistent EARs."""
        return self._cache["EARs"]

    @property
    def num_tasks_cache(self):
        """Cache for number of persistent tasks."""
        return self._cache["num_tasks"]

    @property
    def num_EARs_cache(self):
        """Cache for total number of persistent EARs."""
        return self._cache["num_EARs"]

    @property
    def num_params_cache(self):
        return self._cache["num_params"]

    @property
    def param_sources_cache(self):
        """Cache for persistent parameter sources."""
        return self._cache["param_sources"]

    @property
    def parameter_cache(self):
        """Cache for persistent parameters."""
        return self._cache["parameters"]

    @num_tasks_cache.setter
    def num_tasks_cache(self, value):
        self._cache["num_tasks"] = value

    @num_EARs_cache.setter
    def num_EARs_cache(self, value):
        self._cache["num_EARs"] = value

    @num_params_cache.setter
    def num_params_cache(self, value):
        self._cache["num_params"] = value

    def _reset_cache(self):
        self._cache = {
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
    def cache_ctx(self):
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
    def prepare_test_store_from_spec(task_spec):
        """Generate a valid store from a specification in terms of nested
        elements/iterations/EARs.

        """
        tasks = []
        elements = []
        elem_iters = []
        EARs = []

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
                            dict(
                                id_=len(EARs),
                                is_pending=False,
                                elem_iter_ID=len(elem_iters),
                                action_idx=0,
                                data_idx={},
                                metadata={},
                            )
                        )

                    elem_iters.append(
                        dict(
                            id_=len(elem_iters),
                            is_pending=False,
                            element_ID=len(elements),
                            EAR_IDs=EAR_IDs_dct,
                            data_idx={},
                            schema_parameters=[],
                        )
                    )
                elements.append(
                    dict(
                        id_=len(elements),
                        is_pending=False,
                        element_idx=elem_idx,
                        seq_idx={},
                        src_idx={},
                        task_ID=task_idx,
                        iteration_IDs=iter_IDs,
                    )
                )
            tasks.append(
                dict(
                    id_=len(tasks),
                    is_pending=False,
                    element_IDs=elem_IDs,
                )
            )
        return (tasks, elements, elem_iters, EARs)

    def remove_path(self, path: str, fs) -> None:
        """Try very hard to delete a directory or file.

        Dropbox (on Windows, at least) seems to try to re-sync files if the parent directory
        is deleted soon after creation, which is the case on a failed workflow creation (e.g.
        missing inputs), so in addition to catching PermissionErrors generated when
        Dropbox has a lock on files, we repeatedly try deleting the directory tree.

        """

        @self.app.perm_error_retry()
        def _remove_path(path: str, fs) -> None:
            self.logger.debug(f"_remove_path: path={path}")
            while fs.exists(path):
                fs.rm(path, recursive=True)
                time.sleep(0.5)

        return _remove_path(path, fs)

    def rename_path(self, replaced: str, original: str, fs) -> None:
        """Revert the replaced workflow path to its original name.

        This happens when new workflow creation fails and there is an existing workflow
        with the same name; the original workflow which was renamed, must be reverted."""

        @self.app.perm_error_retry()
        def _rename_path(replaced: str, original: str, fs) -> None:
            self.logger.debug(f"_rename_path: {replaced!r} --> {original!r}.")
            try:
                fs.rename(replaced, original, recursive=True)  # TODO: why need recursive?
            except TypeError:
                # `SFTPFileSystem.rename` has no  `recursive` argument:
                fs.rename(replaced, original)

        return _rename_path(replaced, original, fs)

    def _get_num_total_tasks(self):
        """Get the total number of persistent and pending tasks."""
        return self._get_num_persistent_tasks() + len(self._pending.add_tasks)

    def _get_num_total_loops(self):
        """Get the total number of persistent and pending loops."""
        return self._get_num_persistent_loops() + len(self._pending.add_loops)

    def _get_num_total_submissions(self):
        """Get the total number of persistent and pending submissions."""
        return self._get_num_persistent_submissions() + len(self._pending.add_submissions)

    def _get_num_total_elements(self):
        """Get the total number of persistent and pending elements."""
        return self._get_num_persistent_elements() + len(self._pending.add_elements)

    def _get_num_total_elem_iters(self):
        """Get the total number of persistent and pending element iterations."""
        return self._get_num_persistent_elem_iters() + len(self._pending.add_elem_iters)

    @TimeIt.decorator
    def _get_num_total_EARs(self):
        """Get the total number of persistent and pending EARs."""
        return self._get_num_persistent_EARs() + len(self._pending.add_EARs)

    def _get_task_total_num_elements(self, task_ID: int):
        """Get the total number of persistent and pending elements of a given task."""
        return len(self.get_task(task_ID).element_IDs)

    def _get_num_total_parameters(self):
        """Get the total number of persistent and pending parameters."""
        return self._get_num_persistent_parameters() + len(self._pending.add_parameters)

    def _get_num_total_input_files(self):
        """Get the total number of persistent and pending user-supplied input files."""
        num_pend_inp_files = len([i for i in self._pending.add_files if i["is_input"]])
        return self._get_num_persistent_input_files() + num_pend_inp_files

    def _get_num_total_added_tasks(self):
        """Get the total number of tasks ever added to the workflow."""
        return self._get_num_persistent_added_tasks() + len(self._pending.add_tasks)

    def _get_num_persistent_input_files(self):
        return len(list(self.workflow.input_files_path.glob("*")))

    def save(self):
        """Commit pending changes to disk, if not in batch-update mode."""
        if not self.workflow._in_batch_mode:
            self._pending.commit_all()

    def add_template_components(self, temp_comps: Dict, save: bool = True) -> None:
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

    def add_task(self, idx: int, task_template: Dict, save: bool = True):
        """Add a new task to the workflow."""
        self.logger.debug(f"Adding store task.")
        new_ID = self._get_num_total_added_tasks()
        self._pending.add_tasks[new_ID] = self._store_task_cls(
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
        loop_template: Dict,
        iterable_parameters,
        output_parameters,
        parents: List[str],
        num_added_iterations: Dict[Tuple[int], int],
        iter_IDs: List[int],
        save: bool = True,
    ):
        """Add a new loop to the workflow."""
        self.logger.debug(f"Adding store loop.")
        new_idx = self._get_num_total_loops()
        added_iters = [[list(k), v] for k, v in num_added_iterations.items()]
        self._pending.add_loops[new_idx] = {
            "loop_template": loop_template,
            "iterable_parameters": iterable_parameters,
            "output_parameters": output_parameters,
            "parents": parents,
            "num_added_iterations": added_iters,
        }

        for i in iter_IDs:
            self._pending.update_loop_indices[i].update({loop_template["name"]: 0})

        if save:
            self.save()

    @TimeIt.decorator
    def add_submission(self, sub_idx: int, sub_js: Dict, save: bool = True):
        """Add a new submission."""
        self.logger.debug(f"Adding store submission.")
        self._pending.add_submissions[sub_idx] = sub_js
        if save:
            self.save()

    def add_element_set(self, task_id: int, es_js: Dict, save: bool = True):
        self._pending.add_element_sets[task_id].append(es_js)
        if save:
            self.save()

    def add_element(
        self, task_ID: int, es_idx: int, seq_idx: Dict, src_idx: Dict, save: bool = True
    ):
        """Add a new element to a task."""
        self.logger.debug(f"Adding store element.")
        new_ID = self._get_num_total_elements()
        new_elem_idx = self._get_task_total_num_elements(task_ID)
        self._pending.add_elements[new_ID] = self._store_elem_cls(
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
        data_idx: Dict,
        schema_parameters: List[str],
        loop_idx: Optional[Dict] = None,
        save: bool = True,
    ) -> int:
        """Add a new iteration to an element."""
        self.logger.debug(f"Adding store element-iteration.")
        new_ID = self._get_num_total_elem_iters()
        self._pending.add_elem_iters[new_ID] = self._store_iter_cls(
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
        commands_idx: List[int],
        data_idx: Dict,
        metadata: Dict,
        save: bool = True,
    ) -> int:
        """Add a new EAR to an element iteration."""
        self.logger.debug(f"Adding store EAR.")
        new_ID = self._get_num_total_EARs()
        self._pending.add_EARs[new_ID] = self._store_EAR_cls(
            id_=new_ID,
            is_pending=True,
            elem_iter_ID=elem_iter_ID,
            action_idx=action_idx,
            commands_idx=commands_idx,
            data_idx=data_idx,
            metadata=metadata,
        )
        self._pending.add_elem_iter_EAR_IDs[elem_iter_ID][action_idx].append(new_ID)
        if save:
            self.save()
        return new_ID

    @TimeIt.decorator
    def set_run_dirs(self, run_dir_indices, run_idx, save: bool = True):
        self.logger.debug(f"Setting {run_idx.size} run directory indices.")
        self._pending.set_run_dirs.append((run_dir_indices, run_idx))
        if save:
            self.save()

    def update_at_submit_metadata(
        self, sub_idx: int, submission_parts: Dict[str, List[int]], save: bool = True
    ):
        if submission_parts:
            self._pending.update_at_submit_metadata[sub_idx][
                "submission_parts"
            ] = submission_parts
        if save:
            self.save()

    @TimeIt.decorator
    def set_run_submission_data(
        self, EAR_ID: int, cmds_ID: int, sub_idx: int, save: bool = True
    ) -> None:
        self._pending.set_EAR_submission_data[EAR_ID] = (sub_idx, cmds_ID)
        if save:
            self.save()

    def set_EAR_start(
        self, EAR_ID: int, run_dir: Union[Path, None], port_number: int, save: bool = True
    ) -> datetime:
        dt = datetime.utcnow()
        ss_js = self.app.RunDirAppFiles.take_snapshot() if run_dir else None
        run_hostname = socket.gethostname()
        self._pending.set_EAR_starts[EAR_ID] = (dt, ss_js, run_hostname, port_number)
        if save:
            self.save()
        return dt

    def set_multi_run_starts(
        self,
        run_ids: List[int],
        run_dirs: List[Union[Path, None]],
        port_number: int,
        save: bool = True,
    ) -> datetime:
        dt = datetime.utcnow()
        run_hostname = socket.gethostname()
        run_start_data = {}
        for id_i, dir_i in zip(run_ids, run_dirs):
            ss_js_i = self.app.RunDirAppFiles.take_snapshot(dir_i) if dir_i else None
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
        # TODO: save output files
        dt = datetime.utcnow()
        ss_js = self.app.RunDirAppFiles.take_snapshot() if snapshot else None
        self._pending.set_EAR_ends[EAR_ID] = (dt, ss_js, exit_code, success)
        if save:
            self.save()
        return dt

    def set_multi_run_ends(
        self,
        run_ids: List[int],
        run_dirs: List[Union[Path, None]],
        exit_codes: List[int],
        successes: List[bool],
        save: bool = True,
    ) -> datetime:
        self.logger.info("PersistentStore.set_multi_run_ends.")
        dt = datetime.utcnow()
        run_end_data = {}
        for id_i, dir_i, ex_i, sc_i in zip(run_ids, run_dirs, exit_codes, successes):
            ss_js_i = self.app.RunDirAppFiles.take_snapshot(dir_i) if dir_i else None
            run_end_data[id_i] = (dt, ss_js_i, ex_i, sc_i)

        self._pending.set_EAR_ends.update(run_end_data)
        if save:
            self.save()
        self.logger.info("PersistentStore.set_multi_run_ends finished.")
        return dt

    def set_EAR_skip(self, skip_reasons: Dict[int, int], save: bool = True) -> None:
        self._pending.set_EAR_skips.update(skip_reasons)
        if save:
            self.save()

    def set_EARs_initialised(self, iter_ID: int, save: bool = True) -> None:
        self._pending.set_EARs_initialised.append(iter_ID)
        if save:
            self.save()

    def set_jobscript_metadata(
        self,
        sub_idx: int,
        js_idx: int,
        version_info: Optional[Dict] = None,
        submit_time: Optional[str] = None,
        submit_hostname: Optional[str] = None,
        submit_machine: Optional[str] = None,
        submit_cmdline: Optional[List[str]] = None,
        os_name: Optional[str] = None,
        shell_name: Optional[str] = None,
        scheduler_name: Optional[str] = None,
        scheduler_job_ID: Optional[str] = None,
        process_ID: Optional[int] = None,
        save: bool = True,
    ):
        if version_info:
            self._pending.set_js_metadata[sub_idx][js_idx]["version_info"] = version_info
        if submit_time:
            self._pending.set_js_metadata[sub_idx][js_idx]["submit_time"] = submit_time
        if submit_hostname:
            self._pending.set_js_metadata[sub_idx][js_idx][
                "submit_hostname"
            ] = submit_hostname
        if submit_machine:
            self._pending.set_js_metadata[sub_idx][js_idx][
                "submit_machine"
            ] = submit_machine
        if submit_cmdline:
            self._pending.set_js_metadata[sub_idx][js_idx][
                "submit_cmdline"
            ] = submit_cmdline
        if os_name:
            self._pending.set_js_metadata[sub_idx][js_idx]["os_name"] = os_name
        if shell_name:
            self._pending.set_js_metadata[sub_idx][js_idx]["shell_name"] = shell_name
        if scheduler_name:
            self._pending.set_js_metadata[sub_idx][js_idx][
                "scheduler_name"
            ] = scheduler_name
        if scheduler_job_ID or process_ID:
            self._pending.set_js_metadata[sub_idx][js_idx][
                "scheduler_job_ID"
            ] = scheduler_job_ID
        if process_ID or scheduler_job_ID:
            self._pending.set_js_metadata[sub_idx][js_idx]["process_ID"] = process_ID
        if save:
            self.save()

    @writes_parameter_data
    def _add_parameter(
        self,
        is_set: bool,
        source: Dict,
        data: Any = None,
        file: Dict = None,
        save: bool = True,
    ) -> int:
        self.logger.debug(f"Adding store parameter{f' (unset)' if not is_set else ''}.")
        new_idx = self._get_num_total_parameters()
        self._pending.add_parameters[new_idx] = self._store_param_cls(
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
        path=None,
        contents: str = None,
        filename: str = None,
        clean_up: bool = False,
    ):
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
                assert path is not None
                exec_sub_path = Path(path).relative_to(self.path)
                dst_path = Path(
                    self.workflow.task_artifacts_path, *exec_sub_path.parts[1:]
                )
            if dst_path.is_file():
                dst_path = dst_path.with_suffix(dst_path.suffix + "_2")  # TODO: better!
        else:
            dst_path = path

        file_param_dat = {
            "store_contents": store_contents,
            "path": str(dst_path.relative_to(self.path)),
        }
        self._pending.add_files.append(
            {
                "store_contents": store_contents,
                "is_input": is_input,
                "dst_path": str(dst_path),
                "path": str(path),
                "contents": contents,
                "clean_up": clean_up,
            }
        )

        return file_param_dat

    def set_file(
        self,
        store_contents: bool,
        is_input: bool,
        param_id: int = None,
        path=None,
        contents: str = None,
        filename: str = None,
        clean_up: bool = False,
        save: bool = True,
    ):
        self.logger.debug(f"Setting new file")
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
        source: Dict,
        path=None,
        contents: str = None,
        filename: str = None,
        save: bool = True,
    ):
        self.logger.debug(f"Adding new file")
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

    def _append_files(self, files: Dict[int, Dict]):
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
    def add_set_parameter(self, data: Any, source: Dict, save: bool = True) -> int:
        return self._add_parameter(data=data, is_set=True, source=source, save=save)

    @writes_parameter_data
    def add_unset_parameter(self, source: Dict, save: bool = True) -> int:
        return self._add_parameter(data=None, is_set=False, source=source, save=save)

    @writes_parameter_data
    def set_parameter_value(
        self, param_id: int, value: Any, is_file: bool = False, save: bool = True
    ):
        self.logger.debug(
            f"Setting store parameter ID {param_id} value with type: {type(value)!r})."
        )
        self._pending.set_parameters[param_id] = (value, is_file)
        if save:
            self.save()

    @writes_parameter_data
    def set_parameter_values(self, values: Dict[int, Any], save: bool = True):
        """Set multiple non-file parameter values by parameter IDs."""
        param_ids = values.keys()
        self.logger.debug(f"Setting multiple store parameter IDs {param_ids!r}.")
        self._pending.set_parameters.update({k: (v, False) for k, v in values.items()})
        if save:
            self.save()

    @TimeIt.decorator
    @writes_parameter_data
    def update_param_source(
        self, param_sources: Dict[int, Dict], save: bool = True
    ) -> None:
        self.logger.debug(f"Updating parameter sources with {param_sources!r}.")
        self._pending.update_param_sources.update(param_sources)
        if save:
            self.save()

    def update_loop_num_iters(
        self, index: int, num_added_iters: int, save: bool = True
    ) -> None:
        self.logger.debug(
            f"Updating loop {index!r} num added iterations to {num_added_iters!r}."
        )
        num_added_iters = [[list(k), v] for k, v in num_added_iters.items()]
        self._pending.update_loop_num_iters[index] = num_added_iters
        if save:
            self.save()

    def update_loop_parents(
        self,
        index: int,
        num_added_iters: int,
        parents: List[str],
        save: bool = True,
    ) -> None:
        self.logger.debug(
            f"Updating loop {index!r} parents to {parents!r}, and num added iterations "
            f"to {num_added_iters}."
        )
        num_added_iters = [[list(k), v] for k, v in num_added_iters.items()]
        self._pending.update_loop_num_iters[index] = num_added_iters
        self._pending.update_loop_parents[index] = parents
        if save:
            self.save()

    def update_iter_data_indices(self, data_indices: Dict[int, Dict[str, int]]):
        """Update data indices of one or more iterations."""
        for k, v in data_indices.items():
            self._pending.update_iter_data_idx[k].update(v)

    def update_run_data_indices(self, data_indices: Dict[int, Dict[str, int]]):
        """Update data indices of one or more runs."""
        for k, v in data_indices.items():
            self._pending.update_run_data_idx[k].update(v)

    def get_template_components(self) -> Dict:
        """Get all template components, including pending."""
        tc = copy.deepcopy(self._get_persistent_template_components())
        for typ in TEMPLATE_COMP_TYPES:
            for hash_i, dat_i in self._pending.add_template_components.get(
                typ, {}
            ).items():
                if typ not in tc:
                    tc[typ] = {}
                tc[typ][hash_i] = dat_i

        return tc

    def get_template(self) -> Dict:
        return self._get_persistent_template()

    def _get_task_id_to_idx_map(self) -> Dict[int, int]:
        return {i.id_: i.index for i in self.get_tasks()}

    @TimeIt.decorator
    def get_task(self, task_idx: int) -> AnySTask:
        return self.get_tasks()[task_idx]

    def _process_retrieved_tasks(self, tasks: List[AnySTask]) -> List[AnySTask]:
        """Add pending data to retrieved tasks."""
        tasks_new = []
        for task_i in tasks:
            # consider pending element IDs:
            pend_elems = self._pending.add_elem_IDs.get(task_i.id_)
            if pend_elems:
                task_i = task_i.append_element_IDs(pend_elems)
            tasks_new.append(task_i)
        return tasks_new

    def _process_retrieved_loops(self, loops: Dict[int, Dict]) -> Dict[int, Dict]:
        """Add pending data to retrieved loops."""
        loops_new = {}
        for id_, loop_i in loops.items():
            if "num_added_iterations" not in loop_i:
                loop_i["num_added_iterations"] = 1
            # consider pending changes to num added iterations:
            pend_num_iters = self._pending.update_loop_num_iters.get(id_)
            if pend_num_iters:
                loop_i["num_added_iterations"] = pend_num_iters
            # consider pending change to parents:
            pend_parents = self._pending.update_loop_parents.get(id_)
            if pend_parents:
                loop_i["parents"] = pend_parents

            loops_new[id_] = loop_i
        return loops_new

    def get_tasks_by_IDs(self, id_lst: Iterable[int]) -> List[AnySTask]:
        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_tasks)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        tasks = self._get_persistent_tasks(id_pers) if id_pers else {}
        tasks.update({i: self._pending.add_tasks[i] for i in id_pend})

        # order as requested:
        tasks = [tasks[id_] for id_ in id_lst]

        return self._process_retrieved_tasks(tasks)

    @TimeIt.decorator
    def get_tasks(self) -> List[AnySTask]:
        """Retrieve all tasks, including pending."""
        tasks = self._get_persistent_tasks(range(self._get_num_persistent_tasks()))
        tasks.update({k: v for k, v in self._pending.add_tasks.items()})

        # order by index:
        tasks = sorted((i for i in tasks.values()), key=lambda x: x.index)

        return self._process_retrieved_tasks(tasks)

    def get_loops_by_IDs(self, id_lst: Iterable[int]) -> Dict[int, Dict]:
        """Retrieve loops by index (ID), including pending."""

        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_loops)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        loops = self._get_persistent_loops(id_pers) if id_pers else {}
        loops.update({i: self._pending.add_loops[i] for i in id_pend})

        # order as requested:
        loops = {id_: loops[id_] for id_ in id_lst}

        return self._process_retrieved_loops(loops)

    def get_loops(self) -> Dict[int, Dict]:
        """Retrieve all loops, including pending."""

        loops = self._get_persistent_loops()
        loops.update({k: v for k, v in self._pending.add_loops.items()})

        # order by index/ID:
        loops = dict(sorted(loops.items()))

        return self._process_retrieved_loops(loops)

    @TimeIt.decorator
    def get_submissions(self) -> Dict[int, Dict]:
        """Retrieve all submissions, including pending."""

        subs = self._get_persistent_submissions()
        subs.update({k: v for k, v in self._pending.add_submissions.items()})

        # order by index/ID
        subs = dict(sorted(subs.items()))

        return subs

    @TimeIt.decorator
    def get_submission_at_submit_metadata(
        self, sub_idx: int, metadata_attr: Union[Dict, None]
    ) -> Dict[str, Any]:
        """Retrieve the values of submission attributes that are stored at submit-time.

        Notes
        -----
        This method may need to be overridden if these attributes are stored separately
        from the remainder of the submission attributes.

        """
        return metadata_attr

    @TimeIt.decorator
    def get_jobscript_at_submit_metadata(
        self,
        sub_idx: int,
        js_idx: int,
        metadata_attr: Union[Dict, None],
    ) -> Dict[str, Any]:
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
        self, sub_idx: int, js_idx: int, blk_idx: int, run_ID_arr: Union[np.ndarray, None]
    ) -> np.ndarray:
        """For the specified jobscript-block, retrieve the run ID array.

        Notes
        -----
        This method may need to be overridden if these attributes are stored separately
        from the remainder of the submission attributes.

        """
        return np.asarray(run_ID_arr)

    @TimeIt.decorator
    def get_jobscript_block_task_elements_map(
        self,
        sub_idx: int,
        js_idx: int,
        blk_idx: int,
        task_elems_map: Union[np.ndarray, None],
    ) -> Dict[int, Dict[str, Any]]:
        """For the specified jobscript-block, retrieve the task-elements mapping.

        Notes
        -----
        This method may need to be overridden if these attributes are stored separately
        from the remainder of the submission attributes.

        """
        return task_elems_map

    @TimeIt.decorator
    def get_jobscript_block_task_actions_array(
        self,
        sub_idx: int,
        js_idx: int,
        blk_idx: int,
        task_actions_arr: Union[np.ndarray, None],
    ) -> np.ndarray:
        """For the specified jobscript-block, retrieve the task-actions array.

        Notes
        -----
        This method may need to be overridden if these attributes are stored separately
        from the remainder of the submission attributes.

        """
        return np.asarray(task_actions_arr)

    @TimeIt.decorator
    def get_submissions_by_ID(self, id_lst: Iterable[int]) -> Dict[int, Dict]:
        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_submissions)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        subs = self._get_persistent_submissions(id_pers) if id_pers else {}
        subs.update({i: self._pending.add_submissions[i] for i in id_pend})

        # order by index/ID
        subs = dict(sorted(subs.items()))

        return subs

    @TimeIt.decorator
    def get_elements(self, id_lst: Iterable[int]) -> List[AnySElement]:
        self.logger.debug(
            f"PersistentStore.get_elements: {len(id_lst)} elements: "
            f"{shorten_list_str(id_lst)}."
        )

        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_elements)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        elems = self._get_persistent_elements(id_pers) if id_pers else {}
        elems.update({i: self._pending.add_elements[i] for i in id_pend})

        # order as requested:
        elems = [elems[id_] for id_ in id_lst]

        elems_new = []
        for elem_i in elems:
            # consider pending iteration IDs:
            # TODO: does this consider pending iterations from new loop iterations?
            pend_iters = self._pending.add_elem_iter_IDs.get(elem_i.id_)
            if pend_iters:
                elem_i = elem_i.append_iteration_IDs(pend_iters)
            elems_new.append(elem_i)

        return elems_new

    @TimeIt.decorator
    def get_element_iterations(self, id_lst: Iterable[int]) -> List[AnySElementIter]:
        self.logger.debug(
            f"PersistentStore.get_element_iterations: {len(id_lst)} iterations: "
            f"{shorten_list_str(id_lst)}."
        )

        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_elem_iters)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        iters = self._get_persistent_element_iters(id_pers) if id_pers else {}
        iters.update({i: self._pending.add_elem_iters[i] for i in id_pend})

        # order as requested:
        iters = [iters[id_] for id_ in id_lst]

        iters_new = []
        for iter_i in iters:
            # consider pending EAR IDs:
            pend_EARs = self._pending.add_elem_iter_EAR_IDs.get(iter_i.id_)
            if pend_EARs:
                iter_i = iter_i.append_EAR_IDs(pend_EARs)

            # consider pending loop idx
            pend_loop_idx = self._pending.update_loop_indices.get(iter_i.id_)
            if pend_loop_idx:
                iter_i = iter_i.update_loop_idx(pend_loop_idx)

            # consider pending `EARs_initialised`:
            if iter_i.id_ in self._pending.set_EARs_initialised:
                iter_i = iter_i.set_EARs_initialised()

            iters_new.append(iter_i)

        return iters_new

    @TimeIt.decorator
    def get_EARs(self, id_lst: Iterable[int]) -> List[AnySEAR]:
        self.logger.debug(
            f"PersistentStore.get_EARs: {len(id_lst)} EARs: {shorten_list_str(id_lst)}."
        )

        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_EARs)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        EARs = self._get_persistent_EARs(id_pers) if id_pers else {}
        EARs.update({i: self._pending.add_EARs[i] for i in id_pend})

        # order as requested:
        EARs = [EARs[id_] for id_ in id_lst]

        EARs_new = []
        for EAR_i in EARs:
            # consider updates:
            pend_sub = self._pending.set_EAR_submission_data.get(EAR_i.id_)
            pend_start = self._pending.set_EAR_starts.get(EAR_i.id_)
            pend_end = self._pending.set_EAR_ends.get(EAR_i.id_)
            pend_skip = self._pending.set_EAR_skips.get(EAR_i.id_)

            p_st, p_ss, p_hn, p_pn = (
                pend_start if pend_start else (None, None, None, None)
            )
            p_et, p_se, p_ex, p_sx = pend_end if pend_end else (None, None, None, None)
            p_sub, p_cmd = pend_sub if pend_sub else (None, None)

            updates = {
                "submission_idx": p_sub,
                "commands_file_ID": p_cmd,
                "skip": pend_skip,
                "success": p_sx,
                "start_time": p_st,
                "end_time": p_et,
                "snapshot_start": p_ss,
                "snapshot_end": p_se,
                "exit_code": p_ex,
                "run_hostname": p_hn,
                "port_number": p_pn,
            }
            if any(i is not None for i in updates.values()):
                EAR_i = EAR_i.update(**updates)

            EARs_new.append(EAR_i)

        return EARs_new

    @TimeIt.decorator
    def _get_cached_persistent_items(
        self, id_lst: Iterable[int], cache: Dict
    ) -> Tuple[Dict[int, Any], List[int]]:
        id_lst = list(id_lst)
        if self.use_cache:
            id_set = set(id_lst)
            all_cached = set(cache.keys())
            id_cached = id_set.intersection(all_cached)
            id_non_cached = list(id_set.difference(all_cached))
            items = {k: cache[k] for k in id_cached}
        else:
            items = {}
            id_non_cached = id_lst
        return items, id_non_cached

    def _get_cached_persistent_EARs(
        self, id_lst: Iterable[int]
    ) -> Tuple[Dict[int, AnySEAR], List[int]]:
        return self._get_cached_persistent_items(id_lst, self.EAR_cache)

    def _get_cached_persistent_element_iters(
        self, id_lst: Iterable[int]
    ) -> Tuple[Dict[int, AnySEAR], List[int]]:
        return self._get_cached_persistent_items(id_lst, self.element_iter_cache)

    def _get_cached_persistent_elements(
        self, id_lst: Iterable[int]
    ) -> Tuple[Dict[int, AnySEAR], List[int]]:
        return self._get_cached_persistent_items(id_lst, self.element_cache)

    def _get_cached_persistent_tasks(self, id_lst: Iterable[int]):
        return self._get_cached_persistent_items(id_lst, self.task_cache)

    def _get_cached_persistent_param_sources(self, id_lst: Iterable[int]):
        return self._get_cached_persistent_items(id_lst, self.param_sources_cache)

    def _get_cached_persistent_parameters(self, id_lst: Iterable[int]):
        return self._get_cached_persistent_items(id_lst, self.parameter_cache)

    def get_EAR_skipped(self, EAR_ID: int) -> bool:
        self.logger.debug(f"PersistentStore.get_EAR_skipped: EAR_ID={EAR_ID!r}")
        return self.get_EARs([EAR_ID])[0].skip

    @TimeIt.decorator
    def get_parameters(
        self,
        id_lst: Iterable[int],
        **kwargs: Dict,
    ) -> List[AnySParameter]:
        """
        Parameters
        ----------
        kwargs :
            dataset_copy : bool
                For Zarr stores only. If True, copy arrays as NumPy arrays.
        """
        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_parameters)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        params = self._get_persistent_parameters(id_pers, **kwargs) if id_pers else {}
        params.update({i: self._pending.add_parameters[i] for i in id_pend})

        # order as requested:
        params = [params[id_] for id_ in id_lst]

        return params

    @TimeIt.decorator
    def get_parameter_set_statuses(self, id_lst: Iterable[int]) -> List[bool]:
        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_parameters)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        set_status = self._get_persistent_parameter_set_status(id_pers) if id_pers else {}
        set_status.update({i: self._pending.add_parameters[i].is_set for i in id_pend})

        # order as requested:
        return [set_status[id_] for id_ in id_lst]

    @TimeIt.decorator
    def get_parameter_sources(self, id_lst: Iterable[int]) -> List[Dict]:
        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_parameters)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        src = self._get_persistent_param_sources(id_pers) if id_pers else {}
        src.update({i: self._pending.add_parameters[i].source for i in id_pend})

        # order as requested:
        src_new = []
        for id_i in id_lst:
            # consider pending source updates:
            pend_src = self._pending.update_param_sources.get(id_i)
            src_i = src[id_i]
            if pend_src:
                src_i = {**src_i, **pend_src}
            src_new.append(src_i)

        return src_new

    @TimeIt.decorator
    def get_task_elements(
        self,
        task_id,
        idx_lst: Optional[Iterable[int]] = None,
    ) -> List[Dict]:
        """Get element data by an indices within a given task.

        Element iterations and EARs belonging to the elements are included.

        """

        all_elem_IDs = self.get_task(task_id).element_IDs
        if idx_lst is None:
            req_IDs = all_elem_IDs
        else:
            req_IDs = [all_elem_IDs[i] for i in idx_lst]
        store_elements = self.get_elements(req_IDs)
        iter_IDs = [i.iteration_IDs for i in store_elements]
        iter_IDs_flat, iter_IDs_lens = flatten(iter_IDs)
        store_iters = self.get_element_iterations(iter_IDs_flat)

        # retrieve EARs:
        EAR_IDs = [list((i.EAR_IDs or {}).values()) for i in store_iters]
        EAR_IDs_flat, EAR_IDs_lens = flatten(EAR_IDs)
        EARs_dct = [i.to_dict() for i in self.get_EARs(EAR_IDs_flat)]
        EARs_dct_rs = reshape(EARs_dct, EAR_IDs_lens)

        # add EARs to iterations:
        iters = []
        for idx, i in enumerate(store_iters):
            EARs = None
            if i.EAR_IDs is not None:
                EARs = dict(zip(i.EAR_IDs.keys(), EARs_dct_rs[idx]))
            iters.append(i.to_dict(EARs))

        # reshape iterations:
        iters_rs = reshape(iters, iter_IDs_lens)

        # add iterations to elements:
        elements = []
        for idx, i in enumerate(store_elements):
            elements.append(i.to_dict(iters_rs[idx]))
        return elements

    def check_parameters_exist(self, id_lst: Iterable[int]) -> List[bool]:
        """For each parameter ID, return True if it exists, else False"""

        id_set = set(id_lst)
        all_pending = set(self._pending.add_parameters)
        id_not_pend = id_set.difference(all_pending)
        id_miss = set()
        if id_not_pend:
            all_id_pers = self._get_persistent_parameter_IDs()
            id_miss = id_not_pend.difference(all_id_pers)

        return [False if i in id_miss else True for i in id_lst]

    @contextlib.contextmanager
    def using_resource(self, res_label, action):
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

    def copy(self, path=None) -> str:
        """Copy the workflow store.

        This does not work on remote filesystems.

        """
        if path is None:
            _path = Path(self.path)
            path = _path.parent / Path(_path.stem + "_copy" + _path.suffix)

        if self.fs.exists(str(path)):
            raise ValueError(f"Path already exists: {path}.")
        else:
            path = str(path)

        self.fs.copy(self.path, path)

        new_fs_path = self.workflow.fs_path.replace(self.path, path)

        return new_fs_path

    def delete(self) -> None:
        """Delete the persistent workflow."""
        confirm = input(
            f"Permanently delete the workflow at path {self.path!r}; [y]es or [n]o?"
        )
        if confirm.strip().lower() == "y":
            self.delete_no_confirm()

    def delete_no_confirm(self) -> None:
        """Permanently delete the workflow data with no confirmation."""

        @self.app.perm_error_retry()
        def _delete_no_confirm() -> None:
            self.logger.debug(f"_delete_no_confirm: {self.path!r}.")
            self.fs.rm(self.path, recursive=True)

        return _delete_no_confirm()

    def get_dirs_arr(self) -> np.ndarray:
        return self._get_dirs_arr()[:]
