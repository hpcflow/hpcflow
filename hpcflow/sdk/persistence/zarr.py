"""
Persistence model based on writing Zarr arrays.
"""

from __future__ import annotations

import copy
from contextlib import AbstractContextManager, contextmanager, nullcontext
from collections import defaultdict
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, cast, TYPE_CHECKING
import msgpack
from typing_extensions import override
import shutil
import time

import numpy as np
from numpy.ma.core import MaskedArray
import zarr  # type: ignore
from zarr.errors import BoundsCheckError  # type: ignore
from zarr.storage import DirectoryStore, FSStore  # type: ignore
from zarr.util import guess_chunks  # type: ignore
from fsspec.implementations.zip import ZipFileSystem  # type: ignore
from rich.console import Console
from numcodecs import MsgPack, VLenArray, blosc, Blosc, Zstd  # type: ignore
from reretry import retry  # type: ignore

from hpcflow.sdk.submission.run_file_resolver import get_run_multi_chunk_path
from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core import RUN_DIR_ARR_DTYPE, RUN_DIR_ARR_FILL
from hpcflow.sdk.core.errors import (
    MissingParameterData,
    MissingStoreEARError,
    MissingStoreElementError,
    MissingStoreElementIterationError,
    MissingStoreTaskError,
)
from hpcflow.sdk.core.utils import (
    ensure_in,
    get_relative_path,
    set_in_container,
    get_in_container,
)
from hpcflow.sdk.persistence.base import (
    PARAM_DATA_NOT_SET,
    PersistentStoreFeatures,
    PersistentStore,
    StoreEAR,
    StoreElement,
    StoreElementIter,
    StoreParameter,
    StoreTask,
)
from hpcflow.sdk.persistence.types import (
    LoopDescriptor,
    StoreCreationInfo,
    TemplateMeta,
    ZarrAttrsDict,
)
from hpcflow.sdk.persistence.store_resource import ZarrAttrsStoreResource
from hpcflow.sdk.persistence.utils import ask_pw_on_auth_exc, atomic_write
from hpcflow.sdk.persistence.pending import CommitResourceMap
from hpcflow.sdk.persistence.base import update_param_source_dict
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.submission.submission import (
    JOBSCRIPT_SUBMIT_TIME_KEYS,
    SUBMISSION_SUBMIT_TIME_KEYS,
)
from hpcflow.sdk.utils.arrays import get_2D_idx, split_arr
from hpcflow.sdk.utils.patches import override_module_attrs
from hpcflow.sdk.utils.strings import shorten_list_str

if TYPE_CHECKING:
    from collections.abc import (
        Callable,
        Iterable,
        Iterator,
        Mapping,
        MutableMapping,
        Sequence,
    )
    from datetime import datetime
    from fsspec import AbstractFileSystem  # type: ignore
    from logging import Logger
    from typing import ClassVar, TypeAlias, TypeVar
    from typing_extensions import Self
    from numpy.typing import NDArray
    from zarr import Array, Group  # type: ignore
    from zarr.attrs import Attributes  # type: ignore
    from zarr.storage import Store  # type: ignore
    from ..submission.types import ResolvedJobscriptBlockDependencies
    from .types import TypeLookup
    from ..app import BaseApp
    from ..core.json_like import JSONed, JSONDocument
    from ..typing import ParamSource, PathLike, DataIndex
    from rich.status import Status

    _UpdateT = TypeVar("_UpdateT")

#: List of any (Zarr-serializable) value.
ListAny: TypeAlias = "list[Any]"
#: Zarr attribute mapping context.
ZarrAttrs: TypeAlias = "dict[str, Any]"
#: Soft lower limit for the number of bytes in an array chunk
_ARRAY_CHUNK_MIN: int = 500 * 1024 * 1024  # 500 MiB
#: Hard upper limit for the number of bytes in an array chunk. Should be lower than the
#: maximum buffer size of the blosc encoder, if we're using it (2 GiB)
_ARRAY_CHUNK_MAX: int = 1024 * 1024 * 1024  # 1 GiB
_JS: TypeAlias = "dict[str, list[dict[str, dict]]]"


blosc.use_threads = False  # hpcflow is a multiprocess program in general


@TimeIt.decorator
def _zarr_get_coord_selection(arr: Array, selection: Any, logger: Logger):
    @retry(
        RuntimeError,
        tries=10,
        delay=1,
        backoff=1.5,
        jitter=(0, 5),
        logger=logger,
    )
    @TimeIt.decorator
    def _inner(arr: Array, selection: Any):
        return arr.get_coordinate_selection(selection)

    return _inner(arr, selection)


def _encode_numpy_array(
    obj: NDArray,
    type_lookup: TypeLookup,
    path: list[int],
    root_group: Group,
    arr_path: list[int],
    root_encoder: Callable,
) -> int:
    # Might need to generate new group:
    param_arr_group = root_group.require_group(arr_path)
    new_idx = (
        max((int(i.removeprefix("arr_")) for i in param_arr_group.keys()), default=-1) + 1
    )
    with override_module_attrs(
        "zarr.util", {"CHUNK_MIN": _ARRAY_CHUNK_MIN, "CHUNK_MAX": _ARRAY_CHUNK_MAX}
    ):
        # `guess_chunks` also ensures chunk shape is at least 1 in each dimension:
        chunk_shape = guess_chunks(obj.shape, obj.dtype.itemsize)

    param_arr_group.create_dataset(name=f"arr_{new_idx}", data=obj, chunks=chunk_shape)
    type_lookup["arrays"].append([path, new_idx])

    return len(type_lookup["arrays"]) - 1


def _decode_numpy_arrays(
    obj: dict | None,
    type_lookup: TypeLookup,
    path: list[int],
    arr_group: Group,
    dataset_copy: bool,
):
    # Yuck! Type lies! Zarr's internal types are not modern Python types.
    arrays = cast("Iterable[tuple[list[int], int]]", type_lookup.get("arrays", []))
    obj_: dict | NDArray | None = obj
    for arr_path, arr_idx in arrays:
        try:
            rel_path = get_relative_path(arr_path, path)
        except ValueError:
            continue

        dataset: NDArray = arr_group.get(f"arr_{arr_idx}")
        if dataset_copy:
            dataset = dataset[:]

        if rel_path:
            set_in_container(obj_, rel_path, dataset)
        else:
            obj_ = dataset

    return obj_


def _encode_masked_array(
    obj: MaskedArray,
    type_lookup: TypeLookup,
    path: list[int],
    root_group: Group,
    arr_path: list[int],
    root_encoder: Callable,
):
    """Encode a masked array as two normal arrays, and return the fill value."""
    # no need to add "array" entries to the type lookup, so pass an empty `type_lookup`:
    type_lookup_: TypeLookup = defaultdict(list)
    data_idx = _encode_numpy_array(
        obj.data, type_lookup_, path, root_group, arr_path, root_encoder
    )
    mask_idx = _encode_numpy_array(
        cast("NDArray", obj.mask), type_lookup_, path, root_group, arr_path, root_encoder
    )
    type_lookup["masked_arrays"].append([path, [data_idx, mask_idx]])
    return obj.fill_value.item()


def _decode_masked_arrays(
    obj: dict,
    type_lookup: TypeLookup,
    path: list[int],
    arr_group: Group,
    dataset_copy: bool,
):
    # Yuck! Type lies! Zarr's internal types are not modern Python types.
    masked_arrays = cast(
        "Iterable[tuple[list[int], tuple[int, int]]]",
        type_lookup.get("masked_arrays", []),
    )
    obj_: dict | MaskedArray = obj
    for arr_path, (data_idx, mask_idx) in masked_arrays:
        try:
            rel_path = get_relative_path(arr_path, path)
        except ValueError:
            continue

        fill_value = get_in_container(obj_, rel_path)
        data = arr_group.get(f"arr_{data_idx}")
        mask = arr_group.get(f"arr_{mask_idx}")
        dataset: MaskedArray = MaskedArray(data=data, mask=mask, fill_value=fill_value)

        if rel_path:
            set_in_container(obj_, rel_path, dataset)
        else:
            obj_ = dataset
    return obj_


def _encode_bytes(obj: dict, **kwargs):
    return obj  # msgpack can handle bytes


def append_items_to_ragged_array(arr: Array, items: Sequence[int]):
    """Append an array to a Zarr ragged array.

    I think `arr.append([item])` should work, but does not for some reason, so we do it
    here by resizing and assignment."""
    num = len(items)
    arr.resize((len(arr) + num))
    for idx, i in enumerate(items):
        arr[-(num - idx)] = i


@dataclass
class ZarrStoreTask(StoreTask[dict]):
    """
    Represents a task in a Zarr persistent store.
    """

    @override
    def encode(self) -> tuple[int, dict, dict[str, Any]]:
        """Prepare store task data for the persistent store."""
        wk_task = {"id_": self.id_, "element_IDs": np.array(self.element_IDs)}
        task = {"id_": self.id_, **(self.task_template or {})}
        return self.index, wk_task, task

    @override
    @classmethod
    def decode(cls, task_dat: dict) -> Self:
        """Initialise a `StoreTask` from persistent task data"""
        task_dat["element_IDs"] = task_dat["element_IDs"].tolist()
        return cls(is_pending=False, **task_dat)


@dataclass
class ZarrStoreElement(StoreElement[ListAny, ZarrAttrs]):
    """
    Represents an element in a Zarr persistent store.
    """

    @override
    def encode(self, attrs: ZarrAttrs) -> ListAny:
        """Prepare store elements data for the persistent store.

        This method mutates `attrs`.
        """
        return [
            self.id_,
            self.index,
            self.es_idx,
            [[ensure_in(k, attrs["seq_idx"]), v] for k, v in self.seq_idx.items()],
            [[ensure_in(k, attrs["src_idx"]), v] for k, v in self.src_idx.items()],
            self.task_ID,
            self.iteration_IDs,
        ]

    @override
    @classmethod
    def decode(cls, elem_dat: ListAny, attrs: ZarrAttrs) -> Self:
        """Initialise a `StoreElement` from persistent element data"""
        obj_dat = {
            "id_": elem_dat[0],
            "index": elem_dat[1],
            "es_idx": elem_dat[2],
            "seq_idx": {attrs["seq_idx"][k]: v for (k, v) in elem_dat[3]},
            "src_idx": {attrs["src_idx"][k]: v for (k, v) in elem_dat[4]},
            "task_ID": elem_dat[5],
            "iteration_IDs": elem_dat[6],
        }
        return cls(is_pending=False, **obj_dat)


@dataclass
class ZarrStoreElementIter(StoreElementIter[ListAny, ZarrAttrs]):
    """
    Represents an element iteration in a Zarr persistent store.
    """

    @override
    def encode(self, attrs: ZarrAttrs) -> ListAny:
        """Prepare store element iteration data for the persistent store.

        This method mutates `attrs`.
        """
        return [
            self.id_,
            self.element_ID,
            int(self.EARs_initialised),
            [[ek, ev] for ek, ev in self.EAR_IDs.items()] if self.EAR_IDs else None,
            [
                [ensure_in(dk, attrs["parameter_paths"]), dv]
                for dk, dv in self.data_idx.items()
            ],
            [ensure_in(i, attrs["schema_parameters"]) for i in self.schema_parameters],
            [[ensure_in(dk, attrs["loops"]), dv] for dk, dv in self.loop_idx.items()],
        ]

    @override
    @classmethod
    def decode(cls, iter_dat: ListAny, attrs: ZarrAttrs) -> Self:
        """Initialise a `ZarrStoreElementIter` from persistent element iteration data"""
        obj_dat = {
            "id_": iter_dat[0],
            "element_ID": iter_dat[1],
            "EARs_initialised": bool(iter_dat[2]),
            "EAR_IDs": {i[0]: i[1] for i in iter_dat[3]} if iter_dat[3] else None,
            "data_idx": {attrs["parameter_paths"][i[0]]: i[1] for i in iter_dat[4]},
            "schema_parameters": [attrs["schema_parameters"][i] for i in iter_dat[5]],
            "loop_idx": {attrs["loops"][i[0]]: i[1] for i in iter_dat[6]},
        }
        return cls(is_pending=False, **obj_dat)


@dataclass
class ZarrStoreEAR(StoreEAR[ListAny, ZarrAttrs]):
    """
    Represents an element action run in a Zarr persistent store.
    """

    EXEC_TIME_ATTRIBUTES = [
        "skip",
        "success",
        "start_time",
        "end_time",
        "snapshot_start",
        "snapshot_end",
        "exit_code",
        "run_hostname",
        "port_number",
    ]
    EXEC_TIME_DEFAULTS = {
        "skip": 0,
    }

    @override
    def encode(self, ts_fmt: str, attrs: ZarrAttrs) -> ListAny:
        """Prepare store EAR data for the persistent store.

        This method mutates `attrs`.
        """

        return (
            self.encode_creation_metadata(attrs=attrs)
            + self.encode_submit_time_metadata()
            + self._encode_run_time_metadata(ts_fmt=ts_fmt)
        )

    def encode_creation_metadata(self, attrs: ZarrAttrs) -> ListAny:
        """Run metadata that is generated when the run is created.

        This method mutates `attrs`.
        """
        # task ID, element_idx, iteration_idx is not stored
        return [
            self.id_,
            self.elem_iter_ID,
            self.action_idx,
            [
                [ensure_in(dk, attrs["parameter_paths"]), dv]
                for dk, dv in self.data_idx.items()
            ],
            self.commands_idx,
        ]

    def encode_submit_time_metadata(self):
        """Run metadata that is generated at sumbit-time."""
        return [
            self.submission_idx,
            self.commands_file_ID,
            self.run_file_ID,
            self.run_file_idx,
        ]

    def _encode_run_time_metadata(self, ts_fmt: str):
        """Run metadata that is generated at the start or end of a run's execution."""
        return self.encode_run_time_metadata(
            {name: getattr(self, name) for name in self.EXEC_TIME_ATTRIBUTES},
            ts_fmt=ts_fmt,
        )

    @classmethod
    def encode_run_time_metadata(cls, data: dict[str, Any], ts_fmt: str) -> list[Any]:
        out = [data.get(idx) for idx in range(len(cls.EXEC_TIME_ATTRIBUTES))]
        start_idx = cls.EXEC_TIME_ATTRIBUTES.index("start_time")
        end_idx = cls.EXEC_TIME_ATTRIBUTES.index("end_time")

        for key, default in cls.EXEC_TIME_DEFAULTS.items():
            key_idx = cls.EXEC_TIME_ATTRIBUTES.index(key)
            if out[key_idx] is None:
                out[key_idx] = default

        if start_time := out[start_idx]:
            out[start_idx] = cls._encode_datetime(start_time, ts_fmt)
        if end_time := out[end_idx]:
            out[end_idx] = cls._encode_datetime(end_time, ts_fmt)
        return out

    @override
    @classmethod
    def decode(
        cls,
        EAR_dat: ListAny,
        sub_dat: ListAny,
        run_time_dat: ListAny | None,
        ts_fmt: str,
        attrs: ZarrAttrs,
    ) -> Self:
        """Initialise a `ZarrStoreEAR` from persistent EAR data"""
        if run_time_dat is None:
            run_time_dat = cls.encode_run_time_metadata({}, ts_fmt)
        obj_dat = {
            "id_": EAR_dat[0],
            "elem_iter_ID": EAR_dat[1],
            "action_idx": EAR_dat[2],
            "data_idx": {attrs["parameter_paths"][i[0]]: i[1] for i in EAR_dat[3]},
            "commands_idx": EAR_dat[4],
            "submission_idx": sub_dat[0],
            "commands_file_ID": sub_dat[1],
            "run_file_ID": sub_dat[2],
            "run_file_idx": sub_dat[3],
            "skip": run_time_dat[0],
            "success": run_time_dat[1],
            "start_time": cls._decode_datetime(run_time_dat[2], ts_fmt),
            "end_time": cls._decode_datetime(run_time_dat[3], ts_fmt),
            "snapshot_start": run_time_dat[4],
            "snapshot_end": run_time_dat[5],
            "exit_code": run_time_dat[6],
            "run_hostname": run_time_dat[7],
            "port_number": run_time_dat[8],
        }
        return cls(is_pending=False, **obj_dat)


@dataclass
@hydrate
class ZarrStoreParameter(StoreParameter):
    """
    Represents a parameter in a Zarr persistent store.
    """

    _encoders: ClassVar[dict[type, Callable]] = {  # keys are types
        **StoreParameter._encoders,
        np.ndarray: _encode_numpy_array,
        MaskedArray: _encode_masked_array,
        bytes: _encode_bytes,
    }
    _decoders: ClassVar[dict[str, Callable]] = {  # keys are keys in type_lookup
        **StoreParameter._decoders,
        "arrays": _decode_numpy_arrays,
        "masked_arrays": _decode_masked_arrays,
    }


class ZarrPersistentStore(
    PersistentStore[
        ZarrStoreTask,
        ZarrStoreElement,
        ZarrStoreElementIter,
        ZarrStoreEAR,
        ZarrStoreParameter,
    ]
):
    """
    A persistent store implemented using Zarr.
    """

    _name: ClassVar[str] = "zarr"
    _features: ClassVar[PersistentStoreFeatures] = PersistentStoreFeatures(
        create=True,
        edit=True,
        jobscript_parallelism=True,
        EAR_parallelism=True,
        schedulers=True,
        submission=True,
    )

    @classmethod
    def _store_task_cls(cls) -> type[ZarrStoreTask]:
        return ZarrStoreTask

    @classmethod
    def _store_elem_cls(cls) -> type[ZarrStoreElement]:
        return ZarrStoreElement

    @classmethod
    def _store_iter_cls(cls) -> type[ZarrStoreElementIter]:
        return ZarrStoreElementIter

    @classmethod
    def _store_EAR_cls(cls) -> type[ZarrStoreEAR]:
        return ZarrStoreEAR

    @classmethod
    def _store_param_cls(cls) -> type[ZarrStoreParameter]:
        return ZarrStoreParameter

    _param_grp_name: ClassVar[str] = "parameters"
    _param_base_arr_name: ClassVar[str] = "base"
    _param_sources_arr_name: ClassVar[str] = "sources"
    _param_user_arr_grp_name: ClassVar[str] = "arrays"
    _param_data_arr_grp_name: ClassVar = lambda _, param_idx: f"param_{param_idx}"
    _subs_md_group_name: ClassVar[str] = "submissions"
    _task_arr_name: ClassVar[str] = "tasks"
    _elem_arr_name: ClassVar[str] = "elements"
    _iter_arr_name: ClassVar[str] = "iters"
    _run_metadata_arr_name: ClassVar[str] = "run_metadata"
    _run_sub_metadata_arr_name: ClassVar[str] = "run_sub_dat"
    _run_multi_process_dir_name: ClassVar[str] = "run_multi"
    _run_dir_arr_name: ClassVar[str] = "run_dirs"
    _js_at_submit_md_arr_name: ClassVar[str] = "js_at_submit_md"
    _js_run_IDs_arr_name: ClassVar[str] = "js_run_IDs"
    _js_task_elems_arr_name: ClassVar[str] = "js_task_elems"
    _js_task_acts_arr_name: ClassVar[str] = "js_task_acts"
    _js_deps_arr_name: ClassVar[str] = "js_deps"
    _time_res: ClassVar[str] = "us"  # microseconds; must not be smaller than micro!

    _RUN_SUB_DAT_DTYPE: ClassVar = [
        ("submission_idx", np.uint8),
        ("commands_file_ID", np.uint32),
        ("run_file_ID", np.uint16),
        ("run_file_idx", np.uint32),
    ]
    _RUN_SUB_DAT_FILL: ClassVar = {
        "submission_idx": np.iinfo(np.uint8).max,
        "commands_file_ID": np.iinfo(np.uint32).max,
        "run_file_ID": np.iinfo(np.uint16).max,
        "run_file_idx": np.iinfo(np.uint32).max,
    }

    _res_map: ClassVar[CommitResourceMap] = CommitResourceMap(
        commit_template_components=("attrs",)
    )

    def __init__(self, app, workflow, path: str | Path, fs: AbstractFileSystem) -> None:
        self._zarr_store = None  # assigned on first access to `zarr_store`
        self._resources = {
            "attrs": ZarrAttrsStoreResource(
                app, name="attrs", open_call=self._get_root_group
            ),
        }
        self._jobscript_at_submit_metadata: dict[int, dict[str, Any]] = (
            {}
        )  # this is a cache

        # these are caches; keys are submission index and then tuples of
        # (jobscript index, jobscript-block index):
        self._jobscript_run_ID_arrays: dict[int, dict[tuple[int, int], NDArray]] = {}
        self._jobscript_task_element_maps: dict[
            int, dict[tuple[int, int], dict[int, list[int]]]
        ] = {}
        self._jobscript_task_actions_arrays: dict[int, dict[tuple[int, int], NDArray]] = (
            {}
        )
        self._jobscript_dependencies: dict[
            int,
            dict[
                tuple[int, int], dict[tuple[int, int], ResolvedJobscriptBlockDependencies]
            ],
        ] = {}

        super().__init__(app, workflow, path, fs)

    @contextmanager
    def cached_load(self) -> Iterator[None]:
        """Context manager to cache the root attributes."""
        with self.using_resource("attrs", "read") as attrs:
            yield

    def remove_replaced_dir(self) -> None:
        """
        Remove the directory containing replaced workflow details.
        """
        with self.using_resource("attrs", "update") as md:
            if "replaced_workflow" in md:
                self.logger.debug("removing temporarily renamed pre-existing workflow.")
                self.remove_path(md["replaced_workflow"])
                del md["replaced_workflow"]

    def reinstate_replaced_dir(self) -> None:
        """
        Reinstate the directory containing replaced workflow details.
        """
        with self.using_resource("attrs", "read") as md:
            if "replaced_workflow" in md:
                self.logger.debug(
                    "reinstating temporarily renamed pre-existing workflow."
                )
                self.rename_path(
                    md["replaced_workflow"],
                    self.path,
                )

    @staticmethod
    def _get_zarr_store(path: str | Path, fs: AbstractFileSystem) -> Store:
        return FSStore(url=str(path), fs=fs)

    _CODEC: ClassVar = MsgPack()

    @classmethod
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
        ts_fmt: str,
        ts_name_fmt: str,
        creation_info: StoreCreationInfo,
        compressor: str | None = "blosc",
        compressor_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """
        Write an empty persistent workflow.
        """
        attrs: ZarrAttrsDict = {
            "name": name,
            "ts_fmt": ts_fmt,
            "ts_name_fmt": ts_name_fmt,
            "creation_info": creation_info,
            "template": template_js,
            "template_components": template_components_js,
            "num_added_tasks": 0,
            "tasks": [],
            "loops": [],
            "submissions": [],
        }
        if replaced_wk:
            attrs["replaced_workflow"] = replaced_wk

        store = cls._get_zarr_store(wk_path, fs)
        root = zarr.group(store=store, overwrite=False)
        root.attrs.update(attrs)

        # use a nested directory store for the metadata group so the runs array
        # can be stored as a 2D array in nested directories, thereby limiting the maximum
        # number of files stored in a given directory:
        md_store = zarr.NestedDirectoryStore(Path(root.store.path).joinpath("metadata"))
        md = zarr.group(store=md_store)

        compressor_lookup = {
            "blosc": Blosc,
            "zstd": Zstd,
        }
        if compressor:
            cmp = compressor_lookup[compressor.lower()](**(compressor_kwargs or {}))
        else:
            cmp = None

        tasks_arr = md.create_dataset(
            name=cls._task_arr_name,
            shape=0,
            dtype=object,
            object_codec=VLenArray(int),
            compressor=cmp,
        )

        elems_arr = md.create_dataset(
            name=cls._elem_arr_name,
            shape=0,
            dtype=object,
            object_codec=cls._CODEC,
            chunks=1000,
            compressor=cmp,
        )
        elems_arr.attrs.update({"seq_idx": [], "src_idx": []})

        elem_iters_arr = md.create_dataset(
            name=cls._iter_arr_name,
            shape=0,
            dtype=object,
            object_codec=cls._CODEC,
            chunks=1000,
            compressor=cmp,
        )
        elem_iters_arr.attrs.update(
            {
                "loops": [],
                "schema_parameters": [],
                "parameter_paths": [],
            }
        )

        run_md_arr = md.create_dataset(
            name=cls._run_metadata_arr_name,
            shape=0,
            chunks=200_000,  # TODO: check; probably want a few MB per chunk
            dtype=object,
            object_codec=cls._CODEC,
            compressor=cmp,
        )
        run_md_arr.attrs.update({"parameter_paths": []})

        # array for storing indices that can be used to reproduce run directory paths:
        run_dir_arr = md.create_dataset(
            name=cls._run_dir_arr_name,
            shape=0,
            chunks=10_000,
            dtype=RUN_DIR_ARR_DTYPE,
            fill_value=RUN_DIR_ARR_FILL,
            write_empty_chunks=False,
        )

        parameter_data = root.create_group(name=cls._param_grp_name)
        parameter_data.create_dataset(
            name=cls._param_base_arr_name,
            shape=0,
            dtype=object,
            object_codec=cls._CODEC,
            chunks=1,
            compressor=cmp,
            write_empty_chunks=False,
            fill_value=PARAM_DATA_NOT_SET,
        )
        parameter_data.create_dataset(
            name=cls._param_sources_arr_name,
            shape=0,
            dtype=object,
            object_codec=cls._CODEC,
            chunks=1000,  # TODO: check this is a sensible size with many parameters
            compressor=cmp,
        )
        parameter_data.create_group(name=cls._param_user_arr_grp_name)

        # for storing submission metadata that should not be stored in the root group:
        md.create_group(name=cls._subs_md_group_name)

        # for storing submission index, commands file ID, run file ID and idx for each
        # run:
        md.create_dataset(
            name=cls._run_sub_metadata_arr_name,
            shape=0,
            dtype=cls._RUN_SUB_DAT_DTYPE,
            chunks=100_000,
            compressor=cmp,
            fill_value=tuple(
                cls._RUN_SUB_DAT_FILL[key]
                for key in (
                    "submission_idx",
                    "commands_file_ID",
                    "run_file_ID",
                    "run_file_idx",
                )
            ),
        )

    def _append_tasks(self, tasks: Iterable[ZarrStoreTask]):
        elem_IDs_arr = self._get_tasks_arr(mode="r+")
        elem_IDs: list[int] = []
        with self.using_resource("attrs", "update") as attrs:
            for i_idx, i in enumerate(tasks):
                idx, wk_task_i, task_i = i.encode()
                elem_IDs.append(wk_task_i.pop("element_IDs"))
                wk_task_i["element_IDs_idx"] = len(elem_IDs_arr) + i_idx

                attrs["tasks"].insert(idx, wk_task_i)
                attrs["template"]["tasks"].insert(idx, task_i)
                attrs["num_added_tasks"] += 1

        # tasks array rows correspond to task IDs, and we assume `tasks` have sequentially
        # increasing IDs.
        append_items_to_ragged_array(arr=elem_IDs_arr, items=elem_IDs)

    def _append_loops(self, loops: dict[int, LoopDescriptor]):
        with self.using_resource("attrs", action="update") as attrs:
            for loop in loops.values():
                attrs["loops"].append(
                    {
                        "num_added_iterations": loop["num_added_iterations"],
                        "iterable_parameters": loop["iterable_parameters"],
                        "output_parameters": loop["output_parameters"],
                        "parents": loop["parents"],
                    }
                )
                attrs["template"]["loops"].append(loop["loop_template"])

    @staticmethod
    def _extract_submission_run_IDs_array(
        sub_js: Mapping[str, JSONed],
    ) -> tuple[np.ndarray, list[list[list[int]]]]:
        """For a JSON-like representation of a Submission object, remove and combine all
        jobscript-block run ID lists into a single array with a fill value.

        Notes
        -----
        This mutates `sub_js`, by setting `EAR_ID` jobscript-block keys to `None`.

        Parameters
        ----------
        sub_js
            JSON-like representation of a `Submission` object.

        Returns
        -------
        combined_run_IDs
            Integer Numpy array that contains a concatenation of all 2D run ID arrays
            from each jobscript-block. Technically a "jagged"/"ragged" array that is made
            square with a large fill value.
        block_shapes
            List of length equal to the number of jobscripts in the submission. Each
            sub-list contains a list of shapes (as a two-item list:
            `[num_actions, num_elements]`) of the constituent blocks of that jobscript.

        """
        arrs = []
        max_acts, max_elems = 0, 0

        # a list for each jobscript, containing shapes of run ID arrays in each block:
        block_shapes = []
        for js in cast("Sequence[Mapping[str, JSONed]]", sub_js["jobscripts"]):
            block_shapes_js_i = []
            for blk in cast("Sequence[MutableMapping[str, JSONed]]", js["blocks"]):
                run_IDs_i = np.array(blk["EAR_ID"])
                blk["EAR_ID"] = None  # TODO: how to type?
                block_shapes_js_i.append(list(run_IDs_i.shape))
                if run_IDs_i.shape[0] > max_acts:
                    max_acts = run_IDs_i.shape[0]
                if run_IDs_i.shape[1] > max_elems:
                    max_elems = run_IDs_i.shape[1]
                arrs.append(run_IDs_i)
            block_shapes.append(block_shapes_js_i)

        combined_run_IDs = np.full(
            (len(arrs), max_acts, max_elems),
            dtype=np.int32,
            fill_value=-1,
        )
        for arr_idx, arr in enumerate(arrs):
            combined_run_IDs[arr_idx][: arr.shape[0], : arr.shape[1]] = arr

        return combined_run_IDs, block_shapes

    @staticmethod
    def _extract_submission_task_elements_array(
        sub_js: Mapping[str, JSONed],
    ) -> tuple[np.ndarray, list[list[list[int]]]]:
        """For a JSON-like representation of a Submission object, remove and combine all
        jobscript-block task-element mappings into a single array with a fill value.

        Notes
        -----
        This mutates `sub_js`, by setting `task_elements` jobscript-block keys to `None`.

        Parameters
        ----------
        sub_js
            JSON-like representation of a `Submission` object.

        Returns
        -------
        combined_task_elems
            Integer Numpy array that contains a concatenation of each task-element,
            mapping, where each mapping is expressed as a 2D array whose first column
            corresponds to the keys of the mappings, and whose remaining columns
            correspond to the values of the mappings. Technically a "jagged"/"ragged"
            array that is made square with a large fill value.
        block_shapes
            List of length equal to the number of jobscripts in the submission. Each
            sub-list contains a list of shapes (as a two-item list:
            `[num_actions, num_elements]`) of the constituent blocks of that jobscript.

        """
        arrs = []
        max_x, max_y = 0, 0

        # a list for each jobscript, containing shapes of run ID arrays in each block:
        block_shapes = []
        for js in cast("Sequence[Mapping[str, JSONed]]", sub_js["jobscripts"]):
            block_shapes_js_i = []
            for blk in cast("Sequence[MutableMapping[str, JSONed]]", js["blocks"]):

                task_elems_lst = []
                for k, v in cast("Mapping[int, list[int]]", blk["task_elements"]).items():
                    task_elems_lst.append([k] + v)
                task_elems_i = np.array(task_elems_lst)

                block_shape_j = [task_elems_i.shape[1] - 1, task_elems_i.shape[0]]
                block_shapes_js_i.append(block_shape_j)

                blk["task_elements"] = None  # TODO: how to type?
                if task_elems_i.shape[1] > max_x:
                    max_x = task_elems_i.shape[1]
                if task_elems_i.shape[0] > max_y:
                    max_y = task_elems_i.shape[0]
                arrs.append(task_elems_i)
            block_shapes.append(block_shapes_js_i)

        combined_task_elems = np.full(
            (len(arrs), max_y, max_x),
            dtype=np.uint32,
            fill_value=np.iinfo(np.uint32).max,
        )
        for arr_idx, arr in enumerate(arrs):
            combined_task_elems[arr_idx][: arr.shape[0], : arr.shape[1]] = arr

        return combined_task_elems, block_shapes

    @staticmethod
    def _extract_submission_task_actions_array(
        sub_js: Mapping[str, JSONed],
    ) -> tuple[np.ndarray, list[list[int]]]:
        """For a JSON-like representation of a Submission object, remove and concatenate
        all jobscript-block task-action arrays into a single array.

        Notes
        -----
        This mutates `sub_js`, by setting `task_actions` jobscript-block keys to `None`.

        Parameters
        ----------
        sub_js
            JSON-like representation of a `Submission` object.

        Returns
        -------
        combined_task_acts
            Integer 2D Numpy array which is a concatenation along the first axis of
            task-action actions from all jobscript blocks. The second dimension is of
            length three.
        block_num_acts
            List of length equal to the number of jobscripts in the submission. Each
            sub-list contains a list of `num_actions` of the constituent blocks of that
            jobscript.

        """
        arrs = []

        # a list for each jobscript, containing shapes of run ID arrays in each block:

        blk_num_acts = []
        for js in cast("Sequence[Mapping[str, JSONed]]", sub_js["jobscripts"]):

            blk_num_acts_js_i = []
            for blk in cast("Sequence[MutableMapping[str, JSONed]]", js["blocks"]):

                blk_acts = np.array(blk["task_actions"])
                blk["task_actions"] = None  # TODO: how to type?
                blk_num_acts_js_i.append(blk_acts.shape[0])
                arrs.append(blk_acts)

            blk_num_acts.append(blk_num_acts_js_i)

        combined_task_acts = np.vstack(arrs)

        return combined_task_acts, blk_num_acts

    @staticmethod
    def _encode_jobscript_block_dependencies(sub_js: Mapping[str, JSONed]) -> np.ndarray:
        """For a JSON-like representation of a Submission object, remove jobscript-block
        dependencies for all jobscripts and transform to a single 1D integer array, that
        can be transformed back by `_decode_jobscript_block_dependencies`.

        Notes
        -----
        This mutates `sub_js`, by setting `dependencies` jobscript-block keys to `None`.
        """

        # TODO: avoid this horrible mess of casts

        all_deps_arr = []
        assert sub_js["jobscripts"] is not None
        for js in cast("Sequence[Mapping[str, JSONed]]", sub_js["jobscripts"]):
            for blk in cast("Sequence[MutableMapping[str, JSONed]]", js["blocks"]):
                all_deps_i: list[int] = []
                assert blk["dependencies"] is not None
                blk_deps = cast(
                    "list[tuple[tuple[int, int], Mapping[str, JSONed]]]",
                    blk["dependencies"],
                )
                for (dep_js_idx, dep_blk_idx), dep in blk_deps:
                    deps_arr: list[int] = []
                    for elem_i, elements_j in cast(
                        "Mapping[int, Sequence[int]]", dep["js_element_mapping"]
                    ).items():
                        deps_arr.extend([len(elements_j) + 1, elem_i] + list(elements_j))
                    blk_arr = [
                        dep_js_idx,
                        dep_blk_idx,
                        int(cast("bool", dep["is_array"])),
                    ] + deps_arr
                    blk_arr = [len(blk_arr)] + blk_arr
                    all_deps_i.extend(blk_arr)
                all_deps_i = [
                    cast("int", js["index"]),
                    cast("int", blk["index"]),
                ] + all_deps_i
                blk["dependencies"] = None  # TODO: how to type?
                all_deps_arr.extend([len(all_deps_i)] + all_deps_i)

        return np.array(all_deps_arr)

    @staticmethod
    def _decode_jobscript_block_dependencies(
        arr: np.ndarray,
    ) -> dict[tuple[int, int], dict[tuple[int, int], ResolvedJobscriptBlockDependencies]]:
        """Re-generate jobscript-block dependencies that have been transformed by
        `_encode_jobscript_block_dependencies` into a single 1D integer array.

        Parameters
        ----------
        arr:
            The 1D integer array to transform back to a verbose jobscript-block dependency
            mapping.
        """
        # metadata is js/blk_idx for which the dependencies are stored:
        block_arrs = split_arr(arr, metadata_size=2)
        block_deps = {}
        for i in block_arrs:

            js_idx: int
            blk_idx: int
            dep_js_idx: int
            dep_blk_idx: int
            is_array: int

            js_idx, blk_idx = i[0]
            # metadata is js/blk_idx that this block depends on, plus whether the
            # dependency is an array dependency:
            deps_arrs = split_arr(i[1], metadata_size=3)
            all_deps_ij: dict[tuple[int, int], ResolvedJobscriptBlockDependencies] = {}
            for j in deps_arrs:
                dep_js_idx, dep_blk_idx, is_array = j[0]
                # no metadata:
                elem_deps = split_arr(j[1], metadata_size=0)
                all_deps_ij[(dep_js_idx, dep_blk_idx)] = {
                    "js_element_mapping": {},
                    "is_array": bool(is_array),
                }
                for k in elem_deps:
                    all_deps_ij[(dep_js_idx, dep_blk_idx)]["js_element_mapping"].update(
                        {k[1][0]: list(k[1][1:])}
                    )

            block_deps[(js_idx, blk_idx)] = all_deps_ij
        return block_deps

    def _append_submissions(self, subs: dict[int, Mapping[str, JSONed]]):

        for sub_idx, sub_i in subs.items():

            # add a new metadata group for this submission:
            sub_grp = self._get_all_submissions_metadata_group(mode="r+").create_group(
                sub_idx
            )

            # add a new at-submit metadata array for jobscripts of this submission:
            num_js = len(cast("list", sub_i["jobscripts"]))
            sub_grp.create_dataset(
                name=self._js_at_submit_md_arr_name,
                shape=num_js,
                dtype=object,
                object_codec=MsgPack(),
                chunks=1,
                write_empty_chunks=False,
            )

            # add a new array to store run IDs for each jobscript:
            combined_run_IDs, block_shapes = self._extract_submission_run_IDs_array(sub_i)
            run_IDs_arr = sub_grp.create_dataset(
                name=self._js_run_IDs_arr_name,
                data=combined_run_IDs,
                chunks=(None, None, None),  # single chunk for the whole array
            )
            run_IDs_arr.attrs["block_shapes"] = block_shapes

            # add a new array to store task-element map for each jobscript:
            (
                combined_task_elems,
                block_shapes,
            ) = self._extract_submission_task_elements_array(sub_i)
            task_elems_arr = sub_grp.create_dataset(
                name=self._js_task_elems_arr_name,
                data=combined_task_elems,
                chunks=(None, None, None),
            )
            task_elems_arr.attrs["block_shapes"] = block_shapes

            # add a new array to store task-actions for each jobscript:
            (
                combined_task_acts,
                block_num_acts,
            ) = self._extract_submission_task_actions_array(sub_i)
            task_acts_arr = sub_grp.create_dataset(
                name=self._js_task_acts_arr_name,
                data=combined_task_acts,
                chunks=(None, None),
            )
            task_acts_arr.attrs["block_num_acts"] = block_num_acts

            # add a new array to store jobscript-block dependencies for this submission:
            sub_grp.create_dataset(
                name=self._js_deps_arr_name,
                data=self._encode_jobscript_block_dependencies(sub_i),
                chunks=(None,),
            )

            # TODO: store block shapes in `grp.attrs` since it is defined at the
            # submission level

            # add attributes for at-submit-time submission metadata:
            grp = self._get_submission_metadata_group(sub_idx, mode="r+")
            grp.attrs["submission_parts"] = {}

        with self.using_resource("attrs", action="update") as attrs:
            attrs["submissions"].extend(subs.values())

    def _append_task_element_IDs(self, task_ID: int, elem_IDs: list[int]):
        # I don't think there's a way to "append" to an existing array in a zarr ragged
        # array? So we have to build a new array from existing + new.
        arr = self._get_tasks_arr(mode="r+")
        elem_IDs_cur = arr[task_ID]
        elem_IDs_new = np.concatenate((elem_IDs_cur, elem_IDs))
        arr[task_ID] = elem_IDs_new

    @staticmethod
    def __as_dict(attrs: Attributes) -> ZarrAttrs:
        """
        Type thunk to work around incomplete typing in zarr.
        """
        return cast("ZarrAttrs", attrs.asdict())

    @contextmanager
    def __mutate_attrs(self, arr: Array | Group) -> Iterator[ZarrAttrs]:
        attrs_orig = self.__as_dict(arr.attrs)
        attrs = copy.deepcopy(attrs_orig)
        yield attrs
        if attrs != attrs_orig:
            arr.attrs.put(attrs)

    def _append_elements(self, elems: Sequence[ZarrStoreElement]):
        arr = self._get_elements_arr(mode="r+")
        with self.__mutate_attrs(arr) as attrs:
            arr_add = np.empty((len(elems)), dtype=object)
            arr_add[:] = [elem.encode(attrs) for elem in elems]
            arr.append(arr_add)

    def _append_element_sets(self, task_id: int, es_js: Sequence[Mapping]):
        task_idx = task_idx = self._get_task_id_to_idx_map()[task_id]
        with self.using_resource("attrs", "update") as attrs:
            attrs["template"]["tasks"][task_idx]["element_sets"].extend(es_js)

    def _append_elem_iter_IDs(self, elem_ID: int, iter_IDs: Iterable[int]):
        arr = self._get_elements_arr(mode="r+")
        attrs = self.__as_dict(arr.attrs)
        elem_dat = cast("list", arr[elem_ID])
        store_elem = ZarrStoreElement.decode(elem_dat, attrs)
        store_elem = store_elem.append_iteration_IDs(iter_IDs)
        arr[elem_ID] = store_elem.encode(attrs)
        # attrs shouldn't be mutated (TODO: test!)

    def _append_elem_iters(self, iters: Sequence[ZarrStoreElementIter]):
        arr = self._get_iters_arr(mode="r+")
        with self.__mutate_attrs(arr) as attrs:
            arr_add = np.empty((len(iters)), dtype=object)
            arr_add[:] = [i.encode(attrs) for i in iters]
            arr.append(arr_add)

    def _append_elem_iter_EAR_IDs(
        self, iter_ID: int, act_idx: int, EAR_IDs: Sequence[int]
    ):
        arr = self._get_iters_arr(mode="r+")
        attrs = self.__as_dict(arr.attrs)
        iter_dat = cast("list", arr[iter_ID])
        store_iter = ZarrStoreElementIter.decode(iter_dat, attrs)
        store_iter = store_iter.append_EAR_IDs(pend_IDs={act_idx: EAR_IDs})
        arr[iter_ID] = store_iter.encode(attrs)
        # attrs shouldn't be mutated (TODO: test!)

    def _update_elem_iter_EARs_initialised(self, iter_ID: int):
        arr = self._get_iters_arr(mode="r+")
        attrs = self.__as_dict(arr.attrs)
        iter_dat = cast("list", arr[iter_ID])
        store_iter = ZarrStoreElementIter.decode(iter_dat, attrs)
        store_iter = store_iter.set_EARs_initialised()
        arr[iter_ID] = store_iter.encode(attrs)
        # attrs shouldn't be mutated (TODO: test!)

    def _update_at_submit_metadata(
        self,
        at_submit_metadata: dict[int, dict[str, Any]],
    ):
        for sub_idx, metadata_i in at_submit_metadata.items():
            grp = self._get_submission_metadata_group(sub_idx, mode="r+")
            attrs = self.__as_dict(grp.attrs)
            attrs["submission_parts"].update(metadata_i["submission_parts"])
            grp.attrs.put(attrs)

    def __update_elem_iters(
        self,
        updates: Mapping[int, _UpdateT],
        update_meth: Callable[[ZarrStoreElementIter, _UpdateT], ZarrStoreElementIter],
    ) -> None:
        """Apply an update method to multiple element iterations in a single write."""
        arr = self._get_iters_arr(mode="r+")
        attrs = self.__as_dict(arr.attrs)
        iter_IDs = list(updates)
        iter_dat = arr.get_coordinate_selection(iter_IDs)

        values = np.empty(len(iter_IDs), dtype=object)
        for idx, iter_ID_i in enumerate(iter_IDs):
            store_iter = ZarrStoreElementIter.decode(iter_dat[idx], attrs)
            values[idx] = update_meth(store_iter, updates[iter_ID_i]).encode(attrs)

        arr.set_coordinate_selection(np.asarray(iter_IDs), values)

    def _update_loop_index(self, loop_indices: dict[int, dict[str, int]]):
        self.__update_elem_iters(
            loop_indices,
            lambda elem_iter, update: elem_iter.update_loop_idx(update),
        )

    def _update_loop_num_iters(self, index: int, num_iters: list[list[list[int] | int]]):
        with self.using_resource("attrs", action="update") as attrs:
            attrs["loops"][index]["num_added_iterations"] = num_iters

    def _update_loop_parents(self, index: int, parents: list[str]):
        with self.using_resource("attrs", action="update") as attrs:
            attrs["loops"][index]["parents"] = parents

    def _update_iter_data_indices(self, iter_data_indices: dict[int, DataIndex]):
        self.__update_elem_iters(
            iter_data_indices,
            lambda elem_iter, update: elem_iter.update_data_idx(update),
        )

    def _update_run_data_indices(self, run_data_indices: dict[int, DataIndex]):
        self._update_run_metadata(
            updates={k: {"data_idx": v} for k, v in run_data_indices.items()}
        )

    @TimeIt.decorator
    def _append_EARs(self, EARs: Sequence[ZarrStoreEAR]):
        if not EARs:
            return

        arr = self._get_run_metadata_arr(mode="r+")
        with self.__mutate_attrs(arr) as attrs:
            arr_add = np.empty((len(EARs)), dtype=object)
            arr_add[:] = [run.encode_creation_metadata(attrs=attrs) for run in EARs]
            arr.append(arr_add)

        max_run_ID = max(run.id_ for run in EARs)
        required_size = max_run_ID + 1

        sub_dat_arr = self._get_EARs_sub_dat_arr(mode="r+")
        if required_size > sub_dat_arr.shape[0]:
            sub_dat_arr.resize(required_size)

        # add more rows to run dirs array:
        dirs_arr = self._get_dirs_arr(mode="r+")
        if required_size > dirs_arr.shape[0]:
            dirs_arr.resize(required_size)

    def _set_run_dirs(self, run_dir_arr: np.ndarray, run_idx: np.ndarray):
        dirs_arr = self._get_dirs_arr(mode="r+")
        dirs_arr[run_idx] = run_dir_arr

    @TimeIt.decorator
    def _update_run_execution_metadata(self, updates: dict[int, dict[str, Any]]):
        """Update execution-time metadata for existing runs."""

        run_file_lookup = self._get_run_file_lookup(updates)

        run_exec_data = defaultdict(lambda: defaultdict(dict))

        for submission_idx, files in run_file_lookup.items():
            for file_ID, indices in files.items():
                for run_id, run_idx in indices.items():
                    run_exec_data[submission_idx][file_ID][run_idx] = {
                        ZarrStoreEAR.EXEC_TIME_ATTRIBUTES.index(key): value
                        for key, value in updates[run_id].items()
                    }

        self.write_run_files(
            {
                submission_idx: dict(files)
                for submission_idx, files in run_exec_data.items()
            }
        )

    @TimeIt.decorator
    def _update_run_metadata(self, updates: dict[int, dict[str, Any]]):
        run_IDs = list(updates)
        runs = self._get_persistent_EARs(run_IDs)
        arr_updates = np.empty((len(runs)), dtype=object)
        arr = self._get_run_metadata_arr(mode="r+")
        with self.__mutate_attrs(arr) as attrs:
            arr_updates[:] = [
                runs[run_ID].update(**upd).encode_creation_metadata(attrs=attrs)
                for run_ID, upd in updates.items()
            ]
            arr.set_coordinate_selection((run_IDs,), arr_updates)

    @TimeIt.decorator
    def _update_EAR_submission_data(
        self, sub_data: Mapping[int, tuple[int, int | None, int]]
    ):
        sub_data = {
            run_ID: (
                sub_idx,
                (
                    cmd_ID
                    if cmd_ID is not None
                    else self._RUN_SUB_DAT_FILL["commands_file_ID"]
                ),
                run_file_ID,
                run_file_idx,
            )
            for run_ID, (sub_idx, cmd_ID, run_file_ID, run_file_idx) in sub_data.items()
        }

        arr = self._get_EARs_sub_dat_arr(mode="r+")

        required_size = max(sub_data) + 1
        if required_size > arr.shape[0]:
            arr.resize(required_size)

        sub_run_IDs = np.fromiter(sub_data, dtype=np.uint32, count=len(sub_data))
        sub_dat_values = np.empty(len(sub_run_IDs), dtype=self._RUN_SUB_DAT_DTYPE)
        for i, run_ID in enumerate(sub_run_IDs):
            sub_dat_values[i] = sub_data[int(run_ID)]

        arr.set_coordinate_selection((sub_run_IDs,), sub_dat_values)

    def _update_EAR_start(
        self,
        run_starts: dict[int, tuple[datetime, dict[str, Any] | None, str, int | None]],
    ):
        self._update_run_execution_metadata(
            updates={
                k: {
                    "start_time": v[0],
                    "snapshot_start": v[1],
                    "run_hostname": v[2],
                    "port_number": v[3],
                }
                for k, v in run_starts.items()
            }
        )

    def _update_EAR_end(
        self, run_ends: dict[int, tuple[datetime, dict[str, Any] | None, int, bool]]
    ):
        self._update_run_execution_metadata(
            updates={
                k: {
                    "end_time": v[0],
                    "snapshot_end": v[1],
                    "exit_code": v[2],
                    "success": v[3],
                }
                for k, v in run_ends.items()
            }
        )

    def _update_EAR_skip(self, skips: dict[int, int]):
        self._update_run_execution_metadata(
            updates={k: {"skip": v} for k, v in skips.items()}
        )

    def _update_js_metadata(self, js_meta: dict[int, dict[int, dict[str, Any]]]):

        arr_keys = JOBSCRIPT_SUBMIT_TIME_KEYS  # these items go to the Zarr array

        # split into attributes to save to the root group metadata, and those to save to
        # the submit-time jobscript metadata array

        grp_dat = {}  # keys are tuples of (sub_idx, js_idx), values are metadata dicts

        for sub_idx, all_js_md in js_meta.items():
            js_arr = None
            for js_idx, js_meta_i in all_js_md.items():

                grp_dat_i = {k: v for k, v in js_meta_i.items() if k not in arr_keys}
                if grp_dat_i:
                    grp_dat[(sub_idx, js_idx)] = grp_dat_i
                arr_dat = [js_meta_i.get(k) for k in arr_keys]

                if any(arr_dat):
                    # we are updating the at-sumbmit metadata, so clear the cache:
                    self.clear_jobscript_at_submit_metadata_cache()

                    js_arr = js_arr or self._get_jobscripts_at_submit_metadata_arr(
                        mode="r+", sub_idx=sub_idx
                    )
                    self.logger.info(
                        f"updating submit-time jobscript metadata array: {arr_dat!r}."
                    )
                    js_arr[js_idx] = arr_dat

        if grp_dat:
            with self.using_resource("attrs", action="update") as attrs:
                for (sub_idx, js_idx), js_meta_i in grp_dat.items():
                    self.logger.info(
                        f"updating jobscript metadata in the root group for "
                        f"(sub={sub_idx}, js={js_idx}): {js_meta_i!r}."
                    )
                    sub = cast(
                        "dict[str, list[dict[str, Any]]]", attrs["submissions"][sub_idx]
                    )
                    sub["jobscripts"][js_idx].update(js_meta_i)

    def _append_parameters(self, params: Sequence[StoreParameter]):
        """Add new persistent parameters."""
        self._ensure_all_encoders()
        base_arr = self._get_parameter_base_array(mode="r+", write_empty_chunks=False)
        src_arr = self._get_parameter_sources_array(mode="r+")
        self.logger.debug(
            f"PersistentStore._append_parameters: adding {len(params)} parameters."
        )

        param_encode_root_group = self._get_parameter_user_array_group(mode="r+")
        param_enc: list[dict[str, Any] | int] = []
        src_enc: list[dict] = []
        for param_i in params:
            dat_i = param_i.encode(
                root_group=param_encode_root_group,
                arr_path=self._param_data_arr_grp_name(param_i.id_),
            )
            param_enc.append(dat_i)
            src_enc.append(dict(sorted(param_i.source.items())))

        base_arr.append(param_enc)
        src_arr.append(src_enc)
        self.logger.debug(
            f"PersistentStore._append_parameters: finished adding {len(params)} parameters."
        )

    def _set_parameter_values(self, set_parameters: dict[int, tuple[Any, bool]]):
        """Set multiple unset persistent parameters."""
        self._ensure_all_encoders()
        param_ids = list(set_parameters)
        # the `decode` call in `_get_persistent_parameters` should be quick:
        params = self._get_persistent_parameters(param_ids)
        new_data: list[dict[str, Any] | int] = []
        param_encode_root_group = self._get_parameter_user_array_group(mode="r+")
        for param_id, (value, is_file) in set_parameters.items():
            param_i = params[param_id]
            if is_file:
                param_i = param_i.set_file(value)
            else:
                param_i = param_i.set_data(value)

            new_data.append(
                param_i.encode(
                    root_group=param_encode_root_group,
                    arr_path=self._param_data_arr_grp_name(param_i.id_),
                )
            )

        # no need to update sources array:
        base_arr = self._get_parameter_base_array(mode="r+")
        base_arr.set_coordinate_selection(param_ids, new_data)

    def _update_parameter_sources(self, sources: Mapping[int, ParamSource]):
        """Update the sources of multiple persistent parameters."""

        param_ids = list(sources)
        src_arr = self._get_parameter_sources_array(mode="r+")
        existing_sources = src_arr.get_coordinate_selection(param_ids)
        new_sources = [
            update_param_source_dict(cast("ParamSource", existing_sources[idx]), source_i)
            for idx, source_i in enumerate(sources.values())
        ]
        src_arr.set_coordinate_selection(param_ids, new_sources)

    def _update_template_components(self, tc: dict[str, Any]):
        with self.using_resource("attrs", "update") as md:
            md["template_components"] = tc

    @TimeIt.decorator
    def _get_num_persistent_tasks(self) -> int:
        """Get the number of persistent tasks."""
        if self.use_cache and self.num_tasks_cache is not None:
            num = self.num_tasks_cache
        else:
            num = len(self._get_tasks_arr())
        if self.use_cache and self.num_tasks_cache is None:
            self.num_tasks_cache = num
        return num

    def _get_num_persistent_loops(self) -> int:
        """Get the number of persistent loops."""
        with self.using_resource("attrs", "read") as attrs:
            return len(attrs["loops"])

    def _get_num_persistent_submissions(self) -> int:
        """Get the number of persistent submissions."""
        with self.using_resource("attrs", "read") as attrs:
            return len(attrs["submissions"])

    def _get_num_persistent_elements(self) -> int:
        """Get the number of persistent elements."""
        return len(self._get_elements_arr())

    def _get_num_persistent_elem_iters(self) -> int:
        """Get the number of persistent element iterations."""
        return len(self._get_iters_arr())

    @TimeIt.decorator
    def _get_num_persistent_EARs(self) -> int:
        """Get the number of persistent EARs."""
        if self.use_cache and self.num_EARs_cache is not None:
            num = self.num_EARs_cache
        else:
            num = len(self._get_run_metadata_arr())
        if self.use_cache and self.num_EARs_cache is None:
            self.num_EARs_cache = num
        return num

    @TimeIt.decorator
    def _get_num_persistent_parameters(self):
        if self.use_cache and self.num_params_cache is not None:
            num = self.num_params_cache
        else:
            num = len(self._get_parameter_base_array())
        if self.use_cache and self.num_params_cache is None:
            self.num_params_cache = num
        return num

    def _get_num_persistent_added_tasks(self):
        with self.using_resource("attrs", "read") as attrs:
            return attrs["num_added_tasks"]

    @property
    def zarr_store(self) -> Store:
        """
        The underlying store object.
        """
        if self._zarr_store is None:
            assert self.fs is not None
            self._zarr_store = self._get_zarr_store(self.path, self.fs)
        return self._zarr_store

    def _get_root_group(self, mode: str = "r", **kwargs) -> Group:
        # TODO: investigate if there are inefficiencies in how we retrieve zarr groups
        # and arrays, e.g. opening sub groups sequentially would open the root group
        # multiple times, and so read the root group attrs file multiple times?
        # it might make sense to define a ZarrAttrsStoreResource for each zarr group and
        # array (or at least non-parameter groups/arrays?), there could be some built-in
        # understanding of the hierarchy (e.g. via a `path` attribute) which would then
        # avoid reading parent groups multiple times --- if that is happening currently.
        return zarr.open(self.zarr_store, mode=mode, **kwargs)

    def _get_parameter_group(self, mode: str = "r", **kwargs) -> Group:
        return self._get_root_group(mode=mode, **kwargs).get(self._param_grp_name)

    def _get_parameter_base_array(self, mode: str = "r", **kwargs) -> Array:
        path = f"{self._param_grp_name}/{self._param_base_arr_name}"
        return zarr.open(self.zarr_store, mode=mode, path=path, **kwargs)

    def _get_parameter_sources_array(self, mode: str = "r") -> Array:
        return self._get_parameter_group(mode=mode).get(self._param_sources_arr_name)

    def _get_parameter_user_array_group(self, mode: str = "r") -> Group:
        return self._get_parameter_group(mode=mode).get(self._param_user_arr_grp_name)

    def _get_parameter_data_array_group(
        self,
        parameter_idx: int,
        mode: str = "r",
    ) -> Group:
        return self._get_parameter_user_array_group(mode=mode).get(
            self._param_data_arr_grp_name(parameter_idx)
        )

    def _get_array_group_and_dataset(
        self, mode: str, param_id: int, data_path: list[int]
    ):
        base_dat = self._get_parameter_base_array(mode="r")[param_id]
        for arr_dat_path, arr_idx in base_dat["type_lookup"]["arrays"]:
            if arr_dat_path == data_path:
                break
        else:
            raise ValueError(
                f"Could not find array path {data_path} in the base data for parameter "
                f"ID {param_id}."
            )
        group = self._get_parameter_user_array_group(mode=mode).get(
            f"{self._param_data_arr_grp_name(param_id)}"
        )
        return group, f"arr_{arr_idx}"

    def _get_metadata_group(self, mode: str = "r") -> Group:
        try:
            path = Path(self.workflow.url).joinpath("metadata")
            md_store = zarr.NestedDirectoryStore(path)
            return zarr.open_group(store=md_store, mode=mode)
        except (FileNotFoundError, zarr.errors.GroupNotFoundError):
            # zip store?
            return zarr.open_group(self.zarr_store, path="metadata", mode=mode)

    def _get_all_submissions_metadata_group(self, mode: str = "r") -> Group:
        return self._get_metadata_group(mode=mode).get(self._subs_md_group_name)

    def _get_submission_metadata_group(self, sub_idx: int, mode: str = "r") -> Group:
        return self._get_all_submissions_metadata_group(mode=mode).get(sub_idx)

    def _get_submission_metadata_group_path(self, sub_idx: int) -> Path:
        grp = self._get_submission_metadata_group(sub_idx)
        return Path(grp.store.path).joinpath(grp.path)

    def _get_jobscripts_at_submit_metadata_arr(
        self, sub_idx: int, mode: str = "r"
    ) -> Array:
        return self._get_submission_metadata_group(sub_idx=sub_idx, mode=mode).get(
            self._js_at_submit_md_arr_name
        )

    def _get_jobscripts_at_submit_metadata_arr_path(self, sub_idx: int) -> Path:
        arr = self._get_jobscripts_at_submit_metadata_arr(sub_idx)
        return Path(arr.store.path).joinpath(arr.path)

    @TimeIt.decorator
    def _get_jobscripts_run_ID_arr(self, sub_idx: int, mode: str = "r") -> Array:
        return self._get_submission_metadata_group(sub_idx=sub_idx, mode=mode).get(
            self._js_run_IDs_arr_name
        )

    def _get_jobscripts_task_elements_arr(self, sub_idx: int, mode: str = "r") -> Array:
        return self._get_submission_metadata_group(sub_idx=sub_idx, mode=mode).get(
            self._js_task_elems_arr_name
        )

    def _get_jobscripts_task_actions_arr(self, sub_idx: int, mode: str = "r") -> Array:
        return self._get_submission_metadata_group(sub_idx=sub_idx, mode=mode).get(
            self._js_task_acts_arr_name
        )

    def _get_jobscripts_dependencies_arr(self, sub_idx: int, mode: str = "r") -> Array:
        return self._get_submission_metadata_group(sub_idx=sub_idx, mode=mode).get(
            self._js_deps_arr_name
        )

    def _get_tasks_arr(self, mode: str = "r") -> Array:
        return self._get_metadata_group(mode=mode).get(self._task_arr_name)

    def _get_elements_arr(self, mode: str = "r") -> Array:
        return self._get_metadata_group(mode=mode).get(self._elem_arr_name)

    def _get_iters_arr(self, mode: str = "r") -> Array:
        return self._get_metadata_group(mode=mode).get(self._iter_arr_name)

    def _get_run_metadata_arr(self, mode: str = "r") -> Array:
        return self._get_metadata_group(mode=mode).get(self._run_metadata_arr_name)

    def _get_EARs_sub_dat_arr(self, mode: str = "r") -> Array:
        return self._get_metadata_group(mode=mode).get(self._run_sub_metadata_arr_name)

    def _get_dirs_arr(self, mode: str = "r") -> zarr.Array:
        return self._get_metadata_group(mode=mode).get(self._run_dir_arr_name)

    @classmethod
    def make_test_store_from_spec(
        cls,
        spec,
        dir=None,
        path="test_store",
        overwrite=False,
    ):
        """Generate an store for testing purposes."""
        ts_fmt = "FIXME"

        path = Path(dir or "", path)
        root = zarr.group(store=DirectoryStore(path), overwrite=overwrite)
        md = root.create_group("metadata")

        tasks_arr = md.create_dataset(
            name=cls._task_arr_name,
            shape=0,
            dtype=object,
            object_codec=VLenArray(int),
        )

        elems_arr = md.create_dataset(
            name=cls._elem_arr_name,
            shape=0,
            dtype=object,
            object_codec=cls._CODEC,
            chunks=1000,
        )
        elems_arr.attrs.update({"seq_idx": [], "src_idx": []})

        elem_iters_arr = md.create_dataset(
            name=cls._iter_arr_name,
            shape=0,
            dtype=object,
            object_codec=cls._CODEC,
            chunks=1000,
        )
        elem_iters_arr.attrs.update(
            {
                "loops": [],
                "schema_parameters": [],
                "parameter_paths": [],
            }
        )

        tasks, elems, elem_iters, EARs_ = super().prepare_test_store_from_spec(spec)

        path = Path(path).resolve()
        tasks = [ZarrStoreTask(**i).encode() for i in tasks]
        elements = [ZarrStoreElement(**i).encode(elems_arr.attrs.asdict()) for i in elems]
        elem_iters = [
            ZarrStoreElementIter(**i).encode(elem_iters_arr.attrs.asdict())
            for i in elem_iters
        ]

        append_items_to_ragged_array(tasks_arr, tasks)

        elems_arr.append(np.fromiter(elements, dtype=object))
        elem_iters_arr.append(np.fromiter(elem_iters, dtype=object))

        return cls(path)

    def _get_persistent_template_components(self):
        with self.using_resource("attrs", "read") as attrs:
            return attrs["template_components"]

    def _get_persistent_template(self) -> dict[str, JSONed]:
        with self.using_resource("attrs", "read") as attrs:
            return cast("dict[str, JSONed]", attrs["template"])

    @TimeIt.decorator
    def _get_persistent_tasks(self, id_lst: Iterable[int]) -> dict[int, ZarrStoreTask]:
        tasks, id_lst = self._get_cached_persistent_tasks(id_lst)
        if id_lst:
            with self.using_resource("attrs", action="read") as attrs:
                task_dat: dict[int, dict[str, Any]] = {}
                elem_IDs: list[int] = []
                i: dict[str, Any]
                for idx, i in enumerate(attrs["tasks"]):
                    i = copy.deepcopy(i)
                    elem_IDs.append(i.pop("element_IDs_idx"))
                    if id_lst is None or i["id_"] in id_lst:
                        task_dat[i["id_"]] = {**i, "index": idx}
            if task_dat:
                try:
                    elem_IDs_arr_dat = self._get_tasks_arr().get_coordinate_selection(
                        elem_IDs
                    )
                except BoundsCheckError:
                    raise MissingStoreTaskError(
                        elem_IDs
                    ) from None  # TODO: not an ID list

                new_tasks = {
                    id_: ZarrStoreTask.decode({**i, "element_IDs": elem_IDs_arr_dat[id_]})
                    for id_, i in task_dat.items()
                }
                self.task_cache.update(new_tasks)
                tasks.update(new_tasks)
        return tasks

    @TimeIt.decorator
    def _get_persistent_loops(
        self, id_lst: Iterable[int] | None = None
    ) -> dict[int, LoopDescriptor]:
        with self.using_resource("attrs", "read") as attrs:
            return {
                idx: cast("LoopDescriptor", i)
                for idx, i in enumerate(attrs["loops"])
                if id_lst is None or idx in id_lst
            }

    @TimeIt.decorator
    def _get_persistent_submissions(
        self, id_lst: Iterable[int] | None = None
    ) -> dict[int, Mapping[str, JSONed]]:
        self.logger.debug("loading persistent submissions from the zarr store")
        ids = set(id_lst or ())
        with self.using_resource("attrs", "read") as attrs:
            subs_dat = copy.deepcopy(
                {
                    idx: i
                    for idx, i in enumerate(attrs["submissions"])
                    if id_lst is None or idx in ids
                }
            )

        return subs_dat

    @TimeIt.decorator
    def _get_persistent_elements(
        self, id_lst: Iterable[int]
    ) -> dict[int, ZarrStoreElement]:
        elems, id_lst = self._get_cached_persistent_elements(id_lst)
        if id_lst:
            self.logger.debug(
                f"loading {len(id_lst)} persistent element(s) from disk: "
                f"{shorten_list_str(id_lst)}."
            )
            arr = self._get_elements_arr()
            attrs = arr.attrs.asdict()
            try:
                elem_arr_dat = arr.get_coordinate_selection(id_lst)
            except BoundsCheckError:
                raise MissingStoreElementError(id_lst) from None
            elem_dat = dict(zip(id_lst, elem_arr_dat))
            new_elems = {
                k: ZarrStoreElement.decode(v, attrs) for k, v in elem_dat.items()
            }
            self.element_cache.update(new_elems)
            elems.update(new_elems)
        return elems

    @TimeIt.decorator
    def _get_persistent_element_iters(
        self, id_lst: Iterable[int]
    ) -> dict[int, ZarrStoreElementIter]:
        iters, id_lst = self._get_cached_persistent_element_iters(id_lst)
        if id_lst:
            self.logger.debug(
                f"loading {len(id_lst)} persistent element iteration(s) from disk: "
                f"{shorten_list_str(id_lst)}."
            )
            arr = self._get_iters_arr()
            attrs = arr.attrs.asdict()
            try:
                iter_arr_dat = arr.get_coordinate_selection(id_lst)
            except BoundsCheckError:
                raise MissingStoreElementIterationError(id_lst) from None
            iter_dat = dict(zip(id_lst, iter_arr_dat))
            new_iters = {
                k: ZarrStoreElementIter.decode(v, attrs) for k, v in iter_dat.items()
            }
            self.element_iter_cache.update(new_iters)
            iters.update(new_iters)
        return iters

    @TimeIt.decorator
    def _get_run_submission_metadata(
        self, id_lst: Iterable[int]
    ) -> dict[int, tuple[int | None, ...]]:
        """Get the run file IDs for the provided runs."""
        runs, id_lst = self._get_cached_persistent_EARs(id_lst)
        sub_dat = {
            id_i: (
                run_i.submission_idx,
                run_i.commands_file_ID,
                run_i.run_file_ID,
                run_i.run_file_idx,
            )
            for id_i, run_i in runs.items()
        }
        if id_lst:
            self.logger.debug(
                f"loading {len(id_lst)} persistent run submission metadata from disk: "
                f"{shorten_list_str(id_lst)}."
            )
            # retrieve submission idx, commands file ID, and run file ID for each run:
            sub_dat_arr = self._get_EARs_sub_dat_arr()
            try:
                run_sub_dat = sub_dat_arr[id_lst]
            except BoundsCheckError:
                raise MissingStoreEARError(id_lst) from None

            for id_i, sub_dat_i in zip(id_lst, run_sub_dat):
                sub_idx, cmd_ID, run_file_ID, run_file_idx = sub_dat_i
                if sub_idx == self._RUN_SUB_DAT_FILL["submission_idx"]:
                    sub_idx = None
                if cmd_ID == self._RUN_SUB_DAT_FILL["commands_file_ID"]:
                    cmd_ID = None
                if run_file_ID == self._RUN_SUB_DAT_FILL["run_file_ID"]:
                    run_file_ID = None
                if run_file_idx == self._RUN_SUB_DAT_FILL["run_file_idx"]:
                    run_file_idx = None
                sub_dat[id_i] = (sub_idx, cmd_ID, run_file_ID, run_file_idx)

        return sub_dat

    def _get_run_multi_dir_path(self, submission_idx: int) -> Path:
        return (
            Path(self.workflow.url)
            / "metadata"
            / self._run_multi_process_dir_name
            / str(submission_idx)
        )

    @TimeIt.decorator
    def read_run_files(
        self,
        run_file_lookup: dict[int, dict[int, dict[int, int]]],
    ):
        """
        Parameters
        ----------
        run_file_lookup
            Keys are submission indices. Values map file IDs to dictionaries
            mapping run IDs to indices within those files.
        """

        data = {}

        for submission_idx, files in run_file_lookup.items():
            prefix = self._get_run_multi_dir_path(submission_idx)

            file_IDs = list(files)
            paths = get_run_multi_chunk_path(idx=file_IDs, prefix=prefix)

            for path, indices in zip(paths, files.values()):
                if path.exists():
                    with open(path, "rb") as f:
                        runs = msgpack.unpackb(f.read())

                    for run_id, idx in indices.items():
                        if idx >= len(runs):
                            data[run_id] = None
                        else:
                            data[run_id] = runs[idx]

                else:
                    for run_id in indices:
                        data[run_id] = None

        return data

    @TimeIt.decorator
    def write_run_files(
        self,
        data: dict[int, dict[int, dict[int, dict[int, Any]]]],
    ):
        """
        Parameters
        ----------
        data
            Keys are submission indices.

            Values map file IDs to dictionaries mapping indices within those
            files to dictionaries of execution-metadata field indices and their
            new values.

            Structure should be: like
                dict[
                    int,  # submission_idx
                    dict[
                        int,  # file_ID
                        dict[
                            int,  # local index
                            dict[int, Any],  # field index -> value
                        ],
                    ],
                ]
        """

        for submission_idx, files in data.items():
            prefix = self._get_run_multi_dir_path(submission_idx)

            file_IDs = list(files)
            paths = get_run_multi_chunk_path(idx=file_IDs, prefix=prefix)

            for path, data_i in zip(paths, files.values()):
                path.parent.mkdir(exist_ok=True, parents=True)

                if path.exists():
                    with open(path, "rb") as f:
                        runs = msgpack.unpackb(f.read())
                else:
                    runs = []

                max_idx = max(data_i)

                if len(runs) <= max_idx:
                    num_new = max_idx + 1 - len(runs)
                    runs.extend([None] * num_new)

                for idx, upd_data in data_i.items():
                    if runs[idx] is None:
                        runs[idx] = ZarrStoreEAR.encode_run_time_metadata({}, self.ts_fmt)

                    enc_val = ZarrStoreEAR.encode_run_time_metadata(upd_data, self.ts_fmt)

                    for upd_idx in upd_data:
                        runs[idx][upd_idx] = enc_val[upd_idx]

                encoded = msgpack.packb(runs)
                atomic_write(path, encoded)

    def _get_run_file_lookup(
        self,
        id_lst: Iterable[int],
        submission_metadata: dict[int, tuple[int | None, ...]] | None = None,
    ) -> dict[int, dict[int, dict[int, int]]]:
        sub_dat = submission_metadata or self._get_run_submission_metadata(id_lst)

        run_file_lookup = defaultdict(lambda: defaultdict(dict))

        for run_id, sub_dat_i in sub_dat.items():
            submission_idx = sub_dat_i[0]
            file_ID = sub_dat_i[2]
            file_idx = sub_dat_i[3]

            if file_ID is None:
                continue

            assert submission_idx is not None
            assert file_idx is not None

            run_file_lookup[int(submission_idx)][int(file_ID)][int(run_id)] = int(
                file_idx
            )

        return {
            submission_idx: dict(files)
            for submission_idx, files in run_file_lookup.items()
        }

    @TimeIt.decorator
    def _get_persistent_EARs(self, id_lst: Iterable[int]) -> dict[int, ZarrStoreEAR]:
        runs, id_lst = self._get_cached_persistent_EARs(id_lst)
        if id_lst:
            self.logger.debug(
                f"loading {len(id_lst)} persistent EAR(s) from disk: "
                f"{shorten_list_str(id_lst)}."
            )

            sub_dat = self._get_run_submission_metadata(id_lst)

            # load execution-time metadata:
            run_file_lookup = self._get_run_file_lookup(
                id_lst, submission_metadata=sub_dat
            )
            run_exec_dat = self.read_run_files(run_file_lookup)

            arr = self._get_run_metadata_arr()

            try:
                run_dat = arr[id_lst]
            except BoundsCheckError:
                raise MissingStoreEARError(id_lst) from None

            attrs = arr.attrs.asdict()
            new_runs: dict[int, ZarrStoreEAR] = {}
            for id_i, run_dat_i in zip(id_lst, run_dat):
                new_runs[id_i] = ZarrStoreEAR.decode(
                    EAR_dat=run_dat_i,
                    sub_dat=sub_dat[id_i],
                    run_time_dat=run_exec_dat.get(id_i),
                    ts_fmt=self.ts_fmt,
                    attrs=attrs,
                )

            self.EAR_cache.update(new_runs)
            runs.update(new_runs)

        return runs

    @TimeIt.decorator
    def _get_persistent_parameters(
        self, id_lst: Iterable[int], *, dataset_copy: bool = False, **kwargs
    ) -> dict[int, ZarrStoreParameter]:
        self._ensure_all_decoders()
        params, id_lst = self._get_cached_persistent_parameters(id_lst)
        if id_lst:

            self.logger.debug(
                f"loading {len(id_lst)} persistent parameter(s) from disk: "
                f"{shorten_list_str(id_lst)}."
            )

            # TODO: implement the "parameter_metadata_cache" for zarr stores, which would
            # keep the base_arr and src_arr open
            base_arr = self._get_parameter_base_array(mode="r")
            src_arr = self._get_parameter_sources_array(mode="r")

            try:
                param_arr_dat = base_arr.get_coordinate_selection(list(id_lst))
                src_arr_dat = src_arr.get_coordinate_selection(list(id_lst))
            except BoundsCheckError:
                raise MissingParameterData(id_lst) from None

            param_dat = dict(zip(id_lst, param_arr_dat))
            src_dat = dict(zip(id_lst, src_arr_dat))

            new_params = {
                k: ZarrStoreParameter.decode(
                    id_=k,
                    data=v,
                    source=src_dat[k],
                    arr_group=self._get_parameter_data_array_group(k),
                    dataset_copy=dataset_copy,
                )
                for k, v in param_dat.items()
            }
            self.parameter_cache.update(new_params)
            params.update(new_params)

        return params

    @TimeIt.decorator
    def _get_persistent_param_sources(
        self, id_lst: Iterable[int]
    ) -> dict[int, ParamSource]:
        sources, id_lst = self._get_cached_persistent_param_sources(id_lst)
        if id_lst:
            src_arr = self._get_parameter_sources_array(mode="r")
            try:
                src_arr_dat = src_arr.get_coordinate_selection(list(id_lst))
            except BoundsCheckError:
                raise MissingParameterData(id_lst) from None
            new_sources = dict(zip(id_lst, src_arr_dat))
            self.param_sources_cache.update(new_sources)
            sources.update(new_sources)
        return sources

    def _get_persistent_parameter_set_status(
        self, id_lst: Iterable[int]
    ) -> dict[int, bool]:
        base_arr = self._get_parameter_base_array(mode="r")
        try:
            param_arr_dat = base_arr.get_coordinate_selection(list(id_lst))
        except BoundsCheckError:
            raise MissingParameterData(id_lst) from None

        return dict(zip(id_lst, [i is not None for i in param_arr_dat]))

    def _get_persistent_parameter_IDs(self) -> list[int]:
        # we assume the row index is equivalent to ID, might need to revisit in future
        base_arr = self._get_parameter_base_array(mode="r")
        return list(range(len(base_arr)))

    def get_submission_at_submit_metadata(
        self, sub_idx: int, metadata_attr: dict | None
    ) -> dict[str, Any]:
        """Retrieve the values of submission attributes that are stored at submit-time."""
        grp = self._get_submission_metadata_group(sub_idx)
        attrs = grp.attrs.asdict()
        return {k: attrs[k] for k in SUBMISSION_SUBMIT_TIME_KEYS}

    def clear_jobscript_at_submit_metadata_cache(self):
        """Clear the cache of at-submit-time jobscript metadata."""
        self._jobscript_at_submit_metadata = {}

    def get_jobscript_at_submit_metadata(
        self,
        sub_idx: int,
        js_idx: int,
        metadata_attr: dict | None,
    ) -> dict[str, Any]:
        """For the specified jobscript, retrieve the values of jobscript-submit-time
        attributes.

        Notes
        -----
        If the cache does not exist, this method will retrieve and cache metadata for
        all jobscripts for which metadata has been set. If the cache does exist, but not
        for the requested jobscript, then this method will retrieve and cache metadata for
        all non-cached jobscripts for which metadata has been set. If metadata has not
        yet been set for the specified jobscript, and dict with all `None` values will be
        returned.

        The cache can be cleared using the method
        `clear_jobscript_at_submit_metadata_cache`.

        """
        if self._jobscript_at_submit_metadata:
            # cache exists, but might not include data for the requested jobscript:
            if js_idx in self._jobscript_at_submit_metadata:
                return self._jobscript_at_submit_metadata[js_idx]

        arr = self._get_jobscripts_at_submit_metadata_arr(sub_idx)
        non_cached = set(range(len(arr))) - set(self._jobscript_at_submit_metadata.keys())

        # populate cache:
        arr_non_cached = arr.get_coordinate_selection((list(non_cached),))
        for js_idx_i, arr_item in zip(non_cached, arr_non_cached):
            try:
                self._jobscript_at_submit_metadata[js_idx_i] = {
                    i: arr_item[i_idx]
                    for i_idx, i in enumerate(JOBSCRIPT_SUBMIT_TIME_KEYS)
                }
            except TypeError:
                # data for this jobscript is not set
                pass

        if js_idx not in self._jobscript_at_submit_metadata:
            return {i: None for i in JOBSCRIPT_SUBMIT_TIME_KEYS}

        return self._jobscript_at_submit_metadata[js_idx]

    @TimeIt.decorator
    def get_jobscript_block_run_ID_array(
        self,
        sub_idx: int,
        js_idx: int,
        blk_idx: int,
        run_ID_arr: NDArray | None,
    ) -> NDArray:
        """For the specified jobscript-block, retrieve the run ID array."""

        if run_ID_arr is not None:
            self.logger.debug("jobscript-block run IDs are still in memory.")
            # in the special case when the Submission object has just been created, the
            # run ID arrays will not yet be persistent.
            return np.asarray(run_ID_arr)

        # otherwise, `append_submissions` has been called, the run IDs have been
        # removed from the JSON-representation of the submission object, and have been
        # saved in separate zarr arrays:
        if sub_idx not in self._jobscript_run_ID_arrays:

            self.logger.debug(
                f"retrieving jobscript-block run IDs for submission {sub_idx} from disk,"
                f" and caching."
            )

            # for a given submission, run IDs are stored for all jobscript-blocks in the
            # same array (and chunk), so retrieve all of them and cache:

            arr = self._get_jobscripts_run_ID_arr(sub_idx)
            arr_dat = arr[:]
            block_shapes = arr.attrs["block_shapes"]

            self._jobscript_run_ID_arrays[sub_idx] = {}  # keyed by (js_idx, blk_idx)
            arr_idx = 0
            for js_idx_i, js_blk_shapes in enumerate(block_shapes):
                for blk_idx_j, blk_shape_j in enumerate(js_blk_shapes):
                    self._jobscript_run_ID_arrays[sub_idx][(js_idx_i, blk_idx_j)] = (
                        arr_dat[arr_idx, : blk_shape_j[0], : blk_shape_j[1]]
                    )
                    arr_idx += 1

        else:
            self.logger.debug(
                f"retrieving jobscript-block run IDs for submission {sub_idx} from cache."
            )

        return self._jobscript_run_ID_arrays[sub_idx][(js_idx, blk_idx)]

    def get_jobscript_block_task_elements_map(
        self,
        sub_idx: int,
        js_idx: int,
        blk_idx: int,
        task_elems_map: dict[int, list[int]] | None,
    ) -> dict[int, list[int]]:
        """For the specified jobscript-block, retrieve the task-elements mapping."""

        if task_elems_map is not None:
            self.logger.debug("jobscript-block task elements are still in memory.")
            # in the special case when the Submission object has just been created, the
            # task elements arrays will not yet be persistent.
            return task_elems_map

        # otherwise, `append_submissions` has been called, the task elements have been
        # removed from the JSON-representation of the submission object, and have been
        # saved in separate zarr arrays:
        if sub_idx not in self._jobscript_task_element_maps:

            self.logger.debug(
                f"retrieving jobscript-block task elements for submission {sub_idx} from "
                f"disk, and caching."
            )

            # for a given submission, task elements are stored for all jobscript-blocks in
            # the same array (and chunk), so retrieve all of them and cache:

            arr = self._get_jobscripts_task_elements_arr(sub_idx)
            arr_dat = arr[:]
            block_shapes = arr.attrs["block_shapes"]

            self._jobscript_task_element_maps[sub_idx] = {}  # keys: (js_idx, blk_idx)
            arr_idx = 0
            for js_idx_i, js_blk_shapes in enumerate(block_shapes):
                for blk_idx_j, blk_shape_j in enumerate(js_blk_shapes):
                    arr_i = arr_dat[arr_idx, : blk_shape_j[1], : blk_shape_j[0] + 1]
                    self._jobscript_task_element_maps[sub_idx][(js_idx_i, blk_idx_j)] = {
                        k[0]: list(k[1:]) for k in arr_i
                    }
                    arr_idx += 1

        else:
            self.logger.debug(
                f"retrieving jobscript-block task elements for submission {sub_idx} from "
                "cache."
            )

        return self._jobscript_task_element_maps[sub_idx][(js_idx, blk_idx)]

    @TimeIt.decorator
    def get_jobscript_block_task_actions_array(
        self,
        sub_idx: int,
        js_idx: int,
        blk_idx: int,
        task_actions_arr: NDArray | list[tuple[int, int, int]] | None,
    ) -> NDArray:
        """For the specified jobscript-block, retrieve the task-actions array."""

        if task_actions_arr is not None:
            self.logger.debug("jobscript-block task actions are still in memory.")
            # in the special case when the Submission object has just been created, the
            # task actions arrays will not yet be persistent.
            return np.asarray(task_actions_arr)

        # otherwise, `append_submissions` has been called, the task actions have been
        # removed from the JSON-representation of the submission object, and have been
        # saved in separate zarr arrays:
        if sub_idx not in self._jobscript_task_actions_arrays:

            self.logger.debug(
                f"retrieving jobscript-block task actions for submission {sub_idx} from "
                f"disk, and caching."
            )

            # for a given submission, task actions are stored for all jobscript-blocks in
            # the same array (and chunk), so retrieve all of them and cache:

            arr = self._get_jobscripts_task_actions_arr(sub_idx)
            arr_dat = arr[:]
            block_num_acts = arr.attrs["block_num_acts"]

            num_acts_count = 0
            self._jobscript_task_actions_arrays[sub_idx] = {}  # keys: (js_idx, blk_idx)
            for js_idx_i, js_blk_num_acts in enumerate(block_num_acts):
                for blk_idx_j, blk_num_acts_j in enumerate(js_blk_num_acts):
                    arr_i = arr_dat[num_acts_count : num_acts_count + blk_num_acts_j]
                    num_acts_count += blk_num_acts_j
                    self._jobscript_task_actions_arrays[sub_idx][
                        (js_idx_i, blk_idx_j)
                    ] = arr_i

        else:
            self.logger.debug(
                f"retrieving jobscript-block task actions for submission {sub_idx} from "
                "cache."
            )

        return self._jobscript_task_actions_arrays[sub_idx][(js_idx, blk_idx)]

    @TimeIt.decorator
    def get_jobscript_block_dependencies(
        self,
        sub_idx: int,
        js_idx: int,
        blk_idx: int,
        js_dependencies: dict[tuple[int, int], ResolvedJobscriptBlockDependencies] | None,
    ) -> dict[tuple[int, int], ResolvedJobscriptBlockDependencies]:
        """For the specified jobscript-block, retrieve the dependencies."""

        if js_dependencies is not None:
            self.logger.debug("jobscript-block dependencies are still in memory.")
            # in the special case when the Submission object has just been created, the
            # dependencies will not yet be persistent.
            return js_dependencies

        # otherwise, `append_submissions` has been called, the dependencies have been
        # removed from the JSON-representation of the submission object, and have been
        # saved in separate zarr arrays:
        if sub_idx not in self._jobscript_dependencies:
            self.logger.debug(
                f"retrieving jobscript-block dependencies for submission {sub_idx} from "
                f"disk, and caching."
            )
            # for a given submission, dependencies are stored for all jobscript-blocks in
            # the same array (and chunk), so retrieve all of them and cache:
            arr = self._get_jobscripts_dependencies_arr(sub_idx)
            self._jobscript_dependencies[sub_idx] = (
                self._decode_jobscript_block_dependencies(arr)
            )
        else:
            self.logger.debug(
                f"retrieving jobscript-block dependencies for submission {sub_idx} from "
                "cache."
            )

        return self._jobscript_dependencies[sub_idx][(js_idx, blk_idx)]

    def get_ts_fmt(self):
        """
        Get the format for timestamps.
        """
        with self.using_resource("attrs", action="read") as attrs:
            return attrs["ts_fmt"]

    def get_ts_name_fmt(self):
        """
        Get the format for timestamps to use in names.
        """
        with self.using_resource("attrs", action="read") as attrs:
            return attrs["ts_name_fmt"]

    def get_creation_info(self):
        """
        Get information about the creation of the workflow.
        """
        with self.using_resource("attrs", action="read") as attrs:
            return copy.deepcopy(attrs["creation_info"])

    def get_name(self):
        """
        Get the name of the workflow.
        """
        with self.using_resource("attrs", action="read") as attrs:
            return attrs["name"]

    def zip(
        self,
        path: str = ".",
        log: str | None = None,
        overwrite: bool = False,
        include_execute: bool = False,
        include_rechunk_backups: bool = False,
        status: bool = True,
    ):
        """
        Convert the persistent store to zipped form.

        Parameters
        ----------
        path:
            Path at which to create the new zipped workflow. If this is an existing
            directory, the zip file will be created within this directory. Otherwise,
            this path is assumed to be the full file path to the new zip file.
        """
        status_context: AbstractContextManager[Status] | AbstractContextManager[None] = (
            Console().status(f"Zipping workflow {self.workflow.name!r}...")
            if status
            else nullcontext()
        )
        with status_context:
            # TODO: this won't work for remote file systems
            dst_path = Path(path).resolve()
            if dst_path.is_dir():
                dst_path = dst_path.joinpath(self.workflow.name).with_suffix(".zip")

            if not overwrite and dst_path.exists():
                raise FileExistsError(
                    f"File at path already exists: {dst_path!r}. Pass `overwrite=True` to "
                    f"overwrite the existing file."
                )

            dst_path_s = str(dst_path)

            src_zarr_store = self.zarr_store
            zfs, _ = ask_pw_on_auth_exc(
                ZipFileSystem,
                fo=dst_path_s,
                mode="w",
                target_options={},
                add_pw_to="target_options",
            )
            dst_zarr_store = FSStore(url="", fs=zfs)
            excludes = []
            if not include_execute:
                excludes.append("execute")
            if not include_rechunk_backups:
                excludes.append("runs.bak")
                excludes.append("base.bak")

            zarr.copy_store(
                src_zarr_store,
                dst_zarr_store,
                excludes=excludes or None,
                log=log,
            )
            del zfs  # ZipFileSystem remains open for instance lifetime
        return dst_path_s

    def unzip(self, path: str = ".", log: str | None = None):
        raise ValueError("Not a zip store!")

    def _rechunk_arr(
        self,
        arr: Array,
        chunk_size: int | tuple[int, ...] | None = None,
        backup: bool = True,
        status: bool = True,
    ) -> Array:
        arr_path = Path(arr.store.path) / arr.path
        arr_name = arr.path.split("/")[-1]

        if status:
            s = Console().status("Rechunking...")
            s.start()
        backup_time = None

        if backup:
            if status:
                s.update("Backing up...")
            backup_path = arr_path.with_suffix(".bak")
            if backup_path.is_dir():
                pass
            else:
                tic = time.perf_counter()
                shutil.copytree(arr_path, backup_path)
                toc = time.perf_counter()
                backup_time = toc - tic

        tic = time.perf_counter()
        arr_rc_path = arr_path.with_suffix(".rechunked")
        if status:
            s.update("Creating new array...")

        # use the same store:
        try:
            arr_rc_store = arr.store.__class__(path=arr_rc_path)
        except TypeError:
            # FSStore
            arr_rc_store = arr.store.__class__(url=str(arr_rc_path))

        arr_rc = zarr.create(
            store=arr_rc_store,
            shape=arr.shape,
            chunks=arr.shape if chunk_size is None else chunk_size,
            dtype=object,
            object_codec=self._CODEC,
            write_empty_chunks=False,
        )

        if status:
            s.update("Copying data...")
        data = np.empty(shape=arr.shape, dtype=object)
        bad_data = []
        for idx in range(len(arr)):
            try:
                data[idx] = arr[idx]
            except RuntimeError:
                # blosc decompression errors
                bad_data.append(idx)
        arr_rc[:] = data

        arr_rc.attrs.put(arr.attrs.asdict())

        if status:
            s.update("Deleting old array...")
        shutil.rmtree(arr_path)

        if status:
            s.update("Moving new array into place...")
        shutil.move(arr_rc_path, arr_path)

        toc = time.perf_counter()
        rechunk_time = toc - tic

        if status:
            s.stop()

        if backup_time:
            print(f"Time to backup {arr_name}: {backup_time:.1f} s")

        print(f"Time to rechunk and move {arr_name}: {rechunk_time:.1f} s")

        if bad_data:
            print(f"Bad data at {arr_name} indices: {bad_data}.")

        return arr_rc

    def rechunk_parameter_base(
        self,
        chunk_size: int | tuple[int, ...] | None = None,
        backup: bool = True,
        status: bool = True,
    ) -> Array:
        """
        Rechunk the parameter data to be stored more efficiently.
        """
        arr = self._get_parameter_base_array()
        return self._rechunk_arr(arr, chunk_size, backup, status)

    def rechunk_runs(
        self,
        chunk_size: int | tuple[int, ...] | None = None,
        backup: bool = True,
        status: bool = True,
    ) -> Array:
        """
        Rechunk the run data to be stored more efficiently.
        """
        raise NotImplementedError()  # TODO

    def get_dirs_array(self) -> NDArray:
        """
        Retrieve the run directories array.
        """
        return self._get_dirs_arr()[:]


class ZarrZipPersistentStore(ZarrPersistentStore):
    """A store designed mainly as an archive format that can be uploaded to data
    repositories such as Zenodo.

    Note
    ----
    Archive format persistent stores cannot be updated without being unzipped first.
    """

    _name: ClassVar[str] = "zip"
    _features: ClassVar[PersistentStoreFeatures] = PersistentStoreFeatures(
        create=False,
        edit=False,
        jobscript_parallelism=False,
        EAR_parallelism=False,
        schedulers=False,
        submission=False,
    )

    # TODO: enforce read-only nature

    def zip(
        self,
        path: str = ".",
        log: str | None = None,
        overwrite: bool = False,
        include_execute: bool = False,
        include_rechunk_backups: bool = False,
        status: bool = True,
    ):
        raise ValueError("Already a zip store!")

    def unzip(self, path: str = ".", log: str | None = None, status: bool = True) -> str:
        """
        Expand the persistent store.

        Parameters
        ----------
        path:
            Path at which to create the new unzipped workflow. If this is an existing
            directory, the new workflow directory will be created within this directory.
            Otherwise, this path will represent the new workflow directory path.

        """
        status_context: AbstractContextManager[Status] | AbstractContextManager[None] = (
            Console().status(f"Unzipping workflow {self.workflow.name!r}...")
            if status
            else nullcontext()
        )

        with status_context:
            # TODO: this won't work for remote file systems
            dst_path = Path(path).resolve()
            if dst_path.is_dir():
                dst_path = dst_path.joinpath(self.workflow.name)

            if dst_path.exists():
                raise FileExistsError(f"Directory at path already exists: {dst_path!r}.")

            dst_path_s = str(dst_path)

            src_zarr_store = self.zarr_store
            dst_zarr_store = FSStore(url=dst_path_s)
            zarr.copy_store(src_zarr_store, dst_zarr_store, log=log)
            return dst_path_s

    def copy(self, path: PathLike = None) -> Path:
        # not sure how to do this.
        raise NotImplementedError()

    def delete_no_confirm(self) -> None:
        # `ZipFileSystem.rm()` does not seem to be implemented.
        raise NotImplementedError()

    def _rechunk_arr(
        self,
        arr,
        chunk_size: int | tuple[int, ...] | None = None,
        backup: bool = True,
        status: bool = True,
    ) -> Array:
        raise NotImplementedError

    def get_text_file(self, path: str | Path) -> str:
        """Retrieve the contents of a text file stored within the workflow."""
        path = Path(path)
        if path.is_absolute():
            path = path.relative_to(self.workflow.url)
        path = str(path.as_posix())
        assert self.fs
        try:
            with self.fs.open(path, mode="rt") as fp:
                return fp.read()
        except KeyError:
            raise FileNotFoundError(
                f"File within zip at location {path!r} does not exist."
            ) from None
