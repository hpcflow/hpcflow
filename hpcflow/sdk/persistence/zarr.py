"""
Persistence model based on writing Zarr arrays.
"""

from __future__ import annotations

import copy
from contextlib import contextmanager
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast, TYPE_CHECKING
from typing_extensions import override
import shutil
import time
import json
from collections import defaultdict

import numpy as np
from numpy.ma.core import MaskedArray
import zarr  # type: ignore
from zarr.errors import BoundsCheckError  # type: ignore
from zarr.storage import DirectoryStore, FSStore  # type: ignore
from zarr.util import guess_chunks  # type: ignore
from fsspec.implementations.zip import ZipFileSystem  # type: ignore
from rich.console import Console
from numcodecs import MsgPack, VLenArray, blosc, Blosc, Zstd, JSON  # type: ignore
from reretry import retry  # type: ignore

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
    read_JSON_file,
    write_JSON_file,
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
    UnsetBaseOutput,
)
from hpcflow.sdk.persistence.types import (
    LoopDescriptor,
    StoreCreationInfo,
    TemplateMeta,
    ZarrAttrsDict,
)
from hpcflow.sdk.persistence.store_resource import ZarrAttrsStoreResource
from hpcflow.sdk.persistence.utils import ask_pw_on_auth_exc
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
    from typing import ClassVar
    from typing_extensions import Self, TypeAlias
    from numpy.typing import NDArray
    from zarr import Array, Group  # type: ignore
    from zarr.attrs import Attributes  # type: ignore
    from zarr.storage import Store  # type: ignore
    from ..submission.types import ResolvedJobscriptBlockDependencies
    from .types import TypeLookup
    from ..app import BaseApp
    from ..core.json_like import JSONed, JSONDocument
    from ..typing import ParamSource, PathLike, DataIndex

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

from typing import Any, Dict, Optional
import numpy as np


@TimeIt.decorator
def adjacency_list_to_graph(
    arr: np.ndarray, keys: Optional[np.ndarray] = None, to_list: Optional[bool] = False
) -> Dict[int, np.ndarray]:
    # assumes keys and values refer to separate entities
    graph = {}
    if not arr.size:
        return graph

    structured = False
    if keys is None:
        num_nodes = arr[0]
        keys = arr[1 : num_nodes + 1] - (num_nodes + 1)
        keys_index = keys
        arr = arr[num_nodes + 1 :]
    else:
        if keys.dtype.fields:
            # structured array, so mapping keys will be tuples ("index" field must exist)
            structured = True
            keys_index = keys["index"]
        else:
            keys_index = keys
        num_nodes = len(keys)

    for i in range(num_nodes):
        start_idx = keys_index[i]
        end_idx = keys_index[i + 1] if i + 1 < num_nodes else None
        members_slice = slice(start_idx, end_idx)
        if end_idx != start_idx:
            val = arr[members_slice]
            grph_key_i = keys[i].item() if structured else i
            graph[grph_key_i] = val.tolist() if to_list else val

    return graph


@TimeIt.decorator
def graph_to_adjacency_list(
    graph: dict[int | tuple[Any], np.ndarray | list],
    key_dtype=int,  # TODO: typing
    val_dtype=int,  # TODO: typing
    separate_keys: bool = False,
) -> np.ndarray:
    """
    If `key_dtype` is a structured data type, then graph keys should be tuples whose
    values' types match `key_dtype`.
    """
    # assumed to have max(graph.keys()) + 1 number of nodes (i.e a contiguous range from 0->max)
    # assumes keys and values refer to separate entities

    # TODO: consider non-contiguous keys

    if not graph:
        if separate_keys:
            return np.zeros(0, dtype=val_dtype), np.zeros(0, dtype=key_dtype)
        else:
            return np.zeros(0, dtype=val_dtype)

    if key_dtype != val_dtype and not separate_keys:
        raise ValueError(
            f"Must set `separate_keys` to `True` when `key_dtype` ({key_dtype}) and "
            f"`val_dtype` ({val_dtype}) are not equal."
        )

    if key_dtype != int:
        # key dtype must include an integer field named "index"
        index_dtype_idx = None
        for idx, i in enumerate(key_dtype):
            if i[0] == "index":
                index_dtype_idx = idx
        if index_dtype_idx is None:
            raise ValueError(
                "If set as a structured data type, `key_dtype` must include "
                'an integer field named "index"'
            )
        grp_keys = {i[index_dtype_idx]: i for i in graph}
    else:
        grp_keys = {i: i for i in graph}

    num_nodes = max(grp_keys.keys()) + 1

    num_members = {k: len(v) for k, v in graph.items()}

    num_vals = sum(num_members.values())

    if separate_keys:
        arr = np.zeros(num_vals, dtype=val_dtype)
        keys = np.zeros(num_nodes, dtype=key_dtype)
        start_idx = 0
    else:
        arr = np.zeros(num_nodes + 1 + num_vals, dtype=val_dtype)
        arr[0] = num_nodes
        start_idx = num_nodes + 1

    for node_idx in range(num_nodes):

        graph_key = grp_keys.get(node_idx)

        members = graph.get(graph_key, np.array([]))

        num_members_i = num_members.get(graph_key, 0)

        end_idx = start_idx + num_members_i

        if separate_keys:
            if key_dtype == int or graph_key is None:
                keys[node_idx] = start_idx
            else:
                keys[node_idx] = graph_key
        else:
            arr[node_idx + 1] = start_idx

        arr[start_idx:end_idx] = members
        start_idx += num_members_i

    if separate_keys:
        return arr, keys
    else:
        return arr


@TimeIt.decorator
def nested_graph_to_adjacency_list(graph, depth=2):
    flat_graph = {}
    for k, v in graph.items():
        flat_graph[k] = (
            graph_to_adjacency_list(v)
            if depth == 2
            else nested_graph_to_adjacency_list(v, depth=depth - 1)
        )
    return graph_to_adjacency_list(flat_graph)


@TimeIt.decorator
def adjacency_list_to_nested_graph(adj_lst, depth=2):
    nested_graph = {}
    for k, v in adjacency_list_to_graph(adj_lst).items():
        nested_graph[k] = (
            adjacency_list_to_graph(v)
            if depth == 2
            else adjacency_list_to_nested_graph(v, depth=depth - 1)
        )
    return nested_graph


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


def _encode_numpy_array_NEW(
    obj: NDArray,
    type_lookup: TypeLookup,
    path: list[int],
    root_group: Group,
    root_encoder: Callable,
) -> int:
    new_idx = max((int(i) for i in root_group.keys()), default=-1) + 1
    with override_module_attrs(
        "zarr.util", {"CHUNK_MIN": _ARRAY_CHUNK_MIN, "CHUNK_MAX": _ARRAY_CHUNK_MAX}
    ):
        # `guess_chunks` also ensures chunk shape is at least 1 in each dimension:
        chunk_shape = guess_chunks(obj.shape, obj.dtype.itemsize)

    root_group.create_dataset(name=str(new_idx), data=obj, chunks=chunk_shape)
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


def _decode_numpy_arrays_NEW(
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

        dataset: NDArray = arr_group.get(str(arr_idx))
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


def _encode_masked_array_NEW(
    obj: MaskedArray,
    type_lookup: TypeLookup,
    path: list[int],
    root_group: Group,
    root_encoder: Callable,
):
    """Encode a masked array as two normal arrays, and return the fill value."""
    # no need to add "array" entries to the type lookup, so pass an empty `type_lookup`:
    type_lookup_: TypeLookup = defaultdict(list)
    data_idx = _encode_numpy_array_NEW(
        obj.data, type_lookup_, path, root_group, root_encoder
    )
    mask_idx = _encode_numpy_array_NEW(
        cast("NDArray", obj.mask), type_lookup_, path, root_group, root_encoder
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
            self.es_ID,
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
            "es_ID": elem_dat[7],
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
            self.index,
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
            "index": iter_dat[7],
        }
        return cls(is_pending=False, **obj_dat)


@dataclass
class ZarrStoreEAR(StoreEAR[ListAny, ZarrAttrs]):
    """
    Represents an element action run in a Zarr persistent store.
    """

    @override
    @TimeIt.decorator
    def encode(self, ts_fmt: str, attrs: ZarrAttrs) -> ListAny:
        """Prepare store EAR data for the persistent store.

        This method mutates `attrs`.
        """
        return [
            self.id_,
            self.elem_iter_ID,
            self.action_idx,
            [
                [ensure_in(dk, attrs["parameter_paths"]), dv]
                for dk, dv in self.data_idx.items()
            ],
            self.submission_idx,
            self.skip,
            self.success,
            self._encode_datetime(self.start_time, ts_fmt),
            self._encode_datetime(self.end_time, ts_fmt),
            self.snapshot_start,
            self.snapshot_end,
            self.exit_code,
            self.metadata,
            self.run_hostname,
            self.commands_idx,
            self.port_number,
            self.commands_file_ID,
        ]

    @override
    @classmethod
    @TimeIt.decorator
    def decode(cls, EAR_dat: ListAny, ts_fmt: str, attrs: ZarrAttrs) -> Self:
        """Initialise a `ZarrStoreEAR` from persistent EAR data"""
        obj_dat = {
            "id_": EAR_dat[0],
            "elem_iter_ID": EAR_dat[1],
            "action_idx": EAR_dat[2],
            "data_idx": {attrs["parameter_paths"][i[0]]: i[1] for i in EAR_dat[3]},
            "submission_idx": EAR_dat[4],
            "skip": EAR_dat[5],
            "success": EAR_dat[6],
            "start_time": cls._decode_datetime(EAR_dat[7], ts_fmt),
            "end_time": cls._decode_datetime(EAR_dat[8], ts_fmt),
            "snapshot_start": EAR_dat[9],
            "snapshot_end": EAR_dat[10],
            "exit_code": EAR_dat[11],
            "metadata": EAR_dat[12],
            "run_hostname": EAR_dat[13],
            "commands_idx": EAR_dat[14],
            "port_number": EAR_dat[15],
            "commands_file_ID": EAR_dat[16],
        }
        return cls(is_pending=False, **obj_dat)


@dataclass
@hydrate
class ZarrStoreParameter(StoreParameter):
    """
    Represents a parameter in a Zarr persistent store.
    """

    _encoders: ClassVar[dict[type, Callable]] = {  # keys are types
        np.ndarray: _encode_numpy_array,
        MaskedArray: _encode_masked_array,
        bytes: _encode_bytes,
    }
    _decoders: ClassVar[dict[str, Callable]] = {  # keys are keys in type_lookup
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
    _param_new_arr_grp_name: ClassVar[str] = "arrays_NEW"
    _param_base_outs_arr_grp_name: ClassVar[str] = "base_outputs"
    _base_outputs_fill_value: ClassVar[int] = 0
    _param_data_arr_grp_name: ClassVar = lambda _, param_idx: f"param_{param_idx}"
    _subs_md_group_name: ClassVar[str] = "submissions"
    _task_arr_name: ClassVar[str] = "tasks"
    _task_v2_arr_name: ClassVar[str] = "v2_tasks"
    _elem_set_metadata_arr_name: ClassVar[str] = "elem_set_meta"
    _elem_set_inp_src_iter_IDs_arr_name: ClassVar[str] = "elem_set_inp_src_iter_IDs"
    _elem_arr_name: ClassVar[str] = "elements"
    _elem_v2_arr_name: ClassVar[str] = "elem_meta"
    _elem_seq_idx_arr_name: ClassVar[str] = "elem_seq_idx"
    _elem_seq_idx_lookup_arr_name: ClassVar[str] = "elem_seq_idx_lookup"
    _elem_src_idx_arr_name: ClassVar[str] = "elem_src_idx"
    _elem_src_idx_lookup_arr_name: ClassVar[str] = "elem_src_idx_lookup"
    _elem_iter_IDs_arr_name: ClassVar[str] = "elem_iter_IDs"
    _iter_run_IDs_arr_name: ClassVar[str] = "iter_run_IDs"
    _iter_metadata_arr_name: ClassVar[str] = "iter_meta"
    _iter_loop_idx_arr_name: ClassVar[str] = "iter_loop_idx"
    _iter_loop_idx_lookup_arr_name: ClassVar[str] = "iter_loop_idx_lookup"
    _iter_data_idx_arr_name: ClassVar[str] = "iter_data_idx"
    _iter_data_idx_lookup_arr_name: ClassVar[str] = "iter_data_idx_lookup"
    _run_metadata_arr_name: ClassVar[str] = "run_meta"
    _run_data_idx_arr_name: ClassVar[str] = "run_data_idx"
    _run_data_idx_lookup_arr_name: ClassVar[str] = "run_data_idx_lookup"
    _local_inputs_arr_name: ClassVar[str] = "local_inputs"
    _iter_arr_name: ClassVar[str] = "iters"
    _EAR_arr_name: ClassVar[str] = "runs"
    _run_dir_arr_name: ClassVar[str] = "run_dirs"
    _js_at_submit_md_arr_name: ClassVar[str] = "js_at_submit_md"
    _js_run_IDs_arr_name: ClassVar[str] = "js_run_IDs"
    _js_task_elems_arr_name: ClassVar[str] = "js_task_elems"
    _js_task_acts_arr_name: ClassVar[str] = "js_task_acts"
    _js_deps_arr_name: ClassVar[str] = "js_deps"
    _time_res: ClassVar[str] = "us"  # microseconds; must not be smaller than micro!

    _res_map: ClassVar[CommitResourceMap] = CommitResourceMap(
        commit_template_components=("attrs",)
    )

    elem_seq_idx_dtype = [("sequence_path_idx", np.uint16), ("sequence_idx", np.uint32)]
    elem_seq_idx_lookup_dtype = np.uint32
    elem_src_idx_dtype = [("source_path_idx", np.uint16), ("source_idx", np.uint8)]
    elem_src_idx_lookup_dtype = np.uint32
    iter_loop_idx_dtype = [("loop_name_idx", np.uint8), ("loop_iteration_idx", np.uint32)]
    iter_loop_idx_lookup_dtype = np.uint32
    iter_data_idx_dtype = [
        ("param_path_idx", np.uint16),
        ("param_idx", np.uint32),
        ("param_sub_idx", np.uint32),
    ]
    iter_data_idx_lookup_dtype = np.uint32
    run_data_idx_dtype = [
        ("param_path_idx", np.uint16),
        ("param_idx", np.uint32),
        ("param_sub_idx", np.uint32),
    ]
    run_data_idx_lookup_dtype = np.uint32
    elem_set_inp_src_iter_IDs_dtype = np.uint32

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

        NEW_attrs = {
            "task_ID_list": [],
            "element_src_idx_paths": [],
            "element_seq_idx_paths": [],
            "es_inp_src_paths": [],
            "loop_names": [],
            "template": template_js,
            "loops": [],
            "task_output_array_idx": {},
        }

        if replaced_wk:
            attrs["replaced_workflow"] = replaced_wk

        store = cls._get_zarr_store(wk_path, fs)
        root = zarr.group(store=store, overwrite=False)
        root.attrs.update(attrs)

        new_root_group = root.create_group("NEW")
        new_root_group.attrs.update(NEW_attrs)

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

        tasks_v2_arr = md.create_dataset(
            name=cls._task_v2_arr_name,
            shape=0,
            dtype=np.uint32,
            chunks=1_000_000,
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

        elem_set_arr = md.create_dataset(
            name=cls._elem_set_metadata_arr_name,
            shape=0,
            dtype=cls.elem_set_metadata_dtype,
            chunks=1_000_000,
            compressor=cmp,
        )

        elem_set_inp_src_iter_IDs_arr = md.create_dataset(
            name=cls._elem_set_inp_src_iter_IDs_arr_name,
            shape=0,
            dtype=cls.elem_set_inp_src_iter_IDs_dtype,
            chunks=1_000_000,
            compressor=cmp,
        )

        elems_v2_arr = md.create_dataset(
            name=cls._elem_v2_arr_name,
            shape=0,
            dtype=cls.elem_metadata_dtype,
            chunks=1_000_000,
            compressor=cmp,
        )

        elem_iter_IDs_arr = md.create_dataset(
            name=cls._elem_iter_IDs_arr_name,
            shape=0,
            dtype=np.uint32,
            chunks=1_000_000,
            compressor=cmp,
        )

        iter_metadata_arr = md.create_dataset(
            name=cls._iter_metadata_arr_name,
            shape=0,
            dtype=cls.iter_metadata_dtype,
            chunks=1_000_000,
            compressor=cmp,
        )

        iter_run_IDs_arr = md.create_dataset(
            name=cls._iter_run_IDs_arr_name,
            shape=0,
            dtype=np.uint32,
            chunks=1_000_000,
            compressor=cmp,
        )

        run_metadata_arr = md.create_dataset(
            name=cls._run_metadata_arr_name,
            shape=0,
            dtype=cls.run_metadata_dtype,
            chunks=1_000_000,
            compressor=cmp,
        )

        cls._write_element_src_idx_arrays(md_group=md, element_src_idx={})
        cls._write_element_seq_idx_arrays(md_group=md, element_seq_idx={})
        cls._write_iter_loop_idx_arrays(md_group=md, iter_loop_idx={})
        cls._write_iter_data_idx_arrays(md_group=md, iter_data_idx={})
        cls._write_run_data_idx_arrays(md_group=md, run_data_idx={})

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

        EARs_arr = md.create_dataset(
            name=cls._EAR_arr_name,
            shape=(0, 1000),
            dtype=object,
            object_codec=cls._CODEC,
            chunks=1,  # single-chunk rows for multiprocess writing
            compressor=cmp,
            dimension_separator="/",
        )
        EARs_arr.attrs.update({"parameter_paths": [], "num_runs": 0})

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
        parameter_data.create_group(name=cls._param_new_arr_grp_name)
        parameter_data.create_group(name=cls._param_base_outs_arr_grp_name)

        # for storing submission metadata that should not be stored in the root group:
        md.create_group(name=cls._subs_md_group_name)

        parameter_data.create_dataset(
            name=cls._local_inputs_arr_name,
            shape=0,
            dtype=object,
            object_codec=cls._CODEC,
            chunks=1_000_000,  # TODO: should be able to set this to False, or not in zarr 2?
            compressor=cmp,
        )

    def load_data(self) -> None:

        self.root_attributes = self._get_root_group().get("NEW").attrs.asdict()
        self.task_ID_list = self.root_attributes["task_ID_list"]
        self.element_src_idx_paths = self.root_attributes["element_src_idx_paths"]
        self.element_seq_idx_paths = self.root_attributes["element_seq_idx_paths"]
        self.es_inp_src_paths = self.root_attributes["es_inp_src_paths"]
        self.loop_names = self.root_attributes["loop_names"]
        self.template = self.root_attributes["template"]
        self.loops = self.root_attributes["loops"]

        for idx, val in self.root_attributes["task_output_array_idx"].items():
            for out_name, arr_idx_dat in val.items():
                val[out_name]["action_indices"] = {
                    int(idx_from): idx_to
                    for idx_from, idx_to in arr_idx_dat["action_indices"].items()
                }
            self.task_output_array_idx[int(idx)] = val

        self.task_templates = self.template["tasks"]
        self.loop_templates = self.template["loops"]

        self.element_set_metadata = self._get_v2_element_set_metadata_array()[:]
        self.element_metadata = self._get_v2_element_metadata_array()[:]
        self.iter_metadata = self._get_v2_iter_metadata_array()[:]
        self.run_metadata = self._get_v2_run_metadata_array()[:]

        self.task_element_IDs = adjacency_list_to_graph(
            self._get_v2_task_element_IDs_array()
        )
        self.element_iter_IDs = adjacency_list_to_graph(
            self._get_v2_element_iter_IDs_array()
        )
        self.iter_run_IDs = adjacency_list_to_graph(self._get_v2_iter_run_IDs_array())

        self.element_set_input_source_iter_IDs = {
            es_ID: {
                self.es_inp_src_paths[psrc]: iter_IDs
                for psrc, iter_IDs in inp_src_iters.items()
            }
            for es_ID, inp_src_iters in adjacency_list_to_nested_graph(
                self._get_v2_element_set_input_source_iter_IDs_array()[:], depth=3
            ).items()
        }

        src_arr, src_lookup_arr = self._get_v2_element_src_idx_arrays()
        esrc_idx = adjacency_list_to_graph(src_arr[:], src_lookup_arr[:])
        self.element_src_idx = {
            elem_id: {
                self.element_src_idx_paths[psrc_j["source_path_idx"]]: psrc_j[
                    "source_idx"
                ].item()
                for psrc_j in src_i
            }
            for elem_id, src_i in esrc_idx.items()
        }

        seq_arr, seq_lookup_arr = self._get_v2_element_seq_idx_arrays()
        eseq_idx = adjacency_list_to_graph(seq_arr[:], seq_lookup_arr[:])
        # this includes sequences and inputs from upstream tasks (in which case the
        # indices index into the element iteration IDs list of the input source.):
        self.element_seq_idx = {
            elem_id: {
                self.element_seq_idx_paths[pseq_j["sequence_path_idx"]]: pseq_j[
                    "sequence_idx"
                ].item()
                for pseq_j in seq_i
            }
            for elem_id, seq_i in eseq_idx.items()
        }

        loop_arr, loop_lookup_arr = self._get_v2_iter_loop_idx_arrays()
        loop_idx = adjacency_list_to_graph(loop_arr[:], loop_lookup_arr[:])
        self.iter_loop_idx = {
            iter_id: {
                self.loop_names[loop_j["loop_name_idx"]]: loop_j[
                    "loop_iteration_idx"
                ].item()
                for loop_j in iter_i_loop_idx
            }
            for iter_id, iter_i_loop_idx in loop_idx.items()
        }

        arr = self._get_v2_local_inputs_array()
        if arr.size:
            decoded = (
                self._store_param_cls()
                .decode(
                    id_=None,
                    data=arr[0],
                    source=None,
                    decoders={"array": _decode_numpy_arrays_NEW},
                    arr_group=self._get_parameter_NEW_array_group(),
                    dataset_copy=False,
                )
                .data
            )
        else:
            decoded = {
                "inputs": {},  # keyed by element set ID, then path
                "resources": {},  # keyed by element set ID, then path
                "workflow_resources": {},  # keyed by element set ID, then path
                "sequences": {},  # keyed by element set ID, then path
                "defaults": {},  # keyed by task insert ID (assuming one schema per task), then path
            }

        self.local_inputs = decoded

    def persist_ctx_start(self, ctx):
        ctx["files_to_update"] = set()

    def persist_ctx_end(self, ctx):
        for file_key in ctx["files_to_update"]:
            if file_key == "root_attributes":
                self._get_root_group(mode="r+").get("NEW").attrs.put(self.root_attributes)

    def update_task_templates(self, task_templates, ctx):
        ctx["files_to_update"].add("root_attributes")
        self.template["tasks"] = task_templates

    def update_loop_templates(self, loop_templates, ctx):
        ctx["files_to_update"].add("root_attributes")
        self.template["loops"] = loop_templates

    def update_loops(self, loops, ctx):
        ctx["files_to_update"].add("root_attributes")
        self.loops[:] = loops

    def update_task_ID_list(self, task_ID_list, ctx):
        # metadata is written to disk by persist_ctx_end
        ctx["files_to_update"].add("root_attributes")
        self.task_ID_list[:] = task_ID_list

    def update_task_output_array_idx(self, task_output_array_idx, ctx):
        ctx["files_to_update"].add("root_attributes")
        self.task_output_array_idx.update(task_output_array_idx)
        self.root_attributes["task_output_array_idx"].update(
            {str(k): v for k, v in task_output_array_idx.items()}
        )

    def update_local_inputs(self, local_inputs, ctx):
        inp_arr = self._get_v2_local_inputs_array(mode="r+")
        inp_arr.resize(1)
        inp_arr[0] = self._store_param_cls()(
            id_=None,
            is_pending=None,
            is_set=True,
            data=local_inputs,
            file=None,
            source=None,
        ).encode(
            root_group=self._get_parameter_NEW_array_group(mode="r+"),
            encoders={
                np.ndarray: _encode_numpy_array_NEW,
                MaskedArray: _encode_masked_array_NEW,
            },
        )  # TODO: encode should be something like "extract_non_primitive_types"

    def update_element_set_metadata(self, element_set_metadata, ctx):
        es_md_arr = self._get_v2_element_set_metadata_array(mode="r+")
        es_md_arr.resize(len(element_set_metadata))
        es_md_arr[:] = element_set_metadata

    def update_element_metadata(self, element_metadata, ctx):
        elem_md_arr = self._get_v2_element_metadata_array(mode="r+")
        elem_md_arr.resize(len(element_metadata))
        elem_md_arr[:] = element_metadata

    def update_iter_metadata(self, iter_metadata, ctx):
        iter_md_arr = self._get_v2_iter_metadata_array(mode="r+")
        iter_md_arr.resize(len(iter_metadata))
        iter_md_arr[:] = iter_metadata

    def update_run_metadata(self, run_metadata, ctx):
        run_md_arr = self._get_v2_run_metadata_array(mode="r+")
        run_md_arr.resize(len(run_metadata))
        run_md_arr[:] = run_metadata

    def update_task_element_IDs(self, task_element_IDs, ctx):
        task_element_IDs_v2_arr = self._get_v2_task_element_IDs_array(mode="r+")
        task_element_IDs_adj_lst = graph_to_adjacency_list(task_element_IDs)
        task_element_IDs_v2_arr.resize(len(task_element_IDs_adj_lst))
        task_element_IDs_v2_arr[:] = task_element_IDs_adj_lst

    def update_element_iter_IDs(self, element_iter_IDs, ctx):
        elem_iter_IDs_arr = self._get_v2_element_iter_IDs_array(mode="r+")
        elem_iter_IDs_adj_lst = graph_to_adjacency_list(element_iter_IDs)
        elem_iter_IDs_arr.resize(len(elem_iter_IDs_adj_lst))
        elem_iter_IDs_arr[:] = elem_iter_IDs_adj_lst

    def update_iter_run_IDs(self, iter_run_IDs, ctx):
        iter_run_IDs_arr = self._get_v2_iter_run_IDs_array(mode="r+")
        iter_run_IDs_adj_lst = graph_to_adjacency_list(iter_run_IDs)
        iter_run_IDs_arr.resize(len(iter_run_IDs_adj_lst))
        iter_run_IDs_arr[:] = iter_run_IDs_adj_lst

    def update_element_set_input_source_iter_IDs(self, elem_set_inp_src_iter_IDs, ctx):
        es_iter_IDs_arr = self._get_v2_element_set_input_source_iter_IDs_array(mode="r+")
        es_inp_src_paths = []
        es_inp_src_iter_IDs = {
            es_ID: {
                ensure_in(inp_src_path, es_inp_src_paths): inp_src_iter_IDs_j
                for inp_src_path, inp_src_iter_IDs_j in path_iter_IDs.items()
            }
            for es_ID, path_iter_IDs in elem_set_inp_src_iter_IDs.items()
        }
        self.es_inp_src_paths[:] = es_inp_src_paths
        es_iter_IDs_adj_lst = nested_graph_to_adjacency_list(es_inp_src_iter_IDs, depth=3)
        es_iter_IDs_arr.resize(len(es_iter_IDs_adj_lst))
        es_iter_IDs_arr[:] = es_iter_IDs_adj_lst

    def update_element_src_idx(self, element_src_idx, ctx):
        md_group = self._get_root_group(mode="r+").get("metadata")
        esrc_paths = []
        esrc_idx = {
            elem_id: [(ensure_in(k, esrc_paths), v) for k, v in src_i.items()]
            for elem_id, src_i in element_src_idx.items()
        }
        self.element_src_idx_paths[:] = esrc_paths
        self._write_element_src_idx_arrays(md_group, esrc_idx)

    def update_element_seq_idx(self, element_seq_idx, ctx):
        md_group = self._get_root_group(mode="r+").get("metadata")
        eseq_paths = []
        eseq_idx = {
            elem_id: [(ensure_in(k, eseq_paths), v) for k, v in seq_i.items()]
            for elem_id, seq_i in element_seq_idx.items()
        }
        self.element_seq_idx_paths[:] = eseq_paths
        self._write_element_seq_idx_arrays(md_group, eseq_idx)

    def update_iter_loop_idx(self, iter_loop_idx, ctx):
        md_group = self._get_root_group(mode="r+").get("metadata")
        loop_names = []
        loop_idx = {
            iter_id: [(ensure_in(k, loop_names), v) for k, v in iter_i_loops.items()]
            for iter_id, iter_i_loops in iter_loop_idx.items()
        }
        self.loop_names[:] = loop_names
        self._write_iter_loop_idx_arrays(md_group, loop_idx)

    def has_base_output_arr(self, task_ID: int) -> bool:
        return str(task_ID) in self._get_parameter_base_outputs_array_group()

    def _get_base_outputs_array(self, task_ID: int, mode: str = "r") -> Array:
        return self._get_parameter_base_outputs_array_group(mode).get(str(task_ID))

    def get_base_outputs_array_shape(self, task_ID: int) -> tuple[int, ...]:
        return self._get_base_outputs_array(task_ID).shape

    def _get_array(self, arr_idx: int, mode: str = "r") -> Array:
        return self._get_parameter_NEW_array_group(mode).get(str(arr_idx))

    def init_new_output_arrays(
        self,
        new_base_output_arrays: Mapping[int, int],
        new_array_output_arrays: Mapping[int, Mapping[str, dict[str, Any]]],
        task_arr_shapes: Mapping[int, Sequence[int, int]],
        ctx,
    ) -> dict[int]:
        """
        Initialise new base-output and array-output arrays as specified.

        Note: this mutates the argument `task_array_shapes`.

        Parameters
        ----------
        task_arr_shapes:
            Dictionary whose keys are task IDs and whose values are the outer shapes to
            which base-output and array-output arrays should be initialised (corresponding
            to element and iteration dimensions).

        Returns
        -------
        Nested dictionary keyed by task ID and then output parameter type, with values
        "array_idx", "action_indices", and "dtype", which describe the newly created
        array-output arrays.

        """
        base_grp = self._get_parameter_base_outputs_array_group(mode="r+")
        arr_group = self._get_parameter_NEW_array_group(mode="r+")

        new_task_out_arr_idx = defaultdict(dict)
        for task_ID, num_acts in new_base_output_arrays.items():

            # TODO: store elements across multiple outer dimensions
            outer_shape = task_arr_shapes.pop(task_ID)
            shape = tuple([*outer_shape, num_acts])

            # elements must be distinct files/chunks:
            chunk_shape = [dim_size for dim_size in shape]
            chunk_shape[0] = 1

            base_grp.create_dataset(
                name=str(task_ID),
                shape=shape,
                chunks=chunk_shape,
                write_empty_chunks=False,
                dtype=object,
                object_codec=MsgPack(),  # TODO: option: compressor?
                fill_value=self._base_outputs_fill_value,
            )

            for out_type, arr_dat in new_array_output_arrays[task_ID].items():

                act_indices = arr_dat["action_indices"]
                dtype = arr_dat["dtype"]
                inner_shape = [len(act_indices)]
                if p_shape := arr_dat["shape"]:
                    inner_shape.extend(p_shape)

                # TODO: store elements across multiple outer dimensions
                shape_i = tuple([*outer_shape, *inner_shape])

                # elements must be distinct files/chunks:
                chunk_shape_i = [dim_size for dim_size in shape]
                chunk_shape_i[0] = 1

                new_idx = len(arr_group)
                arr_group.create_dataset(
                    name=str(new_idx),
                    shape=shape_i,
                    chunks=chunk_shape_i,
                    write_empty_chunks=False,
                    dtype=dtype,
                )

                new_task_out_arr_idx[task_ID][out_type] = {
                    "array_idx": new_idx,
                    "action_indices": act_indices,
                    "dtype": str(dtype),
                }

        return new_task_out_arr_idx

    def _resize_base_outputs(self, task_ID: int, add_outer_shape: list[int]):
        # TODO: store elements across multiple outer dimensions
        arr = self._get_base_outputs_array(task_ID, mode="r+")
        arr.resize(self._increment_outer_shape(arr.shape, add_outer_shape))

    def _resize_array_outputs(
        self, arr_indices: tuple[int, ...], add_outer_shape: list[int]
    ):
        # TODO: store elements across multiple outer dimensions
        arr_group = self._get_parameter_NEW_array_group(mode="r+")
        for arr_idx in arr_indices:
            arr = arr_group.get(str(arr_idx))
            arr.resize(self._increment_outer_shape(arr.shape, add_outer_shape))

    def update_output_array_shapes(
        self,
        task_arr_shapes: Mapping[int, Sequence[int, int]],
        arr_indices_by_task: Mapping[int, tuple[int, ...]],
        ctx,
    ):
        """Resize base-output and array-output arrays to accommodate new elements and/or
        iterations.

        Parameters
        ----------
        task_arr_shapes:
            Dictionary whose keys are task IDs and whose values are the outer shapes to
            which base-output and array-output arrays should be resized (corresponding to
            element and iteration dimensions).
        arr_indices_by_task:
            Dictionary whose keys are task IDs and whose values are tuples of
            associated output array indices.

        """
        ctx["files_to_update"].add("parameters_v2")
        for task_ID, outer_shape in task_arr_shapes.items():
            self._resize_base_outputs(task_ID, outer_shape)
            if arr_idx := arr_indices_by_task.get(task_ID) is not None:
                self._resize_array_outputs(arr_idx, outer_shape)

    def get_base_outputs(
        self,
        task_ID: int,
        indices: NDArray[np.void],
        paths: Sequence[str] | None = None,
        arr: Array | None = None,
    ) -> list[Any]:
        """
        `indices` should be a structured array with fields "element_idx", "iteration_idx", and
        "action_idx".

        Parameters
        ----------
        arr
            The base outputs parameter array for the specified task. If not provided,
            since will be loaded.
        """
        arr = arr or self._get_base_outputs_array(task_ID, mode="r")
        all_outs = []
        encoded = arr[
            indices["element_idx"], indices["iteration_idx"], indices["action_idx"]
        ]

        for enc_i in encoded:
            if enc_i is self._base_outputs_fill_value:
                dec_i = UnsetBaseOutput.NULL
            else:
                dec_i = (
                    self._store_param_cls()
                    .decode(
                        id_=None,
                        data=enc_i,
                        source=None,
                        decoders={"array": _decode_numpy_arrays_NEW},
                        arr_group=self._get_parameter_NEW_array_group(),
                        dataset_copy=False,
                    )
                    .data
                )
                if paths:
                    dec_i = {k: v for k, v in dec_i.items() if k in paths}
            all_outs.append(dec_i)
        return all_outs

    def set_base_outputs(
        self, task_ID: int, indices: NDArray[np.void], values: Sequence[dict[str, Any]]
    ):
        arr = self._get_base_outputs_array(task_ID, mode="r+")
        existing = self.get_base_outputs(task_ID, indices=indices, arr=arr)

        all_enc = []
        for idx, val_i in enumerate(values):

            if existing[idx] is UnsetBaseOutput.NULL:
                existing_i = {}
            else:
                existing_i = existing[idx]

            updated = {**existing_i, **val_i}
            enc_i = self._store_param_cls()(
                id_=None,
                is_pending=None,
                is_set=True,
                data=updated,
                file=None,
                source=None,
            ).encode(
                root_group=self._get_parameter_NEW_array_group(mode="r+"),
                encoders={
                    np.ndarray: _encode_numpy_array_NEW,
                    MaskedArray: _encode_masked_array_NEW,
                },
            )  # TODO: separate functions for encoding decoding
            all_enc.append(enc_i)
        arr[indices["element_idx"], indices["iteration_idx"], indices["action_idx"]] = (
            all_enc
        )

    def get_output_array(self, arr_idx) -> Array:
        return self._get_parameter_NEW_array_group().get(str(arr_idx))

    def get_array_items(self, arr_idx, indices):
        arr = self._get_array(arr_idx, mode="r")
        # TODO: store elements across multiple outer dimensions
        return arr[
            indices["element_idx"], indices["iteration_idx"], indices["action_idx"]
        ]

    def set_array_items(self, arr_idx, indices, values):
        arr = self._get_array(arr_idx, mode="r+")
        # TODO: store elements across multiple outer dimensions
        arr[indices["element_idx"], indices["iteration_idx"], indices["action_idx"]] = (
            values
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
        This mutates `sub_js`, by setting `depdendencies` jobscript-block keys to `None`.
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
    def __mutate_attrs(self, arr: Array) -> Iterator[ZarrAttrs]:
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

    def _update_loop_index(self, loop_indices: dict[int, dict[str, int]]):

        arr = self._get_iters_arr(mode="r+")
        attrs = self.__as_dict(arr.attrs)
        iter_IDs = list(loop_indices.keys())
        iter_dat = arr.get_coordinate_selection(iter_IDs)
        store_iters = [ZarrStoreElementIter.decode(i, attrs) for i in iter_dat]

        for idx, iter_ID_i in enumerate(iter_IDs):
            new_iter_i = store_iters[idx].update_loop_idx(loop_indices[iter_ID_i])
            # seems to be a Zarr bug that prevents `set_coordinate_selection` with an
            # object array, so set one-by-one:
            arr[iter_ID_i] = new_iter_i.encode(attrs)

    def _update_loop_num_iters(self, index: int, num_iters: list[list[list[int] | int]]):
        with self.using_resource("attrs", action="update") as attrs:
            attrs["loops"][index]["num_added_iterations"] = num_iters

    def _update_loop_parents(self, index: int, parents: list[str]):
        with self.using_resource("attrs", action="update") as attrs:
            attrs["loops"][index]["parents"] = parents

    def _update_iter_data_indices(self, iter_data_indices: dict[int, DataIndex]):

        arr = self._get_iters_arr(mode="r+")
        attrs = self.__as_dict(arr.attrs)
        iter_IDs = list(iter_data_indices.keys())
        iter_dat = arr.get_coordinate_selection(iter_IDs)
        store_iters = [ZarrStoreElementIter.decode(i, attrs) for i in iter_dat]

        for idx, iter_ID_i in enumerate(iter_IDs):
            new_iter_i = store_iters[idx].update_data_idx(iter_data_indices[iter_ID_i])
            # seems to be a Zarr bug that prevents `set_coordinate_selection` with an
            # object array, so set one-by-one:
            arr[iter_ID_i] = new_iter_i.encode(attrs)

    def _update_run_data_indices(self, run_data_indices: dict[int, DataIndex]):
        self._update_runs(
            updates={k: {"data_idx": v} for k, v in run_data_indices.items()}
        )

    @TimeIt.decorator
    def _append_EARs(self, EARs: Sequence[ZarrStoreEAR]):
        arr = self._get_EARs_arr(mode="r+")
        with self.__mutate_attrs(arr) as attrs:
            num_existing = attrs["num_runs"]
            num_add = len(EARs)
            num_tot = num_existing + num_add
            arr_add = np.empty(num_add, dtype=object)
            arr_add[:] = [i.encode(self.ts_fmt, attrs) for i in EARs]

            # get new 1D indices:
            new_idx: NDArray = np.arange(num_existing, num_tot)

            # transform to 2D indices:
            r_idx, c_idx = get_2D_idx(new_idx, num_cols=arr.shape[1])

            # add rows to accomodate new runs:
            max_r_idx = np.max(r_idx)
            if max_r_idx + 1 > arr.shape[0]:
                arr.resize(max_r_idx + 1, arr.shape[1])

            # fill in new data:
            for arr_add_idx_i, (r_idx_i, c_idx_i) in enumerate(zip(r_idx, c_idx)):
                # seems to be a Zarr bug that prevents `set_coordinate_selection` with an
                # object array, so set one-by-one:
                arr[r_idx_i, c_idx_i] = arr_add[arr_add_idx_i]

            attrs["num_runs"] = num_tot

        # add more rows to run dirs array:
        dirs_arr = self._get_dirs_arr(mode="r+")
        dirs_arr.resize(num_tot)

    def _set_run_dirs(self, run_dir_arr: np.ndarray, run_idx: np.ndarray):
        dirs_arr = self._get_dirs_arr(mode="r+")
        dirs_arr[run_idx] = run_dir_arr

    @TimeIt.decorator
    def _update_runs(self, updates: dict[int, dict[str, Any]]):
        """Update the provided EAR attribute values in the specified existing runs."""
        run_IDs = list(updates.keys())
        runs = self._get_persistent_EARs(run_IDs)

        arr = self._get_EARs_arr(mode="r+")
        with self.__mutate_attrs(arr) as attrs:
            # convert to 2D array indices:
            r_idx, c_idx = get_2D_idx(
                np.array(list(updates.keys())), num_cols=arr.shape[1]
            )
            for ri, ci, rID_i, upd_i in zip(
                r_idx, c_idx, updates.keys(), updates.values()
            ):
                new_run_i = runs[rID_i].update(**upd_i)
                # seems to be a Zarr bug that prevents `set_coordinate_selection` with an
                # object array, so set one-by-one:
                arr[ri, ci] = new_run_i.encode(self.ts_fmt, attrs)

    @TimeIt.decorator
    def _update_EAR_submission_data(self, sub_data: Mapping[int, tuple[int, int | None]]):
        self._update_runs(
            updates={
                k: {"submission_idx": v[0], "commands_file_ID": v[1]}
                for k, v in sub_data.items()
            }
        )

    def _update_EAR_start(
        self,
        run_starts: dict[int, tuple[datetime, dict[str, Any] | None, str, int | None]],
    ):
        self._update_runs(
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
        self._update_runs(
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
        self._update_runs(updates={k: {"skip": v} for k, v in skips.items()})

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
            num = self._get_EARs_arr().attrs["num_runs"]
        if self.use_cache and self.num_EARs_cache is None:
            self.num_EARs_cache = num
        return num

    def _get_num_persistent_parameters(self):
        return len(self._get_parameter_base_array())

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

    def _get_parameter_NEW_array_group(self, mode: str = "r") -> Group:
        return self._get_parameter_group(mode=mode).get(self._param_new_arr_grp_name)

    def _get_parameter_base_outputs_array_group(self, mode: str = "r") -> Group:
        return self._get_parameter_group(mode=mode).get(
            self._param_base_outs_arr_grp_name
        )

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

    def _get_v2_task_element_IDs_array(self, mode: str = "r") -> Array:
        return self._get_root_group(mode=mode).get(f"metadata/{self._task_v2_arr_name}")

    def _get_v2_element_iter_IDs_array(self, mode: str = "r") -> Array:
        return self._get_root_group(mode=mode).get(
            f"metadata/{self._elem_iter_IDs_arr_name}"
        )

    def _get_v2_iter_run_IDs_array(self, mode: str = "r") -> Array:
        return self._get_root_group(mode=mode).get(
            f"metadata/{self._iter_run_IDs_arr_name}"
        )

    def _get_v2_element_set_metadata_array(self, mode: str = "r") -> Array:
        return self._get_root_group(mode=mode).get(
            f"metadata/{self._elem_set_metadata_arr_name}"
        )

    def _get_v2_element_set_input_source_iter_IDs_array(self, mode: str = "r") -> Array:
        return self._get_root_group(mode=mode).get(
            f"metadata/{self._elem_set_inp_src_iter_IDs_arr_name}"
        )

    def _get_v2_element_metadata_array(self, mode: str = "r") -> Array:
        return self._get_root_group(mode=mode).get(f"metadata/{self._elem_v2_arr_name}")

    def _get_v2_element_src_idx_arrays(self, mode: str = "r") -> tuple[Array, Array]:
        root = self._get_root_group(mode=mode)
        return (
            root.get(f"metadata/{self._elem_src_idx_arr_name}"),
            root.get(f"metadata/{self._elem_src_idx_lookup_arr_name}"),
        )

    def _get_v2_iter_metadata_array(self, mode: str = "r") -> Array:
        return self._get_root_group(mode=mode).get(
            f"metadata/{self._iter_metadata_arr_name}"
        )

    def _get_v2_run_metadata_array(self, mode: str = "r") -> Array:
        return self._get_root_group(mode=mode).get(
            f"metadata/{self._run_metadata_arr_name}"
        )

    def _get_v2_local_inputs_array(self, mode: str = "r") -> Array:
        return self._get_root_group(mode=mode).get(
            f"parameters/{self._local_inputs_arr_name}"
        )

    @classmethod
    def _write_element_src_idx_arrays(cls, md_group, element_src_idx):
        esrc, esrc_lookup = graph_to_adjacency_list(
            element_src_idx, val_dtype=cls.elem_src_idx_dtype, separate_keys=True
        )
        elem_src_idx_arr = md_group.create_dataset(
            name=cls._elem_src_idx_arr_name,
            data=esrc,
            dtype=cls.elem_src_idx_dtype,
            chunks=1_000_000,
            overwrite=True,
        )
        elem_src_idx_lookup_arr = md_group.create_dataset(
            name=cls._elem_src_idx_lookup_arr_name,
            data=esrc_lookup,
            dtype=cls.elem_src_idx_lookup_dtype,
            chunks=1_000_000,
            overwrite=True,
        )

    @classmethod
    def _write_element_seq_idx_arrays(cls, md_group, element_seq_idx):
        eseq, eseq_lookup = graph_to_adjacency_list(
            element_seq_idx, val_dtype=cls.elem_seq_idx_dtype, separate_keys=True
        )
        elem_seq_idx_arr = md_group.create_dataset(
            name=cls._elem_seq_idx_arr_name,
            data=eseq,
            dtype=cls.elem_seq_idx_dtype,
            chunks=1_000_000,
            overwrite=True,
        )
        elem_seq_idx_lookup_arr = md_group.create_dataset(
            name=cls._elem_seq_idx_lookup_arr_name,
            data=eseq_lookup,
            dtype=cls.elem_seq_idx_lookup_dtype,
            chunks=1_000_000,
            overwrite=True,
        )

    @classmethod
    def _write_iter_loop_idx_arrays(cls, md_group, iter_loop_idx):
        loop_idx, loop_lookup = graph_to_adjacency_list(
            iter_loop_idx, val_dtype=cls.iter_loop_idx_dtype, separate_keys=True
        )
        iter_loop_idx_arr = md_group.create_dataset(
            name=cls._iter_loop_idx_arr_name,
            data=loop_idx,
            dtype=cls.iter_loop_idx_dtype,
            chunks=1_000_000,
            overwrite=True,
        )
        iter_loop_idx_lookup_arr = md_group.create_dataset(
            name=cls._iter_loop_idx_lookup_arr_name,
            data=loop_lookup,
            dtype=cls.iter_loop_idx_lookup_dtype,
            chunks=1_000_000,
            overwrite=True,
        )

    @classmethod
    def _write_iter_data_idx_arrays(cls, md_group, iter_data_idx):
        data_idx, data_lookup = graph_to_adjacency_list(
            iter_data_idx, val_dtype=cls.iter_data_idx_dtype, separate_keys=True
        )
        iter_loop_idx_arr = md_group.create_dataset(
            name=cls._iter_data_idx_arr_name,
            data=data_idx,
            dtype=cls.iter_data_idx_dtype,
            chunks=1_000_000,
            overwrite=True,
        )
        iter_loop_idx_lookup_arr = md_group.create_dataset(
            name=cls._iter_data_idx_lookup_arr_name,
            data=data_lookup,
            dtype=cls.iter_data_idx_lookup_dtype,
            chunks=1_000_000,
            overwrite=True,
        )

    @classmethod
    def _write_run_data_idx_arrays(cls, md_group, run_data_idx):
        data_idx, data_lookup = graph_to_adjacency_list(
            run_data_idx, val_dtype=cls.run_data_idx_dtype, separate_keys=True
        )
        run_loop_idx_arr = md_group.create_dataset(
            name=cls._run_data_idx_arr_name,
            data=data_idx,
            dtype=cls.run_data_idx_dtype,
            chunks=1_000_000,
            overwrite=True,
        )
        run_loop_idx_lookup_arr = md_group.create_dataset(
            name=cls._run_data_idx_lookup_arr_name,
            data=data_lookup,
            dtype=cls.run_data_idx_lookup_dtype,
            chunks=1_000_000,
            overwrite=True,
        )

    def _get_v2_element_seq_idx_arrays(self, mode: str = "r") -> tuple[Array, Array]:
        root = self._get_root_group(mode=mode)
        return (
            root.get(f"metadata/{self._elem_seq_idx_arr_name}"),
            root.get(f"metadata/{self._elem_seq_idx_lookup_arr_name}"),
        )

    def _get_v2_iter_loop_idx_arrays(self, mode: str = "r") -> tuple[Array, Array]:
        root = self._get_root_group(mode=mode)
        return (
            root.get(f"metadata/{self._iter_loop_idx_arr_name}"),
            root.get(f"metadata/{self._iter_loop_idx_lookup_arr_name}"),
        )

    def _get_v2_iter_data_idx_arrays(self, mode: str = "r") -> tuple[Array, Array]:
        root = self._get_root_group(mode=mode)
        return (
            root.get(f"metadata/{self._iter_data_idx_arr_name}"),
            root.get(f"metadata/{self._iter_data_idx_lookup_arr_name}"),
        )

    def _get_v2_run_data_idx_arrays(self, mode: str = "r") -> tuple[Array, Array]:
        root = self._get_root_group(mode=mode)
        return (
            root.get(f"metadata/{self._run_data_idx_arr_name}"),
            root.get(f"metadata/{self._run_data_idx_lookup_arr_name}"),
        )

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

    def _get_EARs_arr(self, mode: str = "r") -> Array:
        return self._get_metadata_group(mode=mode).get(self._EAR_arr_name)

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

        EARs_arr = md.create_dataset(
            name=cls._EAR_arr_name,
            shape=0,
            dtype=object,
            object_codec=cls._CODEC,
            chunks=1000,
        )
        EARs_arr.attrs["parameter_paths"] = []

        tasks, elems, elem_iters, EARs_ = super().prepare_test_store_from_spec(spec)

        path = Path(path).resolve()
        tasks = [ZarrStoreTask(**i).encode() for i in tasks]
        elements = [ZarrStoreElement(**i).encode(elems_arr.attrs.asdict()) for i in elems]
        elem_iters = [
            ZarrStoreElementIter(**i).encode(elem_iters_arr.attrs.asdict())
            for i in elem_iters
        ]
        EARs = [ZarrStoreEAR(**i).encode(ts_fmt, EARs_arr.attrs.asdict()) for i in EARs_]

        append_items_to_ragged_array(tasks_arr, tasks)

        elems_arr.append(np.fromiter(elements, dtype=object))
        elem_iters_arr.append(np.fromiter(elem_iters, dtype=object))
        EARs_arr.append(np.fromiter(EARs, dtype=object))

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
    def _get_persistent_EARs(self, id_lst: Iterable[int]) -> dict[int, ZarrStoreEAR]:
        runs, id_lst = self._get_cached_persistent_EARs(id_lst)
        if id_lst:
            self.logger.debug(
                f"loading {len(id_lst)} persistent EAR(s) from disk: "
                f"{shorten_list_str(id_lst)}."
            )
            arr = self._get_EARs_arr()
            attrs = arr.attrs.asdict()
            sel: tuple[NDArray, NDArray] | list[int]
            try:
                # convert to 2D array indices:
                sel = get_2D_idx(np.array(id_lst), num_cols=arr.shape[1])
            except IndexError:
                # 1D runs array from before update to 2D in Feb 2025 refactor/jobscript:
                sel = id_lst
            try:
                EAR_arr_dat = _zarr_get_coord_selection(arr, sel, self.logger)
            except BoundsCheckError:
                raise MissingStoreEARError(id_lst) from None
            EAR_dat = dict(zip(id_lst, EAR_arr_dat))
            new_runs = {
                k: ZarrStoreEAR.decode(EAR_dat=v, ts_fmt=self.ts_fmt, attrs=attrs)
                for k, v in EAR_dat.items()
            }
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
        with Console().status(f"Zipping workflow {self.workflow.name!r}..."):
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
        chunk_size: int | None = None,
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
        chunk_size: int | None = None,
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
        chunk_size: int | None = None,
        backup: bool = True,
        status: bool = True,
    ) -> Array:
        """
        Rechunk the run data to be stored more efficiently.
        """
        arr = self._get_EARs_arr()
        return self._rechunk_arr(arr, chunk_size, backup, status)

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
    ):
        raise ValueError("Already a zip store!")

    def unzip(self, path: str = ".", log: str | None = None) -> str:
        """
        Expand the persistent store.

        Parameters
        ----------
        path:
            Path at which to create the new unzipped workflow. If this is an existing
            directory, the new workflow directory will be created within this directory.
            Otherwise, this path will represent the new workflow directory path.

        """

        with Console().status(f"Unzipping workflow {self.workflow.name!r}..."):
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
        chunk_size: int | None = None,
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
