"""
Persistence model based on writing Zarr arrays.
"""

from __future__ import annotations

import copy
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast, TYPE_CHECKING
from typing_extensions import override
import shutil
import time

import numpy as np
from numpy.ma.core import MaskedArray
import zarr  # type: ignore
from zarr.errors import BoundsCheckError  # type: ignore
from zarr.storage import DirectoryStore, FSStore  # type: ignore
from fsspec.implementations.zip import ZipFileSystem  # type: ignore
from rich.console import Console
from numcodecs import MsgPack, VLenArray, blosc, Blosc, Zstd  # type: ignore
from reretry import retry  # type: ignore

from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.errors import (
    MissingParameterData,
    MissingStoreEARError,
    MissingStoreElementError,
    MissingStoreElementIterationError,
    MissingStoreTaskError,
)
from hpcflow.sdk.core.utils import ensure_in, get_relative_path, set_in_container
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
from hpcflow.sdk.persistence.utils import ask_pw_on_auth_exc
from hpcflow.sdk.persistence.pending import CommitResourceMap
from hpcflow.sdk.persistence.base import update_param_source_dict
from hpcflow.sdk.log import TimeIt

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
    from datetime import datetime
    from fsspec import AbstractFileSystem  # type: ignore
    from logging import Logger
    from typing import ClassVar
    from typing_extensions import Self, TypeAlias
    from numpy.typing import NDArray
    from zarr import Array, Group  # type: ignore
    from zarr.attrs import Attributes  # type: ignore
    from zarr.storage import Store  # type: ignore
    from .types import TypeLookup
    from ..app import BaseApp
    from ..core.json_like import JSONed, JSONDocument
    from ..typing import ParamSource, PathLike


#: List of any (Zarr-serializable) value.
ListAny: TypeAlias = "list[Any]"
#: Zarr attribute mapping context.
ZarrAttrs: TypeAlias = "dict[str, list[str]]"
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
) -> int:
    # Might need to generate new group:
    param_arr_group = root_group.require_group(arr_path)
    new_idx = (
        max((int(i.removeprefix("arr_")) for i in param_arr_group.keys()), default=-1) + 1
    )
    param_arr_group.create_dataset(name=f"arr_{new_idx}", data=obj)
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
):
    data_idx = _encode_numpy_array(obj.data, type_lookup, path, root_group, arr_path)
    mask_idx = _encode_numpy_array(obj.mask, type_lookup, path, root_group, arr_path)
    type_lookup["masked_arrays"].append([path, [data_idx, mask_idx]])


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

        data = arr_group.get(f"arr_{data_idx}")
        mask = arr_group.get(f"arr_{mask_idx}")
        dataset: MaskedArray = MaskedArray(data=data, mask=mask)

        if rel_path:
            set_in_container(obj_, rel_path, dataset)
        else:
            obj_ = dataset

    return obj_


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

    @override
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
        ]

    @override
    @classmethod
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
    _param_data_arr_grp_name: ClassVar = lambda _, param_idx: f"param_{param_idx}"
    _task_arr_name: ClassVar[str] = "tasks"
    _elem_arr_name: ClassVar[str] = "elements"
    _iter_arr_name: ClassVar[str] = "iters"
    _EAR_arr_name: ClassVar[str] = "runs"
    _time_res: ClassVar[str] = "us"  # microseconds; must not be smaller than micro!

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

        md = root.create_group("metadata")

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

        EARs_arr = md.create_dataset(
            name=cls._EAR_arr_name,
            shape=0,
            dtype=object,
            object_codec=cls._CODEC,
            chunks=1,  # single-chunk rows for multiprocess writing
            compressor=cmp,
        )
        EARs_arr.attrs["parameter_paths"] = []

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
                        "parents": loop["parents"],
                    }
                )
                attrs["template"]["loops"].append(loop["loop_template"])

    def _append_submissions(self, subs: dict[int, JSONDocument]):
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

    def _append_submission_parts(self, sub_parts: dict[int, dict[str, list[int]]]):
        with self.using_resource("attrs", action="update") as attrs:
            for sub_idx, sub_i_parts in sub_parts.items():
                sub = cast("dict", attrs["submissions"][sub_idx])
                for dt_str, parts_j in sub_i_parts.items():
                    sub["submission_parts"][dt_str] = parts_j

    def _update_loop_index(self, iter_ID: int, loop_idx: Mapping[str, int]):
        arr = self._get_iters_arr(mode="r+")
        attrs = self.__as_dict(arr.attrs)
        iter_dat = cast("list", arr[iter_ID])
        store_iter = ZarrStoreElementIter.decode(iter_dat, attrs)
        store_iter = store_iter.update_loop_idx(loop_idx)
        arr[iter_ID] = store_iter.encode(attrs)

    def _update_loop_num_iters(self, index: int, num_iters: list[list[list[int] | int]]):
        with self.using_resource("attrs", action="update") as attrs:
            attrs["loops"][index]["num_added_iterations"] = num_iters

    def _update_loop_parents(self, index: int, parents: list[str]):
        with self.using_resource("attrs", action="update") as attrs:
            attrs["loops"][index]["parents"] = parents

    def _append_EARs(self, EARs: Sequence[ZarrStoreEAR]):
        arr = self._get_EARs_arr(mode="r+")
        with self.__mutate_attrs(arr) as attrs:
            arr_add = np.empty((len(EARs)), dtype=object)
            arr_add[:] = [ear.encode(self.ts_fmt, attrs) for ear in EARs]
            arr.append(arr_add)

    @TimeIt.decorator
    def _update_EAR_submission_indices(self, sub_indices: Mapping[int, int]):
        EAR_IDs = list(sub_indices)
        EARs = self._get_persistent_EARs(EAR_IDs)

        arr = self._get_EARs_arr(mode="r+")
        with self.__mutate_attrs(arr) as attrs:
            for EAR_ID_i, sub_idx_i in sub_indices.items():
                new_EAR_i = EARs[EAR_ID_i].update(submission_idx=sub_idx_i)
                # seems to be a Zarr bug that prevents `set_coordinate_selection` with an
                # object array, so set one-by-one:
                arr[EAR_ID_i] = new_EAR_i.encode(self.ts_fmt, attrs)

    def _update_EAR_start(
        self, EAR_id: int, s_time: datetime, s_snap: dict[str, Any], s_hn: str
    ):
        arr = self._get_EARs_arr(mode="r+")
        with self.__mutate_attrs(arr) as attrs:
            EAR_i = self._get_persistent_EARs([EAR_id])[EAR_id]
            EAR_i = EAR_i.update(
                start_time=s_time,
                snapshot_start=s_snap,
                run_hostname=s_hn,
            )
            arr[EAR_id] = EAR_i.encode(self.ts_fmt, attrs)

    def _update_EAR_end(
        self,
        EAR_id: int,
        e_time: datetime,
        e_snap: dict[str, Any],
        ext_code: int,
        success: bool,
    ):
        arr = self._get_EARs_arr(mode="r+")
        with self.__mutate_attrs(arr) as attrs:
            EAR_i = self._get_persistent_EARs([EAR_id])[EAR_id]
            EAR_i = EAR_i.update(
                end_time=e_time,
                snapshot_end=e_snap,
                exit_code=ext_code,
                success=success,
            )
            arr[EAR_id] = EAR_i.encode(self.ts_fmt, attrs)

    def _update_EAR_skip(self, EAR_id: int):
        arr = self._get_EARs_arr(mode="r+")
        with self.__mutate_attrs(arr) as attrs:
            EAR_i = self._get_persistent_EARs([EAR_id])[EAR_id]
            EAR_i = EAR_i.update(skip=True)
            arr[EAR_id] = EAR_i.encode(self.ts_fmt, attrs)

    def _update_js_metadata(self, js_meta: dict[int, dict[int, dict[str, Any]]]):
        with self.using_resource("attrs", action="update") as attrs:
            for sub_idx, all_js_md in js_meta.items():
                sub = cast(
                    "dict[str, list[dict[str, Any]]]", attrs["submissions"][sub_idx]
                )
                for js_idx, js_meta_i in all_js_md.items():
                    sub["jobscripts"][js_idx].update(**js_meta_i)

    def _append_parameters(self, params: Sequence[StoreParameter]):
        """Add new persistent parameters."""
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
            num = len(self._get_EARs_arr())
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
        return self._get_root_group(mode=mode).get("metadata")

    def _get_tasks_arr(self, mode: str = "r") -> Array:
        return self._get_metadata_group(mode=mode).get(self._task_arr_name)

    def _get_elements_arr(self, mode: str = "r") -> Array:
        return self._get_metadata_group(mode=mode).get(self._elem_arr_name)

    def _get_iters_arr(self, mode: str = "r") -> Array:
        return self._get_metadata_group(mode=mode).get(self._iter_arr_name)

    def _get_EARs_arr(self, mode: str = "r") -> Array:
        return self._get_metadata_group(mode=mode).get(self._EAR_arr_name)

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
    def _get_persistent_submissions(self, id_lst: Iterable[int] | None = None):
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
            # cast jobscript submit-times and jobscript `task_elements` keys:
            for sub in subs_dat.values():
                for js in cast("_JS", sub)["jobscripts"]:
                    task_elements = js["task_elements"]
                    for key in list(task_elements):
                        task_elements[int(key)] = task_elements.pop(key)

        return subs_dat

    @TimeIt.decorator
    def _get_persistent_elements(
        self, id_lst: Iterable[int]
    ) -> dict[int, ZarrStoreElement]:
        elems, id_lst = self._get_cached_persistent_elements(id_lst)
        if id_lst:
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
            arr = self._get_EARs_arr()
            attrs = arr.attrs.asdict()
            try:
                self.logger.debug(f"_get_persistent_EARs: {id_lst=}")
                EAR_arr_dat = _zarr_get_coord_selection(arr, id_lst, self.logger)
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
        params, id_lst = self._get_cached_persistent_parameters(id_lst)
        if id_lst:
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
        arr_path = Path(self.workflow.path) / arr.path
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
        arr = zarr.open(arr_path)
        if status:
            s.update("Creating new array...")
        arr_rc = zarr.create(
            store=arr_rc_path,
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
