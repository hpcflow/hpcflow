"""
Persistence model based on writing JSON documents.
"""

from __future__ import annotations

from contextlib import contextmanager
import copy
import json
from pathlib import Path
from typing import cast, TYPE_CHECKING
from typing_extensions import override

from fsspec import filesystem, AbstractFileSystem  # type: ignore
import numpy as np
from hpcflow.sdk.core import RUN_DIR_ARR_DTYPE, RUN_DIR_ARR_FILL
from hpcflow.sdk.core.errors import (
    MissingParameterData,
    MissingStoreEARError,
    MissingStoreElementError,
    MissingStoreElementIterationError,
)
from hpcflow.sdk.persistence.base import (
    PersistentStoreFeatures,
    PersistentStore,
    StoreEAR,
    StoreElement,
    StoreElementIter,
    StoreParameter,
    StoreTask,
    update_param_source_dict,
)
from hpcflow.sdk.submission.submission import JOBSCRIPT_SUBMIT_TIME_KEYS
from hpcflow.sdk.persistence.pending import CommitResourceMap
from hpcflow.sdk.persistence.store_resource import JSONFileStoreResource
from hpcflow.sdk.typing import DataIndex

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence
    from datetime import datetime
    from typing import Any, ClassVar, Literal
    from typing_extensions import Self
    from numpy.typing import NDArray
    from ..app import BaseApp
    from ..core.json_like import JSONed, JSONDocument
    from ..core.workflow import Workflow
    from ..typing import ParamSource
    from .types import (
        ElemMeta,
        IterMeta,
        LoopDescriptor,
        Metadata,
        RunMeta,
        StoreCreationInfo,
        TaskMeta,
        TemplateMeta,
    )


class JsonStoreTask(StoreTask["TaskMeta"]):
    """
    Persisted task that is serialized using JSON.
    """

    @override
    def encode(self) -> tuple[int, TaskMeta, dict[str, Any]]:
        """Prepare store task data for the persistent store."""
        assert self.task_template is not None
        wk_task: TaskMeta = {
            "id_": self.id_,
            "element_IDs": self.element_IDs,
            "index": self.index,
        }
        task = {"id_": self.id_, **self.task_template}
        return self.index, wk_task, task

    @override
    @classmethod
    def decode(cls, task_dat: TaskMeta) -> Self:
        """Initialise a `StoreTask` from store task data

        Note: the `task_template` is only needed for encoding because it is retrieved as
        part of the `WorkflowTemplate` so we don't need to load it when decoding.

        """
        return cls(is_pending=False, **task_dat)


class JsonStoreElement(StoreElement["ElemMeta", None]):
    """
    Persisted element that is serialized using JSON.
    """

    @override
    def encode(self, context: None) -> ElemMeta:
        """Prepare store element data for the persistent store."""
        dct = self.__dict__
        del dct["is_pending"]
        return cast("ElemMeta", dct)

    @override
    @classmethod
    def decode(cls, elem_dat: ElemMeta, context: None) -> Self:
        """Initialise a `JsonStoreElement` from store element data"""
        return cls(is_pending=False, **elem_dat)


class JsonStoreElementIter(StoreElementIter["IterMeta", None]):
    """
    Persisted element iteration that is serialized using JSON.
    """

    @override
    def encode(self, context: None) -> IterMeta:
        """Prepare store element iteration data for the persistent store."""
        dct = self.__dict__
        del dct["is_pending"]
        return cast("IterMeta", dct)

    @override
    @classmethod
    def decode(cls, iter_dat: IterMeta, context: None) -> Self:
        """Initialise a `JsonStoreElementIter` from persistent store element iteration data"""

        iter_dat = copy.deepcopy(iter_dat)  # to avoid mutating; can we avoid this?

        # cast JSON string keys to integers:
        if EAR_IDs := iter_dat["EAR_IDs"]:
            for act_idx in list(EAR_IDs):
                EAR_IDs[int(act_idx)] = EAR_IDs.pop(act_idx)

        return cls(is_pending=False, **cast("dict", iter_dat))


class JsonStoreEAR(StoreEAR["RunMeta", None]):
    """
    Persisted element action run that is serialized using JSON.
    """

    @override
    def encode(self, ts_fmt: str, context: None) -> RunMeta:
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

    @override
    @classmethod
    def decode(cls, EAR_dat: RunMeta, ts_fmt: str, context: None) -> Self:
        """Initialise a `JsonStoreEAR` from persistent store EAR data"""
        # don't want to mutate EAR_dat:
        EAR_dat = copy.deepcopy(EAR_dat)
        start_time = cls._decode_datetime(EAR_dat.pop("start_time"), ts_fmt)
        end_time = cls._decode_datetime(EAR_dat.pop("end_time"), ts_fmt)
        return cls(
            is_pending=False,
            **cast("dict", EAR_dat),
            start_time=start_time,
            end_time=end_time,
        )


class JSONPersistentStore(
    PersistentStore[
        JsonStoreTask,
        JsonStoreElement,
        JsonStoreElementIter,
        JsonStoreEAR,
        StoreParameter,
    ]
):
    """
    A store that writes JSON files for all its state serialization.
    """

    _name: ClassVar[str] = "json"
    _features: ClassVar[PersistentStoreFeatures] = PersistentStoreFeatures(
        create=True,
        edit=True,
        jobscript_parallelism=False,
        EAR_parallelism=False,
        schedulers=True,
        submission=True,
    )

    _meta_res: ClassVar[str] = "metadata"
    _params_res: ClassVar[str] = "parameters"
    _subs_res: ClassVar[str] = "submissions"
    _runs_res: ClassVar[str] = "runs"

    _res_file_names: ClassVar[Mapping[str, str]] = {
        _meta_res: "metadata.json",
        _params_res: "parameters.json",
        _subs_res: "submissions.json",
        _runs_res: "runs.json",
    }

    _res_map: ClassVar[CommitResourceMap] = CommitResourceMap(
        commit_tasks=(_meta_res,),
        commit_loops=(_meta_res,),
        commit_loop_num_iters=(_meta_res,),
        commit_loop_parents=(_meta_res,),
        commit_submissions=(_subs_res,),
        commit_at_submit_metadata=(_subs_res,),
        commit_js_metadata=(_subs_res,),
        commit_elem_IDs=(_meta_res,),
        commit_elements=(_meta_res,),
        commit_element_sets=(_meta_res,),
        commit_elem_iter_IDs=(_meta_res,),
        commit_elem_iters=(_meta_res,),
        commit_loop_indices=(_meta_res,),
        commit_elem_iter_EAR_IDs=(_meta_res,),
        commit_EARs_initialised=(_meta_res,),
        commit_EARs=(_runs_res,),
        commit_EAR_submission_indices=(_runs_res,),
        commit_EAR_skips=(_runs_res,),
        commit_EAR_starts=(_runs_res,),
        commit_EAR_ends=(_runs_res,),
        commit_template_components=(_meta_res,),
        commit_parameters=(_params_res,),
        commit_param_sources=(_params_res,),
        commit_set_run_dirs=(_runs_res,),
        commit_iter_data_idx=(_meta_res,),
        commit_run_data_idx=(_runs_res,),
    )

    @classmethod
    def _store_task_cls(cls) -> type[JsonStoreTask]:
        return JsonStoreTask

    @classmethod
    def _store_elem_cls(cls) -> type[JsonStoreElement]:
        return JsonStoreElement

    @classmethod
    def _store_iter_cls(cls) -> type[JsonStoreElementIter]:
        return JsonStoreElementIter

    @classmethod
    def _store_EAR_cls(cls) -> type[JsonStoreEAR]:
        return JsonStoreEAR

    @classmethod
    def _store_param_cls(cls) -> type[StoreParameter]:
        return StoreParameter

    def __init__(
        self, app, workflow: Workflow | None, path: Path, fs: AbstractFileSystem
    ):
        self._resources = {
            self._meta_res: self._get_store_resource(app, "metadata", path, fs),
            self._params_res: self._get_store_resource(app, "parameters", path, fs),
            self._subs_res: self._get_store_resource(app, "submissions", path, fs),
            self._runs_res: self._get_store_resource(app, "runs", path, fs),
        }
        super().__init__(app, workflow, path, fs)

        # store-specific cache data, assigned in `using_resource()` when
        # `_use_parameters_metadata_cache` is True, and set back to None when exiting the
        # `parameters_metadata_cache` context manager.
        self._parameters_file_dat: dict[str, dict[str, Any]] | None = None

    @contextmanager
    def cached_load(self) -> Iterator[None]:
        """Context manager to cache the metadata."""
        with self.using_resource("metadata", "read"):
            with self.using_resource("runs", "read"):
                yield

    @contextmanager
    def using_resource(
        self,
        res_label: Literal["metadata", "submissions", "parameters", "attrs", "runs"],
        action: str,
    ) -> Iterator[Any]:
        """Context manager for managing `StoreResource` objects associated with the store.

        Notes
        -----
        This overridden method facilitates easier use of the
        `JSONPersistentStore`-specific implementation of the `parameters_metadata_cache`,
        which in this case is just a copy of the `parameters.json` file data.

        """

        if (
            self._use_parameters_metadata_cache
            and res_label == "parameters"
            and action == "read"
        ):
            if not self._parameters_file_dat:
                with super().using_resource(
                    cast("Literal['parameters']", res_label), action
                ) as res:
                    self._parameters_file_dat = res
            yield self._parameters_file_dat

        else:
            with super().using_resource(res_label, action) as res:
                yield res

    @contextmanager
    def parameters_metadata_cache(self) -> Iterator[None]:
        """Context manager for using the parameters-metadata cache."""
        self._use_parameters_metadata_cache = True
        try:
            yield
        finally:
            self._use_parameters_metadata_cache = False
            self._parameters_file_dat = None  # clear cache data

    def remove_replaced_dir(self) -> None:
        """
        Remove the directory containing replaced workflow details.
        """
        with self.using_resource("metadata", "update") as md:
            if "replaced_workflow" in md:
                assert self.fs is not None
                self.remove_path(md["replaced_workflow"])
                self.logger.debug("removing temporarily renamed pre-existing workflow.")
                del md["replaced_workflow"]

    def reinstate_replaced_dir(self) -> None:
        """
        Reinstate the directory containing replaced workflow details.
        """
        with self.using_resource("metadata", "read") as md:
            if "replaced_workflow" in md:
                assert self.fs is not None
                self.logger.debug(
                    "reinstating temporarily renamed pre-existing workflow."
                )
                self.rename_path(md["replaced_workflow"], self.path)

    @classmethod
    def _get_store_resource(
        cls, app: BaseApp, name: str, path: str | Path, fs: AbstractFileSystem
    ) -> JSONFileStoreResource:
        return JSONFileStoreResource(
            app=app,
            name=name,
            path=path,
            fs=fs,
            filename=cls._res_file_names[name],
        )

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
        creation_info: StoreCreationInfo,
        ts_fmt: str,
        ts_name_fmt: str,
    ) -> None:
        """
        Write an empty persistent workflow.
        """
        fs.mkdir(wk_path)
        submissions: list[None] = []
        parameters: dict[str, dict[None, None]] = {
            "data": {},
            "sources": {},
        }
        metadata: Metadata = {
            "name": name,
            "ts_fmt": ts_fmt,
            "ts_name_fmt": ts_name_fmt,
            "creation_info": creation_info,
            "template_components": template_components_js,
            "template": template_js,
            "tasks": [],
            "elements": [],
            "iters": [],
            "num_added_tasks": 0,
            "loops": [],
        }
        runs: dict[str, list] = {
            "runs": [],
            "run_dirs": [],
        }
        if replaced_wk:
            metadata["replaced_workflow"] = replaced_wk

        cls._get_store_resource(app, "metadata", wk_path, fs)._dump(metadata)
        cls._get_store_resource(app, "parameters", wk_path, fs)._dump(parameters)
        cls._get_store_resource(app, "submissions", wk_path, fs)._dump(submissions)
        cls._get_store_resource(app, "runs", wk_path, fs)._dump(runs)

    def _append_tasks(self, tasks: Iterable[StoreTask]):
        with self.using_resource("metadata", action="update") as md:
            assert "tasks" in md and "template" in md and "num_added_tasks" in md
            for task in tasks:
                idx, wk_task_i, task_i = task.encode()
                md["tasks"].insert(idx, cast("TaskMeta", wk_task_i))
                md["template"]["tasks"].insert(idx, task_i)
                md["num_added_tasks"] += 1

    def _append_loops(self, loops: dict[int, LoopDescriptor]):
        with self.using_resource("metadata", action="update") as md:
            assert "loops" in md and "template" in md
            for _, loop in loops.items():
                md["loops"].append(
                    {
                        "num_added_iterations": loop["num_added_iterations"],
                        "iterable_parameters": loop["iterable_parameters"],
                        "output_parameters": loop["output_parameters"],
                        "parents": loop["parents"],
                    }
                )
                md["template"]["loops"].append(loop["loop_template"])

    def _append_submissions(self, subs: dict[int, Mapping[str, JSONed]]):
        with self.using_resource("submissions", action="update") as subs_res:
            subs_res.extend(subs.values())

    def _append_task_element_IDs(self, task_ID: int, elem_IDs: list[int]):
        with self.using_resource("metadata", action="update") as md:
            assert "tasks" in md
            md["tasks"][task_ID]["element_IDs"].extend(elem_IDs)

    def _append_elements(self, elems: Sequence[JsonStoreElement]):
        with self.using_resource("metadata", action="update") as md:
            assert "elements" in md
            md["elements"].extend(elem.encode(None) for elem in elems)

    def _append_element_sets(self, task_id: int, es_js: Sequence[Mapping]):
        task_idx = self._get_task_id_to_idx_map()[task_id]
        with self.using_resource("metadata", "update") as md:
            assert "template" in md
            md["template"]["tasks"][task_idx]["element_sets"].extend(es_js)

    def _append_elem_iter_IDs(self, elem_ID: int, iter_IDs: Iterable[int]):
        with self.using_resource("metadata", action="update") as md:
            assert "elements" in md
            md["elements"][elem_ID]["iteration_IDs"].extend(iter_IDs)

    def _append_elem_iters(self, iters: Sequence[JsonStoreElementIter]):
        with self.using_resource("metadata", action="update") as md:
            assert "iters" in md
            md["iters"].extend(it.encode(None) for it in iters)

    def _append_elem_iter_EAR_IDs(
        self, iter_ID: int, act_idx: int, EAR_IDs: Sequence[int]
    ):
        with self.using_resource("metadata", action="update") as md:
            assert "iters" in md
            md["iters"][iter_ID].setdefault("EAR_IDs", {}).setdefault(act_idx, []).extend(
                EAR_IDs
            )

    def _update_elem_iter_EARs_initialised(self, iter_ID: int):
        with self.using_resource("metadata", action="update") as md:
            assert "iters" in md
            md["iters"][iter_ID]["EARs_initialised"] = True

    def _update_at_submit_metadata(self, at_submit_metadata: dict[int, dict[str, Any]]):
        with self.using_resource("submissions", action="update") as subs_res:
            for sub_idx, metadata_i in at_submit_metadata.items():
                sub = subs_res[sub_idx]
                assert isinstance(sub, dict)
                for dt_str, parts_j in metadata_i["submission_parts"].items():
                    sub["at_submit_metadata"]["submission_parts"][dt_str] = parts_j

    def _update_loop_index(self, loop_indices: dict[int, dict[str, int]]):
        with self.using_resource("metadata", action="update") as md:
            assert "iters" in md
            for iter_ID, loop_idx in loop_indices.items():
                md["iters"][iter_ID]["loop_idx"].update(loop_idx)

    def _update_loop_num_iters(self, index: int, num_iters: list[list[list[int] | int]]):
        with self.using_resource("metadata", action="update") as md:
            assert "loops" in md
            md["loops"][index]["num_added_iterations"] = num_iters

    def _update_loop_parents(self, index: int, parents: list[str]):
        with self.using_resource("metadata", action="update") as md:
            assert "loops" in md
            md["loops"][index]["parents"] = parents

    def _update_iter_data_indices(self, iter_data_indices: dict[int, DataIndex]):
        with self.using_resource("metadata", action="update") as md:
            assert "iters" in md
            for iter_ID, dat_idx in iter_data_indices.items():
                md["iters"][iter_ID]["data_idx"].update(dat_idx)

    def _update_run_data_indices(self, run_data_indices: dict[int, DataIndex]):
        with self.using_resource("runs", action="update") as md:
            assert "runs" in md
            for run_ID, dat_idx in run_data_indices.items():
                md["runs"][run_ID]["data_idx"].update(dat_idx)

    def _append_EARs(self, EARs: Sequence[JsonStoreEAR]):
        with self.using_resource("runs", action="update") as md:
            assert "runs" in md
            assert "run_dirs" in md
            md["runs"].extend(i.encode(self.ts_fmt, None) for i in EARs)
            md["run_dirs"].extend([None] * len(EARs))

    def _set_run_dirs(self, run_dir_arr: np.ndarray, run_idx: np.ndarray):
        with self.using_resource("runs", action="update") as md:
            assert "run_dirs" in md
            dirs_lst = md["run_dirs"]
            for idx, r_idx in enumerate(run_idx):
                dirs_lst[r_idx] = run_dir_arr[idx].item()
            md["run_dirs"] = dirs_lst

    def _update_EAR_submission_data(self, sub_data: Mapping[int, tuple[int, int | None]]):
        with self.using_resource("runs", action="update") as md:
            assert "runs" in md
            for EAR_ID_i, (sub_idx_i, cmd_file_ID) in sub_data.items():
                md["runs"][EAR_ID_i]["submission_idx"] = sub_idx_i
                md["runs"][EAR_ID_i]["commands_file_ID"] = cmd_file_ID

    def _update_EAR_start(
        self,
        run_starts: dict[int, tuple[datetime, dict[str, Any] | None, str, int | None]],
    ):
        with self.using_resource("runs", action="update") as md:
            assert "runs" in md
            for run_id, (s_time, s_snap, s_hn, port_number) in run_starts.items():
                md["runs"][run_id]["start_time"] = s_time.strftime(self.ts_fmt)
                md["runs"][run_id]["snapshot_start"] = s_snap
                md["runs"][run_id]["run_hostname"] = s_hn
                md["runs"][run_id]["port_number"] = port_number

    def _update_EAR_end(
        self, run_ends: dict[int, tuple[datetime, dict[str, Any] | None, int, bool]]
    ):
        with self.using_resource("runs", action="update") as md:
            assert "runs" in md
            for run_id, (e_time, e_snap, ext_code, success) in run_ends.items():
                md["runs"][run_id]["end_time"] = e_time.strftime(self.ts_fmt)
                md["runs"][run_id]["snapshot_end"] = e_snap
                md["runs"][run_id]["exit_code"] = ext_code
                md["runs"][run_id]["success"] = success

    def _update_EAR_skip(self, skips: dict[int, int]):
        with self.using_resource("runs", action="update") as md:
            assert "runs" in md
            for run_ID, reason in skips.items():
                md["runs"][run_ID]["skip"] = reason

    def _update_js_metadata(self, js_meta: dict[int, dict[int, dict[str, Any]]]):
        with self.using_resource("submissions", action="update") as sub_res:
            for sub_idx, all_js_md in js_meta.items():
                sub = cast("dict[str, list[dict[str, Any]]]", sub_res[sub_idx])
                for js_idx, js_meta_i in all_js_md.items():
                    self.logger.info(
                        f"updating jobscript metadata for (sub={sub_idx}, js={js_idx}): "
                        f"{js_meta_i!r}."
                    )
                    _at_submit_md = {
                        k: js_meta_i.pop(k)
                        for k in JOBSCRIPT_SUBMIT_TIME_KEYS
                        if k in js_meta_i
                    }
                    sub["jobscripts"][js_idx].update(**js_meta_i)
                    sub["jobscripts"][js_idx]["at_submit_metadata"].update(
                        **_at_submit_md
                    )

    def _append_parameters(self, params: Sequence[StoreParameter]):
        with self.using_resource("parameters", "update") as params_u:
            for param_i in params:
                params_u["data"][str(param_i.id_)] = param_i.encode()
                params_u["sources"][str(param_i.id_)] = param_i.source

    def _set_parameter_values(self, set_parameters: dict[int, tuple[Any, bool]]):
        """Set multiple unset persistent parameters."""
        param_objs = self._get_persistent_parameters(set_parameters)
        with self.using_resource("parameters", "update") as params:
            for param_id, (value, is_file) in set_parameters.items():
                param_i = param_objs[param_id]
                if is_file:
                    param_i = param_i.set_file(value)
                else:
                    param_i = param_i.set_data(value)
                params["data"][str(param_id)] = param_i.encode()

    def _update_parameter_sources(self, sources: Mapping[int, ParamSource]):
        """Update the sources of multiple persistent parameters."""
        param_objs = self._get_persistent_parameters(sources)
        with self.using_resource("parameters", "update") as params:
            # no need to update data array:
            for p_id, src_i in sources.items():
                param_i = param_objs[p_id]
                new_src_i = update_param_source_dict(param_i.source, src_i)
                params["sources"][str(p_id)] = new_src_i

    def _update_template_components(self, tc: dict[str, Any]):
        with self.using_resource("metadata", "update") as md:
            md["template_components"] = tc

    def _get_num_persistent_tasks(self) -> int:
        """Get the number of persistent tasks."""
        if self.use_cache and self.num_tasks_cache is not None:
            num = self.num_tasks_cache
        else:
            with self.using_resource("metadata", action="read") as md:
                assert "tasks" in md
                num = len(md["tasks"])
        if self.use_cache and self.num_tasks_cache is None:
            self.num_tasks_cache = num
        return num

    def _get_num_persistent_loops(self) -> int:
        """Get the number of persistent loops."""
        with self.using_resource("metadata", action="read") as md:
            assert "loops" in md
            return len(md["loops"])

    def _get_num_persistent_submissions(self) -> int:
        """Get the number of persistent submissions."""
        with self.using_resource("submissions", "read") as subs_res:
            return len(subs_res)

    def _get_num_persistent_elements(self) -> int:
        """Get the number of persistent elements."""
        with self.using_resource("metadata", action="read") as md:
            assert "elements" in md
            return len(md["elements"])

    def _get_num_persistent_elem_iters(self) -> int:
        """Get the number of persistent element iterations."""
        with self.using_resource("metadata", action="read") as md:
            assert "iters" in md
            return len(md["iters"])

    def _get_num_persistent_EARs(self) -> int:
        """Get the number of persistent EARs."""
        if self.use_cache and self.num_EARs_cache is not None:
            num = self.num_EARs_cache
        else:
            with self.using_resource("runs", action="read") as md:
                assert "runs" in md
                num = len(md["runs"])
        if self.use_cache and self.num_EARs_cache is None:
            self.num_EARs_cache = num
        return num

    def _get_num_persistent_parameters(self) -> int:
        if self.use_cache and self.num_params_cache is not None:
            num = self.num_params_cache
        else:
            with self.using_resource("parameters", "read") as params:
                assert "data" in params
                num = len(params["data"])
        if self.use_cache and self.num_params_cache is None:
            self.num_params_cache = num
        return num

    def _get_num_persistent_added_tasks(self) -> int:
        with self.using_resource("metadata", "read") as md:
            assert "num_added_tasks" in md
            return md["num_added_tasks"]

    @classmethod
    def make_test_store_from_spec(
        cls,
        app: BaseApp,
        spec,
        dir=None,
        path="test_store.json",
        overwrite=False,
        ts_fmt="%d/%m/%Y, %H:%M:%S",  # FIXME: use the right default timestamp format
    ):
        """Generate an store for testing purposes."""

        tasks_, elems, elem_iters, EARs = super().prepare_test_store_from_spec(spec)

        path_ = Path(path).resolve()
        tasks = [JsonStoreTask(**task_info).encode() for task_info in tasks_]
        elements_ = [JsonStoreElement(**elem_info).encode(None) for elem_info in elems]
        elem_iters_ = [
            JsonStoreElementIter(**it_info).encode(None) for it_info in elem_iters
        ]
        EARs_ = [JsonStoreEAR(**ear_info).encode(ts_fmt, None) for ear_info in EARs]

        persistent_data = {
            "tasks": tasks,
            "elements": elements_,
            "iters": elem_iters_,
            "runs": EARs_,
        }

        path_ = Path(dir or "", path_)
        with path_.open("wt") as fp:
            json.dump(persistent_data, fp, indent=2)

        return cls(app=app, workflow=None, path=path_, fs=filesystem("file"))

    def _get_persistent_template_components(self) -> dict[str, Any]:
        with self.using_resource("metadata", "read") as md:
            assert "template_components" in md
            return md["template_components"]

    def _get_persistent_template(self) -> dict[str, JSONed]:
        with self.using_resource("metadata", "read") as md:
            assert "template" in md
            return cast("dict[str, JSONed]", md["template"])

    def _get_persistent_tasks(self, id_lst: Iterable[int]) -> dict[int, JsonStoreTask]:
        tasks, id_lst = self._get_cached_persistent_tasks(id_lst)
        if id_lst:
            with self.using_resource("metadata", action="read") as md:
                assert "tasks" in md
                new_tasks = {
                    i["id_"]: JsonStoreTask.decode({**i, "index": idx})
                    for idx, i in enumerate(cast("Sequence[TaskMeta]", md["tasks"]))
                    if id_lst is None or i["id_"] in id_lst
                }
                self.task_cache.update(new_tasks)
                tasks.update(new_tasks)
        return tasks

    def _get_persistent_loops(
        self, id_lst: Iterable[int] | None = None
    ) -> dict[int, LoopDescriptor]:
        with self.using_resource("metadata", "read") as md:
            assert "loops" in md
            return {
                idx: cast("LoopDescriptor", i)
                for idx, i in enumerate(md["loops"])
                if id_lst is None or idx in id_lst
            }

    def _get_persistent_submissions(
        self, id_lst: Iterable[int] | None = None
    ) -> dict[int, Mapping[str, JSONed]]:
        with self.using_resource("submissions", "read") as sub_res:
            subs_dat = copy.deepcopy(
                {
                    idx: i
                    for idx, i in enumerate(sub_res)
                    if id_lst is None or idx in id_lst
                }
            )
            # cast jobscript `task_elements` keys:
            for sub in subs_dat.values():
                js: dict[str, Any]
                assert isinstance(sub, dict)
                for js in sub["jobscripts"]:
                    blk: dict[str, Any]
                    assert isinstance(js, dict)
                    for blk in js["blocks"]:
                        for key in list(te := blk["task_elements"]):
                            te[int(key)] = te.pop(key)

        return subs_dat

    def _get_persistent_elements(
        self, id_lst: Iterable[int]
    ) -> dict[int, JsonStoreElement]:
        elems, id_lst_ = self._get_cached_persistent_elements(id_lst)
        if id_lst_:
            # could convert `id_lst` to e.g. slices if more efficient for a given store
            with self.using_resource("metadata", action="read") as md:
                try:
                    if "elements" not in md:
                        raise KeyError
                    elem_dat = {id_: md["elements"][id_] for id_ in id_lst_}
                except KeyError:
                    raise MissingStoreElementError(id_lst_)
                new_elems = {
                    k: JsonStoreElement.decode(v, None) for k, v in elem_dat.items()
                }
                self.element_cache.update(new_elems)
                elems.update(new_elems)
        return elems

    def _get_persistent_element_iters(
        self, id_lst: Iterable[int]
    ) -> dict[int, JsonStoreElementIter]:
        iters, id_lst_ = self._get_cached_persistent_element_iters(id_lst)
        if id_lst_:
            with self.using_resource("metadata", action="read") as md:
                try:
                    if "iters" not in md:
                        raise KeyError
                    iter_dat = {id_: md["iters"][id_] for id_ in id_lst_}
                except KeyError:
                    raise MissingStoreElementIterationError(id_lst_)
                new_iters = {
                    k: JsonStoreElementIter.decode(v, None) for k, v in iter_dat.items()
                }
                self.element_iter_cache.update(new_iters)
                iters.update(new_iters)
        return iters

    def _get_persistent_EARs(self, id_lst: Iterable[int]) -> dict[int, JsonStoreEAR]:
        runs, id_lst_ = self._get_cached_persistent_EARs(id_lst)
        if id_lst_:
            with self.using_resource("runs", action="read") as md:
                try:
                    if "runs" not in md:
                        raise KeyError
                    EAR_dat = {id_: md["runs"][id_] for id_ in id_lst_}
                except KeyError:
                    raise MissingStoreEARError(id_lst_)
                new_runs = {
                    k: JsonStoreEAR.decode(v, self.ts_fmt, None)
                    for k, v in EAR_dat.items()
                }
                self.EAR_cache.update(new_runs)
                runs.update(new_runs)
        return runs

    def _get_persistent_parameters(
        self, id_lst: Iterable[int], **kwargs
    ) -> Mapping[int, StoreParameter]:
        params, id_lst_ = self._get_cached_persistent_parameters(id_lst)
        if id_lst_:
            with self.using_resource("parameters", "read") as params_:
                try:
                    param_dat = {id_: params_["data"][str(id_)] for id_ in id_lst_}
                    src_dat = {id_: params_["sources"][str(id_)] for id_ in id_lst_}
                except KeyError:
                    raise MissingParameterData(id_lst_)

            new_params = {
                k: StoreParameter.decode(id_=k, data=v, source=src_dat[k])
                for k, v in param_dat.items()
            }
            self.parameter_cache.update(new_params)
            params.update(new_params)
        return params

    def _get_persistent_param_sources(
        self, id_lst: Iterable[int]
    ) -> dict[int, ParamSource]:
        sources, id_lst_ = self._get_cached_persistent_param_sources(id_lst)
        if id_lst_:
            with self.using_resource("parameters", "read") as params:
                try:
                    new_sources = {id_: params["sources"][str(id_)] for id_ in id_lst_}
                except KeyError:
                    raise MissingParameterData(id_lst_)
            self.param_sources_cache.update(new_sources)
            sources.update(new_sources)
        return sources

    def _get_persistent_parameter_set_status(
        self, id_lst: Iterable[int]
    ) -> dict[int, bool]:
        with self.using_resource("parameters", "read") as params:
            try:
                param_dat = {id_: params["data"][str(id_)] for id_ in id_lst}
            except KeyError:
                raise MissingParameterData(id_lst)
        return {k: v is not None for k, v in param_dat.items()}

    def _get_persistent_parameter_IDs(self) -> list[int]:
        with self.using_resource("parameters", "read") as params:
            return [int(i) for i in params["data"]]

    def get_ts_fmt(self) -> str:
        """
        Get the format for timestamps.
        """
        with self.using_resource("metadata", action="read") as md:
            assert "ts_fmt" in md
            return md["ts_fmt"]

    def get_ts_name_fmt(self) -> str:
        """
        Get the format for timestamps to use in names.
        """
        with self.using_resource("metadata", action="read") as md:
            assert "ts_name_fmt" in md
            return md["ts_name_fmt"]

    def get_creation_info(self) -> StoreCreationInfo:
        """
        Get information about the creation of the workflow.
        """
        with self.using_resource("metadata", action="read") as md:
            assert "creation_info" in md
            return copy.deepcopy(md["creation_info"])

    def get_name(self) -> str:
        """
        Get the name of the workflow.
        """
        with self.using_resource("metadata", action="read") as md:
            assert "name" in md
            return md["name"]

    def zip(
        self,
        path: str = ".",
        log: str | None = None,
        overwrite=False,
        include_execute=False,
        include_rechunk_backups=False,
    ) -> str:
        raise TypeError("unsupported operation: zipping-json")

    def unzip(self, path: str = ".", log: str | None = None) -> str:
        raise TypeError("unsupported operation: unzipping-json")

    def rechunk_parameter_base(
        self,
        chunk_size: int | None = None,
        backup: bool = True,
        status: bool = True,
    ) -> Any:
        raise TypeError("unsupported operation: rechunk-json")

    def rechunk_runs(
        self,
        chunk_size: int | None = None,
        backup: bool = True,
        status: bool = True,
    ) -> Any:
        raise TypeError("unsupported operation: rechunk-json")

    def get_dirs_array(self) -> NDArray:
        """
        Retrieve the run directories array.
        """
        with self.using_resource("runs", action="read") as md:
            dirs_lst = md["run_dirs"]
            dirs_arr = np.zeros(len(dirs_lst), dtype=RUN_DIR_ARR_DTYPE)
            dirs_arr[:] = RUN_DIR_ARR_FILL
            for idx, i in enumerate(dirs_lst):
                if i is not None:
                    dirs_arr[idx] = tuple(i)
        return dirs_arr
