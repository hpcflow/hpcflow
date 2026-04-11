from __future__ import annotations

from collections import defaultdict, namedtuple
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any
import copy

import numpy as np
from fsspec.implementations.local import LocalFileSystem  # type: ignore
from fsspec.implementations.zip import ZipFileSystem  # type: ignore
from fsspec.core import url_to_fs  # type: ignore

from hpcflow.sdk.core.app_aware import AppAware
from hpcflow.sdk.persistence.utils import ask_pw_on_auth_exc, infer_store
from hpcflow.sdk.persistence import store_cls_from_str
from hpcflow.sdk.log import TimeIt

if TYPE_CHECKING:

    from ..typing import PathLike
    from ..core.types import AbstractFileSystem
    from numpy.typing import NDArray


def resolve_fsspec(
    path: PathLike, **kwargs
) -> tuple[AbstractFileSystem, str, str | None]:

    # TODO: THIS IS COPIED FROM workflow.py!!!!!!!!!!!!!!
    """
    Decide how to handle a particular virtual path.

    Parameters
    ----------
    kwargs
        This can include a `password` key, for connections via SSH.

    """

    path_s = str(path)
    fs: AbstractFileSystem
    if path_s.endswith(".zip"):
        # `url_to_fs` does not seem to work for zip combos e.g. `zip::ssh://`, so we
        # construct a `ZipFileSystem` ourselves and assume it is signified only by the
        # file extension:
        fs, pw = ask_pw_on_auth_exc(
            ZipFileSystem,
            fo=path_s,
            mode="r",
            target_options=kwargs or {},
            add_pw_to="target_options",
        )
        path_s = ""

    else:
        (fs, path_s), pw = ask_pw_on_auth_exc(url_to_fs, path_s, **kwargs)
        path_s = str(Path(path_s).as_posix())
        if isinstance(fs, LocalFileSystem):
            path_s = str(Path(path_s).resolve())

    return fs, path_s, pw


def _process_run_IDs(run_IDs):
    if isinstance(run_IDs, int):
        return [run_IDs], True
    else:
        lst = list(run_IDs)
        if len(set(lst)) < len(lst):
            raise ValueError("Specify unique run IDs.")
        return lst, False


class TrackedDict(dict):

    def __init__(self, callback, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.callback = callback

    def __setitem__(self, key, value):
        self.callback()
        return super().__setitem__(key, value)


class TrackedDefaultDict(defaultdict):

    def __init__(self, default_factory, callback, *args, **kwargs):
        super().__init__(default_factory, *args, **kwargs)
        self.callback = callback

    def __setitem__(self, key, value):
        self.callback()
        return super().__setitem__(key, value)

    def __missing__(self, key):
        value = self.default_factory()
        self[key] = value
        return value


class TrackedArray:
    """A numpy array wrapper that tracks mutations (append, setitem)."""

    def __init__(self, append_cb, set_cb, update_cb=None, data=None):
        self._append_cb = append_cb
        self._set_cb = set_cb
        self._update_cb = update_cb
        self._arr = np.array(data if data is not None else [])

    def append(self, value):
        """Simulate append by concatenation (since np.append makes a copy)."""
        dtype = self._arr.dtype
        upd_slice = slice(len(self._arr), len(value))
        self._arr = np.append(self._arr, np.asarray(value, dtype=dtype))
        self._append_cb(upd_slice)
        return self

    def update_field(
        self, name: str, idx: int | Sequence[int], values: Any | Sequence[Any]
    ):
        """Modify the specified field's values in a structured array."""
        assert name in self._arr.dtype.names
        self._arr[name][idx] = values
        if self._update_cb:
            self._update_cb(idx)

    def __setitem__(self, idx, value):
        self._arr[idx] = value
        self._set_cb(idx)

    def __getitem__(self, idx):
        return self._arr[idx]

    def __len__(self):
        return len(self._arr)

    def __repr__(self):
        return f"TrackedNumpyArray({repr(self._arr)})"


class TrackedList(list):
    """A list wrapper that tracks inserts."""

    def __init__(self, callback, *args):
        super().__init__(*args)
        self.callback = callback

    def append(self, item):
        self.callback(("append", item))
        super().append(item)

    def extend(self, iterable):
        for item in iterable:
            self.callback(("extend", item))
        super().extend(iterable)

    def insert(self, index, item):
        self.callback(("insert", index, item))
        super().insert(index, item)

    def __setitem__(self, index, value):
        self.callback(("setitem", index, value))
        super().__setitem__(index, value)


class TaskTemplatesDict(TrackedDict):

    def __init__(self, callback, element_set_cb, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.callback = callback
        self.element_set_cb = element_set_cb

    def __setitem__(self, key, value):
        assert isinstance(value, dict)
        value["element_sets"] = TrackedList(callback=self.element_set_cb)
        return super().__setitem__(key, value)


class LoopsDict(TrackedDict):

    def __setitem__(self, key, value):
        assert isinstance(value, dict)
        return super().__setitem__(key, TrackedDict(self.callback, value))


class TrackedInt(int):
    def __new__(cls, value, callback):
        # `int` is immutable so initialisation occurs here in `__new__`
        obj = super().__new__(cls, value)
        obj.callback = callback
        return obj

    def __add__(self, other):
        new_value = int(self) + other
        self.callback()
        return TrackedInt(new_value, self.callback)

    def __iadd__(self, other):
        new_value = int(self) + other
        self.callback()
        return TrackedInt(new_value, self.callback)

    def __sub__(self, other):
        new_value = int(self) - other
        self.callback()
        return TrackedInt(new_value, self.callback)

    def __isub__(self, other):
        new_value = int(self) - other
        self.callback()
        return TrackedInt(new_value, self.callback)


class TrackedOutputArrayShape(TrackedList):

    def __init__(self, callback, args=None):
        args = [TrackedInt(i, callback) for i in (args or [0, 0])]
        super().__init__(callback, args)


class BaseOutputs:
    """An interface to the persistent store's base outputs array, providing getting and
    setting methods."""

    def __init__(self, data: WorkflowData):
        self.data = data

    @property
    def store(self):
        return self.data.store

    # mainly a wrapper to get the indices right? then calls store methods?
    # want to do:
    #  workflow._data.base_outputs.get(task_ID, element_idx, iteration_idx, action_idx, path="p2")
    #  workflow._data.base_outputs.set(task_ID, element_idx, iteration_idx, action_idx, path="p2", value=...)
    #  workflow._data.base_outputs.get(run_ID, paths=("p2",))
    #  workflow._data.base_outputs.get(run_ID) # return all base outputs for the specified run
    #  workflow._data.base_outputs.get(run_IDs) # return all base outputs for multiple runs
    #  workflow._data.base_outputs.set({0: {"p2": ..., "p3": ...,}, 1: {"p2": ..., ...}})

    def indices_by_task(self, run_IDs: Sequence[int]):
        run_IDs_arr = np.asarray(run_IDs)
        run_parents = self.data.get_run_membership(run_IDs_arr)
        srt_idx = np.argsort(run_parents, order="task_ID")
        run_parents_srt = run_parents[srt_idx]
        run_IDs_srt = run_IDs_arr[srt_idx]
        uq_tasks, uq_idx = np.unique(run_parents_srt["task_ID"], return_index=True)
        fields = ["element_idx", "iteration_idx", "action_idx"]
        for idx, task_ID_i in enumerate(uq_tasks):
            start = uq_idx[idx].item()
            end = (
                uq_idx[idx + 1].item()
                if idx < len(uq_tasks) - 1
                else len(run_parents_srt)
            )
            idx_slice = slice(start, end)
            yield (
                task_ID_i,
                run_parents_srt[idx_slice][fields],
                run_IDs_srt[idx_slice],
            )

    def get(
        self,
        run_IDs: int | Sequence[int] | NDArray,
        paths: str | Sequence[str] | None = None,
        as_dict: bool = False,
        # task_ID: int | None = None,
        # element_idx: int | None = None,
        # iteration_idx: int | None = None,
        # action_idx: int | None = None,
    ) -> dict | list[dict]:
        paths_ = (
            (list(paths) if not isinstance(paths, Sequence) else paths) if paths else None
        )

        run_IDs, is_single = _process_run_IDs(run_IDs)

        out_by_ID = {}
        for task_ID_i, indices, run_IDs_i in self.indices_by_task(run_IDs):
            out_by_ID.update(
                zip(
                    run_IDs_i,
                    self.store.get_base_outputs(
                        task_ID=task_ID_i, indices=indices, paths=paths_
                    ),
                )
            )

        if as_dict:
            return out_by_ID
        else:
            # order as originally specified:
            out = [out_by_ID[run_ID_i] for run_ID_i in run_IDs]
            return out[0] if is_single else out

    def set(
        self,
        values: Mapping[int, dict[str, Any]],
    ):
        """Set base outputs.

        Parameters
        ----------
        values
            Mapping whose keys are integer run ID, and whose values are dictionaries that
            map output names to output values.
        """
        for task_ID_i, indices, run_IDs_i in self.indices_by_task(list(values)):
            # TODO: should batch up in the store method; e.g. in JSON store, all data in a single file
            self.store.set_base_outputs(
                task_ID=task_ID_i,
                indices=indices,
                values=[values[i] for i in run_IDs_i],
            )


class OutputArray:
    # return a view on an array output,
    # e.g. OutputArray('volume_averaged_eq_stress', num_elements=1200, num_iterations=3, num_actions=1)
    # but would point to an array with lots of dimensions (Ne1, Ne2, Ne3, Ni, Nj, Na, Nn)
    # e.g. Nn for output name where multiple outputs share the same shape and type
    #
    pass

    # this should be subclassed (or use composition) e.g. ZarrOutputArray, JSONOutputArray, SQLiteOutputArray
    # Zarr: for __iter__ we should load on demand (i guess chunks; what does Zarr do?)
    # could have a method: `load()` to cache the whole array (or the "outer" slice)
    # store metadata to identify which elements this corresponds to?
    # store ref to WorkflowData, so we can get corresponding InputArrays/dependencies?

    def __init__(
        self,
        workflow_data,
        task_ID: int,
        name: str,
        iteration_idx: int = -1,
        action_idx: int = -1,
    ):

        # TODO: this should be `ParameterArray` because it could apply for inputs as well?
        #   e.g. a task input that is an output of a previous task?

        self.workflow_data = workflow_data

        self.name = name
        self.task_ID = task_ID
        self.iteration_idx = iteration_idx
        self.action_idx = action_idx
        self._loaded_data = None  # set by `(un)load` to (un)load the whole array

        # TODO: record num element, num iterations num actions
        # TODO: record which task this is an input and output to?

        # TODO: add `iterations` method to loop over iterations and then provide elements?
        # e.g. for iter in OutputArray.iterations:
        #         iter[:] -> all elements of that iterations

        arr_idx_dat = self.workflow_data.task_output_array_idx[task_ID][name]
        arr_idx = arr_idx_dat["array_idx"]
        self._arr = self.store.get_output_array(
            arr_idx
        )  # something that is sliceable! (and has a shape method?)

    @property
    def store(self):
        return self.workflow_data.store

    @property
    def is_loaded(self) -> bool:
        return self._loaded_data is not None

    def load(self):
        self._loaded_data = self._arr[:]

    def unload(self):
        self._loaded_data = None

    def __getitem__(self, selection: slice | tuple[slice, ...]):
        """
        selection may include inner dimensions for the array dtype
        """
        if not isinstance(selection, tuple):
            selection = (selection,)

        # TODO: when using multiple elem dims, logic here for converting user selection
        # into multi-dim element selection.

        slices = (selection[0], self.iteration_idx, self.action_idx, *selection[1:])
        return (self._loaded_data if self.is_loaded else self._arr)[slices]


class WorkflowData(AppAware):

    # this will need to make calls to the store.

    # for metadata, we should either read it all on workflow load,
    # or read it all on first access. It's too small to be messing around
    # with caching.

    # for parameter data and arrays, we do want to consider some sort of caching?

    # the point of tracking is primarily to know *which* metadata to update on disk
    # it would be nice to know within that data, what has changed (to refine the updates
    # more; e.g. append to an array rather than re-write it), but this is not
    # critical for now (the metadata is not that big)!

    # we want defaultdict like containers to make the interface easier within the workflow
    # e.g. `self.workflow._data.element_metadata.append(new_elem_md)`, which would convert
    # the new_elem_md list to an array with the correct dtype (that of the existing data)
    # and append it (in this case it would be easier to track changed indices; perhaps as
    # slices)

    # so the tracking does need to be "nested" given how we intend to set data, but
    # we only need to know (for now) that the "top-level" has been modified, when a nested
    # level is modified.

    def __init__(self, workflow_path, workflow, store_fmt, fs_kwargs, app):

        #: Tracking of modified data:
        self._appended_arrays = defaultdict(list)
        self._set_arrays = defaultdict(list)
        self._updated_arrays = defaultdict(list)
        self._modified_dicts = set()
        self._modified_lists = set()

        self.load_store(workflow_path, workflow, store_fmt, fs_kwargs, app)

    def register_submission(self):
        """Tell the store that a submission has occurred, meaning we might need to reload
        parameter data if the store loads it by default (e.g. as for the JSON store)."""

        self.store.has_just_submitted = True

    @property
    def task_ID_to_index(self) -> dict[int, int]:
        return {id_: idx for idx, id_ in enumerate(self.task_ID_list)}

    _RunParents = namedtuple(
        "run_parents",
        (
            "action_idx",
            "iteration_idx",
            "element_idx",
            "task_ID",
            "element_set_ID",
            "element_ID",
            "run_ID",
        ),
    )

    def get_run_membership(
        self,
        run_IDs: int | Sequence[int] | NDArray | None = None,
        as_dict: bool = False,
        element_set: bool = False,
        element_ID: bool = False,
        include_run_ID: bool = False,
    ) -> np.void | NDArray | dict[int, _RunParents]:
        """Get which action, iteration, element, and task one or more runs belong to.

        Parameters
        ----------
        element_set:
            If True, also include the element set ID of each run.
        element_ID:
            If True, also include the element ID of each run.
        """

        is_single = False
        if run_IDs is None:
            num_runs = len(self.run_metadata)
            run_IDs = range(0, num_runs)
        else:
            try:
                num_runs = len(run_IDs)
            except TypeError:
                is_single = True
                run_IDs = [run_IDs]
                num_runs = 1

        run_IDs = np.asarray(run_IDs)

        act_idx = self.run_metadata["action_idx"][run_IDs]
        iter_IDs = self.run_metadata["iteration_ID"][run_IDs]
        iter_idx = self.iter_metadata["index"][iter_IDs]
        elem_IDs = self.iter_metadata["element_ID"][iter_IDs]
        elem_idx = self.element_metadata["index"][elem_IDs]
        task_IDs = self.element_metadata["task_ID"][elem_IDs]
        dtype = [
            ("action_idx", act_idx.dtype),
            ("iteration_idx", iter_idx.dtype),
            ("element_idx", elem_idx.dtype),
            ("task_ID", task_IDs.dtype),
        ]

        elem_set_IDs = None
        if element_set:
            elem_set_IDs = self.element_metadata["element_set_ID"][elem_IDs]
            dtype.append(("element_set_ID", elem_set_IDs.dtype))
        if element_ID:
            dtype.append(("element_ID", elem_IDs.dtype))
        if include_run_ID:
            dtype.append(("run_ID", run_IDs.dtype))

        out = np.empty(num_runs, dtype=dtype)
        out["action_idx"] = act_idx
        out["iteration_idx"] = iter_idx
        out["element_idx"] = elem_idx
        out["task_ID"] = task_IDs

        if element_set:
            out["element_set_ID"] = elem_set_IDs
        if element_ID:
            out["element_ID"] = elem_IDs
        if include_run_ID:
            out["run_ID"] = run_IDs

        if as_dict:
            return {
                run_IDs[idx]: self._RunParents(
                    action_idx=act_idx[idx],
                    iteration_idx=iter_idx[idx],
                    element_idx=elem_idx[idx],
                    task_ID=task_IDs[idx],
                    element_set_ID=elem_set_IDs,
                    element_ID=elem_IDs if element_ID else None,
                    run_ID=run_IDs if include_run_ID else None,
                )
                for idx in range(num_runs)
            }
        else:
            return out if not is_single else out[0]

    def get_iteration_runs(
        self,
        iteration_IDs: int | Sequence[int] | NDArray | None = None,
        action_idx: int | None = None,
        as_dict: bool = True,
    ) -> list[int] | dict[int, list[int]]:
        """Get run IDs belong to the specified iteration IDs, optionally filtering by
        a specific action index.

        """

        is_single = False
        if iteration_IDs is None:
            num_iters = len(self.iter_metadata)
            iteration_IDs = range(0, num_iters)
        else:
            try:
                num_iters = len(iteration_IDs)
            except TypeError:
                is_single = True
                iteration_IDs = [iteration_IDs]
                num_iters = 1

        iteration_IDs = np.asarray(iteration_IDs)
        run_act_idx = self.run_metadata["action_idx"]
        out = {}
        for iter_ID in iteration_IDs:
            if action_idx is not None:
                out[iter_ID] = [
                    rID
                    for rID in self.iter_run_IDs[iter_ID]
                    if run_act_idx[rID] == action_idx
                ]
            else:
                out[iter_ID] = self.iter_run_IDs[iter_ID]

        if as_dict:
            return out
        elif is_single:
            return out[iteration_IDs[0]]
        else:
            # return a single list of run IDs across all specified iteration IDs:
            return list(chain.from_iterable(out.values()))

    @TimeIt.decorator
    def load_store(
        self,
        workflow_path,
        workflow,  # TODO: need to pass to persistent store for now?
        store_fmt: str | None = None,
        fs_kwargs: dict[str, Any] | None = None,
        app=None,
    ):
        """Load data from the persistent store."""

        def _cb_arr_append(attr_name):
            def inner(new_slice):
                self._appended_arrays[attr_name].append(new_slice)

            return inner

        def _cb_arr_set(attr_name):
            def inner(idx):
                self._set_arrays[attr_name].append(idx)

            return inner

        def _cb_arr_update(attr_name):
            def inner(idx):
                self._updated_arrays[attr_name].append(idx)

            return inner

        def _cb_dict(attr_name):
            def inner(op=None):
                self._modified_dicts.add(attr_name)

            return inner

        def _cb_list(attr_name):
            def inner(op):
                self._modified_lists.add(attr_name)

            return inner

        self._cb_arr_append = _cb_arr_append
        self._cb_arr_set = _cb_arr_set
        self._cb_arr_update = _cb_arr_update
        self._cb_dict = _cb_dict
        self._cb_list = _cb_list

        fs_path = str(workflow_path)
        fs, path_s, _ = resolve_fsspec(workflow_path, **(fs_kwargs or {}))
        store_fmt = store_fmt or infer_store(fs_path, fs)
        store_cls = store_cls_from_str(store_fmt)

        self.workflow_path = path_s
        self.store = store_cls(app, workflow, self.workflow_path, fs)
        self.store.load_data()

        # wrap loaded store attributes to provide basic change tracking:

        self.task_templates = TaskTemplatesDict(
            _cb_dict("task_templates"),
            _cb_dict("task_templates"),
            {idx: val for idx, val in enumerate(self.store.task_templates)},
        )
        self.loop_templates = TrackedDict(
            _cb_dict("loop_templates"),
            {idx: val for idx, val in enumerate(self.store.loop_templates)},
        )

        loops_ = {}
        for loop_idx, loop_dat in enumerate(self.store.loops):
            if "num_added_iterations" in loop_dat:
                # transform (JSON-like compatible) list of lists to dict with tuple keys
                # (the inverse op is in `persist_changes`):
                loop_dat = copy.deepcopy(loop_dat)
                loop_dat["num_added_iterations"] = {
                    tuple(num_add_i[0]): num_add_i[1]
                    for num_add_i in loop_dat["num_added_iterations"]
                }
                loops_[loop_idx] = loop_dat

        self.loops = LoopsDict(_cb_dict("loops"), loops_)

        self.task_ID_list = TrackedList(_cb_list("task_ID_list"), self.store.task_ID_list)

        # this doesn't need to be tracked, because it's only modified by the store itself:
        self.task_output_array_idx = self.store.task_output_array_idx

        self.element_set_metadata = TrackedArray(
            append_cb=_cb_arr_append("element_set_metadata"),
            set_cb=_cb_arr_set("element_set_metadata"),
            data=self.store.element_set_metadata,
        )
        self.element_metadata = TrackedArray(
            append_cb=_cb_arr_append("element_metadata"),
            set_cb=_cb_arr_set("element_metadata"),
            data=self.store.element_metadata,
        )
        self.iter_metadata = TrackedArray(
            append_cb=_cb_arr_append("iter_metadata"),
            set_cb=_cb_arr_set("iter_metadata"),
            data=self.store.iter_metadata,
        )
        self.run_metadata = TrackedArray(
            append_cb=_cb_arr_append("run_metadata"),
            set_cb=_cb_arr_set("run_metadata"),
            update_cb=_cb_arr_update("run_metadata"),
            data=self.store.run_metadata,
        )
        self.task_element_IDs = TrackedDefaultDict(
            list, _cb_dict("task_element_IDs"), self.store.task_element_IDs
        )
        self.element_iter_IDs = TrackedDefaultDict(
            list, _cb_dict("element_iter_IDs"), self.store.element_iter_IDs
        )
        self.iter_run_IDs = TrackedDefaultDict(
            list, _cb_dict("iter_run_IDs"), self.store.iter_run_IDs
        )

        _cb_es_is = _cb_dict("element_set_input_source_iter_IDs")
        self.element_set_input_source_iter_IDs = TrackedDefaultDict(
            lambda: TrackedDefaultDict(
                lambda: TrackedDict(callback=_cb_es_is), callback=_cb_es_is
            ),
            _cb_es_is,
            self.store.element_set_input_source_iter_IDs,
        )

        self.iter_loop_idx = TrackedDefaultDict(
            lambda: TrackedDict(callback=_cb_dict("iter_loop_idx")),
            _cb_dict("iter_loop_idx"),
            self.store.iter_loop_idx,
        )

        self.element_src_idx = TrackedDict(
            _cb_dict("element_src_idx"), self.store.element_src_idx
        )
        self.element_seq_idx = TrackedDict(
            _cb_dict("element_seq_idx"), self.store.element_seq_idx
        )

        self.local_inputs = {
            k: TrackedDefaultDict(
                lambda: TrackedDict(callback=_cb_dict("local_inputs")),
                _cb_dict("local_inputs"),
                v,
            )
            for k, v in self.store.local_inputs.items()
        }

        # number of elements and iterations per element for each modified task (not all
        # tasks!)
        # out_shapes = {
        #     t_ID: TrackedOutputArrayShape(
        #         _cb_dict("output_array_shapes"),
        #         [len(t_element_IDs), len(self.element_iter_IDs[t_element_IDs[0]])],
        #     )
        #     for t_ID, t_element_IDs in self.task_element_IDs.items()
        # }
        self._reset_modified_output_array_shapes()
        self._reset_new_base_output_arrays()
        self._reset_new_array_output_arrays()

        self.base_outputs = BaseOutputs(self)

    def _reset_modified_output_array_shapes(self):
        cb = self._cb_dict("modified_output_array_shapes")
        self.modified_output_array_shapes = TrackedDefaultDict(
            lambda: TrackedOutputArrayShape(cb),
            cb,
        )

    def _reset_new_base_output_arrays(self):
        # keys are task IDs for which new output arrays must be initialised, values are
        # the number of actions for that schema:
        self.new_base_output_arrays = TrackedDict(self._cb_dict("new_output_arrays"))

    def _reset_new_array_output_arrays(self):
        # keys are task IDs for which new output arrays must be initialised, values are
        # dicts whose keys are output parameter names, and whose values are dicts with
        # keys `action_indices` (mapping between the original action index and the index
        # along the action dimension in the array) and an array datatype `dtype`:
        self.new_array_output_arrays = TrackedDefaultDict(
            dict, self._cb_dict("new_output_arrays")
        )

    def ensure_output_arrays(
        self,
        task_ID: int,
        is_new: bool,
        num_new_elements: int,
        num_actions: int,
        output_act_indices: Mapping[int, Mapping[str, dict[str, Any]]],
        arr_specs: Mapping[str, tuple[str, list[int] | None]],
    ):
        self.modified_output_array_shapes[task_ID][0] += num_new_elements
        if is_new:
            self.modified_output_array_shapes[task_ID][1] += 1  # initial iteration
            self.new_base_output_arrays[task_ID] = num_actions
            for out_type, act_indices in output_act_indices.items():
                self.new_array_output_arrays[task_ID][out_type] = {
                    "action_indices": act_indices,
                    "dtype": arr_specs[out_type][0],
                    "shape": arr_specs[out_type][1],
                }

    def batched_update(self):
        # maybe define a transaction api here?
        # this would map to a DB transaction for those type, for other types, would
        # just push to disk at the end
        pass

    @property
    def pending(self) -> set[str]:
        return {
            *self._modified_dicts,
            *self._modified_lists,
            *self._appended_arrays,
            *self._set_arrays,
            *self._updated_arrays,
        }

    @property
    def has_pending(self) -> bool:
        return bool(self.pending)

    def persist_changes(self):
        """Save changes to the persistent store."""

        if not self.has_pending:
            return

        with self.store.persist_ctx() as ctx:

            if (key := "task_templates") in self._modified_dicts:
                task_tmps = self.task_templates
                task_tmps_lst = [task_tmps[idx] for idx in range(len(task_tmps))]
                self.store.update_task_templates(task_tmps_lst, ctx)
                self._modified_dicts.remove(key)

            if (key := "loop_templates") in self._modified_dicts:
                loop_tmps = self.loop_templates
                loop_tmps_lst = [loop_tmps[idx] for idx in range(len(loop_tmps))]
                self.store.update_loop_templates(loop_tmps_lst, ctx)
                self._modified_dicts.remove(key)

            if (key := "loops") in self._modified_dicts:
                loops = self.loops
                loops_ = []
                for idx in range(len(loops)):
                    loop_i = copy.deepcopy(loops[idx])
                    if "num_added_iterations" in loop_i:
                        # remove tuple dict keys; use list of lists instead of dict to
                        # be JSON-like compatible (the inverse op is in `load_store`):
                        loop_i["num_added_iterations"] = [
                            [list(k), v]
                            for k, v in loop_i["num_added_iterations"].items()
                        ]
                    loops_.append(loop_i)
                self.store.update_loops(loops_, ctx)
                self._modified_dicts.remove(key)

            if self._appended_arrays.pop("element_set_metadata", None):
                # must be after `task_templates`?
                self.store.update_element_set_metadata(self.element_set_metadata, ctx)

            if self._appended_arrays.pop("element_metadata", None):
                self.store.update_element_metadata(self.element_metadata, ctx)

            if self._appended_arrays.pop("iter_metadata", None):
                self.store.update_iter_metadata(self.iter_metadata, ctx)

            if self._appended_arrays.pop("run_metadata", None):
                self.store.update_run_metadata(self.run_metadata, ctx)

            if self._updated_arrays.pop("run_metadata", None):
                self.store.update_run_metadata(self.run_metadata, ctx)

            if (key := "local_inputs") in self._modified_dicts:
                self.store.update_local_inputs(self.local_inputs, ctx)
                self._modified_dicts.remove(key)

            if (key := "task_ID_list") in self._modified_lists:
                self.store.update_task_ID_list(self.task_ID_list, ctx)
                self._modified_lists.remove(key)

            if (key := "task_element_IDs") in self._modified_dicts:
                ctx["task_ID_to_index"] = self.task_ID_to_index
                self.store.update_task_element_IDs(self.task_element_IDs, ctx)
                self._modified_dicts.remove(key)

            if (key := "element_iter_IDs") in self._modified_dicts:
                self.store.update_element_iter_IDs(self.element_iter_IDs, ctx)
                self._modified_dicts.remove(key)

            if (key := "iter_run_IDs") in self._modified_dicts:
                self.store.update_iter_run_IDs(self.iter_run_IDs, ctx)
                self._modified_dicts.remove(key)

            if (key := "element_set_input_source_iter_IDs") in self._modified_dicts:
                self.store.update_element_set_input_source_iter_IDs(
                    self.element_set_input_source_iter_IDs, ctx
                )
                self._modified_dicts.remove(key)

            if (key := "element_src_idx") in self._modified_dicts:
                self.store.update_element_src_idx(self.element_src_idx, ctx)
                self._modified_dicts.remove(key)

            if (key := "element_seq_idx") in self._modified_dicts:
                self.store.update_element_seq_idx(self.element_seq_idx, ctx)
                self._modified_dicts.remove(key)

            if (key := "iter_loop_idx") in self._modified_dicts:
                self.store.update_iter_loop_idx(self.iter_loop_idx, ctx)
                self._modified_dicts.remove(key)

            if (key := "new_output_arrays") in self._modified_dicts:

                # note: this covers both base-output and array-output arrays
                assert set(self.new_array_output_arrays).issubset(
                    self.new_base_output_arrays
                )
                # note: this method mutates `modified_output_array_shapes`, by removing
                # the entries for the arrays that are being initialised (since their
                # shapes will be correct on initialisation, and won't need updating
                # below):
                new_task_out_arr_idx = self.store.init_new_output_arrays(
                    self.new_base_output_arrays,
                    self.new_array_output_arrays,
                    self.modified_output_array_shapes,
                    ctx,
                )

                self.store.update_task_output_array_idx(new_task_out_arr_idx, ctx)
                self._reset_new_base_output_arrays()
                self._reset_new_array_output_arrays()
                self._modified_dicts.remove(key)

            if (key := "modified_output_array_shapes") in self._modified_dicts:
                # we need to reshape base-output and array-output arrays to accommodate
                # more elements and/or iterations:
                if self.modified_output_array_shapes:

                    self.store.update_output_array_shapes(
                        self.modified_output_array_shapes,
                        self.task_array_indices,
                        ctx,
                    )
                    self._reset_modified_output_array_shapes()
                self._modified_dicts.remove(key)

        assert (
            not self.has_pending
        ), f"Workflow data still has pending changes: {self.pending}!"

    @property
    def task_array_indices(self) -> dict[int, tuple[int, ...]]:
        """Get the array indices corresponding to all array-outputs for each task."""
        return {
            t_ID: tuple(v["array_idx"] for v in out_arr_idx.values())
            for t_ID, out_arr_idx in self.task_output_array_idx.items()
        }

    @staticmethod
    def __generate_act_idx_map_array(act_indices: Mapping[int, int]) -> NDArray:
        idx_arr = np.full(max(act_indices) + 1, np.iinfo(np.uint8).max)
        for k, v in act_indices.items():
            idx_arr[k] = v
        return idx_arr

    def get_task_run_IDs(self, task_ID: int) -> NDArray:
        """Retrieve the run IDs of"""

    def get_task_output_array(
        self, task_ID: int, name: str, iteration_idx: int = -1, action_idx: int = -1
    ) -> OutputArray:
        return OutputArray(self, task_ID, name, iteration_idx, action_idx)

    def get_task_outputs(self, task_ID: int, name: str) -> Any | OutputArray:
        """Retrieve the values of the specified output across the elements of a task.

        For array outputs, this will return an `OutputArray` instance.
        """
        # TODO: could optimise if we know all run IDs are from specific task?
        #   e.g. for access like: `Workflow.tasks[0].get_output("VE_response.stress")`
        #                     or: `Workflow.tasks[0].output.VE_response`
        #                     or: `Workflow.tasks[0].output.yield_stress`

        # 1. identify which action idx outputs this name
        # 2. assume latest iteration (could in principle get latest-non-skipped idx)

        # run_parents = self.get_run_membership(run_IDs)

        # TODO: this is only if it's an array output

        # TODO: support tuple of slices in get_array_items? or return some sort of pointer?

        # run_IDs, is_single = _process_run_IDs(run_IDs)
        # run_parents = self.get_run_membership(run_IDs)
        # arr_out = []
        # for idx, run_ID in enumerate(run_IDs):
        #     run_ID_idx[run_ID] = idx
        #     task_ID = run_parents[idx]["task_ID"]
        #     if name in self.task_output_array_idx[task_ID]:
        #         arr_outs[task_ID][out_name].append(run_ID)
        #     else:
        #         base_names.append(run_ID)

    def get_inputs(
        self,
        run_IDs: int | Sequence[int] | NDarray,
        names: Sequence[str],
        as_dict: bool = False,
    ) -> dict[str, Any] | list[dict[str, Any]] | dict[int, dict[str, Any]]:

        run_IDs, is_single = _process_run_IDs(run_IDs)
        run_parents = self.get_run_membership(run_IDs)

        for idx, run_ID in enumerate(run_IDs):
            task_ID = run_parents[idx]["task_ID"]
            act_idx = run_parents[idx]["action_idx"]

            # can we consider an action input part of the schema inputs?

    def get_outputs(
        self,
        run_IDs: int | Sequence[int] | NDArray,
        names: Sequence[str],
        as_dict: bool = False,
    ) -> dict[str, Any] | list[dict[str, Any]] | dict[int, dict[str, Any]]:
        """Retrieve multiple outputs from multiple runs.

        Note this is inefficient for retrieving/slicing values of an array output of a
        task. Use `get_task_outputs` instead.

        """
        # TODO: could optimise if we know all run IDs are from specific task?
        #   e.g. for access like: `Workflow.tasks[0].get_outputs()`

        run_IDs, is_single = _process_run_IDs(run_IDs)
        run_parents = self.get_run_membership(run_IDs)

        # allow mapping from run ID to a row in `run_parents`:
        run_ID_idx = np.full(max(run_IDs) + 1, np.iinfo(np.uint32).max)

        # split into those outputs that have a known array type, and the more general
        # outputs:
        arr_outs = defaultdict(lambda: defaultdict(set))
        base_names = set()

        for idx, run_ID in enumerate(run_IDs):
            run_ID_idx[run_ID] = idx
            task_ID = run_parents[idx]["task_ID"]
            for out_name in names:
                if out_name in self.task_output_array_idx[task_ID]:
                    arr_outs[task_ID][out_name].add(run_ID)
                else:
                    base_names.add(run_ID)

        out_by_ID = self.base_outputs.get(base_names, paths=names, as_dict=True)

        for task_ID, run_IDs_by_name in arr_outs.items():
            for out_name, run_IDs_set in run_IDs_by_name.items():
                arr_idx_dat = self.task_output_array_idx[task_ID][out_name]
                arr_idx = arr_idx_dat["array_idx"]
                run_IDs_lst = list(run_IDs_set)
                indices = run_parents[run_ID_idx[run_IDs_lst]][
                    ["element_idx", "iteration_idx", "action_idx"]
                ]
                arr = self.store.get_array_items(arr_idx, indices)
                for out_idx, arr_item in enumerate(arr):
                    # values[out_idx][out_name] = arr_item.item()
                    out_by_ID[run_IDs_lst[out_idx]][out_name] = arr_item.item()

        if as_dict:
            return out_by_ID
        else:
            # order as originally specified:
            out = [out_by_ID[run_ID_i] for run_ID_i in run_IDs]
            return out[0] if is_single else out

        # get_outputs((0, 1), names=("p1", "yield_stress"))
        #   -> [{"p1": 100, "yield_stress": y1}, {"p1": 101, "yield_stress": y2}, ]

    def save_outputs(self, values: Mapping[int, dict[str, Any]]):
        """Save multiple outputs from multiple runs.

        Parameters
        ----------
        values
            Mapping whose keys are integer run IDs, and whose values are the values to set
            multiple named outputs to.

        """

        run_IDs = list(values)
        run_parents = self.get_run_membership(run_IDs)

        # allow mapping from run ID to a row in `run_parents`:
        run_ID_idx = np.full(max(run_IDs) + 1, np.iinfo(np.uint32).max)

        # split into those outputs that have a known array type, and the more general
        # outputs:
        arr_outs = defaultdict(lambda: defaultdict(dict))
        remove = []
        for idx, (run_ID, vals_i) in enumerate(values.items()):
            run_ID_idx[run_ID] = idx
            task_ID = run_parents[idx]["task_ID"]
            for out_name in tuple(vals_i):
                if out_name in self.task_output_array_idx[task_ID]:
                    arr_outs[task_ID][out_name][run_ID] = vals_i.pop(out_name)
            if not vals_i:
                # all values popped
                remove.append(run_ID)

        if base_outs := {k: v for k, v in values.items() if k not in remove}:
            self.base_outputs.set(base_outs)

        # for setting array outputs, need to batch up over (task, output name), which will
        # have distinct arrays:
        for task_ID, task_arr_outs in arr_outs.items():
            for out_name_i, arr_i_vals in task_arr_outs.items():

                run_IDs_j, vals_j = list(zip(*arr_i_vals.items()))
                arr_idx_dat = self.task_output_array_idx[task_ID][out_name_i]
                arr_idx = arr_idx_dat["array_idx"]

                indices_j = run_parents[run_ID_idx[list(run_IDs_j)]][
                    ["element_idx", "iteration_idx", "action_idx"]
                ]

                # modify the action_idx field to index correctly:
                act_idx_arr = self.__generate_act_idx_map_array(
                    arr_idx_dat["action_indices"]
                )
                indices_j["action_idx"] = act_idx_arr[indices_j["action_idx"]]

                self.store.set_array_items(arr_idx, indices_j, vals_j)
