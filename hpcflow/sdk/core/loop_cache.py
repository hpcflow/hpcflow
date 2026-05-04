"""
Cache of loop statuses.
"""

from __future__ import annotations
from dataclasses import dataclass
from collections import defaultdict
from typing import TYPE_CHECKING
from typing_extensions import Generic, TypeVar

from hpcflow.sdk.core.utils import nth_key
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.core.cache import ObjectCache

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from typing_extensions import Self
    from ..typing import DataIndex
    from .loop import Loop
    from .task import WorkflowTask
    from .types import DependentDescriptor, ElementDescriptor
    from .workflow import Workflow

K = TypeVar("K")
V = TypeVar("V")


class _LoopIndexError(TypeError):
    """
    A type error special to loop indices.
    """

    def __init__(self, loop_index: LoopIndex) -> None:
        super().__init__(
            f"{loop_index.__class__.__name__} does not support item assignment"
        )


class LoopIndex(dict[K, V], Generic[K, V]):
    """
    Hashable dict implementation, suitable for use as a key into
    other dicts. Once used as a key, becomes immutable.

    Example
    -------

        >>> h1 = LoopIndex({"apples": 1, "bananas":2})
        >>> h2 = LoopIndex({"bananas": 3, "mangoes": 5})
        >>> h1+h2
        LoopIndex(apples=1, bananas=3, mangoes=5)
        >>> d1 = {}
        >>> d1[h1] = "salad"
        >>> d1[h1]
        'salad'
        >>> d1[h2]
        Traceback (most recent call last):
        ...
        KeyError: LoopIndex(bananas=3, mangoes=5)

    Notes
    -----
    * Based on answers from
      http://stackoverflow.com/questions/1151658/python-hashable-dicts
    * Assumes both keys and values are hashable. True in practice.
    """

    def __init__(self, map: Mapping[K, V] | None = None) -> None:
        """
        Make an instance from another dictionary.
        This object will be mutable until it is used as a key.
        """
        super().__init__(map or {})
        self.__hash: int | None = None

    def __repr__(self):
        return f"""{self.__class__.__name__}({
            ', '.join(f'{k!r}={v!r}' for k, v in self.items())
        })"""

    def __hash__(self):
        if self.__hash is None:
            self.__hash = hash(frozenset(self.items()))
        return self.__hash

    def _validate_update(self) -> None:
        if self.__hash is not None:
            raise _LoopIndexError(self)

    def __setitem__(self, key: K, value: V) -> None:
        self._validate_update()
        super().__setitem__(key, value)

    def __delitem__(self, key: K) -> None:
        self._validate_update()
        super().__delitem__(key)

    def clear(self) -> None:
        self._validate_update()
        super().clear()

    def pop(self, *args, **kwargs) -> V:
        self._validate_update()
        return super().pop(*args, **kwargs)

    def popitem(self) -> tuple[K, V]:
        self._validate_update()
        return super().popitem()

    def setdefault(self, key: K, default: V) -> V:
        self._validate_update()
        return super().setdefault(key, default)

    def update(self, *args, **kwargs) -> None:
        self._validate_update()
        super().update(*args, **kwargs)

    def __add__(self, right: Mapping[K, V]) -> Self:
        result = self.__class__(self)
        result.update(right)
        return result


@dataclass
class LoopCache:
    """Class to store a cache for use in :py:meth:`.Workflow.add_empty_loop` and
    :py:meth:`.WorkflowLoop.add_iterations`. Use :py:meth:`build` to get a new instance.

    Parameters
    ----------
    element_dependents:
        Keys are element IDs, values are dicts whose keys are element IDs that depend on
        the key element ID (via `Element.get_dependent_elements_recursively`), and whose
        values are dicts with keys: `group_names`, which is a tuple of the string group
        names associated with the dependent element's element set.
    elements:
        Keys are element IDs, values are dicts with keys: `input_statuses`,
        `input_sources`, and `task_insert_ID`.
    zeroth_iters:
        Keys are element IDs, values are data associated with the zeroth iteration of that
        element, namely a tuple of iteration ID and `ElementIteration.data_idx`.
    data_idx:
        Keys are element IDs, values are data associated with all iterations of that
        element, namely a dict whose keys are the iteration loop index as a tuple, and
        whose values are data indices via `ElementIteration.get_data_idx()`.
    iterations:
        Keys are iteration IDs, values are tuples of element ID and iteration index within
        that element.
    task_iterations:
        Keys are task insert IDs, values are list of all iteration IDs associated with
        that task.
    """

    #: Keys are element IDs, values are dicts whose keys are element IDs that depend on
    #: the key element ID (via `Element.get_dependent_elements_recursively`), and whose
    #: values are dicts with keys: `group_names`, which is a tuple of the string group
    #: names associated with the dependent element's element set.
    element_dependents: dict[int, dict[int, DependentDescriptor]]
    #: Keys are element IDs, values are dicts with keys: `input_statuses`,
    #: `input_sources`, and `task_insert_ID`.
    elements: dict[int, ElementDescriptor]
    #: Keys are element IDs, values are data associated with the zeroth iteration of that
    #: element, namely a tuple of iteration ID and `ElementIteration.data_idx`.
    zeroth_iters: dict[int, tuple[int, DataIndex]]
    #: Keys are element IDs, values are data associated with all iterations of that
    #: element, namely a dict whose keys are the iteration loop index as a tuple, and
    #: whose values are data indices via `ElementIteration.get_data_idx()`.
    data_idx: dict[int, dict[LoopIndex[str, int], DataIndex]]
    #: Keys are iteration IDs, values are tuples of element ID and iteration index within
    #: that element.
    iterations: dict[int, tuple[int, int]]
    #: Keys are task insert IDs, values are list of all iteration IDs associated with
    #: that task.
    task_iterations: dict[int, list[int]]

    @TimeIt.decorator
    def get_iter_IDs(self, loop: Loop) -> list[int]:
        """Retrieve a list of iteration IDs belonging to a given loop."""
        return [
            i_id for t_id in loop.task_insert_IDs for i_id in self.task_iterations[t_id]
        ]

    @TimeIt.decorator
    def get_iter_loop_indices(self, iter_IDs: list[int]) -> Sequence[Mapping[str, int]]:
        """
        Retrieve the mapping from element to loop index for each given iteration.
        """
        iter_loop_idx: list[LoopIndex[str, int]] = []
        for id_ in iter_IDs:
            elem_id, idx = self.iterations[id_]
            iter_loop_idx.append(nth_key(self.data_idx[elem_id], idx))
        return iter_loop_idx

    @TimeIt.decorator
    def update_loop_indices(self, new_loop_name: str, iter_IDs: list[int]) -> None:
        """
        Set the loop indices for a named loop to the given list of iteration IDs.
        """
        elem_ids = {e_ids[0] for k, e_ids in self.iterations.items() if k in iter_IDs}
        new_loop_entry = {new_loop_name: 0}
        for id_ in elem_ids:
            self.data_idx[id_] = {
                k + new_loop_entry: v for k, v in self.data_idx[id_].items()
            }

    @TimeIt.decorator
    def add_iteration(
        self,
        iter_ID: int,
        task_insert_ID: int,
        element_ID: int,
        loop_idx: LoopIndex[str, int],
        data_idx: DataIndex,
    ):
        """Update the cache to include a newly added iteration."""
        self.task_iterations[task_insert_ID].append(iter_ID)
        new_iter_idx = len(self.data_idx[element_ID])
        self.data_idx[element_ID][loop_idx] = data_idx
        self.iterations[iter_ID] = (element_ID, new_iter_idx)

    @classmethod
    @TimeIt.decorator
    def build(cls, workflow: Workflow, loops: list[Loop] | None = None) -> Self:
        """Build a cache of data for use in adding loops and iterations."""

        deps_cache = ObjectCache.build(workflow, dependencies=True, elements=True)

        loops = [*workflow.template.loops, *(loops or ())]
        task_iIDs = {t_id for loop in loops for t_id in loop.task_insert_IDs}
        tasks: list[WorkflowTask] = [
            workflow.tasks.get(insert_ID=t_id) for t_id in sorted(task_iIDs)
        ]
        elem_deps: dict[int, dict[int, DependentDescriptor]] = {}

        # keys: element IDs, values: dict with keys: tuple(loop_idx), values: data index
        data_idx_cache: dict[int, dict[LoopIndex[str, int], DataIndex]] = {}

        # keys: iteration IDs, values: tuple of (element ID, integer index into values
        # dict in `data_idx_cache` [accessed via `.keys()[index]`])
        iters: dict[int, tuple[int, int]] = {}

        # keys: element IDs, values: dict with keys: "input_statues", "input_sources",
        # "task_insert_ID":
        elements: dict[int, ElementDescriptor] = {}

        zeroth_iters: dict[int, tuple[int, DataIndex]] = {}
        task_iterations = defaultdict(list)
        for task in tasks:
            for elem_id in task.element_IDs:
                element = deps_cache.elements[elem_id]
                inp_statuses = task.template.get_input_statuses(element.element_set)
                elements[element.id_] = {
                    "input_statuses": inp_statuses,
                    "input_sources": element.input_sources,
                    "task_insert_ID": task.insert_ID,
                }
                elem_deps[element.id_] = {
                    de_id: {
                        "group_names": tuple(
                            grp.name
                            for grp in deps_cache.elements[de_id].element_set.groups
                        ),
                    }
                    for de_id in deps_cache.elem_elem_dependents_rec[element.id_]
                }
                elem_iters: dict[LoopIndex[str, int], DataIndex] = {}
                for idx, iter_i in enumerate(element.iterations):
                    if idx == 0:
                        zeroth_iters[element.id_] = (iter_i.id_, iter_i.data_idx)
                    elem_iters[iter_i.loop_idx] = iter_i.get_data_idx()
                    task_iterations[task.insert_ID].append(iter_i.id_)
                    iters[iter_i.id_] = (element.id_, idx)
                data_idx_cache[element.id_] = elem_iters

        task_iterations.default_factory = None
        return cls(
            element_dependents=elem_deps,
            elements=elements,
            zeroth_iters=zeroth_iters,
            data_idx=data_idx_cache,
            iterations=iters,
            task_iterations=task_iterations,
        )
