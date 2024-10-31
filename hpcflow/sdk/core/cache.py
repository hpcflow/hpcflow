"""
Dependency resolution cache.
"""

from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass
from itertools import chain
from typing import TYPE_CHECKING

from hpcflow.sdk.log import TimeIt

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing_extensions import Self
    from .element import Element, ElementIteration
    from .workflow import Workflow
    from ..persistence.base import StoreEAR, StoreElement, StoreElementIter


@dataclass
class DependencyCache:
    """
    Class to bulk-retrieve dependencies between elements, iterations, and runs.
    """

    #: What EARs (by ID) a given EAR depends on.
    run_dependencies: dict[int, set[int]]
    #: What EARs (by ID) are depending on a given EAR.
    run_dependents: dict[int, set[int]]
    #: What EARs (by ID) a given iteration depends on.
    iter_run_dependencies: dict[int, set[int]]
    #: What iterations (by ID) a given iteration depends on.
    iter_iter_dependencies: dict[int, set[int]]
    #: What iterations (by ID) a given element depends on.
    elem_iter_dependencies: dict[int, set[int]]
    #: What elements (by ID) a given element depends on.
    elem_elem_dependencies: dict[int, set[int]]
    #: What elements (by ID) are depending on a given element.
    elem_elem_dependents: dict[int, set[int]]
    #: Transitive closure of :py:attr:`elem_elem_dependents`.
    elem_elem_dependents_rec: dict[int, set[int]]

    #: The elements of the workflow that this cache was built from.
    elements: list[Element]
    #: The iterations of the workflow that this cache was built from.
    iterations: list[ElementIteration]

    @classmethod
    @TimeIt.decorator
    def build(cls, workflow: Workflow) -> Self:
        """
        Build a cache instance.

        Parameters
        ----------
        workflow: ~hpcflow.app.Workflow
            The workflow to build the cache from.
        """
        num_iters = workflow.num_element_iterations
        num_elems = workflow.num_elements
        num_runs = workflow.num_EARs

        all_store_runs: Sequence[StoreEAR] = workflow._store.get_EARs(range(num_runs))
        all_store_iters: Sequence[
            StoreElementIter
        ] = workflow._store.get_element_iterations(range(num_iters))
        all_store_elements: Sequence[StoreElement] = workflow._store.get_elements(
            range(num_elems)
        )
        all_param_sources = workflow.get_all_parameter_sources()
        all_data_idx = (
            {
                k: v if isinstance(v, list) else [v]
                for k, v in store_ear.data_idx.items()
                if not k.startswith("repeats.")
            }
            for store_ear in all_store_runs
        )

        # run dependencies and dependents
        run_dependencies: dict[int, set[int]] = {}
        run_dependents: defaultdict[int, set[int]] = defaultdict(set)
        for idx, dict_i in enumerate(all_data_idx):
            run_i_sources = set(
                run_k
                for idx in chain.from_iterable(dict_i.values())
                if (run_k := all_param_sources[idx].get("EAR_ID")) is not None
                and run_k != idx
            )
            run_dependencies[idx] = run_i_sources
            for m in run_i_sources:
                run_dependents[m].add(idx)

        # add missing and downgrade to dict:
        for run_idx in range(num_runs):
            run_dependents[run_idx]
        run_dependents.default_factory = None

        # iteration dependencies
        all_iter_run_IDs = {
            iter_.id_: tuple(chain.from_iterable((iter_.EAR_IDs or {}).values()))
            for iter_ in all_store_iters
        }
        # for each iteration, which runs does it depend on?
        iter_run_dependencies = {
            k: set(j for idx in v for j in run_dependencies[idx])
            for k, v in all_iter_run_IDs.items()
        }

        # for each run, which iteration does it belong to?
        all_run_iter_IDs = {
            run_ID: iter_ID
            for iter_ID, run_IDs in all_iter_run_IDs.items()
            for run_ID in run_IDs
        }

        # for each iteration, which iterations does it depend on?
        iter_iter_dependencies = {
            k: set(all_run_iter_IDs[i] for i in v)
            for k, v in iter_run_dependencies.items()
        }

        all_elem_iter_IDs = {el.id_: el.iteration_IDs for el in all_store_elements}

        elem_iter_dependencies = {
            elem_ID: set(j for i in iter_IDs for j in iter_iter_dependencies[i])
            for elem_ID, iter_IDs in all_elem_iter_IDs.items()
        }

        # for each iteration, which element does it belong to?
        all_iter_elem_IDs = {
            iter_ID: elem_ID
            for elem_ID, iter_IDs in all_elem_iter_IDs.items()
            for iter_ID in iter_IDs
        }

        # element dependencies
        elem_elem_dependencies = {
            k: set(all_iter_elem_IDs[i] for i in dep_set)
            for k, dep_set in elem_iter_dependencies.items()
        }

        # for each element, which elements depend on it (directly)?
        elem_elem_dependents: defaultdict[int, set[int]] = defaultdict(set)
        for k, dep_set in elem_elem_dependencies.items():
            for i in dep_set:
                elem_elem_dependents[i].add(k)

        # for each element, which elements depend on it (recursively)?
        elem_elem_dependents_rec: defaultdict[int, set[int]] = defaultdict(set)
        for k in tuple(elem_elem_dependents):
            # NB: code below modifies elem_elem_dependents during this loop;
            # copy above is mandatory!
            for i in elem_elem_dependents[k]:
                elem_elem_dependents_rec[k].add(i)
                elem_elem_dependents_rec[k].update(
                    m for m in elem_elem_dependents[i] if m != k
                )

        # add missing keys and downgrade to dict:
        for elem_idx in range(num_elems):
            elem_elem_dependents[elem_idx]
            elem_elem_dependents_rec[elem_idx]
        elem_elem_dependents.default_factory = None
        elem_elem_dependents_rec.default_factory = None

        elements = workflow.get_all_elements()
        iterations = workflow.get_all_element_iterations()

        return cls(
            run_dependencies=run_dependencies,
            run_dependents=run_dependents,
            iter_run_dependencies=iter_run_dependencies,
            iter_iter_dependencies=iter_iter_dependencies,
            elem_iter_dependencies=elem_iter_dependencies,
            elem_elem_dependencies=elem_elem_dependencies,
            elem_elem_dependents=elem_elem_dependents,
            elem_elem_dependents_rec=elem_elem_dependents_rec,
            elements=elements,
            iterations=iterations,
        )
