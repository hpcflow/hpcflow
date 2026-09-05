from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from numbers import Integral
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from hpcflow.sdk.submission.jobscript import Jobscript

# TODO: might make sense to have a maximum size (number of indices) within the run file


def get_run_multi_chunk_path(
    idx: int | Sequence[int], prefix: Path | None, max_per_dir: int = 1000
) -> Path | list[Path]:

    prefix = prefix or Path()

    def get_one(idx: int, prefix: Path | None) -> Path:
        directory, filename = divmod(idx, max_per_dir)
        return prefix / str(directory) / str(filename)

    if isinstance(idx, Integral):
        return get_one(idx, prefix)

    return [get_one(idx_i, prefix) for idx_i in idx]


@dataclass(frozen=True)
class JobscriptUnit:
    jobscript_idx: int
    array_idx: int


class RunMetaDataFileResolver:
    """Class to resolve how to assign a submission's runs to files that can be safely
    written to, given the concurrency of the submission's jobscripts."""

    def __init__(self, jobscripts: list[Jobscript]):

        self.jobscripts = jobscripts
        self.units_by_js = {
            int(js.index): self.get_jobscript_units(js) for js in self.jobscripts
        }

        # assigned on `resolve`:
        self.run_file_lookup = None
        self.chains = None

    @property
    def num_files(self) -> int:
        assert self.chains
        return len(self.chains)

    @staticmethod
    def get_jobscript_units(js: Jobscript):
        js_idx = int(js.index)

        if js.is_array:
            return [JobscriptUnit(js_idx, i) for i in range(js.blocks[0].num_elements)]

        return [JobscriptUnit(js_idx, 0)]

    def build_execution_unit_graph(self):
        """Build the element-expanded jobscript graph, where nodes are represented by a
        JobscriptUnit, and edges by the sets of units: `successors` and `predecessors`."""

        successors = {
            unit: set() for units in self.units_by_js.values() for unit in units
        }

        predecessors = {unit: set() for unit in successors}

        for js in self.jobscripts:
            dst_units = self.units_by_js[int(js.index)]

            for (dep_js_idx, _dep_block_idx), dep_info in js.dependencies.items():
                src_units = self.units_by_js[int(dep_js_idx)]

                if dep_info["is_array"]:
                    # N -> N, e.g. SLURM aftercorr
                    assert len(src_units) == len(dst_units)

                    edges = zip(src_units, dst_units)

                else:
                    # Barrier dependency:
                    #
                    # N -> 1
                    # 1 -> M
                    # N -> M
                    edges = ((src, dst) for src in src_units for dst in dst_units)

                for src, dst in edges:
                    successors[src].add(dst)
                    predecessors[dst].add(src)

        return successors, predecessors

    @staticmethod
    def topological_sort(successors, predecessors):
        """Sort nodes such that all dependencies precede their dependents."""

        # number of predecessors each node has (incoming edges):
        indegree = {node: len(predecessors[node]) for node in successors}

        # start with nodes that have no dependencies:
        ready = deque(node for node, degree in indegree.items() if degree == 0)

        order = []
        while ready:
            # remove the node's outgoing edges by decrementing the degree of each of its
            # successors:
            node = ready.popleft()
            order.append(node)

            for successor in successors[node]:
                indegree[successor] -= 1

                if indegree[successor] == 0:
                    # the successor now has no dependencies:
                    ready.append(successor)

        if len(order) != len(successors):
            raise ValueError("Jobscript dependency graph contains a cycle.")

        return order

    @staticmethod
    def get_reachability(successors, topo_order):
        """
        For each execution unit, which other execution units must occur
        downstream of it?
        """

        reachable = {node: set() for node in successors}
        for node in reversed(topo_order):
            for successor in successors[node]:
                reachable[node].add(successor)
                reachable[node].update(reachable[successor])

        return reachable

    @staticmethod
    def maximum_matching(reachable):
        """
        What's the maximum number of execution units we can link together into
        reusable-file chains?
        """
        left_nodes = list(reachable)

        # what comes after a node in its chain?
        match_left = {}

        # what comes before a node in its chain?
        match_right = {}

        while True:
            distance = {}
            queue = deque()

            for node in left_nodes:
                if node not in match_left:
                    distance[node] = 0
                    queue.append(node)

            found = False

            while queue:
                left = queue.popleft()

                for right in reachable[left]:
                    next_left = match_right.get(right)

                    if next_left is None:
                        found = True
                    elif next_left not in distance:
                        distance[next_left] = distance[left] + 1
                        queue.append(next_left)

            if not found:
                break

            def augment(left):
                for right in reachable[left]:
                    next_left = match_right.get(right)

                    if next_left is None or (
                        distance.get(next_left) == distance[left] + 1
                        and augment(next_left)
                    ):
                        match_left[left] = right
                        match_right[right] = left
                        return True

                distance[left] = -1
                return False

            for node in left_nodes:
                if node not in match_left:
                    augment(node)

        return match_left, match_right

    def get_metadata_file_mapping(self):

        successors, predecessors = self.build_execution_unit_graph()
        topo_order = self.topological_sort(successors, predecessors)
        reachable = self.get_reachability(successors, topo_order)
        match_left, match_right = self.maximum_matching(reachable)

        # any node without a matched predecessor starts a chain:
        chain_starts = [node for node in topo_order if node not in match_right]

        unit_to_file_ID = {}
        chains = []

        for file_ID, start in enumerate(chain_starts):
            chain = []
            node = start

            while True:
                chain.append(node)
                unit_to_file_ID[node] = file_ID

                successor = match_left.get(node)
                if successor is None:
                    break

                node = successor

            chains.append(chain)

        assert len(unit_to_file_ID) == len(successors)

        return unit_to_file_ID, chains

    def get_unit_run_IDs(self, unit: JobscriptUnit):
        """
        Retrieve the run IDs associated with a JobscriptUnit.
        """
        if (js := self.jobscripts[unit.jobscript_idx]).is_array:
            return js.blocks[0].EAR_ID[:, unit.array_idx]
        else:
            return js.all_EAR_IDs

    def resolve(self):
        unit_to_file_ID, chains = self.get_metadata_file_mapping()

        run_file_lookup = {}
        for file_ID, chain in enumerate(chains):
            file_idx = 0
            for unit in chain:
                for run_ID in self.get_unit_run_IDs(unit):
                    run_file_lookup[int(run_ID)] = (file_ID, file_idx)
                    file_idx += 1

        self.run_file_lookup = run_file_lookup
        self.chains = chains
