from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from collections.abc import Sequence

from hpcflow.sdk.wait.utils import remove_superseded_directory


@dataclass(frozen=True)
class JobscriptCompletion:
    submissions_path: Path
    submission_idx: int
    jobscript_idx: int
    num_jobscripts: int
    array_indices: tuple[int, ...] | None = None

    def __post_init__(self) -> None:
        if self.num_jobscripts < 1:
            raise ValueError("num_jobscripts must be at least 1.")

        if not 0 <= self.jobscript_idx < self.num_jobscripts:
            raise ValueError(
                f"jobscript_idx {self.jobscript_idx} is outside the range "
                f"[0, {self.num_jobscripts})."
            )

    @property
    def submission_state_path(self) -> Path:
        return self.submissions_path / str(self.submission_idx) / "js_state"

    @property
    def state_path(self) -> Path:
        return self._get_state_path(self.jobscript_idx)

    @property
    def array_path(self) -> Path:
        return self.state_path / "array"

    @property
    def completion_path(self) -> Path:
        """Marker indicating the whole jobscript is complete."""
        return self._get_completion_path(self.jobscript_idx)

    def _get_state_path(self, js_idx: int) -> Path:
        return self.submission_state_path / str(js_idx)

    def _get_completion_path(self, js_idx: int) -> Path:
        return self._get_state_path(js_idx) / "complete"

    @property
    def submission_complete_path(self) -> Path:
        return self.submission_state_path / "complete"

    def is_complete(self) -> bool:
        return self.completion_path.exists()

    def is_submission_complete(self) -> bool:
        return self.submission_complete_path.exists()

    def mark_complete(self) -> None:
        self.completion_path.parent.mkdir(parents=True, exist_ok=True)
        self.completion_path.touch()
        self._try_mark_submission_complete()

    def _try_mark_submission_complete(self) -> None:
        """Mark the whole submission as complete, if it is."""
        if self.is_submission_complete():
            return

        if all(
            self._get_completion_path(js_idx).exists()
            for js_idx in range(self.num_jobscripts)
        ):
            self.submission_complete_path.touch()

    def is_array_item_complete(self, array_idx: int) -> bool:
        paths = self.get_array_item_completion_paths(array_idx)
        if any(path.exists() for path in paths):
            return True

        # a completion marker may have been consolidated into an ancestor while traversing
        # the paths above; recheck before reporting incomplete:
        return any(path.exists() for path in paths[:-1])

    def mark_array_item_complete(self, array_idx: int) -> None:
        nodes = self.array_tree.path_for(array_idx)
        node_paths = self._get_node_paths(nodes)

        # mark the individual array item complete:
        leaf_marker, _ = node_paths[-1]
        leaf_marker.parent.mkdir(parents=True, exist_ok=True)
        leaf_marker.touch()

        # walk upwards, consolidating completed sibling groups:
        for level in range(len(nodes) - 2, -1, -1):
            parent_node = nodes[level]
            parent_marker, parent_dir = node_paths[level]

            assert parent_dir is not None

            if not self._children_complete(parent_node, parent_dir):
                break

            # establish the stronger completion state before removing the lower-level
            # state from which it was derived:
            parent_marker.parent.mkdir(parents=True, exist_ok=True)
            parent_marker.touch()
            remove_superseded_directory(parent_dir)

        if self.is_complete():
            self._try_mark_submission_complete()

    def _children_complete(self, node: ArrayCompletionNode, children_path: Path) -> bool:
        return all(
            (children_path / f"{child.name}_complete").exists() for child in node.children
        )

    @property
    def array_tree(self) -> ArrayCompletionTree:
        if self.array_indices is None:
            raise ValueError("Jobscript is not a job array.")
        return ArrayCompletionTree.from_indices(self.array_indices)

    def get_array_item_completion_paths(self, array_idx: int) -> tuple[Path, ...]:
        nodes = self.array_tree.path_for(array_idx)
        return tuple(marker for marker, _ in self._get_node_paths(nodes))

    def _get_node_paths(
        self,
        nodes: tuple[ArrayCompletionNode, ...],
    ) -> tuple[tuple[Path, Path | None], ...]:
        """Get the completion-marker and child-directory paths for a tree path.

        The root node is represented by the jobscript-level `complete` marker.
        """

        paths: list[tuple[Path, Path | None]] = [(self.completion_path, self.array_path)]
        parent = self.array_path

        # the root is represented by the jobscript-level `complete` marker, so don't
        # create a separate marker for it:
        for node in nodes[1:]:
            marker = parent / f"{node.name}_complete"
            child_dir = None if node.is_leaf else parent / node.name
            paths.append((marker, child_dir))
            if not node.is_leaf:
                parent = child_dir

        return tuple(paths)


@dataclass(frozen=True)
class ArrayCompletionNode:
    """A node in a jobscript-array completion tree."""

    start: int
    stop: int
    children: tuple[ArrayCompletionNode, ...] = ()

    @property
    def name(self) -> str:
        """Filesystem-friendly name for this node."""
        if self.start == self.stop:
            return str(self.start)
        return f"{self.start}-{self.stop}"

    @property
    def is_leaf(self) -> bool:
        return not self.children

    def contains(self, array_idx: int) -> bool:
        return self.start <= array_idx <= self.stop


@dataclass(frozen=True)
class ArrayCompletionTree:
    """Balanced tree describing completion aggregation for a jobscript array."""

    root: ArrayCompletionNode

    @classmethod
    def from_indices(
        cls,
        indices: Sequence[int],
        *,
        fanout: int = 10,
    ) -> "ArrayCompletionTree":
        indices = tuple(indices)

        if not indices:
            raise ValueError("Array completion tree requires at least one array index.")

        if fanout < 2:
            raise ValueError("fanout must be at least 2.")

        if len(indices) != len(set(indices)):
            raise ValueError("Array indices must be unique.")

        if tuple(sorted(indices)) != indices:
            raise ValueError("Array indices must be sorted.")

        return cls(root=cls._build(indices, fanout))

    @classmethod
    def _build(
        cls,
        indices: tuple[int, ...],
        fanout: int,
    ) -> ArrayCompletionNode:
        if len(indices) == 1:
            idx = indices[0]
            return ArrayCompletionNode(idx, idx)

        if len(indices) <= fanout:
            children = tuple(ArrayCompletionNode(idx, idx) for idx in indices)
        else:
            groups = cls._split_balanced(indices, fanout)
            children = tuple(cls._build(group, fanout) for group in groups)

        return ArrayCompletionNode(
            start=indices[0],
            stop=indices[-1],
            children=children,
        )

    @staticmethod
    def _split_balanced(
        indices: tuple[int, ...],
        num_groups: int,
    ) -> tuple[tuple[int, ...], ...]:
        """Split indices into balanced, contiguous groups."""
        size, remainder = divmod(len(indices), num_groups)

        groups = []
        start = 0

        for group_idx in range(num_groups):
            group_size = size + (group_idx < remainder)
            stop = start + group_size
            groups.append(indices[start:stop])
            start = stop

        return tuple(groups)

    def path_for(
        self,
        array_idx: int,
    ) -> tuple[ArrayCompletionNode, ...]:
        """Get the root-to-leaf path containing an array index."""

        path: list[ArrayCompletionNode] = []
        node = self.root

        while True:
            path.append(node)

            if node.is_leaf:
                if node.start != array_idx:
                    break
                return tuple(path)

            for child in node.children:
                if child.contains(array_idx):
                    node = child
                    break
            else:
                break

        raise ValueError(
            f"Array index {array_idx} is not present in the completion tree."
        )
