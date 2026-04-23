from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import math

import numpy as np
from numpy.typing import NDArray


def vectorized_multi_global_to_local(lengths, g, outer_dtype: str | None = None):
    """
    Vectorised global-to-local index mapping for multiple nested sequences.

    lengths : array of shape (n_seq, n_sublists)
        Lengths of sublists for each sequence (right-padded with zeros if needed).
    g : array of shape (n_g,)
        Global indices (0..N-1).
    Returns:
        outer, inner : arrays of shape (n_seq, n_g)
    """
    # TODO: rename?

    lengths = np.asarray(lengths)
    g = np.asarray(g)

    if outer_dtype:
        if lengths.shape[1] > np.iinfo(np.dtype(outer_dtype)).max:
            raise ValueError("A larger dtype must be used.")

    cumlen = np.cumsum(lengths, axis=1)

    # for each sequence (row), the starting (global) index of each sublist:
    offsets = np.cumsum(
        np.c_[np.zeros(lengths.shape[0]), lengths[:, :-1]], axis=1
    ).astype(int)

    # for each sequence (row), find the first cumlen > g
    mask = g[None, :] < cumlen[:, :, None]

    outer = mask.argmax(axis=1)  # first True along sublist dimension
    if outer_dtype:
        outer = outer.astype(outer_dtype)

    # Compute inner index as g - offset for that outer index
    inner = g[None, :] - np.take_along_axis(offsets, outer, axis=1)

    return outer, inner


@dataclass
class NestingSequence:
    """A sequence of some length that is to be nested with other sequences.

    Attributes
    ----------
    path
        The input path associated with this sequence
    lengths
        The ordered lengths of each input source associated with this sequence.
    nesting_order
        Controls the manner of nesting with other sequences.

    """

    path: str
    lengths: Sequence[int]
    nesting_order: int | float

    def __post_init__(self):
        if self.length < 1:
            raise ValueError(
                f"{self.__class__.__name__} `length` must be at least one, but "
                f"{self.length} was specified."
            )

    @property
    def length(self):
        return sum(self.lengths)


@dataclass
class ZippedNestingSequences:

    sequences: list[NestingSequence]
    length: int  # TODO: don't specify this?
    nesting_order: int | float

    @property
    def num_sequences(self) -> int:
        return len(self.sequences)

    @property
    def paths(self) -> tuple[str, ...]:
        return tuple(ns_i.path for ns_i in self.sequences)

    @property
    def all_lengths(self) -> tuple[list[int]]:
        return tuple(ns_i.lengths for ns_i in self.sequences)

    @property
    def padded_lengths(self):
        """Get an array of lengths, one row for each child `NestingSequence`, zero-padded
        on the right hand side"""
        all_lens = self.all_lengths
        out = np.zeros((self.num_sequences, max(len(lens_i) for lens_i in all_lens)))
        for seq_idx, lens_i in enumerate(self.all_lengths):
            out[seq_idx, : len(lens_i)] = lens_i
        return out


class NestingView:
    """A class to transform flat indices into the set of indices that index the
    specified nesting sequences.

    Each element-set in a task is associated with its own NestingView, which provides a
    method (`get_indices`) to transform task element indices into indices within each of
    the constitutive `NestingSequence` objects (representing e.g. local sequences and
    task input sources).

    Attributes
    ----------
    offset
        An integer offset which represents the cumulative length of any `NestingView`s
        associated with preceding element sets within a given task. This offset is
        subtracted from the index provided to the `get_indices` method, which represents
        a task element index (spanning potentially multiple element sets).

    """

    # TODO: init a NestingView for each element set; maybe in WorkflowData?
    # TODO: store offset to apply when getting indices by element index? (i.e instead of
    # element index within element set)
    # TODO: do we need an offset for different input source indices?
    #  e.g. local(sequence) + default? how does that work?
    #  a given element might have different input source (default vs local vs task)
    # maybe have to combine the lengths of the input sources? and then store offsets?

    # maybe offsets could be stored in another class/container that inits this class,
    # to make unit testing easier; or maybe not, could default to zero offsets.

    _TOL = 1e-8

    def __init__(self, *seqs: Sequence[NestingSequence], offset: int = 0):

        self.offset = offset
        print(f"{offset=!r}")
        print(f"{seqs=!r}")

        # sort by nesting order, in preparation for grouping:
        seqs_srt = sorted(seqs, key=lambda ns_i: ns_i.nesting_order)

        # raise on duplicated paths:
        if len(set(paths := tuple((ns_i.path for ns_i in seqs_srt)))) < len(seqs_srt):
            raise ValueError(
                f"Each nesting sequence must have a distinct path, but "
                f"received: {paths!r}."
            )

        # group by nesting orders:
        self.groups = self._group_by_nesting_order(*seqs_srt)

        # get group indices of non-integer nesting orders:
        self.decimal_indices = self._get_decimal_indices()

        self.all_lengths = tuple(ns.length for ns in self.groups)

        # add a one so indices are more vectorisable:
        self.nested_lengths = tuple(
            [
                i
                for idx, i in enumerate(self.all_lengths)
                if idx not in self.decimal_indices
            ]
            + [1]
        )  # lengths that don't include decimal nesting orders

        # assigned on first access:
        self._nested_len_fwd_prods = None
        self._nested_len_bwd_prods = None
        self._lengths_idx = None
        self.__floor_div = None
        self.__group_expand_idx = None
        self._group_num_sequences = None

        # raise on non-integer nesting order for the first sequence (group):
        if self.decimal_indices and self.decimal_indices[0] == 0:
            raise ValueError(
                f"Non-integer nesting orders cannot be used for the first nesting "
                f"sequence, because there must be preceding sequences to merge into!"
            )

        paths = []
        for idx, group in enumerate(self.groups):
            paths.extend(group.paths)
            if (
                idx in self.decimal_indices
                and group.length
                != self.nested_len_bwd_prods[idx - 1 - self.decimal_indices.index(idx)]
            ):
                prev_group = self.groups[idx - 1]
                raise ValueError(
                    f"Sequences with non-integer nesting orders must have a length equal "
                    f"to the that of the preceding group of merged sequences (when all "
                    f"sequences are ordered by ascending nesting order) but sequences "
                    f"with paths {group.paths!r}, nesting order "
                    f"{group.nesting_order!r}, and length {group.length!r} do "
                    f"not have the same length as the merged preceding sequences with "
                    f"paths {prev_group.paths!r} and length "
                    f"{self.nested_len_bwd_prods[idx - 1]!r}."
                )
        self.paths = tuple(paths)

    def __len__(self) -> int:
        return self.num_items

    @property
    def num_sequences(self) -> int:
        return sum(para_seq.num_sequences for para_seq in self.groups)

    @property
    def num_groups(self) -> int:
        """Get the number of groups into which the provided nesting sequences are
        arranged (nesting sequences with the same nesting order are grouped together)."""
        return len(self.groups)

    @property
    def num_items(self) -> int:
        return math.prod(self.nested_lengths)

    @property
    def nested_len_fwd_prods(self) -> tuple[int, ...]:
        if self._nested_len_fwd_prods is None:
            self._nested_len_fwd_prods = tuple(
                math.prod(self.nested_lengths[nested_len_idx:])
                for nested_len_idx in range(len(self.nested_lengths))
            )
        return self._nested_len_fwd_prods

    @property
    def nested_len_bwd_prods(self) -> tuple[int, ...]:
        if self._nested_len_bwd_prods is None:
            self._nested_len_bwd_prods = tuple(
                math.prod(self.nested_lengths[: nested_len_idx + 1])
                for nested_len_idx in range(len(self.nested_lengths[:-1]))
            )
        return self._nested_len_bwd_prods

    @property
    def lengths_idx(self):
        if self._lengths_idx is None:
            lengths_idx = [i for i in range(1, len(self.nested_lengths))]
            for dec_idx in self.decimal_indices:
                lengths_idx.insert(dec_idx, lengths_idx[dec_idx - 1])
            self._lengths_idx = lengths_idx
        return self._lengths_idx

    @property
    def _floor_div(self):
        if self.__floor_div is None:
            self.__floor_div = np.array(self.nested_len_fwd_prods)[self.lengths_idx]
        return self.__floor_div

    @property
    def group_num_sequences(self):
        if self._group_num_sequences is None:
            self._group_num_sequences = [grp.num_sequences for grp in self.groups]
        return self._group_num_sequences

    @property
    def _group_expand_idx(self):
        if self.__group_expand_idx is None:
            self.__group_expand_idx = np.repeat(
                np.arange(len(self.group_num_sequences)), self.group_num_sequences
            )
        return self.__group_expand_idx

    @property
    def indices_dtype(self):
        return np.dtype([(path_i, np.int32) for path_i in self.paths])

    def _get_decimal_indices(self) -> tuple[int, ...]:
        """Get the group indices that have non-integer nesting orders, signifying that
        they should be merged in parallel (i.e. zipped) with the preceding set of nested
        sequences."""

        return tuple(
            idx
            for idx, ns in enumerate(self.groups)
            if (float(ns.nesting_order) - int(ns.nesting_order) > self._TOL)
        )

    def _group_by_nesting_order(
        self,
        *seqs: Sequence[NestingSequence],
    ) -> list[ZippedNestingSequences]:
        groups = [
            ZippedNestingSequences(
                sequences=[seqs[0]],
                nesting_order=seqs[0].nesting_order,
                length=seqs[0].length,
            )
        ]
        for seq_item in seqs[1:]:
            for group_idx, group in enumerate(groups):
                if (
                    is_vals_equal := abs(
                        seq_item.nesting_order - group.sequences[0].nesting_order
                    )
                    < self._TOL
                ):
                    groups[group_idx].sequences.append(seq_item)
                    break
            if not is_vals_equal:
                groups.append(
                    ZippedNestingSequences(
                        sequences=[seq_item],
                        nesting_order=seq_item.nesting_order,
                        length=seq_item.length,
                    )
                )
        return groups

    def get_indices(
        self,
        element_idx: Sequence[int] | NDArray | None = None,
    ) -> dict[str, tuple[NDArray, NDArray]]:
        """
        Parameters
        ----------
        element_idx:
            The element index within the task, for which we want input indices for the
            paths of this nesting view.
        as_list:
            If True, return a list of tuples. If False, return a Numpy record array whose
            fields correspond to the nesting sequence paths.
        """

        if element_idx is not None and (min_idx := min(element_idx)) < self.offset:
            raise ValueError(
                f"Minimum of specified element index ({min_idx}) cannot be less that the "
                f"offset: {self.offset}."
            )

        element_idx_ = (
            np.arange(self.num_items)
            if element_idx is None
            else np.asarray(element_idx) - self.offset
        ).reshape((-1, 1))

        # array of column vectors, one for each zipped path
        out = (element_idx_ // self._floor_div) % np.array(self.all_lengths)

        # for each column in `out`, generate indices for the input source index and the
        # index within the values for that input source:
        dct = {}
        for global_idx, group in zip(out.T, self.groups):
            inp_src_idx, seq_idx = vectorized_multi_global_to_local(
                group.padded_lengths, global_idx, outer_dtype="uint8"
            )
            for ns_idx, ns_i in enumerate(group.sequences):
                dct[ns_i.path] = (inp_src_idx[ns_idx], seq_idx[ns_idx])

        return dct
