"""
Miscellaneous utilities.
"""

from __future__ import annotations
from collections import Counter
from asyncio import events
import contextvars
import contextlib
import copy
import enum
import functools
import hashlib
from itertools import accumulate, islice
from importlib import resources
import json
import keyword
import os
from pathlib import Path, PurePath
import random
import re
import socket
import string
import subprocess
from datetime import datetime, timedelta, timezone
import sys
import traceback
from typing import Literal, cast, overload, TypeVar, TYPE_CHECKING
import fsspec  # type: ignore
import numpy as np

from ruamel.yaml import YAML
from watchdog.utils.dirsnapshot import DirectorySnapshot

from hpcflow.sdk.core.errors import (
    ContainerKeyError,
    InvalidIdentifier,
    MissingVariableSubstitutionError,
)
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.utils.deferred_file import DeferredFileWriter

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable, Iterable, Mapping, Sequence
    from contextlib import AbstractContextManager
    from types import ModuleType
    from typing import Any, IO, Iterator
    from typing_extensions import TypeAlias
    from numpy.typing import NDArray
    from ..typing import PathLike

T = TypeVar("T")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
TList: TypeAlias = "T | list[TList]"
TD = TypeVar("TD", bound="Mapping[str, Any]")
E = TypeVar("E", bound=enum.Enum)


def make_workflow_id() -> str:
    """
    Generate a random ID for a workflow.
    """
    length = 12
    chars = string.ascii_letters + "0123456789"
    return "".join(random.choices(chars, k=length))


def get_time_stamp() -> str:
    """
    Get the current time in standard string form.
    """
    return datetime.now(timezone.utc).astimezone().strftime("%Y.%m.%d_%H:%M:%S_%z")


def get_duplicate_items(lst: Iterable[T]) -> list[T]:
    """Get a list of all items in an iterable that appear more than once, assuming items
    are hashable.

    Examples
    --------
    >>> get_duplicate_items([1, 1, 2, 3])
    [1]

    >>> get_duplicate_items([1, 2, 3])
    []

    >>> get_duplicate_items([1, 2, 3, 3, 3, 2])
    [2, 3]

    """
    return [x for x, y in Counter(lst).items() if y > 1]


def check_valid_py_identifier(name: str) -> str:
    """Check a string is (roughly) a valid Python variable identifier and return it.

    The rules are:
        1. `name` must not be empty
        2. `name` must not be a Python keyword
        3. `name` must begin with an alphabetic character, and all remaining characters
           must be alphanumeric.

    Notes
    -----
    The following attributes are passed through this function on object initialisation:
        - `ElementGroup.name`
        - `Executable.label`
        - `Parameter.typ`
        - `TaskObjective.name`
        - `TaskSchema.method`
        - `TaskSchema.implementation`
        - `Loop.name`

    """
    try:
        trial_name = name[1:].replace("_", "")  # "internal" underscores are allowed
    except TypeError:
        raise InvalidIdentifier(name) from None
    except KeyError as e:
        raise KeyError(f"unexpected name type {name}") from e
    if (
        not name
        or not (name[0].isalpha() and ((trial_name[1:] or "a").isalnum()))
        or keyword.iskeyword(name)
    ):
        raise InvalidIdentifier(name)

    return name


@overload
def group_by_dict_key_values(  # type: ignore[overload-overlap]
    lst: list[dict[T, T2]], key: T
) -> list[list[dict[T, T2]]]: ...


@overload
def group_by_dict_key_values(lst: list[TD], key: str) -> list[list[TD]]: ...


def group_by_dict_key_values(lst: list, key):
    """Group a list of dicts according to specified equivalent key-values.

    Parameters
    ----------
    lst : list of dict
        The list of dicts to group together.
    key : key value
        Dicts that have identical values for all of these keys will be grouped together
        into a sub-list.

    Returns
    -------
    grouped : list of list of dict

    Examples
    --------
    >>> group_by_dict_key_values([{'a': 1}, {'a': 2}, {'a': 1}], 'a')
    [[{'a': 1}, {'a': 1}], [{'a': 2}]]

    """

    grouped = [[lst[0]]]
    for lst_item in lst[1:]:
        for group_idx, group in enumerate(grouped):
            try:
                is_vals_equal = lst_item[key] == group[0][key]

            except KeyError:
                # dicts that do not have the `key` will be in their own group:
                is_vals_equal = False

            if is_vals_equal:
                grouped[group_idx].append(lst_item)
                break

        if not is_vals_equal:
            grouped.append([lst_item])

    return grouped


def swap_nested_dict_keys(dct: dict[T, dict[T2, T3]], inner_key: T2):
    """Return a copy where top-level keys have been swapped with a second-level inner key.

    Examples:
    ---------
    >>> swap_nested_dict_keys(
        dct={
            'p1': {'format': 'direct', 'all_iterations': True},
            'p2': {'format': 'json'},
            'p3': {'format': 'direct'},
        },
        inner_key="format",
    )
    {
        "direct": {"p1": {"all_iterations": True}, "p3": {}},
        "json": {"p2": {}},
    }

    """
    out: dict[T3, dict[T, dict[T2, T3]]] = {}
    for k, v in copy.deepcopy(dct or {}).items():
        out.setdefault(v.pop(inner_key), {})[k] = v
    return out


def _ensure_int(path_comp: Any, cur_data: Any, cast_indices: bool) -> int:
    """
    Helper for get_in_container() and set_in_container()
    """
    if isinstance(path_comp, int):
        return path_comp
    if not cast_indices:
        raise TypeError(
            f"Path component {path_comp!r} must be an integer index "
            f"since data is a sequence: {cur_data!r}."
        )
    try:
        return int(path_comp)
    except (TypeError, ValueError) as e:
        raise TypeError(
            f"Path component {path_comp!r} must be an integer index "
            f"since data is a sequence: {cur_data!r}."
        ) from e


def get_in_container(
    cont, path: Sequence, cast_indices: bool = False, allow_getattr: bool = False
):
    """
    Follow a path (sequence of indices of appropriate type) into a container to obtain
    a "leaf" value. Containers can be lists, tuples, dicts,
    or any class (with `getattr()`) if ``allow_getattr`` is True.
    """
    cur_data = cont
    err_msg = (
        "Data at path {path_comps!r} is not a sequence, but is of type "
        "{cur_data_type!r} and so sub-data cannot be extracted."
    )
    for idx, path_comp in enumerate(path):
        if isinstance(cur_data, (list, tuple)):
            cur_data = cur_data[_ensure_int(path_comp, cur_data, cast_indices)]
        elif isinstance(cur_data, dict) or hasattr(cur_data, "__getitem__"):
            try:
                cur_data = cur_data[path_comp]
            except KeyError:
                raise ContainerKeyError(path=cast("list[str]", path[: idx + 1]))
        elif allow_getattr:
            try:
                cur_data = getattr(cur_data, path_comp)
            except AttributeError:
                raise ValueError(
                    err_msg.format(cur_data_type=type(cur_data), path_comps=path[:idx])
                )
        else:
            raise ValueError(
                err_msg.format(cur_data_type=type(cur_data), path_comps=path[:idx])
            )
    return cur_data


def set_in_container(
    cont, path: Sequence, value, ensure_path=False, cast_indices=False
) -> None:
    """
    Follow a path (sequence of indices of appropriate type) into a container to update
    a "leaf" value. Containers can be lists, tuples or dicts.
    The "branch" holding the leaf to update must be modifiable.
    """
    if ensure_path:
        num_path = len(path)
        for idx in range(1, num_path):
            try:
                get_in_container(cont, path[:idx], cast_indices=cast_indices)
            except (KeyError, ValueError):
                set_in_container(
                    cont=cont,
                    path=path[:idx],
                    value={},
                    ensure_path=False,
                    cast_indices=cast_indices,
                )

    sub_data = get_in_container(cont, path[:-1], cast_indices=cast_indices)
    path_comp = path[-1]
    if isinstance(sub_data, (list, tuple)):
        path_comp = _ensure_int(path_comp, sub_data, cast_indices)
    sub_data[path_comp] = value


def get_relative_path(path1: Sequence[T], path2: Sequence[T]) -> Sequence[T]:
    """Get relative path components between two paths.

    Parameters
    ----------
    path1 : tuple of (str or int or float) of length N
    path2 : tuple of (str or int or float) of length less than or equal to N

    Returns
    -------
    relative_path : tuple of (str or int or float)
        The path components in `path1` that are not in `path2`.

    Raises
    ------
    ValueError
        If the two paths do not share a common ancestor of path components, or if `path2`
        is longer than `path1`.

    Notes
    -----
    This function behaves like a simplified `PurePath(*path1).relative_to(PurePath(*path2))`
    from the `pathlib` module, but where path components can include non-strings.

    Examples
    --------
    >>> get_relative_path(('A', 'B', 'C'), ('A',))
    ('B', 'C')

    >>> get_relative_path(('A', 'B'), ('A', 'B'))
    ()

    """

    len_path2 = len(path2)
    if len(path1) < len_path2 or any(i != j for i, j in zip(path1[:len_path2], path2)):
        raise ValueError(f"{path1!r} is not in the subpath of {path2!r}.")

    return path1[len_path2:]


def search_dir_files_by_regex(
    pattern: str | re.Pattern[str], directory: str | os.PathLike = "."
) -> list[str]:
    """Search recursively for files in a directory by a regex pattern and return matching
    file paths, relative to the given directory."""
    dir_ = Path(directory)
    return [
        str(entry.relative_to(dir_))
        for entry in dir_.rglob("*")
        if re.search(pattern, entry.name)
    ]


class PrettyPrinter:
    """
    A class that produces a nice readable version of itself with ``str()``.
    Intended to be subclassed.
    """

    def __str__(self) -> str:
        lines = [self.__class__.__name__ + ":"]
        for key, val in vars(self).items():
            lines.extend(f"{key}: {val}".split("\n"))
        return "\n    ".join(lines)


def capitalise_first_letter(chars: str) -> str:
    """
    Convert the first character of a string to upper case (if that makes sense).
    The rest of the string is unchanged.
    """
    return chars[0].upper() + chars[1:]


_STRING_VARS_RE = re.compile(r"\<\<var:(.*?)(?:\[(.*)\])?\>\>")


@TimeIt.decorator
def substitute_string_vars(string: str, variables: dict[str, str]):
    """
    Scan ``string`` and substitute sequences like ``<<var:ABC>>`` with the value
    looked up in the supplied dictionary (with ``ABC`` as the key).

    Default values for the substitution can be supplied like:
    ``<<var:ABC[default=XYZ]>>``

    Examples
    --------
    >>> substitute_string_vars("abc <<var:def>> ghi", {"def": "123"})
    "abc 123 def"
    """

    def var_repl(match_obj: re.Match[str]) -> str:
        kwargs: dict[str, str] = {}
        var_name: str = match_obj[1]
        kwargs_str: str | None = match_obj[2]
        if kwargs_str:
            for i in kwargs_str.split(","):
                k, v = i.split("=")
                kwargs[k.strip()] = v.strip()
        try:
            out = str(variables[var_name])
        except KeyError:
            if "default" in kwargs:
                out = kwargs["default"]
                print(
                    f"Using default value ({out!r}) for workflow template string "
                    f"variable {var_name!r}."
                )
            else:
                raise MissingVariableSubstitutionError(var_name, variables)
        return out

    return _STRING_VARS_RE.sub(
        repl=var_repl,
        string=string,
    )


@TimeIt.decorator
def read_YAML_str(
    yaml_str: str, typ="safe", variables: dict[str, str] | Literal[False] | None = None
) -> Any:
    """Load a YAML string. This will produce basic objects.

    Parameters
    ----------
    yaml_str:
        The YAML string to parse.
    typ:
        Load type passed to the YAML library.
    variables:
        String variables to substitute in `yaml_str`. Substitutions will be attempted if
        the file looks to contain variable references (like "<<var:name>>"). If set to
        `False`, no substitutions will occur.
    """
    if variables is not False and "<<var:" in yaml_str:
        yaml_str = substitute_string_vars(yaml_str, variables=variables or {})
    yaml = YAML(typ=typ)
    return yaml.load(yaml_str)


@TimeIt.decorator
def read_YAML_file(
    path: PathLike, typ="safe", variables: dict[str, str] | Literal[False] | None = None
) -> Any:
    """Load a YAML file. This will produce basic objects.

    Parameters
    ----------
    path:
        Path to the YAML file to parse.
    typ:
        Load type passed to the YAML library.
    variables:
        String variables to substitute in the file given by `path`. Substitutions will be
        attempted if the file looks to contain variable references (like "<<var:name>>").
        If set to `False`, no substitutions will occur.
    """
    with fsspec.open(path, "rt") as f:
        yaml_str: str = f.read()
    return read_YAML_str(yaml_str, typ=typ, variables=variables)


def write_YAML_file(obj, path: str | Path, typ: str = "safe") -> None:
    """Write a basic object to a YAML file."""
    yaml = YAML(typ=typ)
    with Path(path).open("wt") as fp:
        yaml.dump(obj, fp)


def read_JSON_string(
    json_str: str, variables: dict[str, str] | Literal[False] | None = None
) -> Any:
    """Load a JSON string. This will produce basic objects.

    Parameters
    ----------
    json_str:
        The JSON string to parse.
    variables:
        String variables to substitute in `json_str`. Substitutions will be attempted if
        the file looks to contain variable references (like "<<var:name>>"). If set to
        `False`, no substitutions will occur.
    """
    if variables is not False and "<<var:" in json_str:
        json_str = substitute_string_vars(json_str, variables=variables or {})
    return json.loads(json_str)


def read_JSON_file(path, variables: dict[str, str] | Literal[False] | None = None) -> Any:
    """Load a JSON file. This will produce basic objects.

    Parameters
    ----------
    path:
        Path to the JSON file to parse.
    variables:
        String variables to substitute in the file given by `path`. Substitutions will be
        attempted if the file looks to contain variable references (like "<<var:name>>").
        If set to `False`, no substitutions will occur.
    """
    with fsspec.open(path, "rt") as f:
        json_str: str = f.read()
    return read_JSON_string(json_str, variables=variables)


def write_JSON_file(obj, path: str | Path) -> None:
    """Write a basic object to a JSON file."""
    with Path(path).open("wt") as fp:
        json.dump(obj, fp)


def get_item_repeat_index(
    lst: Sequence[T],
    *,
    distinguish_singular: bool = False,
    item_callable: Callable[[T], Hashable] | None = None,
):
    """Get the repeat index for each item in a list.

    Parameters
    ----------
    lst : list
        Must contain hashable items, or hashable objects that are returned via `callable`
        called on each item.
    distinguish_singular : bool
        If True, items that are not repeated will have a repeat index of 0, and items that
        are repeated will have repeat indices starting from 1.
    item_callable : callable
        If specified, comparisons are made on the output of this callable on each item.

    Returns
    -------
    repeat_idx : list of int
        Repeat indices of each item (see `distinguish_singular` for details).

    """

    idx: dict[Any, list[int]] = {}
    if item_callable:
        for i_idx, item in enumerate(lst):
            idx.setdefault(item_callable(item), []).append(i_idx)
    else:
        for i_idx, item in enumerate(lst):
            idx.setdefault(item, []).append(i_idx)

    rep_idx = [0] * len(lst)
    for v in idx.values():
        start = len(v) > 1 if distinguish_singular else 0
        for i_idx, i in enumerate(v, start):
            rep_idx[i] = i_idx

    return rep_idx


def get_process_stamp() -> str:
    """
    Return a globally unique string identifying this process.

    Note
    ----
    This should only be called once per process.
    """
    return "{} {} {}".format(
        datetime.now(),
        socket.gethostname(),
        os.getpid(),
    )


_ANSI_ESCAPE_RE = re.compile(r"(\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]")


def remove_ansi_escape_sequences(string: str) -> str:
    """
    Strip ANSI terminal escape codes from a string.
    """
    return _ANSI_ESCAPE_RE.sub("", string)


def get_md5_hash(obj) -> str:
    """
    Compute the MD5 hash of an object.
    This is the hash of the JSON of the object (with sorted keys) as a hex string.
    """
    json_str = json.dumps(obj, sort_keys=True)
    return hashlib.md5(json_str.encode("utf-8")).hexdigest()


def get_nested_indices(
    idx: int, size: int, nest_levels: int, raise_on_rollover: bool = False
) -> list[int]:
    """Generate the set of nested indices of length `n` that correspond to a global
    `idx`.

    Examples
    --------
    >>> for i in range(4**2): print(get_nest_index(i, nest_levels=2, size=4))
    [0, 0]
    [0, 1]
    [0, 2]
    [0, 3]
    [1, 0]
    [1, 1]
    [1, 2]
    [1, 3]
    [2, 0]
    [2, 1]
    [2, 2]
    [2, 3]
    [3, 0]
    [3, 1]
    [3, 2]
    [3, 3]

    >>> for i in range(4**3): print(get_nested_indices(i, nest_levels=3, size=4))
    [0, 0, 0]
    [0, 0, 1]
    [0, 0, 2]
    [0, 0, 3]
    [0, 1, 0]
       ...
    [3, 2, 3]
    [3, 3, 0]
    [3, 3, 1]
    [3, 3, 2]
    [3, 3, 3]
    """
    if raise_on_rollover and idx >= size**nest_levels:
        raise ValueError(
            f"`idx` ({idx}) is greater than or equal to  size**nest_levels` "
            f"({size**nest_levels})."
        )

    return [(idx // (size ** (nest_levels - (i + 1)))) % size for i in range(nest_levels)]


def ensure_in(item: T, lst: list[T]) -> int:
    """Get the index of an item in a list and append the item if it is not in the
    list."""
    # TODO: add tests
    try:
        return lst.index(item)
    except ValueError:
        lst.append(item)
        return len(lst) - 1


def list_to_dict(
    lst: Sequence[Mapping[T, T2]], exclude: Iterable[T] | None = None
) -> dict[T, list[T2]]:
    """
    Convert a list of dicts to a dict of lists.
    """
    # TODO: test
    exc = frozenset(exclude or ())
    dct: dict[T, list[T2]] = {k: [] for k in lst[0] if k not in exc}
    for d in lst:
        for k, v in d.items():
            if k not in exc:
                dct[k].append(v)
    return dct


def bisect_slice(selection: slice, len_A: int) -> tuple[slice, slice]:
    """Given two sequences (the first of which of known length), get the two slices that
    are equivalent to a given slice if the two sequences were combined."""

    if selection.start < 0 or selection.stop < 0 or selection.step < 0:
        raise NotImplementedError("Can't do negative slices yet.")

    A_idx = selection.indices(len_A)
    B_start = selection.start - len_A
    if len_A != 0 and B_start < 0:
        B_start = B_start % selection.step
    if len_A > selection.stop:
        B_stop = B_start
    else:
        B_stop = selection.stop - len_A

    return slice(*A_idx), slice(B_start, B_stop, selection.step)


def replace_items(lst: list[T], start: int, end: int, repl: list[T]) -> list[T]:
    """Replaced a range of items in a list with items in another list."""
    # Convert to actual indices for our safety checks; handles end-relative addressing
    real_start, real_end, _ = slice(start, end).indices(len(lst))
    if real_end <= real_start:
        raise ValueError(
            f"`end` ({end}) must be greater than or equal to `start` ({start})."
        )
    if real_start >= len(lst):
        raise ValueError(f"`start` ({start}) must be less than length ({len(lst)}).")
    if real_end > len(lst):
        raise ValueError(
            f"`end` ({end}) must be less than or equal to length ({len(lst)})."
        )

    lst = list(lst)
    lst[start:end] = repl
    return lst


def flatten(
    lst: list[int] | list[list[int]] | list[list[list[int]]],
) -> tuple[list[int], tuple[list[int], ...]]:
    """Flatten an arbitrarily (but of uniform depth) nested list and return shape
    information to enable un-flattening.

    Un-flattening can be performed with the :py:func:`reshape` function.

    lst
        List to be flattened. Each element must contain all lists or otherwise all items
        that are considered to be at the "bottom" of the nested structure (e.g. integers).
        For example, `[[1, 2], [3]]` is permitted and flattens to `[1, 2, 3]`, but
        `[[1, 2], 3]` is not permitted because the first element is a list, but the second
        is not.

    """

    def _flatten(
        lst: list[int] | list[list[int]] | list[list[list[int]]], depth=0
    ) -> list[int]:
        out: list[int] = []
        for item in lst:
            if isinstance(item, list):
                out.extend(_flatten(item, depth + 1))
                all_lens[depth].append(len(item))
            else:
                out.append(item)
        return out

    def _get_max_depth(lst: list[int] | list[list[int]] | list[list[list[int]]]) -> int:
        val: Any = lst
        max_depth = 0
        while isinstance(val, list):
            max_depth += 1
            try:
                val = val[0]
            except IndexError:
                # empty list, assume this is max depth
                break
        return max_depth

    max_depth = _get_max_depth(lst) - 1
    all_lens: tuple[list[int], ...] = tuple([] for _ in range(max_depth))

    return _flatten(lst), all_lens


def reshape(lst: Sequence[T], lens: Sequence[Sequence[int]]) -> list[TList[T]]:
    """
    Reverse the destructuring of the :py:func:`flatten` function.
    """

    def _reshape(lst: list[T2], lens: Sequence[int]) -> list[list[T2]]:
        lens_acc = [0, *accumulate(lens)]
        return [lst[lens_acc[idx] : lens_acc[idx + 1]] for idx in range(len(lens))]

    result: list[TList[T]] = list(lst)
    for lens_i in lens[::-1]:
        result = cast("list[TList[T]]", _reshape(result, lens_i))

    return result


@overload
def remap(
    lst: list[int], mapping_func: Callable[[Sequence[int]], Sequence[T]]
) -> list[T]: ...


@overload
def remap(
    lst: list[list[int]], mapping_func: Callable[[Sequence[int]], Sequence[T]]
) -> list[list[T]]: ...


@overload
def remap(
    lst: list[list[list[int]]], mapping_func: Callable[[Sequence[int]], Sequence[T]]
) -> list[list[list[T]]]: ...


def remap(lst, mapping_func):
    """
    Apply a mapping to a structure of lists with ints (typically indices) as leaves to
    get a structure of lists with some objects as leaves.

    Parameters
    ----------
    lst: list[int] | list[list[int]] | list[list[list[int]]]
        The structure to remap.
    mapping_func: Callable[[Sequence[int]], Sequence[T]]
        The mapping function from sequences of ints to sequences of objects.

    Returns
    -------
    list[T] | list[list[T]] | list[list[list[T]]]
        Nested list structure in same form as input, with leaves remapped.
    """
    x, y = flatten(lst)
    return reshape(mapping_func(x), y)


_FSSPEC_URL_RE = re.compile(r"(?:[a-z0-9]+:{1,2})+\/\/")


def is_fsspec_url(url: str) -> bool:
    """
    Test if a URL appears to be one that can be understood by fsspec.
    """
    return bool(_FSSPEC_URL_RE.match(url))


class JSONLikeDirSnapShot(DirectorySnapshot):
    """
    Overridden DirectorySnapshot from watchdog to allow saving and loading from JSON.

    Parameters
    ----------
    root_path: str
        Where to take the snapshot based at.
    data: dict[str, list]
        Serialised snapshot to reload from.
        See :py:meth:`to_json_like`.
    """

    def __init__(
        self,
        root_path: str | None = None,
        data: dict[str, list] | None = None,
        use_strings: bool = False,
    ):
        """
        Create an empty snapshot or load from JSON-like data.
        """

        #: Where to take the snapshot based at.
        self.root_path = root_path
        self._stat_info: dict[str, os.stat_result] = {}
        self._inode_to_path: dict[tuple, str] = {}

        if data:
            assert root_path
            for name, item in data.items():
                # add root path
                full_name = str(PurePath(root_path) / PurePath(name))
                item = [int(i) for i in item] if use_strings else item
                stat_dat, inode_key = item[:-2], item[-2:]
                self._stat_info[full_name] = os.stat_result(stat_dat)
                self._inode_to_path[tuple(inode_key)] = full_name

    def take(self, *args, **kwargs) -> None:
        """Take the snapshot."""
        super().__init__(*args, **kwargs)

    def to_json_like(self, use_strings: bool = False) -> dict[str, Any]:
        """Export to a dict that is JSON-compatible and can be later reloaded.

        The last two integers in `data` for each path are the keys in
        `self._inode_to_path`.

        """
        # first key is the root path:
        root_path = next(iter(self._stat_info))

        # store efficiently:
        inode_invert = {v: k for k, v in self._inode_to_path.items()}
        data: dict[str, list] = {
            str(PurePath(k).relative_to(root_path)): [
                str(i) if use_strings else i for i in [*v, *inode_invert[k]]
            ]
            for k, v in self._stat_info.items()
        }

        return {
            "root_path": root_path,
            "data": data,
            "use_strings": use_strings,
        }


def open_file(filename: str | Path):
    """Open a file or directory using the default system application."""
    if sys.platform == "win32":
        os.startfile(filename)
    else:
        opener = "open" if sys.platform == "darwin" else "xdg-open"
        subprocess.call([opener, filename])


@overload
def get_enum_by_name_or_val(enum_cls: type[E], key: None) -> None: ...


@overload
def get_enum_by_name_or_val(enum_cls: type[E], key: str | int | float | E) -> E: ...


def get_enum_by_name_or_val(
    enum_cls: type[E], key: str | int | float | E | None
) -> E | None:
    """Retrieve an enum by name or value, assuming uppercase names and integer values."""
    if key is None or isinstance(key, enum_cls):
        return key
    elif isinstance(key, (int, float)):
        return enum_cls(int(key))  # retrieve by value
    elif isinstance(key, str):
        try:
            return cast("E", getattr(enum_cls, key.upper()))  # retrieve by name
        except AttributeError:
            pass
    raise ValueError(f"Unknown enum key or value {key!r} for class {enum_cls!r}")


_PARAM_SPLIT_RE = re.compile(r"((?:\w|\.)+)(?:\[(\w+)\])?")


def split_param_label(param_path: str) -> tuple[str, str] | tuple[None, None]:
    """Split a parameter path into the path and the label, if present."""
    if match := _PARAM_SPLIT_RE.match(param_path):
        return match[1], match[2]
    else:
        return None, None


def process_string_nodes(data: T, str_processor: Callable[[str], str]) -> T:
    """Walk through a nested data structure and process string nodes using a provided
    callable."""

    if isinstance(data, dict):
        return cast(
            "T", {k: process_string_nodes(v, str_processor) for k, v in data.items()}
        )

    elif isinstance(data, (list, tuple, set, frozenset)):
        _data = (process_string_nodes(i, str_processor) for i in data)
        if isinstance(data, tuple):
            return cast("T", tuple(_data))
        elif isinstance(data, set):
            return cast("T", set(_data))
        elif isinstance(data, frozenset):
            return cast("T", frozenset(_data))
        else:
            return cast("T", list(_data))

    elif isinstance(data, str):
        return cast("T", str_processor(data))

    return data


def linspace_rect(
    start: Sequence[float],
    stop: Sequence[float],
    num: Sequence[int],
    include: Sequence[str] | None = None,
    **kwargs,
) -> NDArray:
    """Generate a linear space around a rectangle.

    Parameters
    ----------
    start
        Two start values; one for each dimension of the rectangle.
    stop
        Two stop values; one for each dimension of the rectangle.
    num
        Two number values; one for each dimension of the rectangle.
    include
        If specified, include only the specified edges. Choose from "top", "right",
        "bottom", "left".

    Returns
    -------
    rect
        Coordinates of the rectangle perimeter.

    """

    if num[0] <= 1 or num[1] <= 1:
        raise ValueError("Both values in `num` must be greater than 1.")

    inc = set(include) if include else {"top", "right", "bottom", "left"}

    c0_range = np.linspace(start=start[0], stop=stop[0], num=num[0], **kwargs)
    c1_range_all = np.linspace(start=start[1], stop=stop[1], num=num[1], **kwargs)

    c1_range = c1_range_all
    if "bottom" in inc:
        c1_range = c1_range[1:]
    if "top" in inc:
        c1_range = c1_range[:-1]

    c0_range_c1_start = np.vstack([c0_range, np.repeat(start[1], num[0])])
    c0_range_c1_stop = np.vstack([c0_range, np.repeat(c1_range_all[-1], num[0])])

    c1_range_c0_start = np.vstack([np.repeat(start[0], len(c1_range)), c1_range])
    c1_range_c0_stop = np.vstack([np.repeat(c0_range[-1], len(c1_range)), c1_range])

    stacked = []
    if "top" in inc:
        stacked.append(c0_range_c1_stop)
    if "right" in inc:
        stacked.append(c1_range_c0_stop)
    if "bottom" in inc:
        stacked.append(c0_range_c1_start)
    if "left" in inc:
        stacked.append(c1_range_c0_start)

    return np.hstack(stacked)


def dict_values_process_flat(
    d: Mapping[T, T2 | list[T2]], callable: Callable[[list[T2]], list[T3]]
) -> Mapping[T, T3 | list[T3]]:
    """
    Return a copy of a dict, where the values are processed by a callable that is to
    be called only once, and where the values may be single items or lists of items.

    Examples
    --------
    d = {'a': 0, 'b': [1, 2], 'c': 5}
    >>> dict_values_process_flat(d, callable=lambda x: [i + 1 for i in x])
    {'a': 1, 'b': [2, 3], 'c': 6}

    """
    flat: list[T2] = []  # values of `d`, flattened
    is_multi: list[tuple[bool, int]] = (
        []
    )  # whether a list, and the number of items to process
    for i in d.values():
        if isinstance(i, list):
            flat.extend(cast("list[T2]", i))
            is_multi.append((True, len(i)))
        else:
            flat.append(cast("T2", i))
            is_multi.append((False, 1))

    processed = callable(flat)

    out: dict[T, T3 | list[T3]] = {}
    for idx_i, (m, k) in enumerate(zip(is_multi, d)):
        start_idx = sum(i[1] for i in is_multi[:idx_i])
        end_idx = start_idx + m[1]
        proc_idx_k = processed[start_idx:end_idx]
        if not m[0]:
            out[k] = proc_idx_k[0]
        else:
            out[k] = proc_idx_k

    return out


def nth_key(dct: Iterable[T], n: int) -> T:
    """
    Given a dict in some order, get the n'th key of that dict.
    """
    it = iter(dct)
    next(islice(it, n, n), None)
    return next(it)


def nth_value(dct: dict[Any, T], n: int) -> T:
    """
    Given a dict in some order, get the n'th value of that dict.
    """
    return dct[nth_key(dct, n)]


def normalise_timestamp(timestamp: datetime) -> datetime:
    """
    Force a timestamp to have UTC as its timezone,
    then convert to use the local timezone.
    """
    return timestamp.replace(tzinfo=timezone.utc).astimezone()


def parse_timestamp(timestamp: str | datetime, ts_fmt: str) -> datetime:
    """
    Standard timestamp parsing.
    Ensures that timestamps are internally all UTC.
    """
    return normalise_timestamp(
        timestamp
        if isinstance(timestamp, datetime)
        else datetime.strptime(timestamp, ts_fmt)
    )


def current_timestamp() -> datetime:
    """
    Get a UTC timestamp for the current time
    """
    return datetime.now(timezone.utc)


def timedelta_format(td: timedelta) -> str:
    """
    Convert time delta to string in standard form.
    """
    days, seconds = td.days, td.seconds
    hours = seconds // (60 * 60)
    seconds -= hours * (60 * 60)
    minutes = seconds // 60
    seconds -= minutes * 60
    return f"{days}-{hours:02}:{minutes:02}:{seconds:02}"


_TD_RE = re.compile(r"(\d+)-(\d+):(\d+):(\d+)")


def timedelta_parse(td_str: str) -> timedelta:
    """
    Parse a string in standard form as a time delta.
    """
    if not (m := _TD_RE.fullmatch(td_str)):
        raise ValueError("not a supported timedelta form")
    days, hours, mins, secs = map(int, m.groups())
    return timedelta(days=days, hours=hours, minutes=mins, seconds=secs)


def open_text_resource(package: ModuleType | str, resource: str) -> IO[str]:
    """
    Open a file in a package.
    """
    try:
        return resources.files(package).joinpath(resource).open("r")
    except AttributeError:
        # < python 3.9; `resource.open_text` deprecated since 3.11
        return resources.open_text(package, resource)


def get_file_context(
    package: ModuleType | str, src: str | None = None
) -> AbstractContextManager[Path]:
    """
    Find a file or directory in a package.
    """
    try:
        files = resources.files(package)
        return resources.as_file(files.joinpath(src) if src else files)
        # raises ModuleNotFoundError
    except AttributeError:
        # < python 3.9
        return resources.path(package, src or "")


@contextlib.contextmanager
def redirect_std_to_file(
    file,
    mode: Literal["w", "a"] = "a",
    ignore: Callable[[BaseException], Literal[True] | int] | None = None,
) -> Iterator[None]:
    """Temporarily redirect both stdout and stderr to a file, and if an exception is
    raised, catch it, print the traceback to that file, and exit.

    File creation is deferred until an actual write is required.

    Parameters
    ----------
    ignore
        Callable to test if a given exception should be ignored. If an exception is
        not ignored, its traceback will be printed to `file` and the program will
        exit with exit code 1. The callable should accept one parameter, the
        exception, and should return True if that exception should be ignored, or
        an integer representing the exit code to exit the program with if that
        exception should not be ignored.  By default, no exceptions are ignored.

    """
    ignore = ignore or (lambda _: 1)
    with DeferredFileWriter(file, mode=mode) as fp:
        with contextlib.redirect_stdout(fp):
            with contextlib.redirect_stderr(fp):
                try:
                    yield
                except BaseException as exc:
                    ignore_ret = ignore(exc)
                    if ignore_ret is not True:
                        traceback.print_exc()
                        sys.exit(ignore_ret)


async def to_thread(func, /, *args, **kwargs):
    """Copied from https://github.com/python/cpython/blob/4b4227b907a262446b9d276c274feda2590a4e6e/Lib/asyncio/threads.py
    to support Python 3.8, which does not have `asyncio.to_thread`.

    Asynchronously run function *func* in a separate thread.

    Any *args and **kwargs supplied for this function are directly passed
    to *func*. Also, the current :class:`contextvars.Context` is propagated,
    allowing context variables from the main thread to be accessed in the
    separate thread.

    Return a coroutine that can be awaited to get the eventual result of *func*.
    """
    loop = events.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)
