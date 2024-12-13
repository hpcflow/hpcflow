"""
Utilities for working with Zarr.
"""

from __future__ import annotations
from typing import Any
from typing_extensions import Self

import zarr  # type: ignore
import numpy as np

from hpcflow.sdk.core.utils import get_in_container, get_relative_path, set_in_container


#: The basic types that Zarr can handle directly with no special action.
PRIMITIVES: tuple[type, ...] = (
    int,
    float,
    str,
    type(None),
)

#: Maximum nesting depth for encoding.
MAX_DEPTH = 50


def zarr_encode(data, zarr_group: zarr.Group, is_pending_add: bool, is_set: bool):
    """
    Encode data into a zarr group.
    """

    encoded: list[dict] = []

    def encode(obj: Any, path: list) -> Any:
        if len(path) > MAX_DEPTH:
            raise RuntimeError("I'm in too deep!")

        if isinstance(obj, ZarrEncodable):
            return encode(obj.to_dict(), path)
        elif isinstance(obj, (list, tuple, set)):
            out = (encode(item, [*path, idx]) for idx, item in enumerate(obj))
            if isinstance(obj, tuple):
                return tuple(out)
            elif isinstance(obj, set):
                return set(out)
            else:
                return list(out)
        elif isinstance(obj, dict):
            return {
                dct_key: encode(dct_val, [*path, dct_key])
                for dct_key, dct_val in obj.items()
            }
        elif isinstance(obj, PRIMITIVES):
            return obj
        elif isinstance(obj, np.ndarray):
            new_name = str(max((int(i) + 1 for i in zarr_group.keys()), default=0))
            zarr_group.create_dataset(name=new_name, data=obj)
            encoded.append(
                {
                    "path": path,
                    "dataset": new_name,
                }
            )
            return None
        else:
            raise ValueError(f"unserializable type: {type(obj)}")

    zarr_group.attrs["data"] = encode(data, [])
    zarr_group.attrs["encoded"] = encoded
    zarr_group.attrs["is_set"] = is_set
    if is_pending_add:
        zarr_group.attrs["is_pending_add"] = is_pending_add


# TODO: need to separate so this func doesn't need matflow


def _zarr_encode_NEW(
    obj: Any,
    root_group: zarr.Group,
    arr_path: str,
) -> tuple[Any, list[list]]:
    """
    Save arbitrarily-nested Python-primitive, `ZarrEncodable` and numpy array objects into
    Zarr.

    Parameters
    ----------
    obj:
        Object to encode.
    root_group:
        Parent Zarr group into which new Zarr arrays will be added (at `arr_path`).
    arr_path:
        Path relative to `root_group` into which new Zarr arrays will be added.

    Returns
    -------
    data
        The encoded data.
    arr_lookup
        How to look up where to rebuild Numpy arrays.
    """

    arr_lookup: list[list] = []

    def encode(obj: Any, path: list) -> Any:
        if len(path) > MAX_DEPTH:
            raise RuntimeError("I'm in too deep!")

        if isinstance(obj, ZarrEncodable):
            return encode(obj.to_dict(), path)
        elif isinstance(obj, (list, tuple, set)):
            items = (encode(item, [*path, idx]) for idx, item in enumerate(obj))
            if isinstance(obj, tuple):
                return tuple(items)
            elif isinstance(obj, set):
                return set(items)
            else:
                return list(items)
        elif isinstance(obj, dict):
            return {key: encode(val, [*path, key]) for key, val in obj.items()}
        elif isinstance(obj, PRIMITIVES):
            return obj
        elif isinstance(obj, np.ndarray):
            # Might need to generate new group:
            param_arr_group = root_group.require_group(arr_path)
            new_idx = max((int(i) + 1 for i in param_arr_group.keys()), default=0)
            param_arr_group.create_dataset(name=f"arr_{new_idx}", data=obj)
            arr_lookup.append([path, new_idx])
            return None
        else:
            raise ValueError(f"unserializable type: {type(obj)}")

    return encode(obj, []), arr_lookup


def zarr_decode(
    param_data: None | dict,
    arr_group: zarr.Group,
    path: list | None = None,
    dataset_copy: bool = False,
):
    """
    Decode data from a zarr group.
    """
    if param_data is None:
        return None

    path = path or []

    data = get_in_container(param_data["data"], path)
    # data = copy.deepcopy(data)  # TODO: why did we copy?

    for arr_path, arr_idx in param_data["arr_lookup"]:
        try:
            rel_path = get_relative_path(arr_path, path)
        except ValueError:
            continue

        dataset = arr_group.get(f"arr_{arr_idx}")

        if dataset_copy:
            dataset = dataset[:]

        if rel_path:
            set_in_container(data, rel_path, dataset)
        else:
            data = dataset

    return data


class ZarrEncodable:
    """
    Base class of data that can be converted to and from zarr form.
    """

    _typ = None

    def to_dict(self) -> dict[str, Any]:
        """
        Convert this object to a dict.
        """
        if hasattr(self, "__dict__"):
            return dict(self.__dict__)
        elif hasattr(self, "__slots__"):
            return {k: getattr(self, k) for k in self.__slots__}
        else:
            # Should be unreachable
            return {}

    def to_zarr(self, zarr_group: zarr.Group):
        """
        Save this object into the given zarr group.
        """
        zarr_encode(self.to_dict(), zarr_group, is_pending_add=False, is_set=False)

    @classmethod
    def from_zarr(cls, zarr_group: zarr.Group, dataset_copy: bool = False) -> Self:
        """
        Read an instance of this class from the given zarr group.
        """
        # FIXME: Do the read of the data!
        param_data = None
        data = zarr_decode(param_data, zarr_group, dataset_copy=dataset_copy)
        return cls(**data)
