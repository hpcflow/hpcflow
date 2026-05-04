"""
Models of data stores as resources.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
import copy
import json
from typing import Any, Callable, TYPE_CHECKING

from hpcflow.sdk.core.utils import get_md5_hash

if TYPE_CHECKING:
    from collections.abc import Mapping
    from logging import Logger
    from pathlib import Path
    import zarr  # type: ignore
    from fsspec import AbstractFileSystem  # type: ignore
    from ..app import BaseApp


class StoreResource(ABC):
    """Class to represent a persistent resource within which store data lives.

    A `PersistentStore` maps workflow data across zero or more store resources. Updates to
    persistent workflow data that live in the same store resource are performed together.

    Parameters
    ----------
    app: App
        The main application context.
    name:
        The store name.
    """

    def __init__(self, app: BaseApp, name: str) -> None:
        self._app = app
        self.name = name
        self.data: dict[str, Any] = {"read": None, "update": None}
        self.hash = None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"

    @property
    def logger(self) -> Logger:
        """
        The logger.
        """
        return self._app.persistence_logger

    @abstractmethod
    def _load(self) -> Any:
        pass

    @abstractmethod
    def _dump(self, data: dict | list):
        pass

    def open(self, action: str):
        """
        Open the store.

        Parameters
        ----------
        action:
            What we are opening the store for; typically either ``read`` or ``update``.
        """

        # TODO: some tests?

        if action == "read":
            # reuse "update" data if set, rather than re-loading from disk -- but copy,
            # so changes made in the "read" scope do not update!
            update_data = self.data["update"]
            rd_msg = " (using `update` data)" if update_data else ""
            self.logger.debug(f"{self!r}: opening to read{rd_msg}.")
            data = copy.deepcopy(update_data) if update_data else self._load()

        elif action == "update":
            # reuse "read" data if set, rather than re-loading from disk; this also means
            # updates will be reflected in the "read" data as soon as they are made:
            read_data = self.data["read"]
            upd_msg = " (using `read` data)" if read_data else ""
            self.logger.debug(f"{self!r}: opening to update{upd_msg}.")
            data = read_data or self._load()

        else:
            self._check_action(action)

        self.data[action] = data

        try:
            self.hash = get_md5_hash(data)  # type: ignore
        except Exception:
            pass

    def close(self, action: str):
        """
        Close the store for a particular action.

        Parameters
        ----------
        action:
            What we are closing the store for.
            Should match a previous call to :py:meth:`close`.
        """
        if action == "read":
            self.logger.debug(f"{self!r}: closing read.")
        elif action == "update":
            if self.hash:
                # check if it has changed:
                new_hash = get_md5_hash(self.data[action])
            if not self.hash or self.hash != new_hash:
                self.logger.debug(f"{self!r}: data (hash) changed.")
                self._dump(self.data[action])
            self.logger.debug(f"{self!r}: closing update.")
        else:
            self._check_action(action)

        # unset data for this action:
        self.data[action] = None

    def _check_action(self, action: str):
        if action not in self.data:
            raise ValueError(
                f"Action {action!r} not known for {self.__class__.__name__!r}"
            )


class JSONFileStoreResource(StoreResource):
    """
    For caching reads and writes to a JSON file.

    Parameters
    ----------
    app: App
        The main application context.
    name:
        The store name.
    filename:
        The name of the JSON file.
    path:
        The path to the directory containing the JSON file.
    fs:
        The filesystem that the JSON file resides within.
    """

    def __init__(
        self,
        app: BaseApp,
        name: str,
        filename: str,
        path: str | Path,
        fs: AbstractFileSystem,
    ):
        self.filename = filename
        self.path = path
        self.fs = fs
        super().__init__(app, name)

    @property
    def _full_path(self) -> str:
        return f"{self.path}/{self.filename}"

    def _load(self) -> Any:
        self.logger.debug(f"{self!r}: loading JSON from file.")
        with self.fs.open(self._full_path, mode="rt") as fp:
            return json.load(fp)

    def _dump(self, data: Mapping | list):
        self.logger.debug(f"{self!r}: dumping JSON to file")
        with self.fs.open(self._full_path, mode="wt") as fp:
            json.dump(data, fp, indent=2)


class ZarrAttrsStoreResource(StoreResource):
    """
    For caching reads and writes to Zarr attributes on groups and arrays.

    Parameters
    ----------
    app: App
        The main application context.
    name:
        The store name.
    open_call:
        How to actually perform an open on the underlying resource.
    """

    def __init__(self, app: BaseApp, name: str, open_call: Callable[..., zarr.Group]):
        self.open_call = open_call
        super().__init__(app, name)

    def _load(self) -> Any:
        self.logger.debug(f"{self!r}: loading Zarr attributes.")
        item = self.open_call(mode="r")
        return copy.deepcopy(item.attrs.asdict())

    def _dump(self, data: dict | list):
        self.logger.debug(f"{self!r}: dumping Zarr attributes.")
        item = self.open_call(mode="r+")
        item.attrs.put(data)
