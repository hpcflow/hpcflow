from abc import ABC, abstractmethod
import copy
import json
from pathlib import Path
from typing import Callable, Union

from hpcflow.sdk.core.utils import get_md5_hash


class StoreResource(ABC):
    """Class to represent a persistent resource within which store data lives.

    A `PersistentStore` maps workflow data across zero or more store resources. Updates to
    persistent workflow data that live in the same store resource are performed together.

    """

    def __init__(self, app, name: str) -> None:
        self.app = app
        self.name = name
        self.data = {"read": None, "update": None}
        self.hash = None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"

    @property
    def logger(self):
        return self.app.persistence_logger

    @abstractmethod
    def _load(self):
        pass

    @abstractmethod
    def _dump(self, data):
        pass

    def open(self, action):

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
            self.hash = get_md5_hash(data)
        except Exception:
            pass

    def close(self, action):
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
    """For caching reads and writes to a JSON file."""

    def __init__(self, app, name: str, filename: str, path: Union[str, Path], fs):
        self.filename = filename
        self.path = path
        self.fs = fs
        super().__init__(app, name)

    @property
    def _full_path(self):
        return f"{self.path}/{self.filename}"

    def _load(self):
        self.logger.debug(f"{self!r}: loading JSON from file.")
        with self.fs.open(self._full_path, mode="rt") as fp:
            return json.load(fp)

    def _dump(self, data):
        self.logger.debug(f"{self!r}: dumping JSON to file")
        with self.fs.open(self._full_path, mode="wt") as fp:
            json.dump(data, fp, indent=2)


class ZarrAttrsStoreResource(StoreResource):
    """For caching reads and writes to Zarr attributes on groups and arrays."""

    def __init__(self, app, name: str, open_call: Callable):
        self.open_call = open_call
        super().__init__(app, name)

    def _load(self):
        self.logger.debug(f"{self!r}: loading Zarr attributes.")
        item = self.open_call(mode="r")
        return copy.deepcopy(item.attrs.asdict())

    def _dump(self, data):
        self.logger.debug(f"{self!r}: dumping Zarr attributes.")
        item = self.open_call(mode="r+")
        item.attrs.put(data)
