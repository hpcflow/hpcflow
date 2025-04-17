"""
Configuration system class.
"""

from __future__ import annotations
import contextlib

from copy import deepcopy
import copy
import functools
import json
import logging
import os
import socket
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast, overload, TYPE_CHECKING
import fsspec  # type: ignore

from rich.console import Console, Group
from rich.table import Table
from rich.pretty import Pretty
from rich.panel import Panel
from rich import print as rich_print
from fsspec.registry import known_implementations as fsspec_protocols  # type: ignore
from fsspec.implementations.local import LocalFileSystem  # type: ignore
from hpcflow.sdk.core.utils import get_in_container, read_YAML_file, set_in_container

from hpcflow.sdk.core.validation import get_schema, Schema
from hpcflow.sdk.submission.shells import DEFAULT_SHELL_NAMES
from hpcflow.sdk.typing import PathLike

from hpcflow.sdk.config.callbacks import (
    callback_bool,
    callback_lowercase,
    callback_scheduler_set_up,
    callback_supported_schedulers,
    callback_supported_shells,
    callback_update_log_console_level,
    callback_unset_log_console_level,
    callback_vars,
    callback_file_paths,
    exists_in_schedulers,
    set_callback_file_paths,
    check_load_data_files,
    set_scheduler_invocation_match,
    callback_update_log_file_path,
    callback_update_log_file_level,
    callback_unset_log_file_level,
    callback_unset_log_file_path,
    callback_log_file_path,
)
from hpcflow.sdk.config.config_file import ConfigFile
from hpcflow.sdk.config.errors import (
    ConfigChangeInvalidJSONError,
    ConfigChangePopIndexError,
    ConfigChangeTypeInvalidError,
    ConfigChangeValidationError,
    ConfigItemAlreadyUnsetError,
    ConfigItemCallbackError,
    ConfigNonConfigurableError,
    ConfigReadOnlyError,
    ConfigUnknownItemError,
    ConfigUnknownOverrideError,
    ConfigValidationError,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence
    from typing import Any, Literal
    from .types import (
        ConfigDescriptor,
        ConfigMetadata,
        DefaultConfiguration,
        SchedulerConfigDescriptor,
        ShellConfigDescriptor,
        GetterCallback,
        SetterCallback,
        UnsetterCallback,
        T,
    )
    from ..app import BaseApp
    from ..core.types import AbstractFileSystem


logger = logging.getLogger(__name__)

_DEFAULT_SHELL = DEFAULT_SHELL_NAMES[os.name]
#: The default configuration descriptor.
DEFAULT_CONFIG: DefaultConfiguration = {
    "invocation": {"environment_setup": None, "match": {}},
    "config": {
        "machine": socket.gethostname(),
        "log_file_path": "logs/<<app_name>>_v<<app_version>>.log",
        "environment_sources": [],
        "task_schema_sources": [],
        "command_file_sources": [],
        "parameter_sources": [],
        "default_scheduler": "direct",
        "default_shell": _DEFAULT_SHELL,
        "schedulers": {"direct": {"defaults": {}}},
        "shells": {_DEFAULT_SHELL: {"defaults": {}}},
    },
}


@dataclass
class ConfigOptions:
    """Application-level options for configuration"""

    #: The default directory.
    default_directory: Path | str
    #: The environment variable containing the directory name.
    directory_env_var: str
    #: The default configuration.
    default_config: DefaultConfiguration = field(
        default_factory=lambda: deepcopy(DEFAULT_CONFIG)
    )
    #: Any extra schemas to apply.
    extra_schemas: Sequence[Schema] = field(default_factory=list)
    #: Default directory of known configurations.
    default_known_configs_dir: str | None = None
    _schemas: Sequence[Schema] = field(init=False)
    _configurable_keys: Sequence[str] = field(init=False)

    def __post_init__(self) -> None:
        self._schemas, self._configurable_keys = self.init_schemas()

    def init_schemas(self) -> tuple[Sequence[Schema], Sequence[str]]:
        """
        Get allowed configurable keys from config schemas.
        """
        cfg_schemas = [get_schema("config_schema.yaml"), *self.extra_schemas]
        cfg_keys: list[str] = []
        for cfg_schema in cfg_schemas:
            for rule in cfg_schema.rules:
                if not rule.path and rule.condition.callable.name == "allowed_keys":
                    cfg_keys.extend(rule.condition.callable.args)

        return (cfg_schemas, cfg_keys)

    def validate(
        self,
        data: T,
        logger: logging.Logger,
        metadata: ConfigMetadata | None = None,
        raise_with_metadata: bool = True,
    ) -> T:
        """Validate configuration items of the loaded invocation."""

        logger.debug("Validating configuration...")
        validated_data = data

        for cfg_schema in self._schemas:
            cfg_validated = cfg_schema.validate(validated_data)
            if not cfg_validated.is_valid:
                if not raise_with_metadata:
                    metadata = None
                raise ConfigValidationError(
                    message=cfg_validated.get_failures_string(),
                    meta_data=metadata,
                )
            validated_data = cfg_validated.cast_data

        logger.debug("Configuration is valid.")
        return validated_data


class Config:
    """
    Application configuration as defined in one or more config files.

    This class supports indexing into the collection of properties via Python dot notation.

    Notes
    -----
    On modifying/setting existing values, modifications are not automatically copied
    to the configuration file; use :meth:`save()` to save to the file. Items in `overrides`
    are not saved into the file.

    `schedulers` is used for specifying the available schedulers on this machine, and the
    default arguments that should be used when initialising the
    :py:class:`Scheduler` object.

    `shells` is used for specifying the default arguments that should be used when
    initialising the :py:class:`Shell` object.

    Parameters
    ----------
    app:
        The main hpcflow application instance.
    config_file:
        The configuration file that contains this config.
    options:
        Configuration options to be applied.
    logger:
        Where to log messages relating to configuration.
    config_key:
        The name of the configuration within the configuration file.
    uid: int
        User ID.
    callbacks: dict
        Overrides for the callback system.
    variables: dict[str, str]
        Variables to substitute when processing the configuration.

    Attributes
    ----------
    user_name: str
        User to submit as.
        Mapped to a field in the configuration file.
    user_orcid: str
        User's ORCID.
        Mapped to a field in the configuration file.
    user_affiliation: str
        User's institutional affiliation.
        Mapped to a field in the configuration file.
    linux_release_file: str
        Where to get the description of the Linux release version data.
        Mapped to a field in the configuration file.
    log_file_level: str
        At what level to do logging to the file.
        Mapped to a field in the configuration file.
    log_console_level: str
        At what level to do logging to the console. Usually coarser than to a file.
        Mapped to a field in the configuration file.
    demo_data_manifest_file: str
        Where the manifest describing the demo data is.
        Mapped to a field in the configuration file.
    """

    def __init__(
        self,
        app: BaseApp,
        config_file: ConfigFile,
        options: ConfigOptions,
        logger: logging.Logger,
        config_key: str | None,
        uid: str | None = None,
        callbacks: dict[str, tuple[GetterCallback, ...]] | None = None,
        variables: dict[str, str] | None = None,
        **overrides,
    ):
        self._app = app
        self._file = config_file
        self._options = options
        self._overrides = overrides
        self._logger = logger
        self._variables = variables or {}

        self._file._configs.append(self)

        self._config_key = self._file.select_invocation(
            configs=self._file.data["configs"],
            run_time_info=self._app.run_time_info.to_dict(),
            path=self._file.path,
            config_key=config_key,
        )

        # Callbacks are run on get:
        self._get_callbacks: dict[str, tuple[GetterCallback, ...]] = {
            "task_schema_sources": (callback_file_paths,),
            "environment_sources": (callback_file_paths,),
            "parameter_sources": (callback_file_paths,),
            "command_file_sources": (callback_file_paths,),
            "log_file_path": (callback_vars, callback_log_file_path),
            "telemetry": (callback_bool,),
            "schedulers": (callback_lowercase, callback_supported_schedulers),
            "shells": (callback_lowercase,),
            "default_scheduler": (callback_lowercase, exists_in_schedulers),
            "default_shell": (callback_lowercase, callback_supported_shells),
            "demo_data_manifest_file": (callback_file_paths,),
            **(callbacks or {}),
        }

        # Set callbacks are run on set:
        self._set_callbacks: dict[str, tuple[SetterCallback, ...]] = {
            "task_schema_sources": (set_callback_file_paths, check_load_data_files),
            "environment_sources": (set_callback_file_paths, check_load_data_files),
            "parameter_sources": (set_callback_file_paths, check_load_data_files),
            "command_file_sources": (set_callback_file_paths, check_load_data_files),
            "default_scheduler": (exists_in_schedulers, set_scheduler_invocation_match),
            "default_shell": (callback_supported_shells,),
            "schedulers": (callback_supported_schedulers, callback_scheduler_set_up),
            "log_file_path": (callback_update_log_file_path,),
            "log_file_level": (callback_update_log_file_level,),
            "log_console_level": (callback_update_log_console_level,),
            "demo_data_manifest_file": (set_callback_file_paths,),
        }

        self._unset_callbacks: dict[str, tuple[UnsetterCallback, ...]] = {
            "log_console_level": (callback_unset_log_console_level,),
            "log_file_level": (callback_unset_log_file_level,),
            "log_file_path": (callback_unset_log_file_path,),
        }

        self._configurable_keys = self._options._configurable_keys
        self._modified_keys: ConfigDescriptor = {}
        self._unset_keys: set[str] = set()

        if any((unknown := name) not in self._configurable_keys for name in overrides):
            raise ConfigUnknownOverrideError(name=unknown)

        host_uid, host_uid_file_path = self._get_user_id()

        metadata: ConfigMetadata = {
            "config_directory": self._file.directory,
            "config_file_name": self._file.path.name,
            "config_file_path": self._file.path,
            "config_file_contents": self._file.contents,
            "config_key": self._config_key,
            "config_schemas": self._options._schemas,
            "invoking_user_id": uid or host_uid,
            "host_user_id": host_uid,
            "host_user_id_file_path": host_uid_file_path,
        }
        self._meta_data = metadata

        # used within context manager `cached_config`:
        self._use_cache = False
        self._config_cache: dict[tuple[str, bool, bool, bool], Any] = {}

        # note: this must go at the end, after all instance attributes have been set!
        self._options.validate(
            data=self.get_all(include_overrides=True),
            logger=self._logger,
            metadata=metadata,
        )

    def __dir__(self) -> Iterator[str]:
        yield from super().__dir__()
        yield from self._all_keys

    @property
    def config_directory(self) -> Path:
        """
        The directory containing the configuration file.
        """
        return self._get("config_directory")

    @property
    def config_file_name(self) -> str:
        """
        The name of the configuration file.
        """
        return self._get("config_file_name")

    @property
    def config_file_path(self) -> Path:
        """
        The full path to the configuration file.
        """
        return self._get("config_file_path")

    @property
    def config_file_contents(self) -> str:
        """
        The cached contents of the configuration file.
        """
        return self._get("config_file_contents")

    @property
    def config_key(self) -> str:
        """
        The primary key to select the configuration within the configuration file.
        """
        return self._get("config_key")

    @property
    def config_schemas(self) -> Sequence[Schema]:
        """
        The schemas that apply to the configuration file.
        """
        return self._get("config_schemas")

    @property
    def invoking_user_id(self) -> str:
        """
        User ID that created the workflow.
        """
        return self._get("invoking_user_id")

    @property
    def host_user_id(self) -> str:
        """
        User ID as understood by the script.
        """
        return self._get("host_user_id")

    @property
    def host_user_id_file_path(self) -> Path:
        """
        Where user ID information is stored.
        """
        return self._get("host_user_id_file_path")

    @property
    def machine(self) -> str:
        """
        Machine to submit to.
        Mapped to a field in the configuration file.
        """
        return self._get("machine")

    @machine.setter
    def machine(self, value: str):
        self._set("machine", value)

    @property
    def log_file_path(self) -> str:
        """
        Where to log to.
        Mapped to a field in the configuration file.
        """
        return self._get("log_file_path")

    @log_file_path.setter
    def log_file_path(self, value: str):
        self._set("log_file_path", value)

    @property
    def environment_sources(self) -> Sequence[Path]:
        """
        Where to get execution environment descriptors.
        Mapped to a field in the configuration file.
        """
        return self._get("environment_sources")

    @environment_sources.setter
    def environment_sources(self, value: Sequence[Path]):
        self._set("environment_sources", value)

    @property
    def task_schema_sources(self) -> Sequence[str]:
        """
        Where to get task schemas.
        Mapped to a field in the configuration file.
        """
        return self._get("task_schema_sources")

    @task_schema_sources.setter
    def task_schema_sources(self, value: Sequence[str]):
        self._set("task_schema_sources", value)

    @property
    def command_file_sources(self) -> Sequence[str]:
        """
        Where to get command files.
        Mapped to a field in the configuration file.
        """
        return self._get("command_file_sources")

    @command_file_sources.setter
    def command_file_sources(self, value: Sequence[str]):
        self._set("command_file_sources", value)

    @property
    def parameter_sources(self) -> Sequence[str]:
        """
        Where to get parameter descriptors.
        Mapped to a field in the configuration file.
        """
        return self._get("parameter_sources")

    @parameter_sources.setter
    def parameter_sources(self, value: Sequence[str]):
        self._set("parameter_sources", value)

    @property
    def default_scheduler(self) -> str:
        """
        The name of the default scheduler.
        Mapped to a field in the configuration file.
        """
        return self._get("default_scheduler")

    @default_scheduler.setter
    def default_scheduler(self, value: str):
        self._set("default_scheduler", value)

    @property
    def default_shell(self) -> str:
        """
        The name of the default shell.
        Mapped to a field in the configuration file.
        """
        return self._get("default_shell")

    @default_shell.setter
    def default_shell(self, value: str):
        self._set("default_shell", value)

    @property
    def schedulers(self) -> Mapping[str, SchedulerConfigDescriptor]:
        """
        Settings for supported scheduler(s).
        Mapped to a field in the configuration file.
        """
        return self._get("schedulers")

    @schedulers.setter
    def schedulers(self, value: Mapping[str, SchedulerConfigDescriptor]):
        self._set("schedulers", value)

    @property
    def shells(self) -> Mapping[str, ShellConfigDescriptor]:
        """
        Settings for supported shell(s).
        Mapped to a field in the configuration file.
        """
        return self._get("shells")

    @shells.setter
    def shells(self, value: Mapping[str, ShellConfigDescriptor]):
        self._set("shells", value)

    @property
    def demo_data_dir(self) -> str | None:
        """
        Location of demo data.
        Mapped to a field in the configuration file.
        """
        return self._get("demo_data_dir")

    @demo_data_dir.setter
    def demo_data_dir(self, value: str | None):
        self._set("demo_data_dir", value)

    def __getattr__(self, name: str):
        if name.startswith("__"):
            raise AttributeError(f"Attribute not known: {name!r}.")
        return self._get(name)

    def __setattr__(self, name: str, value):
        if (
            "_configurable_keys" in self.__dict__
            and name in self.__dict__["_configurable_keys"]
        ):
            self._set(name, value)
        else:
            super().__setattr__(name, value)

    def _disable_callbacks(self, callbacks: Sequence[str]) -> tuple[
        dict[str, tuple[GetterCallback, ...]],
        dict[str, tuple[SetterCallback, ...]],
        dict[str, tuple[UnsetterCallback, ...]],
    ]:
        """
        Disable named get, set, and unset callbacks.

        Returns
        -------
        The original get and set callback dictionaries.
        """
        self._logger.info(f"disabling config callbacks: {callbacks!r}")
        get_callbacks_tmp: dict[str, tuple[GetterCallback, ...]] = {
            k: tuple(cb for cb in v if cb.__name__ not in callbacks)
            for k, v in self._get_callbacks.items()
        }
        set_callbacks_tmp: dict[str, tuple[SetterCallback, ...]] = {
            k: tuple(cb for cb in v if cb.__name__ not in callbacks)
            for k, v in self._set_callbacks.items()
        }
        unset_callbacks_tmp = {
            k: tuple(i for i in v if i.__name__ not in callbacks)
            for k, v in self._unset_callbacks.items()
        }
        get_callbacks = copy.deepcopy(self._get_callbacks)
        set_callbacks = copy.deepcopy(self._set_callbacks)
        unset_callbacks = copy.deepcopy(self._unset_callbacks)
        self._get_callbacks = get_callbacks_tmp
        self._set_callbacks = set_callbacks_tmp
        self._unset_callbacks = unset_callbacks_tmp
        return (get_callbacks, set_callbacks, unset_callbacks)

    @contextlib.contextmanager
    def _without_callbacks(self, *callbacks: str) -> Iterator[None]:
        """Context manager to temporarily exclude named get, set, and unset callbacks."""
        get_cb, set_cb, unset_cb = self._disable_callbacks(callbacks)
        try:
            yield
        finally:
            self._get_callbacks = get_cb
            self._set_callbacks = set_cb
            self._unset_callbacks = unset_cb

    def _validate(self) -> None:
        data = self.get_all(include_overrides=True)
        self._options.validate(
            data=data,
            logger=self._logger,
            metadata=self._meta_data,
            raise_with_metadata=True,
        )

    def _resolve_path(self, path: PathLike) -> PathLike:
        """Resolve a file path, but leave fsspec protocols alone."""
        if path is None:
            return None
        if any(str(path).startswith(i + ":") for i in fsspec_protocols):
            self._logger.debug(
                f"Not resolving path {path!r} because it looks like an `fsspec` URL."
            )
            return path
        real_path = Path(path).expanduser()
        if real_path.is_absolute():
            return real_path
        return self._meta_data["config_directory"].joinpath(real_path)

    def register_config_get_callback(
        self, name: str
    ) -> Callable[[GetterCallback], GetterCallback]:
        """
        Decorator to register a function as a configuration callback for a specified
        configuration item name, to be invoked on `get` of the item.
        """

        def decorator(func: GetterCallback) -> GetterCallback:
            if name in self._get_callbacks:
                self._get_callbacks[name] = self._get_callbacks[name] + (func,)
            else:
                self._get_callbacks[name] = (func,)

            @functools.wraps(func)
            def wrap(config: Config, value: T) -> T:
                return func(config, value)

            return wrap

        return decorator

    def register_config_set_callback(
        self, name: str
    ) -> Callable[[SetterCallback], SetterCallback]:
        """
        Decorator to register a function as a configuration callback for a specified
        configuration item name, to be invoked on `set` of the item.
        """

        def decorator(func: SetterCallback) -> SetterCallback:
            if name in self._set_callbacks:
                self._set_callbacks[name] = self._set_callbacks[name] + (func,)
            else:
                self._set_callbacks[name] = (func,)

            @functools.wraps(func)
            def wrap(config: Config, value: T) -> Any:
                return func(config, value)

            return wrap

        return decorator

    @property
    def _all_keys(self) -> list[str]:
        return [*self._configurable_keys, *self._meta_data]

    @overload
    def get_all(
        self, *, include_overrides: bool = True, as_str: Literal[True]
    ) -> Mapping[str, str]: ...

    @overload
    def get_all(
        self, *, include_overrides: bool = True, as_str: Literal[False] = False
    ) -> Mapping[str, Any]: ...

    def get_all(
        self, *, include_overrides: bool = True, as_str: bool = False
    ) -> Mapping[str, Any]:
        """Get all configurable items."""
        items: dict[str, Any] = {}
        for key in self._configurable_keys:
            if key in self._unset_keys:
                continue
            try:
                if as_str:
                    items[key] = self._get(
                        name=key,
                        include_overrides=include_overrides,
                        raise_on_missing=True,
                        as_str=True,
                    )
                else:
                    items[key] = self._get(
                        name=key,
                        include_overrides=include_overrides,
                        raise_on_missing=True,
                    )
            except ValueError:
                continue
        return items

    def _show(self, config: bool = True, metadata: bool = False):
        group_args: list[Panel] = []
        if metadata:
            tab = Table(show_header=False, box=None)
            tab.add_column()
            tab.add_column()
            for k, v in self._meta_data.items():
                if k == "config_file_contents":
                    continue
                tab.add_row(k, Pretty(v))
            group_args.append(Panel(tab, title="Config metadata"))

        if config:
            tab = Table(show_header=False, box=None)
            tab.add_column()
            tab.add_column()
            for k, v in self.get_all().items():
                tab.add_row(k, Pretty(v))
            group_args.append(Panel(tab, title=f"Config {self._config_key!r}"))

        rich_print(Group(*group_args))

    def _get_callback_value(self, name: str, value):
        if name in self._get_callbacks and value is not None:
            for cb in self._get_callbacks.get(name, ()):
                self._logger.debug(
                    f"Invoking `config.get` callback ({cb.__name__!r}) for item {name!r}={value!r}"
                )
                try:
                    value = cb(self, value)
                except Exception as err:
                    raise ConfigItemCallbackError(name, cb, err) from None
        return value

    @overload
    def _get(
        self,
        name: str,
        *,
        include_overrides=True,
        raise_on_missing=False,
        as_str: Literal[False] = False,
        callback=True,
        default_value=None,
    ) -> Any: ...

    @overload
    def _get(
        self,
        name: str,
        *,
        include_overrides=True,
        raise_on_missing=False,
        as_str: Literal[True],
        callback=True,
        default_value=None,
    ) -> list[str] | str: ...

    def _get(
        self,
        name: str,
        *,
        include_overrides=True,
        raise_on_missing=False,
        as_str=False,
        callback=True,
        default_value=None,
    ):
        """Get a configuration item."""

        if self._use_cache:
            # note: we default_value is not necessarily hashable, so we can't cache on it!
            key = (
                name,
                include_overrides,
                raise_on_missing,
                as_str,
            )
            if key in self._config_cache:
                return self._config_cache[key]

        if name not in self._all_keys:
            raise ConfigUnknownItemError(name=name)

        elif name in self._meta_data:
            val = cast("dict", self._meta_data)[name]

        elif include_overrides and name in self._overrides:
            val = self._overrides[name]

        elif name in self._unset_keys:
            if raise_on_missing:
                raise ValueError("Not set.")
            val = None
            if default_value:
                val = default_value

        elif name in self._modified_keys:
            val = cast("dict", self._modified_keys)[name]

        elif name in self._configurable_keys:
            val = self._file.get_config_item(
                config_key=self._config_key,
                name=name,
                raise_on_missing=raise_on_missing,
                default_value=default_value,
            )

        if callback:
            val = self._get_callback_value(name, val)

        if as_str:
            if isinstance(val, (list, tuple, set)):
                val = [str(i) for i in val]
            else:
                val = str(val)

        if self._use_cache:
            self._config_cache[key] = val

        return val

    def _parse_JSON(self, name: str, value: str) -> Any:
        try:
            return json.loads(value)
        except json.decoder.JSONDecodeError as err:
            raise ConfigChangeInvalidJSONError(name=name, json_str=value, err=err)

    @overload
    def _set(
        self, name: str, value: str, *, is_json: Literal[True], callback=True, quiet=False
    ) -> None: ...

    @overload
    def _set(
        self,
        name: str,
        value: Any,
        *,
        is_json: Literal[False] = False,
        callback=True,
        quiet=False,
    ) -> None: ...

    def _set(
        self, name: str, value, *, is_json=False, callback=True, quiet=False
    ) -> None:
        """
        Set a configuration item.
        """
        if self._use_cache:
            raise ConfigReadOnlyError()

        if name not in self._configurable_keys:
            raise ConfigNonConfigurableError(name=name)
        if is_json:
            value = self._parse_JSON(name, cast("str", value))
        current_val = self._get(name)
        callback_val = self._get_callback_value(name, value)
        file_val = self._get_callback_value(
            name, self._file.get_config_item(self._config_key, name)
        )

        if callback_val != current_val:
            was_in_modified = False
            was_in_unset = False
            prev_modified_val = None
            modified_updated = False
            mk = cast("dict", self._modified_keys)

            if name in self._modified_keys:
                was_in_modified = True
                prev_modified_val = mk[name]

            if name in self._unset_keys:
                was_in_unset = True
                self._unset_keys.remove(name)

            if callback_val != file_val:
                mk[name] = value
                modified_updated = True

            try:
                self._validate()

                if callback:
                    for cb in self._set_callbacks.get(name, ()):
                        self._logger.debug(
                            f"Invoking `config.set` callback for item {name!r}: {cb.__name__!r}"
                        )
                        cb(self, callback_val)

            except ConfigValidationError as err:
                # revert:
                if modified_updated:
                    if was_in_modified:
                        mk[name] = prev_modified_val
                    else:
                        del mk[name]
                if was_in_unset:
                    self._unset_keys.add(name)

                raise ConfigChangeValidationError(name, validation_err=err) from None

            self._logger.debug(
                f"Successfully set config item {name!r} to {callback_val!r}."
            )
        elif not quiet:
            print(f"value is already: {callback_val!r}")

    @overload
    def set(
        self,
        path: str,
        value: Any,
        *,
        is_json: Literal[False] = False,
        quiet: bool = False,
    ) -> None: ...

    @overload
    def set(
        self, path: str, value: str, *, is_json: Literal[True], quiet: bool = False
    ) -> None: ...

    def set(
        self, path: str, value: Any, *, is_json: bool = False, quiet: bool = False
    ) -> None:
        """
        Set the value of a configuration item.

        Parameters
        ----------
        path:
            Which configuration item to set.
        value:
            What to set it to.
        """
        self._logger.debug(f"Attempting to set config item {path!r} to {value!r}.")

        if is_json:
            value = self._parse_JSON(path, value)

        name, *path_suffix = path.split(".")
        root = deepcopy(self._get(name, callback=False))
        if path_suffix:
            if root is None:
                root = {}
                self.set(path=name, value={}, quiet=True)
            set_in_container(
                root,
                path=path_suffix,
                value=value,
                ensure_path=True,
                cast_indices=True,
            )
        else:
            root = value
        self._set(name, root, quiet=quiet)

    def unset(self, name: str, callback: bool = True) -> None:
        """
        Unset the value of a configuration item.

        Parameters
        ----------
        name:
            The name of the configuration item.

        Notes
        -----
            Only top level configuration items may be unset.
        """
        if name not in self._configurable_keys:
            raise ConfigNonConfigurableError(name=name)
        if name in self._unset_keys or not self._file.is_item_set(self._config_key, name):
            raise ConfigItemAlreadyUnsetError(name=name)

        self._unset_keys.add(name)
        try:
            self._validate()
            if callback:
                for cb in self._unset_callbacks.get(name, []):
                    self._logger.debug(
                        f"Invoking `config.unset` callback for item {name!r}: "
                        f"{cb.__name__!r}."
                    )
                    cb(self)
        except ConfigValidationError as err:
            self._unset_keys.remove(name)
            raise ConfigChangeValidationError(name, validation_err=err) from None

    @overload
    def get(
        self,
        path: str,
        *,
        callback: bool = True,
        copy: bool = False,
        ret_root_and_parts: Literal[False] = False,
        default: Any | None = None,
    ) -> Any: ...

    @overload
    def get(
        self,
        path: str,
        *,
        callback: bool = True,
        copy: bool = False,
        ret_root_and_parts: Literal[True],
        default: Any | None = None,
    ) -> tuple[Any, Any, list[str]]: ...

    def get(
        self,
        path: str,
        *,
        callback: bool = True,
        copy: bool = False,
        ret_root_and_parts: bool = False,
        default: Any | None = None,
    ) -> Any:
        """
        Get the value of a configuration item.

        Parameters
        ----------
        path:
            The name of or path to the configuration item.
        """
        name, *suffix = parts = path.split(".")
        root = deepcopy(self._get(name, callback=callback))
        try:
            out = get_in_container(root, suffix, cast_indices=True)
        except KeyError:
            out = default
        if copy:
            out = deepcopy(out)
        if not ret_root_and_parts:
            return out
        return out, root, parts

    def append(self, path: str, value, *, is_json: bool = False) -> None:
        """
        Append a value to a list-like configuration item.

        Parameters
        ----------
        path: str
            The name of or path to the configuration item.
        value:
            The value to append.
        """
        if is_json:
            value = self._parse_JSON(path, value)

        existing, root, parts = self.get(
            path,
            ret_root_and_parts=True,
            callback=False,
            default=[],
        )

        try:
            new = existing + [value]
        except TypeError:
            raise ConfigChangeTypeInvalidError(path, typ=type(existing)) from None

        if parts[1:]:
            set_in_container(
                root,
                path=parts[1:],
                value=new,
                ensure_path=True,
                cast_indices=True,
            )
        else:
            root = new
        self._set(parts[0], root)

    def prepend(self, path: str, value, *, is_json: bool = False) -> None:
        """
        Prepend a value to a list-like configuration item.

        Parameters
        ----------
        path: str
            The name of or path to the configuration item.
        value:
            The value to prepend.
        """
        if is_json:
            value = self._parse_JSON(path, value)

        existing, root, parts = self.get(
            path, ret_root_and_parts=True, callback=False, default=[]
        )

        try:
            new = [value] + existing
        except TypeError:
            raise ConfigChangeTypeInvalidError(path, typ=type(existing)) from None

        if parts[1:]:
            set_in_container(
                root,
                path=parts[1:],
                value=new,
                ensure_path=True,
                cast_indices=True,
            )
        else:
            root = new
        self._set(parts[0], root)

    def pop(self, path: str, index) -> None:
        """
        Remove a value from a specified index of a list-like configuration item.

        Parameters
        ----------
        path: str
            The name of or path to the configuration item.
        index: int
            Where to remove the value from. 0 for the first item, -1 for the last.
        """
        existing, root, parts = self.get(
            path,
            ret_root_and_parts=True,
            callback=False,
            default=[],
        )
        new = deepcopy(existing)
        try:
            new.pop(index)
        except AttributeError:
            raise ConfigChangeTypeInvalidError(path, typ=type(existing)) from None
        except IndexError:
            raise ConfigChangePopIndexError(
                path, length=len(existing), index=index
            ) from None

        if parts[1:]:
            set_in_container(
                root,
                path=parts[1:],
                value=new,
                ensure_path=True,
                cast_indices=True,
            )
        else:
            root = new
        self._set(parts[0], root)

    def update(self, path: str, value, *, is_json: bool = False) -> None:
        """
        Update a map-like configuration item.

        Parameters
        ----------
        path: str
            A dot-delimited string of the nested path to update.
        value: dict
            A dictionary to merge in.
        """
        if is_json:
            value = self._parse_JSON(path, value)

        val_mod, root, parts = self.get(
            path,
            copy=True,
            ret_root_and_parts=True,
            callback=False,
            default={},
        )

        try:
            val_mod.update(value)
        except TypeError:
            raise ConfigChangeTypeInvalidError(path, typ=type(val_mod)) from None

        if parts[1:]:
            set_in_container(
                root,
                path=parts[1:],
                value=val_mod,
                ensure_path=True,
                cast_indices=True,
            )
        else:
            root = val_mod
        self._set(parts[0], root)

    def save(self) -> None:
        """Save any modified/unset configuration items into the file."""
        if not self._modified_keys and not self._unset_keys:
            print("No modifications to save!")
        else:
            self._file.save()

    def get_configurable(self) -> Sequence[str]:
        """Get a list of all configurable keys."""
        return self._configurable_keys

    def _get_user_id(self) -> tuple[str, Path]:
        """
        Retrieve (and set if non-existent) a unique user ID that is independent of the
        config directory.
        """

        uid_file_path = self._app.user_data_dir.joinpath("user_id.txt")
        if not uid_file_path.exists():
            uid = str(uuid.uuid4())
            with uid_file_path.open("wt") as fh:
                fh.write(uid)
        else:
            with uid_file_path.open("rt") as fh:
                uid = fh.read().strip()

        return uid, uid_file_path

    def reset(self) -> None:
        """Reset to the default configuration."""
        self._logger.info("Resetting config file to defaults.")
        self._app.reset_config()

    def add_scheduler(self, scheduler: str, **defaults) -> None:
        """
        Add a scheduler.
        """
        if scheduler in self.get("schedulers"):
            print(f"Scheduler {scheduler!r} already exists.")
            return
        self.update(f"schedulers.{scheduler}.defaults", defaults)

    def add_shell(self, shell: str, **defaults) -> None:
        """
        Add a shell.
        """
        if shell in self.get("shells"):
            return
        if shell.lower() == "wsl":
            # check direct_posix scheduler is added:
            self.add_scheduler("direct_posix")
        self.update(f"shells.{shell}.defaults", defaults)

    def add_shell_WSL(self, **defaults) -> None:
        """
        Add shell with WSL prefix.
        """
        if "WSL_executable" not in defaults:
            defaults["WSL_executable"] = "wsl.exe"
        self.add_shell("wsl", **defaults)

    def import_from_file(
        self, file_path: Path | str, *, rename=True, make_new=False
    ) -> None:
        """
        Import config items from a (remote or local) YAML file. Existing config items
        of the same names will be overwritten.

        Parameters
        ----------
        file_path:
            Local or remote path to a config import YAML file which may have top-level
            keys "invocation" and "config".
        rename:
            If True, the current config will be renamed to the stem of the file specified
            in `file_path`. Ignored if `make_new` is True.
        make_new:
            If True, add the config items as a new config, rather than modifying the
            current config. The name of the new config will be the stem of the file
            specified in `file_path`.
        """
        self._logger.debug(f"import from file: {file_path!r}")

        console = Console()
        with console.status(f"Importing config from file {file_path!r}...") as status:
            file_dat: DefaultConfiguration = read_YAML_file(file_path)
            if rename or make_new:
                file_stem = Path(file_path).stem
                name = file_stem
            else:
                name = self._config_key

            obj = self  # `Config` object to update
            if make_new:
                status.update("Adding a new config...")
                # add a new default config:
                self._file.add_default_config(
                    name=file_stem,
                    config_options=self._options,
                )

                # load it:
                new_config_obj = Config(
                    app=self._app,
                    config_file=self._file,
                    options=self._options,
                    config_key=file_stem,
                    logger=self._logger,
                    variables=self._variables,
                )
                obj = new_config_obj

            elif rename:
                if self._config_key != file_stem:
                    self._file.rename_config_key(
                        config_key=self._config_key,
                        new_config_key=file_stem,
                    )

            new_invoc = file_dat.get("invocation")
            new_config = file_dat.get("config", {})

            if new_invoc is not None:
                status.update("Updating invocation details...")
                config_key = file_stem if (make_new or rename) else self._config_key
                obj._file.update_invocation(
                    config_key=config_key,
                    environment_setup=new_invoc.get("environment_setup"),
                    match=new_invoc.get("match", {}),
                )

            # sort in reverse so "schedulers" and "shells" are set before
            # "default_scheduler" and "default_shell" which might reference the former:
            for k, v in sorted(new_config.items(), reverse=True):
                status.update(f"Updating configurable item {k!r}")
                obj.set(k, value=v, quiet=True)

            obj.save()

        print(f"Config {name!r} updated.")

    def init(self, known_name: str, path: str | None = None) -> None:
        """Configure from a known importable config."""
        if not path:
            if not (path := self._options.default_known_configs_dir):
                raise ValueError("Specify an `path` to search for known config files.")
        elif path == ".":
            path = str(Path(path).resolve())

        self._logger.debug(f"init with `path` = {path!r}")

        fs: AbstractFileSystem = fsspec.open(path).fs
        local_path = f"{path}/" if isinstance(fs, LocalFileSystem) else ""
        files = fs.glob(f"{local_path}*.yaml") + fs.glob(f"{local_path}*.yml")
        self._logger.debug(f"All YAML files found in file-system {fs!r}: {files}")

        if not (files := [i for i in files if Path(i).stem.startswith(known_name)]):
            print(f"No configuration-import files found matching name {known_name!r}.")
            return

        print(f"Found configuration-import files: {files!r}")
        for file_i in files:
            self.import_from_file(file_path=file_i, make_new=True)

        print("imports complete")
        # if current config is named "default", rename machine to DEFAULT_CONFIG:
        if self._config_key == "default":
            self.set("machine", "DEFAULT_MACHINE")
            self.save()

    def set_github_demo_data_dir(self, sha: str) -> None:
        """
        Set the `demo_data_dir` item, to an fsspec Github URL.

        We use this (via the CLI) when testing the frozen app on Github, because, by
        default, the SHA is set to the current version tag, which might not include recent
        changes to the demo data.
        """
        assert self._app.demo_data_dir is not None
        self.set(
            "demo_data_dir",
            self._app._get_github_url(
                sha=sha, path=self._app.demo_data_dir.replace(".", "/")
            ),
        )

    @contextlib.contextmanager
    def cached_config(self) -> Iterator[None]:
        try:
            self._use_cache = True
            yield
        finally:
            self._use_cache = False
            self._config_cache = {}  # reset the cache

    def _is_set(self, name: str) -> bool:
        """Check if a (non-metadata) config item is set."""
        if name in self._unset_keys:
            return False
        elif name in self._modified_keys:
            return True
        else:
            return self._file.is_item_set(self._config_key, name)

    @contextlib.contextmanager
    def _with_updates(self, updates: dict[str, Any]) -> Iterator[None]:
        # need to run callbacks for unsetting?
        prev_unset = copy.deepcopy(self._unset_keys)
        prev_modified = copy.deepcopy(self._modified_keys)
        to_unset = []
        try:
            for k, v in updates.items():
                if not self._is_set(k):
                    to_unset.append(k)
                self.set(k, v)
            yield
        finally:
            self._unset_keys = prev_unset
            self._modified_keys = prev_modified
