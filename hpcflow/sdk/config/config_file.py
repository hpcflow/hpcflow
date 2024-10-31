"""
Configuration file adapter.
"""

from __future__ import annotations

import copy
import fnmatch
import io
import logging
import os
from pathlib import Path
import random
import string
from typing import cast, TYPE_CHECKING

from ruamel.yaml import YAML

from hpcflow.sdk.core.validation import Schema, get_schema

from hpcflow.sdk.config.errors import (
    ConfigChangeFileUpdateError,
    ConfigDefaultValidationError,
    ConfigFileInvocationIncompatibleError,
    ConfigFileInvocationUnknownMatchKey,
    ConfigFileValidationError,
    ConfigInvocationKeyNotFoundError,
    ConfigValidationError,
)

if TYPE_CHECKING:
    from typing import Any
    from ..typing import PathLike
    from .config import Config, ConfigOptions
    from .types import ConfigDict, DefaultConfiguration, InvocationDescriptor


class ConfigFile:
    """
    Configuration file.

    Parameters
    ----------
    directory:
        The directory containing the configuration file.
    logger:
        Where to log messages.
    config_options:
        Configuration options.
    """

    def __init__(self, directory, logger: logging.Logger, config_options: ConfigOptions):
        #: Where to log messages.
        self.logger = logger
        #: The directory containing the configuration file.
        self.directory = self._resolve_config_dir(
            config_opt=config_options,
            logger=self.logger,
            directory=directory,
        )

        self._configs: list[Config] = []

        # set by _load_file_data:
        self.__path: Path | None = None
        self.__contents: str | None = None
        self.__data: ConfigDict | None = None
        self.__data_rt: ConfigDict | None = None

        self._load_file_data(config_options)
        self.file_schema = self._validate(self.__data)

    @property
    def data(self) -> ConfigDict:
        """
        The parsed contents of the config file.
        """
        d = self.__data
        assert d is not None
        return d

    @property
    def data_rt(self) -> ConfigDict:
        """
        The parsed contents of the config file where the alternate parser was used.
        """
        drt = self.__data_rt
        assert drt is not None
        return drt

    @property
    def path(self) -> Path:
        """
        The path to the config file.
        """
        p = self.__path
        assert p is not None
        return p

    @property
    def contents(self) -> str:
        """
        The cached contents of the config file.
        """
        c = self.__contents
        assert c is not None
        return c

    @staticmethod
    def select_invocation(
        configs: dict[str, Any],
        run_time_info: dict[str, Any],
        path: PathLike,
        config_key: str | None = None,
    ) -> str:
        """Select a matching configuration for this invocation using run-time info."""
        if not config_key:
            all_matches = {}  # keys are config keys; values are lengths of match dict
            for c_name_i, c_dat_i in configs.items():
                # for a config to "match", each "match key" must match the relevant run
                # time info attribute. If a "match key" has multiple values, at least
                # one value must match the run time info attribute:
                for match_k, match_v in c_dat_i["invocation"]["match"].items():
                    # test for a matching glob pattern (where multiple may be specified):
                    if not isinstance(match_v, list):
                        match_v = [match_v]

                    try:
                        k_value = run_time_info[match_k]
                    except KeyError:
                        raise ConfigFileInvocationUnknownMatchKey(match_k)

                    if not any(
                        fnmatch.filter(names=[k_value], pat=match_i)
                        for match_i in match_v
                    ):
                        break
                else:
                    all_matches[c_name_i] = len(c_dat_i["invocation"]["match"])

            if not all_matches:
                raise ConfigFileInvocationIncompatibleError(config_key)
            # for multiple matches select the more specific one:
            config_key = max(all_matches.items(), key=lambda x: x[1])[0]

        elif config_key not in configs:
            raise ConfigInvocationKeyNotFoundError(config_key, path, list(configs))

        return config_key

    def _validate(self, data: dict[str, Any] | None) -> Schema:
        file_schema = get_schema("config_file_schema.yaml")
        if not (file_validated := file_schema.validate(data)).is_valid:
            raise ConfigFileValidationError(file_validated.get_failures_string())
        return file_schema

    def get_invoc_data(self, config_key: str) -> DefaultConfiguration:
        """
        Get the invocation data for the given configuration.

        Parameters
        ----------
        config_key: str
            The name of the configuration within the configuration file.
        """
        return self.data["configs"][config_key]

    def get_invocation(self, config_key: str) -> InvocationDescriptor:
        """
        Get the invocation for the given configuration.

        Parameters
        ----------
        config_key: str
            The name of the configuration within the configuration file.
        """
        return self.get_invoc_data(config_key)["invocation"]

    def save(self) -> None:
        """
        Write the (modified) configuration to the configuration file.
        """
        new_data = copy.deepcopy(self.data)
        new_data_rt = copy.deepcopy(self.data_rt)
        new_contents = ""

        modified_names: list[str] = []
        for config in self._configs:
            modified_names.extend(config._modified_keys)
            modified_names.extend(config._unset_keys)

            new_data_config = new_data["configs"][config._config_key]["config"]
            new_data_rt_config = new_data_rt["configs"][config._config_key]["config"]
            new_data_config.update(config._modified_keys)
            new_data_rt_config.update(config._modified_keys)

            for k in config._unset_keys:
                del cast("dict", new_data_config)[k]
                del cast("dict", new_data_rt_config)[k]

        try:
            new_contents = self._dump(new_data_rt)
        except Exception as err:
            raise ConfigChangeFileUpdateError(names=modified_names, err=err) from None

        self.__data = new_data
        self.__data_rt = new_data_rt
        self.__contents = new_contents

        for config in self._configs:
            config._unset_keys = set()
            config._modified_keys = {}

    @staticmethod
    def _resolve_config_dir(
        config_opt: ConfigOptions,
        logger: logging.Logger,
        directory: str | Path | None = None,
    ) -> Path:
        """Find the directory in which to locate the configuration file.

        If no configuration directory is specified, look first for an environment variable
        (given by config option `directory_env_var`), and then in the default
        configuration directory (given by config option `default_directory`).

        The configuration directory will be created if it does not exist.

        Parameters
        ----------
        directory
            Directory in which to find the configuration file. Optional.

        Returns
        -------
        directory : Path
            Absolute path to the configuration directory.

        """

        if not directory:
            path = Path(
                os.getenv(config_opt.directory_env_var, config_opt.default_directory)
            ).expanduser()
        else:
            path = Path(directory)

        if not path.is_dir():
            logger.debug(
                f"Configuration directory does not exist. Generating here: {str(path)!r}."
            )
            path.mkdir()
        else:
            logger.debug(f"Using configuration directory: {str(path)!r}.")

        return path.resolve()

    def _dump(self, config_data: ConfigDict, path: Path | None = None) -> str:
        """Dump the specified config data to the specified config file path.

        Parameters
        ----------
        config_data
            New configuration file data that will be dumped using the "round-trip" dumper.
        path
            Path to dump the config file data to. If not specified the `path` instance
            attribute will be used. If the file already exists, an "atomic-ish" overwrite
            will be used, where we firstly create a temporary file, which then replaces
            the existing file.

        Returns
        -------
        new_contents
            String contents of the new file.

        """

        if path is None:
            path = self.path

        yaml = YAML(typ="rt")
        if path.exists():
            # write a new temporary config file
            cfg_tmp_file = path.with_suffix(path.suffix + ".tmp")
            self.logger.debug(f"Creating temporary config file: {cfg_tmp_file!r}.")
            with cfg_tmp_file.open("wt", newline="\n") as fh:
                yaml.dump(config_data, fh)

            # atomic rename, overwriting original:
            self.logger.debug("Replacing original config file with temporary file.")
            os.replace(src=cfg_tmp_file, dst=path)

        else:
            with path.open("w", newline="\n") as handle:
                yaml.dump(config_data, handle)

        buff = io.BytesIO()
        yaml.dump(config_data, buff)
        new_contents = str(buff.getvalue())

        return new_contents

    def add_default_config(
        self, config_options: ConfigOptions, name: str | None = None
    ) -> str:
        """Add a new default config to the config file, and create the file if it doesn't
        exist."""

        if self.path.exists():
            is_new_file = False
            new_data: ConfigDict = copy.deepcopy(self.data)
            new_data_rt: ConfigDict = copy.deepcopy(self.data_rt)
        else:
            is_new_file = True
            new_data = {"configs": {}}
            new_data_rt = {"configs": {}}

        if not name:
            name = "".join(random.choices(string.ascii_letters, k=6))

        def_config = copy.deepcopy(config_options.default_config)
        new_config = {name: def_config}

        new_data["configs"].update(new_config)
        new_data_rt["configs"].update(new_config)

        try:
            if is_new_file:
                # validate default config "file" structure:
                self._validate(data=new_data)

            # validate default config items for the newly added default config:
            config_options.validate(
                data=def_config["config"],
                logger=self.logger,
                raise_with_metadata=False,
            )

        except (ConfigFileValidationError, ConfigValidationError) as err:
            raise ConfigDefaultValidationError(err) from None

        self.__data_rt = new_data_rt
        self.__data = new_data
        self.__contents = self._dump(new_data_rt)

        return name

    @staticmethod
    def get_config_file_path(directory: Path) -> Path:
        """
        Get the path to the configuration file.
        """
        # Try both ".yml" and ".yaml" extensions:
        path_yaml = directory.joinpath("config.yaml")
        if path_yaml.is_file():
            return path_yaml
        path_yml = directory.joinpath("config.yml")
        if path_yml.is_file():
            return path_yml
        return path_yaml

    def _load_file_data(self, config_options: ConfigOptions):
        """Load data from the configuration file (config.yaml or config.yml)."""

        self.__path = self.get_config_file_path(self.directory)
        if not self.path.is_file():
            self.logger.info(
                "No config.yaml found in the configuration directory. Generating "
                "a config.yaml file."
            )
            self.add_default_config(name="default", config_options=config_options)

        yaml = YAML(typ="safe")
        yaml_rt = YAML(typ="rt")
        with self.path.open() as handle:
            contents = handle.read()
            handle.seek(0)
            data = yaml.load(handle)
            handle.seek(0)
            data_rt = yaml_rt.load(handle)

        self.__contents = contents
        self.__data = data
        self.__data_rt = data_rt

    def get_config_item(
        self, config_key: str, name: str, *, raise_on_missing=False, default_value=None
    ) -> Any | None:
        """
        Get a configuration item.

        Parameters
        ----------
        config_key: str
            The name of the configuration within the configuration file.
        name: str
            The name of the configuration item.
        raise_on_missing: bool
            Whether to raise an error if the config item is absent.
        default_value:
            The default value to use when the config item is absent
            (and ``raise_on_missing`` is not specified).
        """
        cfg = self.get_invoc_data(config_key)["config"]
        if raise_on_missing and name not in cfg:
            raise ValueError(f"missing from file: {name!r}")
        return cfg.get(name, default_value)

    def is_item_set(self, config_key: str, name: str) -> bool:
        """
        Determine if a configuration item is set.

        Parameters
        ----------
        config_key: str
            The name of the configuration within the configuration file.
        name: str
            The name of the configuration item.
        """
        try:
            self.get_config_item(config_key, name, raise_on_missing=True)
            return True
        except ValueError:
            return False

    def rename_config_key(self, config_key: str, new_config_key: str) -> None:
        """
        Change the config key of the loaded config.

        Parameters
        ----------
        config_key: str
            The old name of the configuration within the configuration file.
        new_config_key: str
            The new name of the configuration.
        """

        new_data = copy.deepcopy(self.data)
        new_data_rt = copy.deepcopy(self.data_rt)

        new_data["configs"][new_config_key] = new_data["configs"].pop(config_key)
        new_data_rt["configs"][new_config_key] = new_data_rt["configs"].pop(config_key)

        for config in self._configs:
            if config._config_key == config_key:
                config._meta_data["config_key"] = new_config_key
                config._config_key = new_config_key

        self.__data_rt = new_data_rt
        self.__data = new_data
        self.__contents = self._dump(new_data_rt)

    def update_invocation(
        self,
        config_key: str,
        environment_setup: str | None = None,
        match: dict[str, str | list[str]] | None = None,
    ) -> None:
        """
        Modify the invocation parameters of the loaded config.

        Parameters
        ----------
        config_key:
            The name of the configuration within the configuration file.
        environment_setup:
            The new value of the ``environment_setup`` key.
        match:
            The new values to merge into the ``match`` key.
        """

        new_data = copy.deepcopy(self.data)
        new_data_rt = copy.deepcopy(self.data_rt)

        for dat in (new_data, new_data_rt):
            invoc = dat["configs"][config_key]["invocation"]
            if environment_setup:
                invoc["environment_setup"] = environment_setup
            if match:
                invoc["match"].update(match)

        self.__data_rt = new_data_rt
        self.__data = new_data
        self.__contents = self._dump(new_data_rt)
