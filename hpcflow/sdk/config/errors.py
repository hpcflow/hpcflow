"""
Miscellaneous configuration-related errors.
"""

from __future__ import annotations
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence, Iterable
    from .types import ConfigMetadata
    from ..typing import PathLike


class ConfigError(Exception):
    """
    Raised when a valid configuration can not be associated with the current
    invocation.
    """


class ConfigUnknownItemError(ConfigError):
    """
    Raised when the configuration contains an unknown item.
    """

    def __init__(self, name: str, message: str = ""):
        super().__init__(
            message
            or (
                f"Specified name {name!r} is not a valid meta-data or configurable "
                f"configuration item."
            )
        )


class ConfigUnknownOverrideError(ConfigError):
    """
    Raised when the configuration override contains an unknown item.
    """

    def __init__(self, name: str, message: str = ""):
        super().__init__(
            message
            or (
                f"Specified configuration override {name!r} is not a valid configurable item."
            )
        )


class ConfigNonConfigurableError(ConfigError):
    """
    Raised when the configuration contains an item that can't be configured.
    """

    def __init__(self, name: str | Iterable[str], message: str | None = None):
        if not message:
            if not isinstance(name, str):
                names_str = ", ".join(f"{i!r}" for i in name)
                msg = f"Specified names {names_str} are not configurable items."
            else:
                msg = f"Specified name {name!r} is not a configurable item."
        self.message = message or msg
        super().__init__(self.message)


class ConfigItemAlreadyUnsetError(ConfigError):
    """
    Raised when the configuration tries to unset an unset item.
    """

    def __init__(self, name: str, message: str = ""):
        super().__init__(message or f"Configuration item {name!r} is already not set.")


class ConfigFileValidationError(ConfigError):
    """
    Raised when the configuration file fails validation.
    """

    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class ConfigItemCallbackError(ConfigError):
    """
    Raised when a configuration callback errors.
    """

    def __init__(self, name: str, callback: Any, err: Any, message: str = ""):
        super().__init__(
            message
            or (
                f"Callback function {callback.__name__!r} for configuration item {name!r} "
                f"failed with exception: \n\n{str(err)}"
            )
        )


class ConfigFileInvocationIncompatibleError(ConfigError):
    """Raised when, given the run time information of the invocation, no compatible
    configuration can be found in the config file."""

    def __init__(self, message: str | None = ""):
        super().__init__(
            message or ("No config could be found that matches the current invocation.")
        )


class ConfigFileInvocationUnknownMatchKey(ConfigError):
    """
    Raised when the configuration contains an invalid match key.
    """

    def __init__(self, match_key: str, message: str = ""):
        self.match_key = match_key
        super().__init__(
            message
            or (
                f"Specified match key ({match_key!r}) is not a valid run time info "
                f"attribute."
            )
        )


class ConfigInvocationKeyNotFoundError(ConfigError):
    """Raised when a configuration invocation key is passed but it is not a valid key."""

    def __init__(
        self, invoc_key: str, file_path: PathLike, available_keys: Sequence[str]
    ):
        self.invoc_key = invoc_key
        self.file_path = file_path
        self.available_keys = available_keys
        super().__init__(
            f"Invocation key {self.invoc_key!r} does not exist in configuration file. "
            f"Available keys in configuration file {str(self.file_path)!r} are "
            f"{self.available_keys!r}."
        )


class ConfigValidationError(ConfigError):
    """Raised when the matching config data is invalid."""

    def __init__(self, message: str, meta_data: ConfigMetadata | None = None):
        self.meta_data = meta_data
        super().__init__(message + (f"config {self.meta_data}\n" if meta_data else ""))


class ConfigDefaultValidationError(ConfigError):
    """Raised when the specified default configuration in the `ConfigOptions` object is
    invalid."""

    def __init__(self, validation_err: Any, message: str = ""):
        super().__init__(
            message
            or (
                f"The default configuration specified in the `ConfigOptions` object is "
                f"invalid.\n\n{validation_err}"
            )
        )


class ConfigChangeInvalidError(ConfigError):
    """Raised when trying to set an invalid key in the Config."""

    def __init__(self, name: str, message: str = ""):
        super().__init__(
            message
            or (
                f"Cannot modify value for invalid config item {name!r}. Use the `config list`"
                f" sub-command to list all configurable items."
            )
        )


class ConfigChangeInvalidJSONError(ConfigError):
    """Raised when attempting to set a config key using an invalid JSON string."""

    def __init__(self, name: str, json_str: str, err: Any, message: str = ""):
        super().__init__(
            message
            or (
                f"The config file has not been modified. Invalid JSON string for config item "
                f"{name!r}. {json_str!r}\n\n{err!r}"
            )
        )


class ConfigChangeValidationError(ConfigError):
    """Raised when a change to the configurable data would invalidate the config."""

    def __init__(self, name: str, validation_err: Any, message: str = ""):
        super().__init__(
            message
            or (
                f"The configuration has not been modified. Requested modification to item "
                f"{name!r} would invalidate the config in the following way."
                f"\n\n{validation_err}"
            )
        )


class ConfigChangeFileUpdateError(ConfigError):
    """Raised when the updating of the config YAML file fails."""

    def __init__(self, names: Sequence[str], err, message: str = ""):
        super().__init__(
            message
            or (
                f"Failed to update the config file for modification of config items {names!r}."
                f"\n\n{err!r}"
            )
        )


class ConfigChangeTypeInvalidError(ConfigError):
    """Raised when trying to modify a config item using a list operation, when the config
    item is not a list."""

    def __init__(self, name: str, typ: type, message: str = ""):
        super().__init__(
            message
            or (
                f"The configuration has not been modified. The config item {name!r} has type "
                f"{typ!r} and so cannot be modified in that way."
            )
        )


class ConfigChangePopIndexError(ConfigError):
    """Raised when trying to pop an item from a config item with an invalid index."""

    def __init__(self, name: str, length: int, index: int, message: str = ""):
        super().__init__(
            message
            or (
                f"The configuration has not been modified. The config item {name!r} has length "
                f"{length!r} and so cannot be popped with index {index}."
            )
        )


class MissingTaskSchemaFileError(ConfigError):
    """Raised when a task schema file specified in the config file does not exist."""

    def __init__(self, file_name: str, err: Any, message: str = ""):
        super().__init__(
            message or (f"The task schema file {file_name!r} cannot be found. \n{err!s}")
        )


class MissingEnvironmentFileError(ConfigError):
    """Raised when an environment file specified in the config file does not exist."""

    def __init__(self, file_name: str, err: Any, message: str = ""):
        super().__init__(
            message or (f"The environment file {file_name!r} cannot be found. \n{err!s}")
        )


class ConfigReadOnlyError(ConfigError):
    pass


class UnknownMetaTaskConstitutiveSchema(ValueError):
    pass
