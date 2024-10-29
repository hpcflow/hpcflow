class ConfigError(Exception):
    """Raised when a valid configuration can not be associated with the current
    invocation."""

    pass


class ConfigUnknownItemError(ConfigError):
    def __init__(self, name, message=None):
        self.message = message or (
            f"Specified name {name!r} is not a valid meta-data or configurable "
            f"configuration item."
        )
        super().__init__(self.message)


class ConfigUnknownOverrideError(ConfigError):
    def __init__(self, name, message=None):
        self.message = message or (
            f"Specified configuration override {name!r} is not a valid configurable item."
        )
        super().__init__(self.message)


class ConfigNonConfigurableError(ConfigError):
    def __init__(self, name, message=None):
        if not message:
            if not isinstance(name, str):
                names_str = ", ".join(f"{i!r}" for i in name)
                msg = f"Specified names {names_str} are not configurable items."
            else:
                msg = f"Specified name {name!r} is not a configurable item."
        self.message = message or msg
        super().__init__(self.message)


class ConfigItemAlreadyUnsetError(ConfigError):
    def __init__(self, name, message=None):
        self.message = message or f"Configuration item {name!r} is already not set."
        super().__init__(self.message)


class ConfigFileValidationError(ConfigError):
    pass


class ConfigItemCallbackError(ConfigError):
    def __init__(self, name, callback, err, message=None):
        self.message = message or (
            f"Callback function {callback.__name__!r} for configuration item {name!r} "
            f"failed with exception: \n\n{str(err)}"
        )
        super().__init__(self.message)


class ConfigFileInvocationIncompatibleError(ConfigError):
    """Raised when, given the run time information of the invocation, no compatible
    configuration can be found in the config file."""

    def __init__(self, message=None):
        self.message = message or (
            "No config could be found that matches the current invocation."
        )
        super().__init__(self.message)


class ConfigFileInvocationUnknownMatchKey(ConfigError):
    def __init__(self, match_key, message=None):
        self.message = message or (
            f"Specified match key ({match_key!r}) is not a valid run time info "
            f"attribute."
        )
        self.match_key = match_key
        super().__init__(self.message)


class ConfigInvocationKeyNotFoundError(ConfigError):
    """Raised when a configuration invocation key is passed but it is not a valid key."""

    def __init__(self, invoc_key, file_path, available_keys) -> None:
        self.invoc_key = invoc_key
        self.file_path = file_path
        self.available_keys = available_keys
        self.message = (
            f"Invocation key {self.invoc_key!r} does not exist in configuration file. "
            f"Available keys in configuration file {str(self.file_path)!r} are "
            f"{self.available_keys!r}."
        )
        super().__init__(self.message)


class ConfigValidationError(ConfigError):
    """Raised when the matching config data is invalid."""

    def __init__(self, message, meta_data=None):
        self.meta_data = meta_data
        self.message = message + (f"config {self.meta_data}\n" if meta_data else "")
        super().__init__(self.message)


class ConfigDefaultValidationError(ConfigError):
    """Raised when the specified default configuration in the `ConfigOptions` object is
    invalid."""

    def __init__(self, validation_err, message=None):
        self.message = message or (
            f"The default configuration specified in the `ConfigOptions` object is "
            f"invalid.\n\n{validation_err}"
        )
        super().__init__(self.message)


class ConfigChangeInvalidError(ConfigError):
    """Raised when trying to set an invalid key in the Config."""

    def __init__(self, name, message=None):
        self.message = message or (
            f"Cannot modify value for invalid config item {name!r}. Use the `config list`"
            f" sub-command to list all configurable items."
        )
        super().__init__(self.message)


class ConfigChangeInvalidJSONError(ConfigError):
    """Raised when attempting to set a config key using an invalid JSON string."""

    def __init__(self, name, json_str, err, message=None):
        self.message = message or (
            f"The config file has not been modified. Invalid JSON string for config item "
            f"{name!r}. {json_str!r}\n\n{err!r}"
        )
        super().__init__(self.message)


class ConfigChangeValidationError(ConfigError):
    """Raised when a change to the configurable data would invalidate the config."""

    def __init__(self, name, validation_err, message=None):
        self.message = message or (
            f"The configuration has not been modified. Requested modification to item "
            f"{name!r} would invalidate the config in the following way."
            f"\n\n{validation_err}"
        )
        super().__init__(self.message)


class ConfigChangeFileUpdateError(ConfigError):
    """Raised when the updating of the config YAML file fails."""

    def __init__(self, names, err, message=None):
        self.message = message or (
            f"Failed to update the config file for modification of config items {names!r}."
            f"\n\n{err!r}"
        )
        super().__init__(self.message)


class ConfigChangeTypeInvalidError(ConfigError):
    """Raised when trying to modify a config item using a list operation, when the config
    item is not a list."""

    def __init__(self, name, typ, message=None):
        self.message = message or (
            f"The configuration has not been modified. The config item {name!r} has type "
            f"{typ!r} and so cannot be modified in that way."
        )
        super().__init__(self.message)


class ConfigChangePopIndexError(ConfigError):
    """Raised when trying to pop an item from a config item with an invalid index."""

    def __init__(self, name, length, index, message=None):
        self.message = message or (
            f"The configuration has not been modified. The config item {name!r} has length "
            f"{length!r} and so cannot be popped with index {index}."
        )
        super().__init__(self.message)


class MissingTaskSchemaFileError(ConfigError):
    """Raised when a task schema file specified in the config file does not exist."""

    def __init__(self, file_name, err, message=None):
        self.message = message or (
            f"The task schema file {file_name!r} cannot be found. \n{err!s}"
        )
        super().__init__(self.message)


class MissingEnvironmentFileError(ConfigError):
    """Raised when an environment file specified in the config file does not exist."""

    def __init__(self, file_name, err, message=None):
        self.message = message or (
            f"The environment file {file_name!r} cannot be found. \n{err!s}"
        )
        super().__init__(self.message)


class ConfigReadOnlyError(ConfigError):
    pass


class UnknownMetaTaskConstitutiveSchema(ValueError):
    pass
