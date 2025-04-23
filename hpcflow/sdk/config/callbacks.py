"""Module that defines built-in callback functions for configuration item values."""

from __future__ import annotations
import os
import re
import fsspec  # type: ignore
from typing import overload, TYPE_CHECKING
from hpcflow.sdk.core.errors import UnsupportedSchedulerError, UnsupportedShellError
from hpcflow.sdk.submission.shells import get_supported_shells

if TYPE_CHECKING:
    from typing import Any, TypeVar
    from .config import Config
    from ..typing import PathLike

    T = TypeVar("T")


def callback_vars(config: Config, value) -> str:
    """
    Callback that substitutes configuration variables.
    """

    def vars_repl(match_obj: re.Match[str]) -> str:
        return config._variables[match_obj[1]]

    vars_regex = rf"\<\<({ '|'.join(config._variables) })\>\>"
    return re.sub(
        pattern=vars_regex,
        repl=vars_repl,
        string=str(value),
    )


@overload
def callback_file_paths(config: Config, file_path: PathLike) -> PathLike: ...


@overload
def callback_file_paths(config: Config, file_path: list[PathLike]) -> list[PathLike]: ...


def callback_file_paths(config: Config, file_path: PathLike | list[PathLike]):
    """
    Callback that resolves file paths.
    """
    if isinstance(file_path, list):
        return [config._resolve_path(path) for path in file_path]
    else:
        return config._resolve_path(file_path)


def callback_bool(config: Config, value: str | bool) -> bool:
    """
    Callback that coerces values to boolean.
    """
    if not isinstance(value, bool):
        if value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False
        else:
            raise TypeError(f"Cannot cast {value!r} to a bool type.")
    return value


@overload
def callback_lowercase(config: Config, value: list[str]) -> list[str]: ...


@overload
def callback_lowercase(config: Config, value: dict[str, T]) -> dict[str, T]: ...


@overload
def callback_lowercase(config: Config, value: str) -> str: ...


def callback_lowercase(
    config: Config, value: list[str] | dict[str, T] | str
) -> list[str] | dict[str, T] | str:
    """
    Callback that forces a string to lower case.
    """
    if isinstance(value, list):
        return [item.lower() for item in value]
    elif isinstance(value, dict):
        return {k.lower(): v for k, v in value.items()}
    else:
        return value.lower()


def exists_in_schedulers(config: Config, value: T) -> T:
    """
    Callback that tests that a value is a supported scheduler name.
    """
    if value not in config.schedulers:
        raise ValueError(
            f"Cannot set default scheduler; {value!r} is not a supported scheduler "
            f"according to the config file, which lists these schedulers as available "
            f"on this machine: {config.schedulers!r}."
        )
    return value


def callback_supported_schedulers(
    config: Config, schedulers: dict[str, Any]
) -> dict[str, Any]:
    """
    Callback that tests that all values are names of supported schedulers.
    """
    # validate against supported schedulers according to the OS - this won't validate that
    # a particular scheduler actually exists on this system:
    available = set(config._app.get_OS_supported_schedulers())
    if any((witness := k) not in available for k in schedulers):
        raise UnsupportedSchedulerError(scheduler=witness, available=available)
    return schedulers


def _hostname_in_invocation(config: Config) -> bool:
    return "hostname" in config._file.get_invocation(config._config_key)["match"]


def set_scheduler_invocation_match(config: Config, scheduler: str) -> None:
    """Invoked on set of `default_scheduler`.

    For clusters with "proper" schedulers (SGE, SLURM, etc.), login nodes are typically
    named using the word "login". So we can use this knowledge to set a default for the
    "hostname" invocation match key, if it is not manually set. However, it is preferable
    that on clusters the hostname match is explicitly set.

    """
    sched = config._app.get_scheduler(
        scheduler_name=scheduler,
        os_name=os.name,
        scheduler_args=config.get(f"schedulers.{scheduler}").get("defaults", {}),
    )
    if isinstance(sched, config._app.QueuedScheduler):
        if not _hostname_in_invocation(config):
            config._file.update_invocation(
                config_key=config._config_key,
                match={"hostname": sched.DEFAULT_LOGIN_NODE_MATCH},
            )


def callback_scheduler_set_up(
    config: Config, schedulers: dict[str, Any]
) -> dict[str, Any]:
    """Invoked on set of `schedulers`.

    Runs scheduler-specific config initialisation.
    """
    for k, v in schedulers.items():
        sched = config._app.get_scheduler(
            scheduler_name=k,
            os_name=os.name,
            scheduler_args=v.get("defaults", {}),
        )

        if isinstance(sched, config._app.SGEPosix):
            # some `QueuedScheduler` classes have a `get_login_nodes` method which can be used
            # to populate the names of login nodes explicitly, if not already set:
            if not _hostname_in_invocation(config):
                config._file.update_invocation(
                    config_key=config._config_key,
                    match={"hostname": sched.get_login_nodes()},
                )
    return schedulers


def callback_supported_shells(config: Config, shell_name: str) -> str:
    """
    Callback that tests if a shell names is supported on this OS.
    """
    supported = get_supported_shells(os.name)
    if shell_name not in supported:
        raise UnsupportedShellError(shell=shell_name, supported=supported)
    return shell_name


def set_callback_file_paths(config: Config, value: PathLike | list[PathLike]) -> None:
    """Check the file(s) is/are accessible. This is only done on `config.set` (and not on
    `config.get` or `config._validate`) because it could be expensive in the case of remote
    files."""
    value = callback_file_paths(config, value)

    to_check = value if isinstance(value, list) else [value]

    for file_path in to_check:
        if file_path is None:
            continue
        with fsspec.open(file_path, mode="rt") as fh:
            pass
            # TODO: also check something in it?
        print(f"Checked access to: {file_path}")


def check_load_data_files(config: Config, value: Any) -> None:
    """Check data files (e.g., task schema files) can be loaded successfully. This is only
    done on `config.set` (and not on `config.get` or `config._validate`) because it could
    be expensive in the case of remote files."""
    config._app.reload_template_components(warn=False)


def callback_log_file_path(config, value):
    value = value.strip()
    if value:
        return config._resolve_path(value)
    else:
        return value


def callback_update_log_console_level(config: Config, value: str) -> None:
    """
    Callback to set the logging level.
    """
    config._app.log.update_console_level(new_level=value)


def callback_unset_log_console_level(config: Config) -> None:
    """Reset the console handler to the default level."""
    config._app.log.update_console_level()


def callback_update_log_file_level(config: Config, value: str) -> None:
    """Callback to set the level of the log file handler."""
    config._app.log.update_file_level(new_level=value)


def callback_update_log_file_path(config: Config, value: str) -> None:
    """
    Callback to update the log file path, or remove the file handler if no path specifed.
    """
    config._app.log.remove_file_handler()
    if value:
        config._app.log.add_file_logger(path=value, level=config.get("log_file_level"))


def callback_unset_log_file_level(config: Config) -> None:
    """Callback to reset the file handler to the default level."""
    config._app.log.update_file_level()


def callback_unset_log_file_path(config: Config) -> None:
    """Callback to remove the log file handler."""
    config._app.log.remove_file_handler()
