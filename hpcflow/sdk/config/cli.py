"""Module defining a function that returns the click CLI group for manipulating the app
configuration."""

from __future__ import annotations
import json
import logging
import warnings
from functools import wraps
from contextlib import contextmanager
from typing import TYPE_CHECKING

import click
from colorama import init as colorama_init
from termcolor import colored  # type: ignore

from hpcflow.sdk.core.utils import open_file

from hpcflow.sdk.config.errors import ConfigError
from hpcflow.sdk.config.config import Config

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from ..app import BaseApp

logger = logging.getLogger(__name__)

colorama_init(autoreset=True)


def CLI_exception_wrapper_gen(*exception_cls):
    """
    Decorator factory that enhances the wrapped function to display a nice message on
    success or failure.
    """

    @contextmanager
    def warning_formatter():
        """
        Context manager to apply a simple warning formatter that shows just the warning
        type and the message. We use this in the CLI to avoid producing distracting
        output.
        """

        def custom_warning_formatter(
            message, category, filename, lineno, file=None, line=None
        ):
            return f"{colored(category.__name__, 'yellow')}: {message}\n"

        existing_func = warnings.formatwarning
        try:
            warnings.formatwarning = custom_warning_formatter
            yield
        finally:
            warnings.formatwarning = existing_func

    def CLI_exception_wrapper(func: Callable):
        """Decorator

        Parameters
        ----------
        func
            Function that return a non-None value if the operation succeeds
        """

        @wraps(func)
        @click.pass_context
        def wrapper(ctx: click.Context, *args, **kwargs):
            try:
                with warning_formatter():
                    out = func(*args, **kwargs)
                if out is not None:
                    click.echo(f"{colored('✔ Config file updated.', 'green')}")
                return out
            except exception_cls as err:
                click.echo(f"{colored(err.__class__.__name__, 'red')}: {err}")
                ctx.exit(1)

        return wrapper

    return CLI_exception_wrapper


def get_config_CLI(app: BaseApp) -> click.Group:
    """Generate the configuration CLI for the app."""

    pass_config = click.make_pass_decorator(Config)

    def find_config(ctx: click.Context) -> Config:
        if (cfg := ctx.find_object(Config)) is None:
            raise RuntimeError("no configuration defined")
        return cfg

    def show_all_config(ctx: click.Context, param, value: bool):
        if not value or ctx.resilient_parsing:
            return
        find_config(ctx)._show(config=True, metadata=False)
        ctx.exit()

    def show_all_metadata(ctx: click.Context, param, value: bool):
        if not value or ctx.resilient_parsing:
            return
        find_config(ctx)._show(config=False, metadata=True)
        ctx.exit()

    def show_config_file(ctx: click.Context, param, value: bool):
        if not value or ctx.resilient_parsing:
            return
        print(find_config(ctx).config_file_contents)
        ctx.exit()

    @click.group()
    @click.option(
        "--no-callback",
        multiple=True,
        help="Exclude a named get/set callback function during execution of the command.",
    )
    @click.pass_context
    def config(ctx: click.Context, no_callback: Sequence[str]):
        """Configuration sub-command for getting and setting data in the configuration
        file(s)."""
        ctx.obj = app.config
        if no_callback:
            app.config._disable_callbacks(no_callback)

    @config.command("list")
    @pass_config
    def config_list(config: Config):
        """Show a list of all configurable keys."""
        click.echo("\n".join(config.get_configurable()))

    @config.command("import")
    @click.argument("file_path")
    @click.option(
        "--rename/--no-rename",
        default=True,
        help=(
            "Rename the currently loaded config file according to the name of the file "
            "that is being imported (default is to rename). Ignored if `--new` is "
            "specified."
        ),
    )
    @click.option(
        "--new",
        type=click.BOOL,
        is_flag=True,
        default=False,
        help=(
            "If True, generate a new default config, and import the file into this "
            "config. If False, modify the currently loaded config."
        ),
    )
    @pass_config
    def import_from_file(config: Config, file_path: str, rename: bool, new: bool):
        """Update the config file with keys from a YAML file."""
        config.import_from_file(file_path, rename=rename, make_new=new)

    @config.command()
    @click.argument("name")
    @click.option(
        "--all",
        is_flag=True,
        expose_value=False,
        is_eager=True,
        help="Show all configuration items.",
        callback=show_all_config,
    )
    @click.option(
        "--metadata",
        is_flag=True,
        expose_value=False,
        is_eager=True,
        help="Show all metadata items.",
        callback=show_all_metadata,
    )
    @click.option(
        "--file",
        is_flag=True,
        expose_value=False,
        is_eager=True,
        help="Show the contents of the configuration file.",
        callback=show_config_file,
    )
    @pass_config
    @CLI_exception_wrapper_gen(ConfigError)
    def get(config: Config, name: str):
        """Show the value of the specified configuration item."""
        val = config.get(name)
        if isinstance(val, list):
            val = "\n".join(str(i) for i in val)
        click.echo(val)

    @config.command()
    @click.argument("name")
    @click.argument("value")
    @click.option(
        "--json",
        "is_json",
        is_flag=True,
        default=False,
        help="Interpret VALUE as a JSON string.",
    )
    @pass_config
    @CLI_exception_wrapper_gen(ConfigError)
    def set(config: Config, name: str, value: str, is_json: bool):
        """Set and save the value of the specified configuration item."""
        if is_json:
            config.set(name, value, is_json=True)
        else:
            config.set(name, value, is_json=False)
        config.save()

    @config.command()
    @click.argument("name")
    @pass_config
    @CLI_exception_wrapper_gen(ConfigError)
    def unset(config: Config, name: str):
        """Unset and save the value of the specified configuration item."""
        config.unset(name)
        config.save()

    @config.command()
    @click.argument("name")
    @click.argument("value")
    @click.option(
        "--json",
        "is_json",
        is_flag=True,
        default=False,
        help="Interpret VALUE as a JSON string.",
    )
    @pass_config
    @CLI_exception_wrapper_gen(ConfigError)
    def append(config: Config, name: str, value: str, is_json: bool):
        """Append a new value to the specified configuration item.

        NAME is the dot-delimited path to the list to be appended to.

        """
        if is_json:
            config.append(name, value, is_json=True)
        else:
            config.append(name, value, is_json=False)
        config.save()

    @config.command()
    @click.argument("name")
    @click.argument("value")
    @click.option(
        "--json",
        "is_json",
        is_flag=True,
        default=False,
        help="Interpret VALUE as a JSON string.",
    )
    @pass_config
    @CLI_exception_wrapper_gen(ConfigError)
    def prepend(config: Config, name: str, value: str, is_json: bool):
        """Prepend a new value to the specified configuration item.

        NAME is the dot-delimited path to the list to be prepended to.

        """
        if is_json:
            config.prepend(name, value, is_json=True)
        else:
            config.prepend(name, value, is_json=False)
        config.save()

    @config.command(context_settings={"ignore_unknown_options": True})
    @click.argument("name")
    @click.argument("index", type=click.types.INT)
    @pass_config
    @CLI_exception_wrapper_gen(ConfigError)
    def pop(config: Config, name: str, index: int):
        """Remove a value from a list-like configuration item.

        NAME is the dot-delimited path to the list to be modified.

        """
        config.pop(name, index)
        config.save()

    @config.command()
    @click.argument("name")
    @click.argument("value")
    @click.option(
        "--json",
        "is_json",
        is_flag=True,
        default=False,
        help="Interpret VALUE as a JSON string.",
    )
    @pass_config
    @CLI_exception_wrapper_gen(ConfigError)
    def update(config: Config, name: str, value: str, is_json: bool):
        """Update a map-like value in the configuration.

        NAME is the dot-delimited path to the map to be updated.

        """
        if is_json:
            config.update(name, value, is_json=True)
        else:
            config.update(name, value, is_json=False)
        config.save()

    @config.command()
    @click.argument("name")
    @click.option("--defaults")
    @pass_config
    @CLI_exception_wrapper_gen(ConfigError)
    def add_scheduler(config: Config, name: str, defaults: str | None):
        if defaults:
            loaded_defaults: dict = json.loads(defaults)
        else:
            loaded_defaults = {}
        config.add_scheduler(name, **loaded_defaults)
        config.save()

    @config.command()
    @click.argument("name")
    @click.option("--defaults")
    @pass_config
    @CLI_exception_wrapper_gen(ConfigError)
    def add_shell(config: Config, name: str, defaults: str | None):
        if defaults:
            loaded_defaults: dict = json.loads(defaults)
        else:
            loaded_defaults = {}
        config.add_shell(name, **loaded_defaults)
        config.save()

    @config.command()
    @click.option("--defaults")
    @pass_config
    @CLI_exception_wrapper_gen(ConfigError)
    def add_shell_wsl(config: Config, defaults: str | None):
        if defaults:
            loaded_defaults: dict = json.loads(defaults)
        else:
            loaded_defaults = {}
        config.add_shell_WSL(**loaded_defaults)
        config.save()

    @config.command()
    @click.argument("sha")
    @pass_config
    @CLI_exception_wrapper_gen(ConfigError)
    def set_github_demo_data_dir(config: Config, sha: str):
        config.set_github_demo_data_dir(sha=sha)
        config.save()

    @config.command()
    def load_data_files():
        """Check we can load the data files (e.g. task schema files) as specified in the
        configuration."""
        app.load_data_files()
        # FIXME: No such method?

    @config.command()
    @click.option("--path", is_flag=True, default=False)
    @pass_config
    def open(config: Config, path: bool = False):
        """Alias for `{package_name} open config`: open the configuration file, or retrieve
        it's path."""
        file_path = config.get("config_file_path")
        if path:
            click.echo(file_path)
        else:
            open_file(file_path)

    @config.command()
    @click.argument("known_name")
    @click.option(
        "--path",
        default=None,
        help=(
            "An `fsspec`-compatible path in which to look for configuration-import "
            "files."
        ),
    )
    @pass_config
    def init(config: Config, known_name: str, path: str | None):
        config.init(known_name=known_name, path=path)

    open.help = (open.help or "").format(package_name=app.package_name)

    return config
