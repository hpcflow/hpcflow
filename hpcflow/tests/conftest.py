from __future__ import annotations
from pathlib import Path
import pytest
from click.testing import CliRunner

from hpcflow.app import app as hf


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]):
    if config.getoption("--slurm"):
        # --slurm given in cli: only run slurm tests
        for item in items:
            if "slurm" not in item.keywords:
                item.add_marker(pytest.mark.skip(reason="need no --slurm option to run"))
    elif config.getoption("--wsl"):
        # --wsl given in CLI: only run wsl tests
        for item in items:
            if "wsl" not in item.keywords:
                item.add_marker(pytest.mark.skip(reason="need no --wsl option to run"))
    elif config.getoption("--direct-linux"):
        # --direct-linux in CLI: only run these tests
        for item in items:
            if "direct_linux" not in item.keywords:
                item.add_marker(
                    pytest.mark.skip(reason="remove --direct-linux option to run")
                )
    elif config.getoption("--integration"):
        # --integration in CLI: only run these tests
        for item in items:
            if "integration" not in item.keywords:
                item.add_marker(
                    pytest.mark.skip(reason="remove --integration option to run")
                )
    else:
        # --slurm not given in cli: skip slurm tests and do not skip other tests
        for item in items:
            if "slurm" in item.keywords:
                item.add_marker(pytest.mark.skip(reason="need --slurm option to run"))
            elif "wsl" in item.keywords:
                item.add_marker(pytest.mark.skip(reason="need --wsl option to run"))
            elif "direct_linux" in item.keywords:
                item.add_marker(
                    pytest.mark.skip(reason="add --direct_linux option to run")
                )
            elif "integration" in item.keywords:
                item.add_marker(
                    pytest.mark.skip(reason="add --integration option to run")
                )


@pytest.fixture(scope="session", autouse=True)
def isolated_app_config(tmp_path_factory, pytestconfig):
    """Pytest session-scoped fixture to apply a new default config for tests, and then
    restore the original config after testing has completed."""
    hf.run_time_info.in_pytest = True
    original_config_dir = hf.config.config_directory
    original_config_key = hf.config.config_key
    hf.unload_config()
    new_config_dir = tmp_path_factory.mktemp("app_config")
    hf.load_config(config_dir=new_config_dir)

    if pytestconfig.getoption("--configure-python-env"):
        # for setting up a Python env using the currently active virtual/conda env:
        hf.env_configure_python(use_current=True, save=True)
        hf.print_envs()
        hf.show_env(label="python")

    if env_src_file := pytestconfig.getoption("--with-env-source"):
        # for including envs (e.g. Python) from an existing env source file:
        hf.config.append("environment_sources", env_src_file)
        hf.config.save()
        hf.print_envs()
        hf.show_env(label="python")

    yield
    hf.unload_config()
    hf.load_config(config_dir=original_config_dir, config_key=original_config_key)
    hf.run_time_info.in_pytest = False


@pytest.fixture()
def modifiable_config(tmp_path: Path):
    """Pytest fixture to provide a fresh config which can be safely modified within the
    test without affecting other tests."""
    config_dir = hf.config.config_directory
    config_key = hf.config.config_key
    hf.unload_config()
    hf.load_config(config_dir=tmp_path)
    yield
    hf.unload_config()
    hf.load_config(config_dir=config_dir, config_key=config_key)


@pytest.fixture()
def reload_template_components():
    """Pytest fixture to reload the template components at the end of the test."""
    yield
    hf.reload_template_components()


@pytest.fixture
def unload_config():
    hf.unload_config()


@pytest.fixture
def cli_runner():
    """Pytest fixture to ensure the current config directory and key are used when
    invoking the CLI."""
    runner = CliRunner()
    common_args = [
        "--config-dir",
        str(hf.config.config_directory),
        "--config-key",
        hf.config.config_key,
    ]

    # to avoid warnings about config already loaded, we unload first (the CLI command
    # will immediately reload it):
    hf.unload_config()

    def invoke(args=None, cli=None, **kwargs):
        all_args = common_args + (args or [])
        cli = cli or hf.cli
        return runner.invoke(cli, args=all_args, **kwargs)

    return invoke


def pytest_generate_tests(metafunc):
    repeats_num = int(metafunc.config.getoption("--repeat"))
    if repeats_num > 1:
        metafunc.fixturenames.append("tmp_ct")
        metafunc.parametrize("tmp_ct", range(repeats_num))
