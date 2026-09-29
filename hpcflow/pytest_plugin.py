"""Pytest plugin for test CLI options.

Note this was separated from `conftest.py` due to an issue with running tests from the
single-file Pyinstaller-built executable on Windows, where it would seemingly be invoked
twice (on GitHub actions at least), resulting in `addoption` failing, since the option
already existed.

If running tests via `python -m pytest` (instead of via the app `test` command), this
plugin module must be provided as follows: `python -m pytest -p hpcflow.pytest_plugin`

"""

import pytest
from hpcflow.app import app as hf


def pytest_addoption(parser: pytest.Parser):

    parser.addoption(
        "--slurm",
        action="store_true",
        default=False,
        help="run slurm tests",
    )

    parser.addoption(
        "--wsl",
        action="store_true",
        default=False,
        help="run Windows Subsystem for Linux tests",
    )
    parser.addoption(
        "--direct-linux",
        action="store_true",
        default=False,
        help="run direct-linux submission tests",
    )
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="run integration-like workflow submission tests",
    )
    parser.addoption(
        "--repeat",
        action="store",
        default=1,
        type=int,
        help="number of times to repeat each test",
    )
    parser.addoption(
        "--configure-python-env",
        action="store_true",
        default=False,
        help=(
            "Configure the app Python environment using the currently activate Python "
            "virtual (or conda) environment. This is necessary for running integration "
            "tests that use `script_data_in/out: direct`."
        ),
    )
    parser.addoption(
        "--with-env-source",
        action="store",
        help=(
            "Provide the path to an environment sources file to load into the app config "
            "during testing."
        ),
    )


def pytest_configure(config: pytest.Config):
    config.addinivalue_line("markers", "slurm: mark test as slurm to run")
    config.addinivalue_line("markers", "wsl: mark test as wsl to run")
    config.addinivalue_line(
        "markers", "direct_linux: mark test as a direct-linux submission test to run"
    )
    config.addinivalue_line(
        "markers",
        "integration: mark test as an integration-like workflow submission test to run",
    )
    hf.run_time_info.in_pytest = True
