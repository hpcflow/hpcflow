from __future__ import annotations
from pathlib import Path
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


def pytest_unconfigure(config: pytest.Config):
    hf.run_time_info.in_pytest = False


@pytest.fixture
def null_config(tmp_path: Path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)
    hf.run_time_info.in_pytest = True


@pytest.fixture
def new_null_config(tmp_path: Path):
    hf.load_config(config_dir=tmp_path, warn=False)
    hf.load_template_components(warn=False)
    hf.run_time_info.in_pytest = True


@pytest.fixture
def unload_config():
    hf.unload_config()


def pytest_generate_tests(metafunc):
    repeats_num = int(metafunc.config.getoption("--repeat"))
    if repeats_num > 1:
        metafunc.fixturenames.append("tmp_ct")
        metafunc.parametrize("tmp_ct", range(repeats_num))
