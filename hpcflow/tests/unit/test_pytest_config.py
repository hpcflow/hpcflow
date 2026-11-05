from inspect import unwrap
from types import SimpleNamespace

import pytest

from hpcflow.app import app as hf
from hpcflow.tests import conftest


@pytest.mark.parametrize(
    ("stored_host", "overrides"),
    [
        (None, {}),
        ("127.0.0.1", {}),
        ("127.0.0.1", {"log_file_path": "logs/test.log"}),
        ("127.0.0.1", {"server_advertise_host": "localhost"}),
        ("127.0.0.1", {"server_advertise_host": ""}),
    ],
)
@pytest.mark.usefixtures("modifiable_config")
def test_isolated_app_config_preserves_wait_server_host(
    tmp_path_factory: pytest.TempPathFactory,
    stored_host: str | None,
    overrides: dict[str, str],
) -> None:
    hf.config._overrides.pop("server_advertise_host", None)
    if stored_host is not None:
        hf.config.server_advertise_host = stored_host
        hf.config.save()

    original_directory = hf.config.config_directory
    original_key = hf.config.config_key
    hf.unload_config()
    hf.load_config(
        config_dir=original_directory, config_key=original_key, overrides=overrides
    )
    expected_host = overrides.get("server_advertise_host", stored_host)
    original_in_pytest = hf.run_time_info.in_pytest
    config = SimpleNamespace(getoption=lambda _: None)
    fixture = unwrap(conftest.isolated_app_config)(tmp_path_factory, config)
    try:
        next(fixture)
        assert hf.config.server_advertise_host == expected_host
        assert hf.config._overrides == overrides
        if "server_advertise_host" not in overrides:
            isolated_directory = hf.config.config_directory
            hf.unload_config()
            hf.load_config(config_dir=isolated_directory)
            assert hf.config.server_advertise_host == stored_host
            if stored_host is not None:
                hf.config.server_advertise_host = "localhost"
                assert hf.config.server_advertise_host == "localhost"
    finally:
        try:
            next(fixture, None)
        finally:
            hf.run_time_info.in_pytest = original_in_pytest

    assert hf.config.config_directory == original_directory
    assert hf.config.server_advertise_host == expected_host
    assert hf.config._overrides == overrides
