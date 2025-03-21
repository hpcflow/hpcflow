import sys

import pytest
from hpcflow.sdk.core.utils import redirect_std_to_file


def test_stdout_redirect(tmp_path):
    file_name = tmp_path / "test.txt"
    expected = "stdout"
    with redirect_std_to_file(file_name, mode="w"):
        print(expected)
    with file_name.open("r") as fp:
        contents = fp.read().strip()
    assert contents == expected


def test_stderr_redirect(tmp_path):
    file_name = tmp_path / "test.txt"
    expected = "stderr"
    with redirect_std_to_file(file_name, mode="w"):
        print(expected, file=sys.stderr)
    with file_name.open("r") as fp:
        contents = fp.read().strip()
    assert contents == expected


def test_exception_exits_with_code(tmp_path):
    file_name = tmp_path / "test.txt"
    with pytest.raises(SystemExit) as exc:
        with redirect_std_to_file(file_name, mode="w"):
            raise ValueError("oh no!")
    assert exc.value.code == 1


def test_exception_prints_to_file(tmp_path):
    file_name = tmp_path / "test.txt"
    with pytest.raises(SystemExit):
        with redirect_std_to_file(file_name, mode="w"):
            raise ValueError("oh no!")
    with file_name.open("r") as fp:
        contents = fp.read().strip()
    assert 'ValueError("oh no!")' in contents


def test_file_not_created(tmp_path):
    file_name = tmp_path / "test.txt"
    assert not file_name.is_file()
    with redirect_std_to_file(file_name, mode="w"):
        pass
    assert not file_name.is_file()
