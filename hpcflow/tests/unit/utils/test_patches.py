from hpcflow.sdk.utils.patches import resolve_path


def test_absolute_path():
    assert resolve_path("my_file_path").is_absolute()
