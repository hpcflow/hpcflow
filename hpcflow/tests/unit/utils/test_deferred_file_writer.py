from hpcflow.sdk.utils.deferred_file import DeferredFileWriter


def test_file_not_created(tmp_path):
    file_name = tmp_path / "test.txt"
    assert not file_name.is_file()
    with DeferredFileWriter(file_name, mode="w") as fp:
        assert not fp._is_open
    assert not file_name.is_file()


def test_append_file_not_opened(tmp_path):
    file_name = tmp_path / "test.txt"
    with DeferredFileWriter(file_name, mode="a") as fp:
        assert not fp._is_open
    assert not file_name.is_file()


def test_file_created_write(tmp_path):
    file_name = tmp_path / "test.txt"
    assert not file_name.is_file()
    with DeferredFileWriter(file_name, mode="w") as fp:
        fp.write("contents\n")
        assert fp._is_open
    assert file_name.is_file()


def test_file_created_writelines(tmp_path):
    file_name = tmp_path / "test.txt"
    assert not file_name.is_file()
    with DeferredFileWriter(file_name, mode="w") as fp:
        fp.writelines(["contents\n"])
        assert fp._is_open
    assert file_name.is_file()
