"""Indexed archive integrity and unusual array shapes/dtypes."""

from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

import numpy as np
import pytest
from fsspec.implementations.local import LocalFileSystem  # type: ignore[import-untyped]

from hpcflow.sdk.persistence.array_outputs import (
    METADATA_NAMES,
    read_chunked_array_output,
    write_chunked_array_output,
)


def _contents(data: np.ndarray) -> dict[str, np.ndarray]:
    contents = {key: np.zeros(len(data), dtype=np.uint32) for key in METADATA_NAMES}
    contents["run_ID"] = np.arange(len(data), dtype=np.uint32)
    contents["data"] = data
    return contents


@pytest.mark.parametrize(
    "values",
    [
        np.array([1.0, 2.0, 3.0]),
        np.empty((3, 0, 2), dtype=np.int32),
        np.array([["a", "long"], ["c", "d"]]),
        np.array([[b"a", b"long"], [b"c", b"d"]]),
        np.array([(1, 2.0), (3, 4.0)], dtype=[("id", "<i4"), ("value", "<f8")]),
    ],
)
def test_array_output_chunk_dtype_roundtrip(tmp_path: Path, values: np.ndarray) -> None:
    path = tmp_path / "consolidated.zip"
    write_chunked_array_output(path, _contents(values), chunk_bytes=1)
    data, metadata, shape = read_chunked_array_output(LocalFileSystem(), str(path))
    assert data.dtype == values.dtype
    assert shape is None
    np.testing.assert_array_equal(metadata["run_ID"], np.arange(len(values)))
    for index in range(len(values)):
        np.testing.assert_array_equal(data[index], values[index])
    data.verify()


def test_array_output_metadata_has_no_pickle_option_array(tmp_path: Path) -> None:
    path = tmp_path / "consolidated.zip"
    write_chunked_array_output(path, _contents(np.ones((2, 3))))
    with ZipFile(path) as archive:
        with np.load(
            BytesIO(archive.read("metadata.npz")), allow_pickle=False
        ) as metadata:
            assert set(metadata.files) == {*METADATA_NAMES, "chunk_idx", "row_idx"}


@pytest.mark.parametrize(
    "corruption", ["version", "chunk", "row", "order", "duplicate", "dtype"]
)
def test_array_output_rejects_invalid_chunk_index(
    tmp_path: Path, corruption: str
) -> None:
    path = tmp_path / "consolidated.zip"
    write_chunked_array_output(
        path, _contents(np.arange(6, dtype=np.int64).reshape(3, 2)), chunk_bytes=16
    )
    with ZipFile(path) as archive:
        files = {name: archive.read(name) for name in archive.namelist()}
    if corruption == "version":
        header = json.loads(files["format.json"])
        header["version"] = 999
        files["format.json"] = json.dumps(header).encode()
    else:
        with np.load(BytesIO(files["metadata.npz"]), allow_pickle=False) as metadata:
            arrays = {name: metadata[name] for name in metadata.files}
        if corruption == "chunk":
            arrays["chunk_idx"][0] = 999
        elif corruption == "row":
            arrays["row_idx"][0] = 999
        elif corruption == "order":
            arrays["run_ID"] = arrays["run_ID"][::-1]
        elif corruption == "duplicate":
            arrays["run_ID"][1] = arrays["run_ID"][0]
        else:
            arrays["run_ID"] = arrays["run_ID"].astype(np.int64)
        buffer = BytesIO()
        np.savez(buffer, allow_pickle=False, **arrays)
        files["metadata.npz"] = buffer.getvalue()
    with ZipFile(path, "w") as archive:
        for name, value in files.items():
            archive.writestr(name, value)
    with pytest.raises(ValueError):
        read_chunked_array_output(LocalFileSystem(), str(path))


def test_array_output_unrequested_chunk_is_not_read(tmp_path: Path) -> None:
    path = tmp_path / "consolidated.zip"
    write_chunked_array_output(
        path, _contents(np.arange(6, dtype=np.int64).reshape(3, 2)), chunk_bytes=16
    )
    with ZipFile(path) as archive:
        files = {name: archive.read(name) for name in archive.namelist()}
    files["chunks/000002.npy"] = b"corrupt NPY data"
    with ZipFile(path, "w") as archive:
        for name, value in files.items():
            archive.writestr(name, value)
    data, _, _ = read_chunked_array_output(LocalFileSystem(), str(path))
    np.testing.assert_array_equal(data[0], [0, 1])
    with pytest.raises(ValueError):
        data[2]
    with pytest.raises(ValueError):
        data.verify()
