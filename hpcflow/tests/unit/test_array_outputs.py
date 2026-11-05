"""Indexed archive integrity and unusual array shapes/dtypes."""

from io import BytesIO
import json
from pathlib import Path
from zipfile import ZipFile

import numpy as np
import pytest
import zstandard as zstd
from fsspec.implementations.local import LocalFileSystem  # type: ignore[import-untyped]

from hpcflow.sdk.persistence.array_outputs import (
    METADATA_NAMES,
    read_chunked_array_output,
    write_chunked_array_output,
    write_streamed_array_output,
)
from hpcflow.sdk.persistence.array_output_sources import open_array_output_source


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


def test_array_output_legacy_source_reads_rows_without_loading_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    values = np.arange(24, dtype=np.int64).reshape(3, 2, 4)
    contents = _contents(values)
    contents["shape"] = np.array([[1, 2], [2, 4], [0, 4]], dtype=np.int32)
    buffer = BytesIO()
    np.savez(buffer, allow_pickle=False, **contents)
    path = tmp_path / "legacy.npz.zst"
    path.write_bytes(zstd.ZstdCompressor().compress(buffer.getvalue()))
    with np.load(BytesIO(buffer.getvalue()), allow_pickle=False) as archive:
        npz_type = type(archive)
    original_getitem = npz_type.__getitem__

    def guarded_getitem(archive, key):
        assert key != "data", "Full payload was loaded into RAM"
        return original_getitem(archive, key)

    monkeypatch.setattr(npz_type, "__getitem__", guarded_getitem)
    with open_array_output_source(LocalFileSystem(), path) as source:
        np.testing.assert_array_equal(source[0], values[0, :1, :2])
        np.testing.assert_array_equal(source[1], values[1])
        assert source[2].shape == (0, 4)


@pytest.mark.parametrize("compressed", [False, True])
@pytest.mark.parametrize("fortran", [False, True])
def test_array_output_legacy_source_preserves_numpy_layouts(
    tmp_path: Path, compressed: bool, fortran: bool
) -> None:
    values = np.arange(24, dtype=np.int64).reshape(3, 2, 4)
    if fortran:
        values = np.asfortranarray(values)
    buffer = BytesIO()
    save = np.savez_compressed if compressed else np.savez
    save(buffer, allow_pickle=False, **_contents(values))
    path = tmp_path / "legacy.npz.zst"
    path.write_bytes(zstd.ZstdCompressor().compress(buffer.getvalue()))
    with open_array_output_source(LocalFileSystem(), path) as source:
        for index in range(len(values)):
            np.testing.assert_array_equal(source[index], values[index])


def test_array_output_legacy_streaming_checks_source_crc(tmp_path: Path) -> None:
    import struct
    from zipfile import BadZipFile

    buffer = BytesIO()
    np.savez(buffer, allow_pickle=False, **_contents(np.arange(4096).reshape(4, 1024)))
    raw = bytearray(buffer.getvalue())
    with ZipFile(BytesIO(raw)) as archive:
        info = archive.getinfo("data.npy")
    name_length, extra_length = struct.unpack_from("<HH", raw, info.header_offset + 26)
    last_byte = info.header_offset + 30 + name_length + extra_length + info.file_size - 1
    raw[last_byte] ^= 1  # retain the original CRC, but change the source payload
    path = tmp_path / "corrupt.npz.zst"
    path.write_bytes(zstd.ZstdCompressor().compress(raw))
    with pytest.raises(BadZipFile):
        with open_array_output_source(LocalFileSystem(), path) as source:
            for index in range(4):
                source[index]


@pytest.mark.parametrize(
    "values",
    [
        np.array([(1,), (2,)], dtype=[("材料", "<i4")]),
        np.empty((3, 2), dtype="V0"),
        np.array([["é"], ["文"]]),
    ],
)
def test_array_output_legacy_source_dtype_headers(
    tmp_path: Path, values: np.ndarray
) -> None:
    buffer = BytesIO()
    np.savez(buffer, allow_pickle=False, **_contents(values))
    path = tmp_path / "legacy.npz.zst"
    path.write_bytes(zstd.ZstdCompressor().compress(buffer.getvalue()))
    with open_array_output_source(LocalFileSystem(), path) as source:
        assert source.dtype == values.dtype
        for index in range(len(values)):
            np.testing.assert_array_equal(source[index], values[index])


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


def test_array_output_batch_reads_reuse_and_close_zip_handle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from unittest.mock import Mock

    path = tmp_path / "consolidated.zip"
    values = np.arange(128, dtype=np.int64).reshape(64, 2)
    write_chunked_array_output(path, _contents(values), chunk_bytes=16)
    data, _, _ = read_chunked_array_output(LocalFileSystem(), str(path))
    opened = Mock(wraps=ZipFile)
    monkeypatch.setattr("hpcflow.sdk.persistence.array_outputs.ZipFile", opened)
    data.verify()
    assert opened.call_count == 1
    opened.reset_mock()
    with data.reading():
        for index in range(len(values)):
            np.testing.assert_array_equal(data[index], values[index])
    assert opened.call_count == 1
    assert data._archive is None
    assert data._cached_data is None
    path.rename(
        tmp_path / "renamed.zip"
    )  # Windows rejects this if a handle remains open.


def test_array_output_indexed_source_reads_each_chunk_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from unittest.mock import Mock
    from hpcflow.sdk.persistence.array_outputs import ChunkedArray

    contents = _contents(np.zeros(4))
    del contents["data"]
    contents.update(
        data_0=np.array([[0, 0], [2, 2]]),
        data_1=np.array([[1, 1, 1], [3, 3, 3]]),
        group_idx=np.array([0, 1, 0, 1]),
        row_idx=np.array([0, 0, 1, 1]),
    )
    path = tmp_path / "consolidated.zip"
    write_chunked_array_output(path, contents, chunk_bytes=64)
    read = Mock(wraps=ChunkedArray._read_chunk)

    def tracked_read(data: ChunkedArray, index: int) -> np.ndarray:
        return read(data, index)

    monkeypatch.setattr(ChunkedArray, "_read_chunk", tracked_read)
    with open_array_output_source(LocalFileSystem(), path) as source:
        np.testing.assert_array_equal(source.entry_order, [0, 2, 1, 3])
        for index in source.entry_order:
            np.testing.assert_array_equal(
                source[int(index)], np.full(tuple(source.shapes[index]), index)
            )
    assert read.call_count == 2


def test_array_output_streaming_bounds_shape_buffers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import weakref

    count = 36
    shapes = np.asarray(
        [[128] if i == 7 else [i % 5 + 1] for i in range(count)], dtype=np.int32
    )
    metadata = _contents(np.zeros(count))
    del metadata["data"]
    allocations: list[weakref.ReferenceType[np.ndarray]] = []
    peak_buffer_bytes = 0
    original_empty = np.empty

    def tracked_empty(*args, **kwargs):
        nonlocal peak_buffer_bytes
        result = original_empty(*args, **kwargs)
        if result.ndim == 2 and result.dtype == np.dtype(np.float64):
            allocations.append(weakref.ref(result))
            live_bytes = sum(
                array.nbytes for ref in allocations if (array := ref()) is not None
            )
            peak_buffer_bytes = max(peak_buffer_bytes, live_bytes)
            assert live_bytes <= 256
        return result

    path = tmp_path / "streamed.zip"
    entries = (
        (i, np.full(tuple(shapes[i]), i, dtype=np.float64))
        for i in reversed(range(count))
    )
    with monkeypatch.context() as patch:
        patch.setattr(np, "empty", tracked_empty)
        write_streamed_array_output(
            path, metadata, np.dtype(np.float64), shapes, entries, chunk_bytes=128
        )
    assert peak_buffer_bytes > 0
    data, _, restored_shapes = read_chunked_array_output(LocalFileSystem(), str(path))
    np.testing.assert_array_equal(restored_shapes, shapes)
    for index in range(count):
        np.testing.assert_array_equal(data[index], np.full(tuple(shapes[index]), index))


@pytest.mark.parametrize("indices", [[0, 0], [0], [0, 2]])
def test_array_output_streaming_rejects_missing_duplicate_entries(
    tmp_path: Path, indices: list[int]
) -> None:
    metadata = _contents(np.zeros(2))
    del metadata["data"]
    shapes = np.ones((2, 1), dtype=np.int32)
    entries = ((index, np.array([index])) for index in indices)
    with pytest.raises(ValueError):
        write_streamed_array_output(
            tmp_path / "bad.zip", metadata, np.dtype(np.int64), shapes, entries
        )
