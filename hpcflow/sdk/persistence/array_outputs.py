"""Indexed, independently compressed chunks for consolidated array outputs."""

from __future__ import annotations

from dataclasses import dataclass, field
from contextlib import contextmanager
from collections import Counter, OrderedDict
from io import BytesIO, BufferedIOBase
import json
from pathlib import Path
import struct
from typing import Any, BinaryIO, Iterable, Iterator, SupportsIndex, TYPE_CHECKING
from zipfile import ZIP_DEFLATED, ZIP_STORED, ZipFile

from fsspec.core import OpenFile  # type: ignore
from fsspec.implementations.zip import ZipFileSystem  # type: ignore

import numpy as np

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem  # type: ignore


ARRAY_OUTPUT_CHUNK_BYTES = 16 * 1024**2
METADATA_NAMES = (
    "task_insert_ID",
    "task_idx",
    "element_idx",
    "element_ID",
    "iteration_idx",
    "iteration_ID",
    "action_idx",
    "run_idx",
    "run_ID",
)


class _StoredMember(BufferedIOBase):
    """A bounded, seekable view of an uncompressed member in an outer ZIP."""

    def __init__(self, stream: BinaryIO, offset: int, size: int):
        self.stream, self.offset, self.size = stream, offset, size
        self.position = 0

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.position

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence not in (0, 1, 2):
            raise ValueError("Invalid seek origin.")
        position = offset + (
            0 if whence == 0 else self.position if whence == 1 else self.size
        )
        if position < 0:
            raise ValueError("Negative seek position.")
        self.position = position
        return position

    def read(self, size: int | None = -1) -> bytes:
        remaining = max(0, self.size - self.position)
        size = remaining if size is None or size < 0 else min(size, remaining)
        self.stream.seek(self.offset + self.position)
        data = self.stream.read(size)
        self.position += len(data)
        return data


@contextmanager
def _open_archive(fs: AbstractFileSystem, path: str) -> Iterator[Any]:
    # ZipExtFile.seek reads intervening bytes on Python 3.10--3.12, even for
    # ZIP_STORED. Open a separate outer stream and seek directly to the member.
    if isinstance(fs, ZipFileSystem) and isinstance(fs.of, OpenFile):
        try:
            info = fs.zip.getinfo(path.lstrip("/"))
        except KeyError:
            raise FileNotFoundError(path) from None
        if info.compress_type == ZIP_STORED and not info.flag_bits & 1:
            with fs.of.fs.open(fs.of.path, "rb") as stream:
                stream.seek(info.header_offset)
                header = stream.read(30)
                if len(header) != 30 or header[:4] != b"PK\x03\x04":
                    raise ValueError(f"Invalid outer ZIP header for {path}.")
                name_length, extra_length = struct.unpack_from("<HH", header, 26)
                offset = info.header_offset + 30 + name_length + extra_length
                yield _StoredMember(stream, offset, info.file_size)
            return
    with fs.open(path, "rb") as stream:
        yield stream


def write_chunked_array_output(
    path: Path,
    contents: dict[str, np.ndarray],
    *,
    chunk_bytes: int = ARRAY_OUTPUT_CHUNK_BYTES,
) -> None:
    """Write shape groups as ZIP members, with metadata mapping entries to chunks."""
    if chunk_bytes < 1:
        raise ValueError("Chunk size must be positive.")
    groups = (
        [contents["data"]]
        if "data" in contents
        else [
            contents[f"data_{i}"]
            for i in range(sum(k.startswith("data_") for k in contents))
        ]
    )
    count = len(contents["run_ID"])
    group_idx = contents.get("group_idx", np.zeros(count, dtype=np.uint32))
    rows = contents.get("row_idx", np.arange(count, dtype=np.uint32))
    chunk_idx = np.empty(count, dtype=np.uint32)
    row_idx = np.empty(count, dtype=np.uint32)
    chunks: list[dict[str, Any]] = []
    with ZipFile(path, "w", compression=ZIP_DEFLATED, compresslevel=1) as archive:
        for group_id, group in enumerate(groups):
            entry_bytes = group.dtype.itemsize * int(
                np.prod(group.shape[1:], dtype=object)
            )
            per_chunk = max(1, chunk_bytes // max(1, entry_bytes))
            indices = np.flatnonzero(group_idx == group_id)
            group_rows = rows[indices]
            base_chunk = len(chunks)
            chunk_idx[indices] = base_chunk + group_rows // per_chunk
            row_idx[indices] = group_rows % per_chunk
            for start in range(0, len(group), per_chunk):
                data = group[start : start + per_chunk]
                name = f"chunks/{len(chunks):06d}.npy"
                chunks.append({"name": name, "shape": list(data.shape)})
                with archive.open(name, "w", force_zip64=True) as stream:
                    np.save(stream, data, allow_pickle=False)
        _write_index(archive, contents, groups[0].dtype, chunks, chunk_idx, row_idx)


def _write_index(
    archive: ZipFile,
    contents: dict[str, np.ndarray],
    dtype: np.dtype,
    chunks: list[dict[str, Any]],
    chunk_idx: np.ndarray,
    row_idx: np.ndarray,
) -> None:
    metadata = {key: contents[key] for key in METADATA_NAMES}
    metadata.update(chunk_idx=chunk_idx, row_idx=row_idx)
    if "member_idx" in contents:
        metadata["member_idx"] = contents["member_idx"]
    buffer = BytesIO()
    # NumPy 1.x interprets savez(allow_pickle=False) as an extra array.
    with ZipFile(buffer, "w", compression=ZIP_STORED) as metadata_archive:
        for key, values in metadata.items():
            with metadata_archive.open(f"{key}.npy", "w", force_zip64=True) as member:
                np.save(member, values, allow_pickle=False)
    archive.writestr("metadata.npz", buffer.getvalue())
    archive.writestr(
        "format.json",
        json.dumps(
            {
                "version": 1,
                "dtype": np.lib.format.dtype_to_descr(dtype),
                "chunks": chunks,
            }
        ),
    )


def write_streamed_array_output(
    path: Path,
    metadata: dict[str, np.ndarray],
    dtype: np.dtype,
    shapes: np.ndarray,
    entries: Iterable[tuple[int, np.ndarray]],
    *,
    chunk_bytes: int = ARRAY_OUTPUT_CHUNK_BYTES,
) -> None:
    """Consume entries once with at most two target chunks of active shape buffers.

    Metadata is already sorted; entries may arrive in source order. Evicting a
    partly filled buffer writes a smaller valid chunk. Oversized entries are
    written directly after flushing other buffers.
    """
    if chunk_bytes < 1:
        raise ValueError("Chunk size must be positive.")
    count = len(metadata["run_ID"])
    shape_counts = Counter(tuple(int(dim) for dim in shape) for shape in shapes)
    buffers: OrderedDict[tuple[int, ...], tuple[np.ndarray, list[int]]] = OrderedDict()
    chunk_idx = np.empty(count, dtype=np.uint32)
    row_idx = np.empty(count, dtype=np.uint32)
    seen = np.zeros(count, dtype=bool)
    chunks: list[dict[str, Any]] = []
    with ZipFile(path, "w", compression=ZIP_DEFLATED, compresslevel=1) as archive:

        def write_chunk(data: np.ndarray, indices: list[int]) -> None:
            index = len(chunks)
            if index > 2**32 - 1:
                raise OverflowError("Too many array-output chunks for uint32 indices.")
            name = f"chunks/{index:06d}.npy"
            chunks.append({"name": name, "shape": list(data.shape)})
            chunk_idx[indices] = index
            row_idx[indices] = np.arange(len(indices), dtype=np.uint32)
            with archive.open(name, "w", force_zip64=True) as stream:
                np.save(stream, data, allow_pickle=False)

        def flush() -> None:
            _, (buffer, indices) = buffers.popitem(last=False)
            write_chunk(buffer[: len(indices)], indices)

        for index, value in entries:
            if index < 0 or index >= count or seen[index]:
                raise ValueError("Duplicate or invalid array-output entry index.")
            shape = tuple(int(dim) for dim in shapes[index])
            if value.shape != shape:
                raise ValueError(
                    "Array-output source changed shape during consolidation."
                )
            entry_bytes = int(np.prod(shape, dtype=object)) * dtype.itemsize
            if entry_bytes > chunk_bytes:
                while buffers:
                    flush()
                write_chunk(np.asarray(value, dtype=dtype)[None], [index])
            else:
                if shape not in buffers:
                    capacity = min(
                        shape_counts[shape], max(1, chunk_bytes // max(1, entry_bytes))
                    )
                    needed = capacity * entry_bytes
                    while (
                        buffers
                        and sum(buf.nbytes for buf, _ in buffers.values()) + needed
                        > 2 * chunk_bytes
                    ):
                        flush()
                    buffers[shape] = (np.empty((capacity, *shape), dtype=dtype), [])
                buffer, indices = buffers[shape]
                buffer[len(indices)] = value
                indices.append(index)
                buffers.move_to_end(shape)
                if len(indices) == len(buffer):
                    del buffers[shape]
                    write_chunk(buffer, indices)
                # Do not retain an evicted/finished buffer through local variables.
                del buffer, indices
            seen[index] = True
            del value
        while buffers:
            flush()
        if not np.all(seen):
            raise ValueError("Missing array-output entries during consolidation.")
        _write_index(archive, metadata, dtype, chunks, chunk_idx, row_idx)


@dataclass
class ChunkedArray:
    """Lazy array entries backed by a seekable archive; cache only the last chunk."""

    fs: AbstractFileSystem
    path: str
    dtype: np.dtype
    chunks: tuple[tuple[str, tuple[int, ...]], ...]
    chunk_idx: np.ndarray
    row_idx: np.ndarray
    _cached_index: int = field(default=-1, init=False)
    _cached_data: np.ndarray | None = field(default=None, init=False)
    _archive: ZipFile | None = field(default=None, init=False, repr=False)

    @property
    def ndim(self) -> int:
        return len(self.chunks[0][1])

    @property
    def shape(self) -> tuple[int, ...]:
        """Uniform archives have an implied shape, matching legacy dense arrays."""
        return (len(self), *self.chunks[0][1][1:])

    def __len__(self) -> int:
        return len(self.chunk_idx)

    def _read_chunk(self, index: int) -> np.ndarray:
        if self._archive is not None:
            return self._load_chunk(index, self._archive)
        with _open_archive(self.fs, self.path) as stream, ZipFile(stream) as archive:
            return self._load_chunk(index, archive)

    def _load_chunk(self, index: int, archive: ZipFile) -> np.ndarray:
        name, shape = self.chunks[index]
        with archive.open(name) as member:
            data = np.load(member, allow_pickle=False)
        if (
            not isinstance(data, np.ndarray)
            or data.shape != shape
            or data.dtype != self.dtype
        ):
            raise ValueError(f"Invalid array-output chunk {name!r} in {self.path}.")
        return data

    @contextmanager
    def reading(self) -> Iterator[ChunkedArray]:
        """Share one ZIP handle for a request; close it before replacing the file."""
        if self._archive is not None:
            yield self
            return
        with _open_archive(self.fs, self.path) as stream, ZipFile(stream) as archive:
            self._archive = archive
            try:
                yield self
            finally:
                self._archive = None
                self._cached_data = None
                self._cached_index = -1

    def __getitem__(self, index: SupportsIndex) -> np.ndarray:
        chunk = int(self.chunk_idx[index])
        if chunk != self._cached_index:
            self._cached_data = None
            self._cached_data = self._read_chunk(chunk)
            self._cached_index = chunk
        assert self._cached_data is not None
        return self._cached_data[self.row_idx[index]]

    def verify(self) -> None:
        """Read each chunk once to validate its dtype, shape, and ZIP CRC."""
        with self.reading():
            for index in range(len(self.chunks)):
                self._read_chunk(index)


def read_chunked_array_output(
    fs: AbstractFileSystem, path: str
) -> tuple[ChunkedArray, dict[str, np.ndarray], np.ndarray | None]:
    """Read only the format and metadata; payload chunks remain on disk."""
    with _open_archive(fs, path) as stream, ZipFile(stream) as archive:
        header = json.loads(archive.read("format.json"))
        if header.get("version") != 1:
            raise ValueError(f"Unsupported array-output archive version in {path}.")
        dtype = np.lib.format.descr_to_dtype(header["dtype"])
        if dtype.hasobject:
            raise ValueError(f"Object dtype is not supported in {path}.")
        chunks = tuple((item["name"], tuple(item["shape"])) for item in header["chunks"])
        with np.load(
            BytesIO(archive.read("metadata.npz")), allow_pickle=False
        ) as metadata:
            arrays = {name: metadata[name] for name in METADATA_NAMES}
            if "member_idx" in metadata.files:
                arrays["member_idx"] = metadata["member_idx"]
            chunk_idx, row_idx = metadata["chunk_idx"], metadata["row_idx"]
        names = set(archive.namelist())
    if arrays["run_ID"].ndim != 1:
        raise ValueError(f"Invalid array-output index in {path}.")
    count = len(arrays["run_ID"])
    if (
        not count
        or not chunks
        or any(a.shape != (count,) or a.dtype.kind not in "iu" for a in arrays.values())
        or any(arrays[name].dtype != np.dtype(np.uint32) for name in METADATA_NAMES)
        or chunk_idx.shape != (count,)
        or row_idx.shape != (count,)
        or chunk_idx.dtype != np.dtype(np.uint32)
        or row_idx.dtype != np.dtype(np.uint32)
        or np.any(chunk_idx >= len(chunks))
    ):
        raise ValueError(f"Invalid array-output index in {path}.")
    members = arrays.get("member_idx", np.full(count, -1, dtype=np.int64))
    if np.any(members < -1) or members.dtype.kind != "i":
        raise ValueError(f"Invalid member indices in {path}.")
    runs = arrays["run_ID"]
    if not np.array_equal(np.lexsort((members, arrays["run_ID"])), np.arange(count)) or (
        np.any((runs[1:] == runs[:-1]) & (members[1:] == members[:-1]))
    ):
        raise ValueError(f"Array-output index must be sorted and unique in {path}.")
    if any(not shape or shape[0] < 1 for _, shape in chunks):
        raise ValueError(f"Invalid array-output chunk shapes in {path}.")
    shapes = np.empty((count, len(chunks[0][1]) - 1), dtype=np.int32)
    chunk_order = np.argsort(chunk_idx, kind="stable")
    boundaries = np.concatenate(
        ([0], np.cumsum(np.bincount(chunk_idx, minlength=len(chunks))))
    )
    for index, (name, shape) in enumerate(chunks):
        indices = chunk_order[boundaries[index] : boundaries[index + 1]]
        if (
            name != f"chunks/{index:06d}.npy"
            or name not in names
            or len(shape) != shapes.shape[1] + 1
            or any(
                not isinstance(dim, int) or dim < 0 or dim > np.iinfo(np.int32).max
                for dim in shape
            )
            or not np.array_equal(np.sort(row_idx[indices]), np.arange(shape[0]))
        ):
            raise ValueError(f"Invalid array-output chunk index in {path}.")
        shapes[indices] = shape[1:]
    data = ChunkedArray(fs, path, dtype, chunks, chunk_idx, row_idx)
    return data, arrays, shapes if not np.all(shapes == shapes[0]) else None
