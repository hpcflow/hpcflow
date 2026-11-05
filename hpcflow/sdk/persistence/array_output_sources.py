"""Bounded-memory access to live, legacy, and indexed array-output sources."""

from __future__ import annotations

from contextlib import contextmanager
import ast
from dataclasses import dataclass
from pathlib import Path
from tempfile import SpooledTemporaryFile
import struct
from typing import Any, Callable, Iterator, TYPE_CHECKING
from zipfile import BadZipFile, ZIP_STORED, ZipFile
import zlib

import numpy as np
import zstandard as zstd

from hpcflow.sdk.persistence.array_outputs import (
    METADATA_NAMES,
    _open_archive,
    read_chunked_array_output,
)

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem  # type: ignore


class ArrayOutputBuffer(SpooledTemporaryFile[bytes]):
    """Expose the seekable interface ZipFile needs on Python 3.10."""

    def seekable(self) -> bool:
        self.tell()  # raise ValueError after closing, like a regular binary file
        return True


@dataclass
class ArrayOutputSource:
    metadata: dict[str, np.ndarray]
    shapes: np.ndarray
    dtype: np.dtype
    _read_entry: Callable[[int], np.ndarray]
    entry_order: np.ndarray

    def __getitem__(self, index: int) -> np.ndarray:
        shape = self.shapes[index]
        return self._read_entry(index)[tuple(slice(0, int(dim)) for dim in shape)]


def normalize_metadata(
    metadata: dict[str, np.ndarray], name: str
) -> dict[str, np.ndarray]:
    """Range-check IDs before converting signed legacy metadata to uint32."""
    result = {}
    if metadata["run_ID"].ndim != 1:
        raise ValueError(f"Invalid array output {name!r} run_ID metadata.")
    count = len(metadata["run_ID"])
    for key in METADATA_NAMES:
        values = metadata[key]
        if values.shape != (count,) or values.dtype.kind not in "iu":
            raise ValueError(f"Invalid array output {name!r} metadata {key!r}.")
        if values.size and (int(values.min()) < 0 or int(values.max()) > 2**32 - 1):
            raise OverflowError(
                f"Array output {name!r} metadata {key!r} is outside the uint32 range."
            )
        result[key] = values.astype(np.uint32, copy=False)
    if "member_idx" in metadata:
        members = metadata["member_idx"]
        if (
            members.shape != (count,)
            or members.dtype.kind not in "iu"
            or (
                members.size
                and (int(members.min()) < -1 or int(members.max()) > 2**63 - 1)
            )
        ):
            raise ValueError(f"Invalid array output {name!r} member indices.")
        result["member_idx"] = members.astype(np.int64, copy=False)
    return result


def _npy_header(stream: Any) -> tuple[tuple[int, ...], bool, np.dtype]:
    version = np.lib.format.read_magic(stream)
    if version == (1, 0):
        return np.lib.format.read_array_header_1_0(stream)
    if version == (2, 0):
        return np.lib.format.read_array_header_2_0(stream)
    if version == (3, 0):
        length = struct.unpack("<I", stream.read(4))[0]
        if length > 10000:
            raise ValueError("Legacy NPY header exceeds the safe size limit.")
        header = ast.literal_eval(stream.read(length).decode("utf-8"))
        return (
            tuple(header["shape"]),
            header["fortran_order"],
            np.lib.format.descr_to_dtype(header["descr"]),
        )
    raise ValueError(f"Unsupported legacy NPY header version {version}.")


@contextmanager
def open_array_output_source(
    fs: AbstractFileSystem, path: Path
) -> Iterator[ArrayOutputSource]:
    """Keep only metadata and individual entries in RAM, spilling legacy NPZs to disk."""
    if not path.name.endswith(".npz.zst"):
        data, metadata, shapes = read_chunked_array_output(fs, path.as_posix())
        if shapes is None:
            shapes = np.broadcast_to(
                np.asarray(data.shape[1:], dtype=np.int32), (len(data), data.ndim - 1)
            )
        with data.reading():
            yield ArrayOutputSource(
                metadata,
                shapes,
                data.dtype,
                lambda index: data[index],
                np.lexsort((data.row_idx, data.chunk_idx)),
            )
        return
    with ArrayOutputBuffer(max_size=8 * 1024**2) as buffer:
        with _open_archive(fs, path.as_posix()) as compressed:
            zstd.ZstdDecompressor().copy_stream(compressed, buffer)
        buffer.seek(0)
        with np.load(buffer, allow_pickle=False) as contents, ZipFile(buffer) as archive:
            metadata = {key: contents[key] for key in METADATA_NAMES}
            if "member_idx" in contents.files:
                metadata["member_idx"] = contents["member_idx"]
            metadata = normalize_metadata(metadata, str(path))
            count = len(metadata["run_ID"])
            names = (
                ["data"]
                if "data" in contents.files
                else [
                    f"data_{i}"
                    for i in range(sum(k.startswith("data_") for k in contents.files))
                ]
            )
            if not names:
                raise ValueError(f"Missing array data in {path}.")
            headers = []
            progress: list[tuple[int, int]] = []
            for name in names:
                info = archive.getinfo(f"{name}.npy")
                with archive.open(info) as member:
                    shape, fortran, dtype = _npy_header(member)
                    header_bytes = member.tell()
                if not shape or dtype.hasobject:
                    raise ValueError(f"Invalid array data in {path}.")
                buffer.seek(info.header_offset + 26)
                name_length, extra_length = struct.unpack("<HH", buffer.read(4))
                offset = info.header_offset + 30 + name_length + extra_length
                header_crc = 0
                if info.compress_type == ZIP_STORED:
                    buffer.seek(offset)
                    header_crc = zlib.crc32(buffer.read(header_bytes))
                headers.append((info, shape, fortran, dtype, offset + header_bytes))
                progress.append((0, header_crc))
            dtype = headers[0][3]
            ndim = len(headers[0][1]) - 1
            if any(
                header[3] != dtype or len(header[1]) != ndim + 1 for header in headers
            ):
                raise ValueError(f"Inconsistent shape groups in {path}.")
            groups = (
                contents["group_idx"]
                if len(names) > 1 or "data" not in names
                else np.zeros(count, dtype=np.int64)
            )
            rows = contents["row_idx"] if "data" not in names else np.arange(count)
            if (
                groups.shape != (count,)
                or rows.shape != (count,)
                or groups.dtype.kind not in "iu"
                or rows.dtype.kind not in "iu"
                or np.any(groups < 0)
                or np.any(groups >= len(names))
            ):
                raise ValueError(f"Invalid shape-group index in {path}.")
            stored_shapes = np.empty((count, ndim), dtype=np.int32)
            for group, header in enumerate(headers):
                selected = np.flatnonzero(groups == group)
                if not np.array_equal(np.sort(rows[selected]), np.arange(header[1][0])):
                    raise ValueError(f"Invalid shape-group rows in {path}.")
                stored_shapes[selected] = header[1][1:]
            shapes = contents["shape"] if "shape" in contents.files else stored_shapes
            if (
                shapes.shape != (count, ndim)
                or shapes.dtype.kind not in "iu"
                or np.any(shapes < 0)
                or np.any(shapes > stored_shapes)
            ):
                raise ValueError(f"Invalid array shapes in {path}.")

            def read_entry(index: int) -> np.ndarray:
                group = int(groups[index])
                info, shape, fortran, dtype, offset = headers[group]
                row = int(rows[index])
                # Standard live/grouped archives contain uncompressed C-order NPYs.
                if info.compress_type == ZIP_STORED and not fortran:
                    entry_bytes = int(np.prod(shape[1:], dtype=object)) * dtype.itemsize
                    buffer.seek(offset + row * entry_bytes)
                    raw = buffer.read(entry_bytes)
                    if len(raw) != entry_bytes:
                        raise ValueError(f"Truncated array in {path}.")
                    next_row, crc = progress[group]
                    if row == next_row:
                        crc = zlib.crc32(raw, crc)
                        progress[group] = (next_row + 1, crc)
                        if next_row + 1 == shape[0] and crc != info.CRC:
                            raise BadZipFile(f"Bad CRC in array-output source {path}.")
                    else:
                        progress[group] = (-1, 0)
                    if dtype.itemsize == 0:
                        return np.empty(shape[1:], dtype=dtype)
                    return np.frombuffer(raw, dtype=dtype).reshape(shape[1:])
                # Preserve compatibility with externally compressed/Fortran NPZs.
                # This fallback loads one source array, never all source shards.
                return contents[names[int(groups[index])]][row]

            yield ArrayOutputSource(
                metadata, shapes, dtype, read_entry, np.lexsort((rows, groups))
            )
            # Unusual row mappings/partial traversal still get the same CRC
            # protection as np.load, without materializing the full payload.
            for (rows_read, _), header in zip(progress, headers):
                info, shape, fortran, _, _ = header
                if (
                    info.compress_type == ZIP_STORED
                    and not fortran
                    and rows_read not in (0, shape[0])
                ):
                    with archive.open(info) as member:
                        while member.read(1024**2):
                            pass
