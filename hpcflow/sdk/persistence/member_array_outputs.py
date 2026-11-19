"""Indexed per-run records for arrays belonging to one output parameter."""

from __future__ import annotations

from contextlib import contextmanager
from io import BytesIO
import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
from typing import Any, Iterator, TYPE_CHECKING
from uuid import uuid4
from zipfile import ZIP_DEFLATED, ZIP_STORED, ZipFile

import numpy as np
import zstandard as zstd
from fsspec.implementations.local import LocalFileSystem  # type: ignore

from hpcflow.sdk.persistence.array_outputs import _open_archive
from hpcflow.sdk.persistence.array_output_sources import ArrayOutputBuffer

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem  # type: ignore


_FORMAT_VERSION = 2
_LEGACY_FORMAT_VERSION = 1
_FORMAT_NAME = "format.json"
_INDEX_NAME = "index.npz"
_PATHS_NAME = "paths.json"
_SPOOL_LIMIT = 8 * 1024**2
_SMALL_ARRAY_BYTES = 64 * 1024
_RECORD_METADATA_NAMES = (
    "task_insert_ID",
    "task_idx",
    "element_idx",
    "element_ID",
    "iteration_idx",
    "iteration_ID",
    "action_idx",
    "run_idx",
)


@contextmanager
def _open_zip(fs: AbstractFileSystem, path: str) -> Iterator[ZipFile]:
    with _open_archive(fs, path) as stream, ZipFile(stream) as archive:
        yield archive


def _array_id(name: str) -> str:
    return hashlib.sha256(name.encode("utf-8")).hexdigest()


def _legacy_schema_id(array_paths: list[str]) -> str:
    payload = json.dumps(array_paths, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _dtype_info(dtype: np.dtype) -> Any:
    return np.lib.format.dtype_to_descr(dtype)


def _dtype_from_info(info: Any) -> np.dtype:
    return np.lib.format.descr_to_dtype(info)


def _merge_dtype_info(dtypes: dict[str, Any], values: dict[str, np.ndarray]) -> None:
    for name, value in values.items():
        existing_info = dtypes.get(name)
        if existing_info is None:
            dtypes[name] = _dtype_info(value.dtype)
            continue
        existing = _dtype_from_info(existing_info)
        if existing == value.dtype:
            continue
        if existing.kind == value.dtype.kind and existing.kind in "US":
            dtypes[name] = _dtype_info(np.result_type(existing, value.dtype))
            continue
        raise TypeError(
            f"Cannot append to array output {name!r}: dtype {value.dtype} is "
            f"incompatible with existing dtype {existing}."
        )


def _record_prefix(run_ID: int, member_idx: int) -> str:
    return f"records/{run_ID}_{member_idx}"


def _encode_array_mask(array_paths: list[str], values: dict[str, np.ndarray]) -> bytes:
    mask = bytearray((len(array_paths) + 7) // 8)
    for index, name in enumerate(array_paths):
        if name in values:
            mask[index // 8] |= 1 << (index % 8)
    return bytes(mask)


def _decode_legacy_array_mask(record: dict[str, Any], array_paths: list[str]) -> bytes:
    try:
        mask = base64.b64decode(record["array_mask"], validate=True)
    except (KeyError, TypeError, ValueError):
        raise ValueError("Invalid member-array presence mask.") from None
    expected_size = (len(array_paths) + 7) // 8
    if len(mask) != expected_size:
        raise ValueError("Invalid member-array presence mask size.")
    if array_paths and len(array_paths) % 8:
        valid_bits = (1 << (len(array_paths) % 8)) - 1
        if mask[-1] & ~valid_bits:
            raise ValueError("Invalid member-array presence mask padding.")
    return mask


def _decode_array_mask(mask: bytes, array_paths: list[str], path: str) -> bytes:
    expected_size = (len(array_paths) + 7) // 8
    if len(mask) != expected_size:
        raise ValueError(f"Invalid array presence mask in {path}.")
    if array_paths and len(array_paths) % 8:
        valid_bits = (1 << (len(array_paths) % 8)) - 1
        if mask[-1] & ~valid_bits:
            raise ValueError(f"Invalid array presence mask padding in {path}.")
    return mask


def _decode_v1_index(archive: ZipFile, path: str) -> dict[str, Any]:
    try:
        index = json.loads(archive.read("index.json"))
    except KeyError:
        raise ValueError(f"Missing member-array index in {path}.") from None
    if (
        not isinstance(index, dict)
        or type(index.get("version")) is not int
        or index["version"] != _LEGACY_FORMAT_VERSION
        or not isinstance(index.get("output_name"), str)
        or not index["output_name"]
        or not isinstance(index.get("schemas"), dict)
        or not isinstance(index.get("records"), list)
    ):
        raise ValueError(f"Invalid member-array index in {path}.")
    for schema_id, array_paths in index["schemas"].items():
        if (
            not isinstance(schema_id, str)
            or not isinstance(array_paths, list)
            or not array_paths
            or any(not isinstance(name, str) for name in array_paths)
            or array_paths != sorted(set(array_paths))
            or schema_id != _legacy_schema_id(array_paths)
        ):
            raise ValueError(f"Invalid member-array schema in {path}.")

    seen: set[tuple[int, int]] = set()
    seen_entries: set[str] = set()
    for record in index["records"]:
        if (
            not isinstance(record, dict)
            or type(record.get("run_ID")) is not int
            or type(record.get("member_idx")) is not int
            or record["run_ID"] < 0
            or record["member_idx"] < 0
            or not isinstance(record.get("entry"), str)
            or not isinstance(record.get("metadata"), dict)
            or set(record["metadata"]) != set(_RECORD_METADATA_NAMES)
            or any(
                type(value) is not int or value < 0 or value > 2**32 - 1
                for value in record["metadata"].values()
            )
            or not isinstance(record.get("schema_id"), str)
            or record["schema_id"] not in index["schemas"]
        ):
            raise ValueError(f"Invalid member-array record in {path}.")
        key = record["run_ID"], record["member_idx"]
        if key in seen:
            raise ValueError(f"Duplicate member-array record {key} in {path}.")
        seen.add(key)
        if record["entry"] in seen_entries:
            raise ValueError(
                f"Duplicate member-array entry {record['entry']!r} in {path}."
            )
        seen_entries.add(record["entry"])
        try:
            _decode_legacy_array_mask(record, index["schemas"][record["schema_id"]])
        except ValueError as exc:
            raise ValueError(f"Invalid member-array record in {path}: {exc}") from exc
    return index


def _read_v2_index(archive: ZipFile, path: str) -> dict[str, Any]:
    try:
        format_info = json.loads(archive.read(_FORMAT_NAME))
        array_paths = json.loads(archive.read(_PATHS_NAME))
        index_buffer = ArrayOutputBuffer(max_size=_SPOOL_LIMIT)
        index_buffer.write(archive.read(_INDEX_NAME))
        index_buffer.seek(0)
        with index_buffer:
            with np.load(index_buffer, allow_pickle=False) as arrays:
                data = {name: arrays[name] for name in arrays.files}
    except (KeyError, json.JSONDecodeError, ValueError, OSError):
        raise ValueError(f"Invalid array-output index in {path}.") from None
    if (
        not isinstance(format_info, dict)
        or format_info.get("version") != _FORMAT_VERSION
        or not isinstance(format_info.get("output_name"), str)
        or not format_info["output_name"]
        or not isinstance(format_info.get("dtypes"), dict)
        or not isinstance(array_paths, list)
        or not array_paths
        or any(not isinstance(name, str) or not name for name in array_paths)
        or array_paths != sorted(set(array_paths))
    ):
        raise ValueError(f"Invalid array-output index in {path}.")

    expected_names = {
        "run_ID",
        "member_idx",
        *_RECORD_METADATA_NAMES,
        "array_mask",
        "small_mask",
    }
    if set(data) != expected_names:
        raise ValueError(f"Invalid array-output index arrays in {path}.")
    if set(format_info["dtypes"]) != set(array_paths):
        raise ValueError(f"Invalid array-output dtypes in {path}.")
    for name, info in format_info["dtypes"].items():
        if not isinstance(name, str):
            raise ValueError(f"Invalid array-output dtype name in {path}.")
        try:
            dtype = _dtype_from_info(info)
        except (TypeError, ValueError):
            raise ValueError(
                f"Invalid array-output dtype for {name!r} in {path}."
            ) from None
        if dtype.hasobject:
            raise ValueError(f"Object dtype for array output {name!r} in {path}.")
    count = len(data["run_ID"])
    mask_size = (len(array_paths) + 7) // 8
    if (
        data["run_ID"].shape != (count,)
        or data["run_ID"].dtype.kind not in "iu"
        or data["member_idx"].shape != (count,)
        or data["member_idx"].dtype.kind not in "iu"
        or data["array_mask"].shape != (count, mask_size)
        or data["array_mask"].dtype != np.dtype("uint8")
        or data["small_mask"].shape != (count, mask_size)
        or data["small_mask"].dtype != np.dtype("uint8")
    ):
        raise ValueError(f"Invalid array-output index shapes in {path}.")
    for name in _RECORD_METADATA_NAMES:
        if data[name].shape != (count,) or data[name].dtype.kind not in "iu":
            raise ValueError(f"Invalid array-output metadata {name!r} in {path}.")
    records: list[dict[str, Any]] = []
    seen: set[tuple[int, int]] = set()
    previous_key: tuple[int, int] | None = None
    for row in range(count):
        run_ID, member_idx = int(data["run_ID"][row]), int(data["member_idx"][row])
        key = run_ID, member_idx
        if (
            run_ID < 0
            or member_idx < -1
            or key in seen
            or (previous_key is not None and key < previous_key)
        ):
            raise ValueError(f"Invalid or duplicate array-output record {key} in {path}.")
        seen.add(key)
        previous_key = key
        array_mask = bytes(data["array_mask"][row])
        small_mask = bytes(data["small_mask"][row])
        _decode_array_mask(array_mask, array_paths, path)
        _decode_array_mask(small_mask, array_paths, path)
        if any(small & ~present for small, present in zip(small_mask, array_mask)):
            raise ValueError(f"Invalid small-array mask in {path}.")
        present_paths = [
            name
            for idx, name in enumerate(array_paths)
            if array_mask[idx // 8] & (1 << (idx % 8))
        ]
        small_paths = [
            name
            for idx, name in enumerate(array_paths)
            if small_mask[idx // 8] & (1 << (idx % 8))
        ]
        if not present_paths:
            raise ValueError(f"Empty array-output record {key} in {path}.")
        records.append(
            {
                "run_ID": run_ID,
                "member_idx": member_idx,
                "metadata": {
                    name: int(data[name][row]) for name in _RECORD_METADATA_NAMES
                },
                "array_names": present_paths,
                "small_names": small_paths,
            }
        )
    return {
        "version": _FORMAT_VERSION,
        "output_name": format_info["output_name"],
        "paths": array_paths,
        "dtypes": format_info["dtypes"],
        "records": records,
    }


def read_member_array_output_index(fs: AbstractFileSystem, path: str) -> dict[str, Any]:
    """Read and validate a member-array archive's index."""
    with _open_zip(fs, path) as archive:
        try:
            format_info = json.loads(archive.read(_FORMAT_NAME))
        except KeyError:
            return _decode_v1_index(archive, path)
        if not isinstance(format_info, dict):
            raise ValueError(f"Invalid array-output index in {path}.")
        if format_info.get("version") == _LEGACY_FORMAT_VERSION:
            return _decode_v1_index(archive, path)
        if format_info.get("version") != _FORMAT_VERSION:
            raise ValueError(f"Unsupported array-output format in {path}.")
        return _read_v2_index(archive, path)


def member_array_output_record_has_array(
    index: dict[str, Any],
    record: dict[str, Any],
    name: str,
    *,
    schema_positions: dict[str, dict[str, int]] | None = None,
) -> bool:
    """Check a record's sparse path mask without opening its compressed payload."""
    if index["version"] == _FORMAT_VERSION:
        try:
            array_idx = index["paths"].index(name)
        except ValueError:
            return False
        return name in record["array_names"]
    schema_id = record["schema_id"]
    if schema_positions is None:
        schema_positions = {
            key: {path: idx for idx, path in enumerate(paths)}
            for key, paths in index["schemas"].items()
        }
    array_idx = schema_positions[schema_id].get(name)
    if array_idx is None:
        return False
    mask = _decode_legacy_array_mask(record, index["schemas"][schema_id])
    return bool(mask[array_idx // 8] & (1 << (array_idx % 8)))


def _read_v1_record(
    fs: AbstractFileSystem,
    path: str,
    run_ID: int,
    member_idx: int,
    index: dict[str, Any],
) -> dict[str, np.ndarray]:
    record = next(
        (
            item
            for item in index["records"]
            if item["run_ID"] == run_ID and item["member_idx"] == member_idx
        ),
        None,
    )
    if record is None:
        raise KeyError((run_ID, member_idx))
    array_paths = index["schemas"][record["schema_id"]]
    mask = _decode_legacy_array_mask(record, array_paths)
    array_indices = [
        idx for idx in range(len(array_paths)) if mask[idx // 8] & (1 << (idx % 8))
    ]
    with _open_zip(fs, path) as archive:
        try:
            source = archive.open(record["entry"])
        except KeyError:
            raise ValueError(
                f"Missing member-array record {record['entry']!r} in {path}."
            ) from None
        with source, ArrayOutputBuffer(max_size=_SPOOL_LIMIT) as buffer:
            zstd.ZstdDecompressor().copy_stream(source, buffer)
            buffer.seek(0)
            with np.load(buffer, allow_pickle=False) as arrays:
                expected_keys = {f"arr_{idx}" for idx in array_indices}
                if set(arrays.files) != expected_keys:
                    raise ValueError(f"Invalid arrays in member record in {path}.")
                result = {array_paths[idx]: arrays[f"arr_{idx}"] for idx in array_indices}
    if any(value.dtype.hasobject for value in result.values()):
        raise ValueError(f"Object dtype in member-array record in {path}.")
    return result


def _read_npy_zstd(archive: ZipFile, entry: str, path: str) -> np.ndarray:
    try:
        source = archive.open(entry)
    except KeyError:
        raise ValueError(f"Missing array entry {entry!r} in {path}.") from None
    with source, ArrayOutputBuffer(max_size=_SPOOL_LIMIT) as buffer:
        zstd.ZstdDecompressor().copy_stream(source, buffer)
        buffer.seek(0)
        value = np.load(buffer, allow_pickle=False)
    if not isinstance(value, np.ndarray) or value.dtype.hasobject:
        raise ValueError(f"Invalid array entry {entry!r} in {path}.")
    return value


def _read_v2_record_from_archive(
    archive: ZipFile,
    path: str,
    record: dict[str, Any],
    name: str | None,
) -> dict[str, np.ndarray]:
    run_ID = record["run_ID"]
    member_idx = record["member_idx"]
    selected = (
        record["array_names"]
        if name is None
        else ([name] if name in record["array_names"] else [])
    )
    if not selected:
        return {}
    small_names = set(record["small_names"])
    selected_small = [item for item in selected if item in small_names]
    result: dict[str, np.ndarray] = {}
    if selected_small:
        entry = f"{_record_prefix(run_ID, member_idx)}/small.npz.zst"
        try:
            source = archive.open(entry)
        except KeyError:
            raise ValueError(f"Missing small-array group {entry!r} in {path}.") from None
        with source, ArrayOutputBuffer(max_size=_SPOOL_LIMIT) as buffer:
            zstd.ZstdDecompressor().copy_stream(source, buffer)
            buffer.seek(0)
            with np.load(buffer, allow_pickle=False) as arrays:
                expected = {f"a_{_array_id(array_name)}" for array_name in small_names}
                if set(arrays.files) != expected:
                    raise ValueError(f"Invalid small-array group in {path}.")
                result.update(
                    {
                        array_name: arrays[f"a_{_array_id(array_name)}"]
                        for array_name in selected_small
                    }
                )
    for array_name in selected:
        if array_name in small_names:
            continue
        entry = (
            f"{_record_prefix(run_ID, member_idx)}/" f"{_array_id(array_name)}.npy.zst"
        )
        result[array_name] = _read_npy_zstd(archive, entry, path)
    if any(value.dtype.hasobject for value in result.values()):
        raise ValueError(f"Object dtype in member-array record in {path}.")
    return result


def _read_v2_record(
    fs: AbstractFileSystem,
    path: str,
    run_ID: int,
    member_idx: int,
    index: dict[str, Any],
    name: str | None,
) -> dict[str, np.ndarray]:
    record = next(
        (
            item
            for item in index["records"]
            if item["run_ID"] == run_ID and item["member_idx"] == member_idx
        ),
        None,
    )
    if record is None:
        raise KeyError((run_ID, member_idx))
    with _open_zip(fs, path) as archive:
        return _read_v2_record_from_archive(archive, path, record, name)


def read_member_array_output_record(
    fs: AbstractFileSystem,
    path: str,
    run_ID: int,
    member_idx: int,
    *,
    index: dict[str, Any] | None = None,
    name: str | None = None,
) -> dict[str, np.ndarray]:
    """Read one run/member record, optionally selecting one logical array path."""
    index = index or read_member_array_output_index(fs, path)
    if index["version"] == _LEGACY_FORMAT_VERSION:
        result = _read_v1_record(fs, path, run_ID, member_idx, index)
        return result if name is None else {name: result[name]} if name in result else {}
    return _read_v2_record(fs, path, run_ID, member_idx, index, name)


def read_member_array_output_records(
    fs: AbstractFileSystem,
    path: str,
    record_keys: list[tuple[int, int]],
    *,
    index: dict[str, Any],
    name: str,
) -> dict[tuple[int, int], dict[str, np.ndarray]]:
    """Read selected paths from multiple records while opening the archive once."""
    records = {
        (record["run_ID"], record["member_idx"]): record for record in index["records"]
    }
    if index["version"] == _LEGACY_FORMAT_VERSION:
        return {
            key: read_member_array_output_record(fs, path, *key, index=index, name=name)
            for key in record_keys
        }
    with _open_zip(fs, path) as archive:
        result = {}
        for key in record_keys:
            try:
                record = records[key]
            except KeyError:
                raise KeyError(key) from None
            result[key] = _read_v2_record_from_archive(archive, path, record, name)
        return result


def _write_record_payload(
    archive: ZipFile,
    run_ID: int,
    member_idx: int,
    values: dict[str, np.ndarray],
) -> list[str]:
    prefix = _record_prefix(run_ID, member_idx)
    small = {
        name: value
        for name, value in values.items()
        if value.nbytes <= _SMALL_ARRAY_BYTES
    }
    large = {name: value for name, value in values.items() if name not in small}
    if small:
        with ArrayOutputBuffer(max_size=_SPOOL_LIMIT) as npz_buffer:
            with ZipFile(npz_buffer, "w", compression=ZIP_STORED) as npz:
                for name, value in small.items():
                    with npz.open(
                        f"a_{_array_id(name)}.npy", "w", force_zip64=True
                    ) as out:
                        np.save(out, value, allow_pickle=False)
            npz_buffer.seek(0)
            with archive.open(f"{prefix}/small.npz.zst", "w", force_zip64=True) as out:
                zstd.ZstdCompressor(level=1).copy_stream(npz_buffer, out)
    for name, value in large.items():
        with ArrayOutputBuffer(max_size=_SPOOL_LIMIT) as npy_buffer:
            np.save(npy_buffer, value, allow_pickle=False)
            npy_buffer.seek(0)
            entry = f"{prefix}/{_array_id(name)}.npy.zst"
            with archive.open(entry, "w", force_zip64=True) as out:
                zstd.ZstdCompressor(level=1).copy_stream(npy_buffer, out)
    return sorted(small)


def _copy_record_payload(
    source: ZipFile,
    destination: ZipFile,
    run_ID: int,
    member_idx: int,
    array_names: list[str],
    small_names: list[str],
) -> None:
    prefix = _record_prefix(run_ID, member_idx)
    entries = ([f"{prefix}/small.npz.zst"] if small_names else []) + [
        f"{prefix}/{_array_id(name)}.npy.zst"
        for name in array_names
        if name not in small_names
    ]
    for entry in entries:
        try:
            member = source.open(entry)
        except KeyError:
            raise ValueError(
                f"Missing array entry {entry!r} during consolidation."
            ) from None
        with member, destination.open(entry, "w", force_zip64=True) as output:
            shutil.copyfileobj(member, output)


def _record_summary(
    run_ID: int,
    member_idx: int,
    metadata: dict[str, int],
    array_names: list[str],
    small_names: list[str],
) -> dict[str, Any]:
    return {
        "run_ID": run_ID,
        "member_idx": member_idx,
        "metadata": metadata,
        "array_names": sorted(array_names),
        "small_names": sorted(small_names),
    }


def _write_v2_index(
    archive: ZipFile,
    output_name: str,
    paths: list[str],
    records: list[dict[str, Any]],
    dtypes: dict[str, Any],
) -> None:
    records.sort(key=lambda item: (item["run_ID"], item["member_idx"]))
    path_positions = {name: idx for idx, name in enumerate(paths)}
    mask_size = (len(paths) + 7) // 8
    array_masks = np.zeros((len(records), mask_size), dtype=np.uint8)
    small_masks = np.zeros((len(records), mask_size), dtype=np.uint8)
    data: dict[str, np.ndarray] = {
        "run_ID": np.asarray([row["run_ID"] for row in records], dtype=np.uint64),
        "member_idx": np.asarray([row["member_idx"] for row in records], dtype=np.int64),
    }
    for name in _RECORD_METADATA_NAMES:
        data[name] = np.asarray(
            [row["metadata"][name] for row in records], dtype=np.uint32
        )
    for row, record in enumerate(records):
        for name in record["array_names"]:
            idx = path_positions[name]
            array_masks[row, idx // 8] |= 1 << (idx % 8)
        for name in record["small_names"]:
            idx = path_positions[name]
            small_masks[row, idx // 8] |= 1 << (idx % 8)
    data["array_mask"] = array_masks
    data["small_mask"] = small_masks
    buffer = ArrayOutputBuffer(max_size=_SPOOL_LIMIT)
    try:
        with ZipFile(buffer, "w", compression=ZIP_STORED) as npz:
            for name, values in data.items():
                with npz.open(f"{name}.npy", "w", force_zip64=True) as member:
                    np.save(member, values, allow_pickle=False)
        buffer.seek(0)
        archive.writestr(
            _FORMAT_NAME,
            json.dumps(
                {
                    "version": _FORMAT_VERSION,
                    "output_name": output_name,
                    "dtypes": dtypes,
                }
            ),
            compress_type=ZIP_DEFLATED,
        )
        archive.writestr(_PATHS_NAME, json.dumps(paths), compress_type=ZIP_DEFLATED)
        with archive.open(_INDEX_NAME, "w", force_zip64=True) as index_member:
            shutil.copyfileobj(buffer, index_member)
    finally:
        buffer.close()


def _convert_v1_records(
    fs: AbstractFileSystem,
    source: ZipFile,
    path: str,
    index: dict[str, Any],
    output: ZipFile,
    *,
    exclude_run_ID: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    converted = []
    dtypes: dict[str, Any] = {}
    for record in index["records"]:
        run_ID, member_idx = record["run_ID"], record["member_idx"]
        if run_ID == exclude_run_ID:
            continue
        values = _read_v1_record(fs, path, run_ID, member_idx, index)
        _merge_dtype_info(dtypes, values)
        small_names = _write_record_payload(output, run_ID, member_idx, values)
        converted.append(
            _record_summary(
                run_ID,
                member_idx,
                dict(record["metadata"]),
                list(values),
                small_names,
            )
        )
    return converted, dtypes


def write_member_array_output_archive(
    path: Path,
    output_name: str,
    run_ID: int,
    run_file_ID: int,
    metadata: dict[str, int],
    members: dict[int, dict[str, np.ndarray]],
) -> None:
    """Append one run's whole-value or member arrays to its shard archive."""
    if not members:
        raise ValueError(f"No arrays supplied for output {output_name!r}.")
    if run_ID < 0 or run_file_ID < 0 or any(member_idx < -1 for member_idx in members):
        raise ValueError("Run IDs must be nonnegative and member indices at least -1.")
    if set(metadata) != set(_RECORD_METADATA_NAMES) or not all(
        type(value) is int and 0 <= value <= 2**32 - 1 for value in metadata.values()
    ):
        raise ValueError("Array-output metadata must contain nonnegative integer IDs.")
    if any(
        not isinstance(name, str)
        or not isinstance(value, np.ndarray)
        or value.dtype.hasobject
        for values in members.values()
        for name, value in values.items()
    ):
        raise TypeError("Arrays must have string paths and non-object NumPy arrays.")
    if any(not values for values in members.values()):
        raise ValueError("Every run/member record must contain at least one array.")

    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{uuid4().hex}.partial")
    previous = (
        read_member_array_output_index(LocalFileSystem(), path.as_posix())
        if path.exists()
        else None
    )
    if previous is not None and previous["output_name"] != output_name:
        raise ValueError(
            f"Array archive {path} belongs to {previous['output_name']!r}, "
            f"not {output_name!r}."
        )
    paths = sorted(
        set(previous.get("paths", [])) if previous and previous["version"] == 2 else set()
    )
    paths = sorted(set(paths).union(*(set(values) for values in members.values())))
    dtypes: dict[str, Any] = (
        dict(previous["dtypes"])
        if previous is not None and previous["version"] == 2
        else {}
    )
    records: list[dict[str, Any]] = []
    try:
        with ZipFile(temp_path, "w", compression=ZIP_STORED) as archive:
            if previous is not None:
                if previous["version"] == 1:
                    with ZipFile(path, "r") as source:
                        converted, converted_dtypes = _convert_v1_records(
                            LocalFileSystem(),
                            source,
                            path.as_posix(),
                            previous,
                            archive,
                            exclude_run_ID=run_ID,
                        )
                        records.extend(converted)
                        dtypes.update(converted_dtypes)
                else:
                    with ZipFile(path, "r") as source:
                        for record in previous["records"]:
                            if record["run_ID"] == run_ID:
                                continue
                            _copy_record_payload(
                                source,
                                archive,
                                record["run_ID"],
                                record["member_idx"],
                                record["array_names"],
                                record["small_names"],
                            )
                            records.append(
                                _record_summary(
                                    record["run_ID"],
                                    record["member_idx"],
                                    dict(record["metadata"]),
                                    record["array_names"],
                                    record["small_names"],
                                )
                            )
            for member_idx, values in sorted(members.items()):
                _merge_dtype_info(dtypes, values)
                small_names = _write_record_payload(archive, run_ID, member_idx, values)
                records.append(
                    _record_summary(
                        run_ID, member_idx, dict(metadata), list(values), small_names
                    )
                )
            paths = sorted({name for record in records for name in record["array_names"]})
            dtypes = {name: dtypes[name] for name in paths if name in dtypes}
            _write_v2_index(archive, output_name, paths, records, dtypes)
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def consolidate_member_array_output_archives(
    fs: AbstractFileSystem,
    paths: list[Path],
    destination: Path,
    output_name: str,
) -> dict[str, Any]:
    """Merge per-run records into one indexed archive without recompressing v2 data."""
    records: list[dict[str, Any]] = []
    all_paths: set[str] = set()
    entries_seen: set[tuple[int, int]] = set()
    dtypes: dict[str, Any] = {}
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_name(f".{destination.name}.{uuid4().hex}.partial")
    try:
        with ZipFile(temp_path, "w", compression=ZIP_STORED) as target:
            for path in paths:
                path_s = path.as_posix()
                index = read_member_array_output_index(fs, path_s)
                if index["output_name"] != output_name:
                    raise ValueError(
                        f"Array archive {path} belongs to {index['output_name']!r}, "
                        f"not {output_name!r}."
                    )
                source_paths = (
                    index["paths"]
                    if index["version"] == _FORMAT_VERSION
                    else sorted(
                        {
                            name
                            for record in index["records"]
                            for idx, name in enumerate(
                                index["schemas"][record["schema_id"]]
                            )
                            if _decode_legacy_array_mask(
                                record, index["schemas"][record["schema_id"]]
                            )[idx // 8]
                            & (1 << (idx % 8))
                        }
                    )
                )
                all_paths.update(source_paths)
                if index["version"] == _FORMAT_VERSION:
                    for name, info in index["dtypes"].items():
                        _merge_dtype_info(
                            dtypes,
                            {name: np.empty(0, dtype=_dtype_from_info(info))},
                        )
                with _open_zip(fs, path_s) as source:
                    if index["version"] == _LEGACY_FORMAT_VERSION:
                        converted, converted_dtypes = _convert_v1_records(
                            fs, source, path_s, index, target
                        )
                        for record in converted:
                            key = record["run_ID"], record["member_idx"]
                            if key in entries_seen:
                                raise ValueError(
                                    f"Duplicate array-output record {key} for "
                                    f"{output_name!r}."
                                )
                            entries_seen.add(key)
                        records.extend(converted)
                        for name, info in converted_dtypes.items():
                            _merge_dtype_info(
                                dtypes,
                                {name: np.empty(0, dtype=_dtype_from_info(info))},
                            )
                        continue
                    for record in index["records"]:
                        key = record["run_ID"], record["member_idx"]
                        if key in entries_seen:
                            raise ValueError(
                                f"Duplicate array-output record {key} for "
                                f"{output_name!r}."
                            )
                        entries_seen.add(key)
                        _copy_record_payload(
                            source,
                            target,
                            record["run_ID"],
                            record["member_idx"],
                            record["array_names"],
                            record["small_names"],
                        )
                        records.append(
                            _record_summary(
                                record["run_ID"],
                                record["member_idx"],
                                dict(record["metadata"]),
                                record["array_names"],
                                record["small_names"],
                            )
                        )
            if not records:
                raise ValueError(f"No array-output records found for {output_name!r}.")
            _write_v2_index(
                target,
                output_name,
                sorted(all_paths),
                records,
                {name: dtypes[name] for name in sorted(all_paths)},
            )
        read_member_array_output_index(fs, temp_path.as_posix())
        os.replace(temp_path, destination)
    finally:
        temp_path.unlink(missing_ok=True)
    return read_member_array_output_index(fs, destination.as_posix())
