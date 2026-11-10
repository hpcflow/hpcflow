"""Indexed member records for arrays belonging to one output parameter."""

from __future__ import annotations

from contextlib import contextmanager
from io import BytesIO
import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
from tempfile import SpooledTemporaryFile
from typing import Any, Iterator, TYPE_CHECKING
from uuid import uuid4
from zipfile import ZIP_DEFLATED, ZIP_STORED, ZipFile

import numpy as np
import zstandard as zstd
from fsspec.implementations.local import LocalFileSystem  # type: ignore

from hpcflow.sdk.persistence.array_outputs import _open_archive

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem  # type: ignore


_FORMAT_VERSION = 1
_INDEX_NAME = "index.json"
_SPOOL_LIMIT = 8 * 1024**2
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


def _schema_id(array_paths: list[str]) -> str:
    payload = json.dumps(array_paths, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _encode_array_mask(array_paths: list[str], values: dict[str, np.ndarray]) -> str:
    mask = bytearray((len(array_paths) + 7) // 8)
    for index, name in enumerate(array_paths):
        if name in values:
            mask[index // 8] |= 1 << (index % 8)
    return base64.b64encode(mask).decode("ascii")


def _decode_array_mask(record: dict[str, Any], array_paths: list[str]) -> bytes:
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


def member_array_output_record_has_array(
    index: dict[str, Any],
    record: dict[str, Any],
    name: str,
    *,
    schema_positions: dict[str, dict[str, int]] | None = None,
) -> bool:
    """Check a record's sparse path mask without opening its compressed payload."""
    schema_id = record["schema_id"]
    if schema_positions is None:
        schema_positions = {
            key: {path: idx for idx, path in enumerate(paths)}
            for key, paths in index["schemas"].items()
        }
    array_idx = schema_positions[schema_id].get(name)
    if array_idx is None:
        return False
    mask = _decode_array_mask(record, index["schemas"][schema_id])
    return bool(mask[array_idx // 8] & (1 << (array_idx % 8)))


def read_member_array_output_index(fs: AbstractFileSystem, path: str) -> dict[str, Any]:
    """Read and validate a member archive's small JSON index."""
    with _open_zip(fs, path) as archive:
        try:
            index = json.loads(archive.read(_INDEX_NAME))
        except KeyError:
            raise ValueError(f"Missing member-array index in {path}.") from None
    if (
        not isinstance(index, dict)
        or type(index.get("version")) is not int
        or index["version"] != _FORMAT_VERSION
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
            or schema_id != _schema_id(array_paths)
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
            _decode_array_mask(record, index["schemas"][record["schema_id"]])
        except ValueError as exc:
            raise ValueError(f"Invalid member-array record in {path}: {exc}") from exc
    return index


def read_member_array_output_record(
    fs: AbstractFileSystem,
    path: str,
    run_ID: int,
    member_idx: int,
    *,
    index: dict[str, Any] | None = None,
) -> dict[str, np.ndarray]:
    """Read one independently compressed member and return arrays by logical path."""
    index = index or read_member_array_output_index(fs, path)
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
    mask = _decode_array_mask(record, array_paths)
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
        with source, SpooledTemporaryFile(max_size=_SPOOL_LIMIT) as buffer:
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


def write_member_array_output_archive(
    path: Path,
    output_name: str,
    run_ID: int,
    run_file_ID: int,
    metadata: dict[str, int],
    members: dict[int, dict[str, np.ndarray]],
) -> None:
    """Write member records into the execution shard for one output parameter."""
    if not members:
        raise ValueError(f"No member arrays supplied for output {output_name!r}.")
    if run_ID < 0 or run_file_ID < 0 or any(member_idx < 0 for member_idx in members):
        raise ValueError("Run and member indices must be nonnegative.")
    if set(metadata) != set(_RECORD_METADATA_NAMES) or not all(
        type(value) is int and 0 <= value <= 2**32 - 1 for value in metadata.values()
    ):
        raise ValueError("Member-array metadata must contain nonnegative integer IDs.")
    if any(
        not isinstance(name, str)
        or not isinstance(value, np.ndarray)
        or value.dtype.hasobject
        for values in members.values()
        for name, value in values.items()
    ):
        raise TypeError("Member arrays must have string paths and non-object arrays.")

    array_paths = sorted({name for values in members.values() for name in values})
    schema_id = _schema_id(array_paths)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{uuid4().hex}.partial")
    records: list[dict[str, Any]] = []
    schemas: dict[str, list[str]] = {}
    previous_index = (
        read_member_array_output_index(LocalFileSystem(), path.as_posix())
        if path.exists()
        else None
    )
    if previous_index is not None and previous_index["output_name"] != output_name:
        raise ValueError(
            f"Member archive {path} belongs to {previous_index['output_name']!r}, "
            f"not {output_name!r}."
        )
    try:
        with ZipFile(temp_path, "w", compression=ZIP_STORED) as archive:
            if previous_index is not None:
                replaced_run_records = [
                    record
                    for record in previous_index["records"]
                    if record["run_ID"] != run_ID
                ]
                with ZipFile(path, "r") as previous:
                    for record in replaced_run_records:
                        with previous.open(record["entry"]) as source:
                            with archive.open(
                                record["entry"], "w", force_zip64=True
                            ) as destination:
                                shutil.copyfileobj(source, destination)
                        records.append(record)
                        schema_id_ = record["schema_id"]
                        schemas[schema_id_] = previous_index["schemas"][schema_id_]
            for member_idx, values in sorted(members.items()):
                if not values:
                    raise ValueError(
                        f"Member {member_idx} has no arrays for {output_name!r}."
                    )
                entry_name = f"records/{run_ID}_{member_idx}.npz.zst"
                with SpooledTemporaryFile(max_size=_SPOOL_LIMIT) as npz_buffer:
                    with SpooledTemporaryFile(max_size=_SPOOL_LIMIT) as zstd_buffer:
                        with ZipFile(npz_buffer, "w", compression=ZIP_STORED) as npz:
                            for array_idx, array_name in enumerate(array_paths):
                                if array_name not in values:
                                    continue
                                with npz.open(
                                    f"arr_{array_idx}.npy",
                                    "w",
                                    force_zip64=True,
                                ) as array_stream:
                                    np.save(
                                        array_stream,
                                        values[array_name],
                                        allow_pickle=False,
                                    )
                        npz_buffer.seek(0)
                        zstd.ZstdCompressor(level=1).copy_stream(npz_buffer, zstd_buffer)
                        zstd_buffer.seek(0)
                        with archive.open(
                            entry_name, "w", force_zip64=True
                        ) as destination:
                            shutil.copyfileobj(zstd_buffer, destination)
                records.append(
                    {
                        "run_ID": run_ID,
                        "member_idx": member_idx,
                        "metadata": dict(metadata),
                        "schema_id": schema_id,
                        "array_mask": _encode_array_mask(array_paths, values),
                        "entry": entry_name,
                    }
                )
            schemas[schema_id] = array_paths
            records.sort(key=lambda item: (item["run_ID"], item["member_idx"]))
            archive.writestr(
                _INDEX_NAME,
                json.dumps(
                    {
                        "version": _FORMAT_VERSION,
                        "output_name": output_name,
                        "schemas": schemas,
                        "records": records,
                    },
                    separators=(",", ":"),
                ),
                compress_type=ZIP_DEFLATED,
            )
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def consolidate_member_array_output_archives(
    fs: AbstractFileSystem,
    paths: list[Path],
    destination: Path,
    output_name: str,
) -> dict[str, Any]:
    """Merge member records into one indexed archive without recompressing payloads."""
    records: list[dict[str, Any]] = []
    schemas: dict[str, list[str]] = {}
    entries_seen: set[tuple[int, int]] = set()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_name(f".{destination.name}.{uuid4().hex}.partial")
    try:
        with ZipFile(temp_path, "w", compression=ZIP_STORED) as target:
            for path in paths:
                path_s = path.as_posix()
                index = read_member_array_output_index(fs, path_s)
                if index["output_name"] != output_name:
                    raise ValueError(
                        f"Member archive {path} belongs to {index['output_name']!r}, "
                        f"not {output_name!r}."
                    )
                for schema_id, array_paths in index["schemas"].items():
                    if schema_id in schemas and schemas[schema_id] != array_paths:
                        raise ValueError(f"Conflicting member-array schema {schema_id}.")
                    schemas[schema_id] = array_paths
                with _open_zip(fs, path_s) as source:
                    for source_record in index["records"]:
                        key = source_record["run_ID"], source_record["member_idx"]
                        if key in entries_seen:
                            raise ValueError(
                                f"Duplicate member-array record {key} for "
                                f"{output_name!r}."
                            )
                        entries_seen.add(key)
                        record = dict(source_record)
                        record["entry"] = f"records/{key[0]}_{key[1]}.npz.zst"
                        with source.open(source_record["entry"]) as member:
                            with target.open(
                                record["entry"], "w", force_zip64=True
                            ) as output:
                                shutil.copyfileobj(member, output)
                        records.append(record)
            if not records:
                raise ValueError(f"No member records found for output {output_name!r}.")
            records.sort(key=lambda item: (item["run_ID"], item["member_idx"]))
            index = {
                "version": _FORMAT_VERSION,
                "output_name": output_name,
                "schemas": schemas,
                "records": records,
            }
            target.writestr(
                _INDEX_NAME,
                json.dumps(index, separators=(",", ":")),
                compress_type=ZIP_DEFLATED,
            )
        with ZipFile(temp_path, "r") as archive:
            if (corrupt_entry := archive.testzip()) is not None:
                raise ValueError(f"Corrupt member-array archive entry {corrupt_entry!r}.")
        os.replace(temp_path, destination)
    finally:
        temp_path.unlink(missing_ok=True)
    read_member_array_output_index(fs, destination.as_posix())
    return index
