"""Metadata and mzML extraction helpers."""

from __future__ import annotations

import hashlib
import html
import json
import os
import platform
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCAN_NUMBER_RE = re.compile(r"(?:^|\s)scan=(\d+)(?:\s|$)")
SPECTRUM_LIST_RE = re.compile(br"<(?:[A-Za-z_][\w.\-]*:)?spectrumList\b")
SPECTRUM_LIST_START_RE = re.compile(
    rb"<(?:[A-Za-z_][\w.\-]*:)?spectrumList\b(?P<attrs>[^>]*)>",
    re.DOTALL,
)
ATTR_RE = re.compile(br"([A-Za-z_][\w:.\-]*)\s*=\s*([\"'])(.*?)\2", re.DOTALL)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def source_sha256(path: str | os.PathLike[str], chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def to_jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def dumps_json(value: Any) -> str:
    return json.dumps(to_jsonable(value), sort_keys=True, separators=(",", ":"))


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_unit(unit: Any, *, default: str | None = None) -> str | None:
    if unit is None:
        return default
    text = str(unit).strip()
    lowered = text.lower()
    if lowered in {"minute", "minutes", "min", "mins", "uo:0000031"}:
        return "minute"
    if lowered in {"second", "seconds", "sec", "secs", "s", "uo:0000010"}:
        return "second"
    if lowered in {
        "millisecond",
        "milliseconds",
        "ms",
        "msec",
        "msecs",
        "uo:0000028",
    }:
        return "millisecond"
    return text or default


def unit_of(value: Any) -> str | None:
    for attr in ("unit_info", "unit", "unit_name", "unitName"):
        unit = getattr(value, attr, None)
        if unit:
            return str(unit)
    return None


def numeric_with_unit(value: Any, *, default_unit: str | None = None):
    return as_float(value), normalize_unit(unit_of(value), default=default_unit)


def parse_scan_number(native_id: str | None) -> int | None:
    if not native_id:
        return None
    match = SCAN_NUMBER_RE.search(native_id)
    if match is None:
        return None
    return int(match.group(1))


def first_nested(mapping: Mapping[str, Any] | None, *path, default=None):
    current = mapping
    for part in path:
        if current is None:
            return default
        if isinstance(part, int):
            if not isinstance(current, (list, tuple)) or len(current) <= part:
                return default
            current = current[part]
        else:
            if not isinstance(current, Mapping) or part not in current:
                return default
            current = current[part]
    return current


def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


HEADER_XML_KEYS = {
    "cvList": "cv_list_xml",
    "fileDescription": "file_description_xml",
    "referenceableParamGroupList": "referenceable_param_groups_xml",
    "softwareList": "software_list_xml",
    "instrumentConfigurationList": "instrument_configuration_xml",
    "dataProcessingList": "data_processing_xml",
    "sourceFileList": "source_file_list_xml",
}


def read_mzml_header_prefix(
    path: str | os.PathLike[str],
    *,
    chunk_size: int = 1024 * 1024,
    max_header_bytes: int = 64 * 1024 * 1024,
) -> bytes:
    """Read bytes before ``spectrumList`` without parsing the whole mzML file."""
    data = bytearray()
    with Path(path).open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                return bytes(data)
            data.extend(chunk)
            match = SPECTRUM_LIST_RE.search(data)
            if match is not None:
                return bytes(data[: match.start()])
            if len(data) > max_header_bytes:
                raise ValueError(
                    f"mzML header exceeded {max_header_bytes} bytes before spectrumList"
                )


def read_mzml_prefix_through_spectrum_list(
    path: str | os.PathLike[str],
    *,
    chunk_size: int = 1024 * 1024,
    max_header_bytes: int = 64 * 1024 * 1024,
) -> bytes:
    """Read bytes through the opening ``spectrumList`` tag."""
    data = bytearray()
    with Path(path).open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                return bytes(data)
            data.extend(chunk)
            match = SPECTRUM_LIST_START_RE.search(data)
            if match is not None:
                return bytes(data[: match.end()])
            if len(data) > max_header_bytes:
                raise ValueError(
                    "mzML prefix exceeded "
                    f"{max_header_bytes} bytes before spectrumList start tag"
                )


def extract_xml_fragment(header: bytes, tag: str) -> str | None:
    tag_bytes = tag.encode("ascii")
    pattern = re.compile(
        rb"<(?:[A-Za-z_][\w.\-]*:)?" + tag_bytes +
        rb"\b[^>]*>.*?</(?:[A-Za-z_][\w.\-]*:)?" + tag_bytes + rb">",
        re.DOTALL,
    )
    match = pattern.search(header)
    if match is None:
        return None
    return match.group(0).decode("utf-8", errors="replace")


def extract_run_attributes(header: bytes) -> dict[str, str]:
    pattern = re.compile(
        rb"<(?:[A-Za-z_][\w.\-]*:)?run\b(?P<attrs>[^>]*)>",
        re.DOTALL,
    )
    match = pattern.search(header)
    if match is None:
        return {}
    attrs = {}
    for attr_match in ATTR_RE.finditer(match.group("attrs")):
        key = attr_match.group(1).decode("utf-8", errors="replace")
        value = attr_match.group(3).decode("utf-8", errors="replace")
        attrs[key] = html.unescape(value)
    return {
        "run_id": attrs.get("id", ""),
        "run_start_time": attrs.get("startTimeStamp", ""),
        "instrument_config_ref": attrs.get("defaultInstrumentConfigurationRef", ""),
    }


def extract_spectrum_list_attributes(prefix: bytes) -> dict[str, str]:
    match = SPECTRUM_LIST_START_RE.search(prefix)
    if match is None:
        return {}
    attrs = {}
    for attr_match in ATTR_RE.finditer(match.group("attrs")):
        key = attr_match.group(1).decode("utf-8", errors="replace")
        value = attr_match.group(3).decode("utf-8", errors="replace")
        attrs[key] = html.unescape(value)
    return {
        "spectrum_list_default_data_processing_ref": attrs.get(
            "defaultDataProcessingRef", ""
        ),
    }


def extract_header_metadata(path: str | os.PathLike[str]) -> dict[str, str]:
    """Extract mzML run attributes and selected header XML fragments."""
    metadata: dict[str, str] = {}
    header = read_mzml_header_prefix(path)
    spectrum_prefix = read_mzml_prefix_through_spectrum_list(path)
    metadata.update(extract_run_attributes(header))
    metadata.update(extract_spectrum_list_attributes(spectrum_prefix))
    for tag, key in HEADER_XML_KEYS.items():
        fragment = extract_xml_fragment(header, tag)
        if fragment is not None:
            metadata[key] = fragment
    return metadata


def provenance_metadata(
    source_path: str | os.PathLike[str],
    *,
    creator: str,
    mzduck_version: str,
    compute_sha256: bool,
) -> dict[str, str]:
    path = Path(source_path)
    stat = path.stat()
    try:
        import duckdb

        duckdb_version = duckdb.__version__
    except Exception:
        duckdb_version = ""
    metadata = {
        "schema_version": "1",
        "format_name": "mzDuck",
        "creator": creator,
        "mzduck_version": mzduck_version,
        "duckdb_version": duckdb_version,
        "python_version": platform.python_version(),
        "conversion_timestamp": now_utc_iso(),
        "source_path": str(path.resolve()),
        "source_filename": path.name,
        "source_size_bytes": str(stat.st_size),
        "source_sha256": source_sha256(path) if compute_sha256 else "",
        "source_format": "mzML",
    }
    return metadata
