"""mzML to mzDuck conversion."""

from __future__ import annotations

import json
import os
import uuid
from collections import Counter
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
from pyteomics import mzml

from . import __version__
from .metadata import (
    as_float,
    as_int,
    extract_header_metadata,
    first_nested,
    normalize_unit,
    numeric_with_unit,
    parse_scan_number,
    provenance_metadata,
    unit_of,
)
from .reconstruction import (
    DEFAULT_MGF_TITLE_TEMPLATE,
    FILTER_STRING_ENCODING_RAW,
    FilterStringDetector,
    infer_scan_template,
)
from .schema import (
    EXTRA_PARAM_COLUMNS,
    MS1_COLUMNS,
    MS2_COLUMNS,
    MSN_COLUMNS,
    TEXT_OVERRIDE_COLUMNS,
    create_scan_index,
    create_schema,
    default_metadata_values,
    metadata_json,
    msn_table_name,
    table_count,
    table_peak_count,
    table_registry,
    upsert_metadata,
    validate_required_schema,
)

ACTIVATION_MAP = {
    "beam-type collision-induced dissociation": "HCD",
    "beam-type collisional dissociation": "HCD",
    "higher-energy collision-induced dissociation": "HCD",
    "collision-induced dissociation": "CID",
    "electron transfer dissociation": "ETD",
    "electron capture dissociation": "ECD",
    "infrared multiphoton dissociation": "IRMPD",
    "supplemental collision-induced dissociation": "SID",
}

NON_ACTIVATION_KEYS = {
    "collision energy",
    "collision energy ramp start",
    "collision energy ramp end",
}

SCALAR_ARROW_TYPES = {
    "scan_number": pa.int32(),
    "source_index": pa.int32(),
    "instrument_configuration_ref": pa.string(),
    "native_id": pa.string(),
    "ms_level": pa.uint8(),
    "rt": pa.float32(),
    "precursor_mz": pa.float64(),
    "precursor_charge": pa.int8(),
    "precursor_intensity": pa.float32(),
    "collision_energy": pa.float32(),
    "activation_type": pa.string(),
    "isolation_window_target": pa.float64(),
    "isolation_window_lower": pa.float32(),
    "isolation_window_upper": pa.float32(),
    "precursor_scan_number": pa.int32(),
    "base_peak_mz": pa.float32(),
    "base_peak_intensity": pa.float32(),
    "tic": pa.float32(),
    "lowest_mz": pa.float32(),
    "highest_mz": pa.float32(),
    "filter_string": pa.string(),
    "ion_injection_time": pa.float32(),
    "monoisotopic_mz": pa.float64(),
    "scan_window_lower": pa.float32(),
    "scan_window_upper": pa.float32(),
    "field_name": pa.string(),
    "value": pa.string(),
    "scope": pa.string(),
    "ordinal": pa.int32(),
    "accession": pa.string(),
    "name": pa.string(),
    "unit_accession": pa.string(),
    "unit_name": pa.string(),
    "cv_ref": pa.string(),
}

TOP_LEVEL_PARAM_SKIP = {
    "index",
    "id",
    "defaultArrayLength",
    "count",
    "scanList",
    "precursorList",
    "m/z array",
    "intensity array",
    "charge array",
    "MS1 spectrum",
    "MSn spectrum",
    "ms level",
    "positive scan",
    "negative scan",
    "centroid spectrum",
    "profile spectrum",
    "base peak m/z",
    "base peak intensity",
    "total ion current",
    "lowest observed m/z",
    "highest observed m/z",
}

SCAN_PARAM_SKIP = {
    "scanWindowList",
    "scan start time",
    "instrumentConfigurationRef",
    "filter string",
    "ion injection time",
    "[Thermo Trailer Extra]Monoisotopic M/Z:",
}

PRECURSOR_PARAM_SKIP = {
    "selectedIonList",
    "activation",
    "isolationWindow",
    "spectrumRef",
}

SELECTED_ION_PARAM_SKIP = {
    "selected ion m/z",
    "charge state",
    "peak intensity",
}

ISOLATION_PARAM_SKIP = {
    "isolation window target m/z",
    "isolation window lower offset",
    "isolation window upper offset",
}


def convert_mzml_to_mzduck(
    mzml_path,
    output_path,
    *,
    overwrite=False,
    batch_size=5000,
    compression="zstd",
    compression_level=6,
    index_scan=False,
    index_scan_number=False,
    compute_sha256=True,
    ms2_mgf_only=False,
    no_ms1=False,
    ms2_only=False,
    ms1_only=False,
    start_scan=None,
    end_scan=None,
):
    """Convert one mzML run into an mzDuck database."""
    source = Path(mzml_path)
    output = Path(output_path)
    validate_input_paths(source, output, overwrite=overwrite)
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    compression = validate_compression(compression)
    compression_level = validate_compression_level(compression_level)
    options = resolve_import_options(
        ms2_mgf_only=ms2_mgf_only,
        no_ms1=no_ms1,
        ms2_only=ms2_only,
        ms1_only=ms1_only,
        start_scan=start_scan,
        end_scan=end_scan,
    )
    index_scan = bool(index_scan or index_scan_number)
    pre_scan = pre_scan_mzml(source, options)
    warnings = list(pre_scan["warnings"])

    include_ms1 = pre_scan["included_counts"].get(1, 0) > 0 and options["include_ms1"]
    include_ms2 = pre_scan["included_counts"].get(2, 0) > 0 and not options["ms1_only"]
    msn_levels = [
        level
        for level, count in pre_scan["included_counts"].items()
        if level >= 3 and count > 0 and options["include_msn_detail"]
    ]

    staging = make_staging_path(output)
    compact = make_compact_path(output)
    conn = duckdb.connect(str(staging))
    try:
        warnings.extend(apply_compression(conn, compression, compression_level))
        create_schema(
            conn,
            include_ms1=include_ms1,
            include_ms2=include_ms2 or bool(msn_levels),
            msn_levels=msn_levels,
            mz_array_type=pre_scan["mz_array_storage_dtype"],
            intensity_array_type=pre_scan["intensity_array_storage_dtype"],
        )

        metadata = provenance_metadata(
            source,
            creator="mzduck convert",
            mzduck_version=__version__,
            compute_sha256=compute_sha256,
        )
        metadata.update(default_metadata_values())
        metadata.update(extract_header_metadata(source))
        metadata.update(
            {
                "import_mode": options["mode"],
                "no_ms1": "true" if options["no_ms1"] else "false",
                "start_scan": ""
                if options["start_scan"] is None
                else str(options["start_scan"]),
                "end_scan": ""
                if options["end_scan"] is None
                else str(options["end_scan"]),
                "rt_unit": pre_scan["rt_unit"],
                "polarity": pre_scan["polarity"],
                "centroided": "true" if pre_scan["centroided"] else "false",
                "ion_injection_time_unit": pre_scan["ion_injection_time_unit"],
                "native_id_template": pre_scan["native_id_template"] or "",
                "spectrum_ref_template": pre_scan["spectrum_ref_template"] or "",
                "filter_string_encoding": FILTER_STRING_ENCODING_RAW,
                "mz_array_dtype": ",".join(pre_scan["mz_array_dtypes"]),
                "intensity_array_dtype": ",".join(pre_scan["intensity_array_dtypes"]),
                "mz_array_storage_dtype": pre_scan["mz_array_storage_dtype"],
                "intensity_array_storage_dtype": pre_scan[
                    "intensity_array_storage_dtype"
                ],
                "compression": compression,
                "compression_level": str(compression_level),
                "index_scan": "true" if index_scan else "false",
                "index_scan_number": "true" if index_scan else "false",
                "mgf_title_source": output.stem,
                "mgf_title_template": DEFAULT_MGF_TITLE_TEMPLATE,
                "source_ms_level_counts": metadata_json(pre_scan["source_counts"]),
                "source_ms_level_peak_counts": metadata_json(
                    pre_scan["source_peak_counts"]
                ),
                "included_ms_level_counts": metadata_json(
                    pre_scan["included_counts"]
                ),
                "included_ms_level_peak_counts": metadata_json(
                    pre_scan["included_peak_counts"]
                ),
            }
        )
        upsert_metadata(conn, metadata)

        batches = build_batches(
            conn=conn,
            include_ms1=include_ms1,
            include_ms2=include_ms2,
            msn_levels=msn_levels,
            batch_size=batch_size,
            mz_storage_type=pre_scan["mz_array_storage_dtype"],
            intensity_storage_type=pre_scan["intensity_array_storage_dtype"],
            include_text_overrides=(include_ms2 or bool(msn_levels)),
            include_extra_params=(include_ms2 or bool(msn_levels)),
        )
        inserted_counts = Counter()
        inserted_peak_counts = Counter()
        filter_detector = FilterStringDetector()

        with mzml.MzML(str(source)) as reader:
            for source_index, spectrum in enumerate(reader):
                native_id = str(spectrum.get("id") or "")
                scan_number = resolve_scan_number(spectrum, native_id, source_index)
                ms_level = as_int(spectrum.get("ms level"))
                if not include_spectrum(ms_level, scan_number, options):
                    continue

                (
                    record,
                    mz_array,
                    intensity_array,
                    text_overrides,
                    extra_params,
                    row_warnings,
                ) = spectrum_to_record(
                    spectrum,
                    source_index=source_index,
                    constants=pre_scan,
                )
                warnings.extend(row_warnings)
                inserted_counts[ms_level] += 1
                inserted_peak_counts[ms_level] += len(mz_array)
                if ms_level >= 2:
                    filter_detector.observe(record)

                if ms_level == 1:
                    if include_ms1:
                        batches["ms1_spectra"].append(
                            ms1_row(record, mz_array, intensity_array)
                        )
                elif ms_level == 2:
                    if include_ms2:
                        batches["ms2_spectra"].append(
                            ms2_row(record, mz_array, intensity_array)
                        )
                else:
                    table_name = msn_table_name(ms_level)
                    if table_name in batches:
                        batches[table_name].append(
                            msn_row(record, mz_array, intensity_array)
                        )

                if "spectrum_text_overrides" in batches:
                    for row in text_overrides:
                        batches["spectrum_text_overrides"].append(row)
                if "spectrum_extra_params" in batches:
                    for row in extra_params:
                        batches["spectrum_extra_params"].append(row)

        for batch in batches.values():
            batch.flush()

        filter_encoding = filter_detector.encoding()
        if filter_encoding != FILTER_STRING_ENCODING_RAW:
            conn.execute(
                """
                DELETE FROM spectrum_text_overrides
                WHERE field_name = 'filter_string'
                """
            )

        validate_import(
            conn,
            expected_counts=inserted_counts,
            expected_peak_counts=inserted_peak_counts,
            include_ms1=include_ms1,
            include_ms2=include_ms2,
            msn_levels=msn_levels,
        )
        if index_scan:
            create_scan_index(conn)

        summary_metadata = build_summary_metadata(
            conn,
            inserted_counts=inserted_counts,
            inserted_peak_counts=inserted_peak_counts,
            warnings=warnings,
            filter_encoding=filter_encoding,
        )
        upsert_metadata(conn, summary_metadata)
        conn.execute("CHECKPOINT")
        conn.close()
        compact_database(staging, compact, output)
    except Exception:
        safe_close(conn)
        safe_unlink(staging)
        safe_unlink(compact)
        safe_unlink(output)
        raise
    else:
        safe_unlink(staging)
        safe_unlink(compact)
    return output


def resolve_import_options(
    *,
    ms2_mgf_only=False,
    no_ms1=False,
    ms2_only=False,
    ms1_only=False,
    start_scan=None,
    end_scan=None,
):
    mode_flags = [bool(ms2_mgf_only), bool(ms2_only), bool(ms1_only)]
    if sum(mode_flags) > 1:
        raise ValueError(
            "--ms2-mgf-only, --ms2-only, and --ms1-only are mutually exclusive"
        )
    if no_ms1 and ms1_only:
        raise ValueError("--no-ms1 cannot be used with --ms1-only")
    start_scan = coerce_optional_scan("start_scan", start_scan)
    end_scan = coerce_optional_scan("end_scan", end_scan)
    if start_scan is not None and end_scan is not None and start_scan > end_scan:
        raise ValueError("--start-scan cannot be greater than --end-scan")

    mode = "default"
    if ms2_mgf_only:
        mode = "ms2_mgf_only"
    elif ms2_only:
        mode = "ms2_only"
    elif ms1_only:
        mode = "ms1_only"

    return {
        "mode": mode,
        "ms2_mgf_only": bool(ms2_mgf_only),
        "ms2_only": bool(ms2_only),
        "ms1_only": bool(ms1_only),
        "no_ms1": bool(no_ms1),
        "start_scan": start_scan,
        "end_scan": end_scan,
        "include_ms1": ms1_only
        or (not no_ms1 and not ms2_mgf_only and not ms2_only),
        "include_msn_detail": mode == "default",
    }


def coerce_optional_scan(name, value):
    if value is None:
        return None
    try:
        scan = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer: {value!r}") from exc
    if scan < 0:
        raise ValueError(f"{name} must be non-negative: {scan}")
    return scan


def validate_input_paths(source: Path, output: Path, *, overwrite: bool):
    if source.suffix.lower() != ".mzml":
        raise ValueError(f"Unsupported input format for mzDuck conversion: {source}")
    if not source.exists():
        raise FileNotFoundError(f"Input mzML does not exist: {source}")
    if not source.is_file():
        raise ValueError(f"Input mzML is not a file: {source}")
    if output.exists():
        if not overwrite:
            raise FileExistsError(f"Output already exists: {output}")
        output.unlink()
    if output.parent and not output.parent.exists():
        raise FileNotFoundError(f"Output directory does not exist: {output.parent}")


def make_staging_path(output: Path):
    return output.with_name(f".{output.name}.staging.{uuid.uuid4().hex}.duckdb")


def make_compact_path(output: Path):
    return output.with_name(f".{output.name}.compact.{uuid.uuid4().hex}.duckdb")


def compact_database(staging: Path, compact: Path, output: Path):
    safe_unlink(compact)
    conn = duckdb.connect(str(compact))
    try:
        conn.execute(f"ATTACH '{staging}' AS src (READ_ONLY)")
        database_name = conn.execute("PRAGMA database_list").fetchone()[1]
        conn.execute(f'COPY FROM DATABASE src TO "{database_name}"')
    finally:
        conn.close()
    validate_compacted_output(compact)
    os.replace(compact, output)


def validate_compacted_output(path: Path):
    conn = duckdb.connect(str(path), read_only=True)
    try:
        validate_required_schema(conn)
    finally:
        conn.close()


def safe_close(conn):
    try:
        conn.close()
    except Exception:
        pass


def safe_unlink(path: Path):
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def validate_compression(value):
    compression = str(value).lower()
    if compression not in {"zstd", "auto", "uncompressed"}:
        raise ValueError(
            "compression must be one of {'zstd', 'auto', 'uncompressed'}, "
            f"got {value!r}"
        )
    return compression


def validate_compression_level(value):
    try:
        level = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"compression_level must be a non-negative integer: {value!r}"
        ) from exc
    if level < 0:
        raise ValueError(f"compression_level must be non-negative: {level}")
    return level


def apply_compression(conn, compression, compression_level):
    warnings = []
    conn.execute(f"SET force_compression = '{compression}'")
    try:
        conn.execute(f"SET zstd_compression_level = {int(compression_level)}")
    except duckdb.Error as exc:
        warnings.append(
            "DuckDB did not accept compression_level="
            f"{compression_level}; using DuckDB's default zstd level: {exc}"
        )
    return warnings


def pre_scan_mzml(source: Path, options: dict) -> dict:
    rt_units = set()
    polarities = set()
    centroided_values = set()
    ion_injection_units = set()
    mz_dtypes = set()
    intensity_dtypes = set()
    id_scan_pairs = []
    spectrum_ref_pairs = []
    spectrum_ref_unparseable = False
    warnings = []
    source_counts = Counter()
    source_peak_counts = Counter()
    included_counts = Counter()
    included_peak_counts = Counter()
    included_scan_numbers = set()

    with mzml.MzML(str(source)) as reader:
        for source_index, spectrum in enumerate(reader):
            ms_level = as_int(spectrum.get("ms level"))
            if ms_level is None:
                raise ValueError(f"Spectrum {source_index} is missing ms level")
            native_id = str(spectrum.get("id") or "")
            scan_number = resolve_scan_number(spectrum, native_id, source_index)
            mz_array, intensity_array = required_arrays(spectrum, source_index)

            source_counts[ms_level] += 1
            source_peak_counts[ms_level] += len(mz_array)
            if not include_spectrum(ms_level, scan_number, options):
                continue

            if scan_number in included_scan_numbers:
                raise ValueError(
                    f"Duplicate scan_number in selected spectra: {scan_number}"
                )
            included_scan_numbers.add(scan_number)

            mz_dtypes.add(str(mz_array.dtype))
            intensity_dtypes.add(str(intensity_array.dtype))
            included_counts[ms_level] += 1
            included_peak_counts[ms_level] += len(mz_array)

            scan = first_nested(spectrum, "scanList", "scan", 0)
            if scan is None or "scan start time" not in scan:
                raise ValueError(f"Spectrum {source_index} is missing scan start time")
            rt, rt_unit = numeric_with_unit(
                scan["scan start time"], default_unit="minute"
            )
            if rt is None:
                raise ValueError(
                    f"Spectrum {source_index} has non-numeric scan start time"
                )
            rt_units.add(normalize_unit(rt_unit, default="minute"))

            polarity = spectrum_polarity(spectrum)
            if polarity is not None:
                polarities.add(polarity)

            centroided = spectrum_centroided(spectrum)
            if centroided is not None:
                centroided_values.add(centroided)

            if "ion injection time" in scan:
                _, unit = numeric_with_unit(
                    scan["ion injection time"], default_unit="millisecond"
                )
                ion_injection_units.add(normalize_unit(unit, default="millisecond"))

            id_scan_pairs.append((native_id, scan_number))

            precursor = first_nested(
                spectrum, "precursorList", "precursor", 0, default={}
            )
            raw_ref = precursor.get("spectrumRef") if isinstance(precursor, dict) else None
            if raw_ref:
                precursor_scan_number = parse_scan_number(str(raw_ref))
                if precursor_scan_number is None:
                    spectrum_ref_unparseable = True
                else:
                    spectrum_ref_pairs.append((str(raw_ref), precursor_scan_number))

    if not included_counts:
        raise ValueError("No spectra matched the requested mzDuck import options")
    if len(rt_units) != 1:
        raise ValueError(
            f"Mixed or missing retention time units in mzML: {sorted(rt_units)}"
        )
    if len(polarities) > 1:
        raise ValueError(f"Mixed polarity values in mzML: {sorted(polarities)}")
    if False in centroided_values:
        raise ValueError("mzDuck only supports centroid spectra")
    if True not in centroided_values:
        raise ValueError("No centroid spectrum indicator found in selected mzML spectra")
    if len(ion_injection_units) > 1:
        raise ValueError(
            "Mixed ion injection time units in mzML: "
            f"{sorted(ion_injection_units)}"
        )

    mz_storage, mz_warning = storage_type_for_dtypes(mz_dtypes, "m/z")
    intensity_storage, intensity_warning = storage_type_for_dtypes(
        intensity_dtypes, "intensity"
    )
    warnings.extend(item for item in (mz_warning, intensity_warning) if item)

    spectrum_ref_template = None
    if spectrum_ref_pairs and not spectrum_ref_unparseable:
        spectrum_ref_template = infer_scan_template(
            spectrum_ref_pairs, placeholder_name="precursor_scan_number"
        )

    return {
        "rt_unit": next(iter(rt_units)),
        "polarity": next(iter(polarities)) if polarities else "",
        "centroided": True,
        "ion_injection_time_unit": (
            next(iter(ion_injection_units)) if ion_injection_units else "millisecond"
        ),
        "native_id_template": infer_scan_template(
            id_scan_pairs, placeholder_name="scan_number"
        ),
        "spectrum_ref_template": spectrum_ref_template,
        "mz_array_dtypes": sorted(mz_dtypes),
        "intensity_array_dtypes": sorted(intensity_dtypes),
        "mz_array_storage_dtype": mz_storage,
        "intensity_array_storage_dtype": intensity_storage,
        "source_counts": dict(sorted(source_counts.items())),
        "source_peak_counts": dict(sorted(source_peak_counts.items())),
        "included_counts": dict(sorted(included_counts.items())),
        "included_peak_counts": dict(sorted(included_peak_counts.items())),
        "warnings": warnings,
    }


def include_spectrum(ms_level, scan_number, options):
    if ms_level is None:
        return False
    start_scan = options.get("start_scan")
    end_scan = options.get("end_scan")
    if start_scan is not None and scan_number < start_scan:
        return False
    if end_scan is not None and scan_number > end_scan:
        return False
    if options["ms1_only"]:
        return ms_level == 1
    if options["ms2_mgf_only"] or options["ms2_only"]:
        return ms_level == 2
    if options["no_ms1"] and ms_level == 1:
        return False
    return ms_level >= 1


def required_arrays(spectrum, source_index):
    if "m/z array" not in spectrum or "intensity array" not in spectrum:
        raise ValueError(f"Spectrum {source_index} is missing required arrays")
    mz_array = np.asarray(spectrum["m/z array"])
    intensity_array = np.asarray(spectrum["intensity array"])
    if len(mz_array) != len(intensity_array):
        raise ValueError(
            f"Spectrum {source_index} has mismatched m/z and intensity lengths: "
            f"{len(mz_array)} != {len(intensity_array)}"
        )
    return mz_array, intensity_array


def storage_type_for_dtypes(dtypes, label):
    normalized = {np.dtype(dtype) for dtype in dtypes}
    if normalized == {np.dtype("float32")}:
        return "FLOAT", None
    if normalized and normalized <= {np.dtype("float32"), np.dtype("float64")}:
        return "DOUBLE", None
    return (
        "DOUBLE",
        f"Observed non-floating or exotic {label} dtype(s): "
        f"{','.join(sorted(map(str, dtypes)))}; stored as DOUBLE",
    )


def spectrum_to_record(spectrum, *, source_index: int, constants: dict):
    warnings: list[str] = []
    native_id = str(spectrum.get("id") or "")
    scan_number = resolve_scan_number(spectrum, native_id, source_index)
    ms_level = as_int(spectrum.get("ms level"))
    if ms_level is None:
        raise ValueError(f"Spectrum {source_index} is missing ms level")

    mz_array, intensity_array = required_arrays(spectrum, source_index)
    mz_array = mz_array.astype(
        numpy_dtype_for_storage(constants["mz_array_storage_dtype"]), copy=False
    )
    intensity_array = intensity_array.astype(
        numpy_dtype_for_storage(constants["intensity_array_storage_dtype"]),
        copy=False,
    )

    scan = first_nested(spectrum, "scanList", "scan", 0)
    if scan is None or "scan start time" not in scan:
        raise ValueError(f"Spectrum {source_index} is missing scan start time")
    rt, rt_unit = numeric_with_unit(scan["scan start time"], default_unit="minute")
    rt_unit = normalize_unit(rt_unit, default="minute")
    if rt_unit != constants["rt_unit"]:
        raise ValueError(
            f"Spectrum {source_index} has RT unit {rt_unit!r}; expected "
            f"{constants['rt_unit']!r}"
        )
    if rt is None:
        raise ValueError(f"Spectrum {source_index} has non-numeric scan start time")

    polarity = spectrum_polarity(spectrum)
    if polarity and constants["polarity"] and polarity != constants["polarity"]:
        raise ValueError(
            f"Spectrum {source_index} has polarity {polarity!r}; expected "
            f"{constants['polarity']!r}"
        )
    centroided = spectrum_centroided(spectrum)
    if centroided is False:
        raise ValueError("mzDuck only supports centroid spectra")

    precursor = first_nested(spectrum, "precursorList", "precursor", 0, default={})
    precursor_list = first_nested(spectrum, "precursorList", "precursor", default=[])
    if isinstance(precursor_list, list) and len(precursor_list) > 1:
        warnings.append(
            f"Spectrum {source_index} has multiple precursors; stored first precursor"
        )
    selected_ion = first_nested(
        precursor, "selectedIonList", "selectedIon", 0, default={}
    )
    isolation = precursor.get("isolationWindow", {}) if isinstance(precursor, dict) else {}
    activation = precursor.get("activation", {}) if isinstance(precursor, dict) else {}

    isolation_target = as_float(isolation.get("isolation window target m/z"))
    selected_mz = as_float(selected_ion.get("selected ion m/z"))
    activation_type = activation_type_from_dict(activation)
    if activation_type and activation_type not in set(ACTIVATION_MAP.values()):
        warnings.append(
            f"Spectrum {source_index} has unknown activation term: {activation_type}"
        )

    ion_injection_time = None
    if "ion injection time" in scan:
        ion_injection_time, injection_unit = numeric_with_unit(
            scan["ion injection time"], default_unit="millisecond"
        )
        injection_unit = normalize_unit(injection_unit, default="millisecond")
        if injection_unit != constants["ion_injection_time_unit"]:
            raise ValueError(
                f"Spectrum {source_index} has ion injection unit {injection_unit!r}; "
                f"expected {constants['ion_injection_time_unit']!r}"
            )

    lowest_mz = as_float(spectrum.get("lowest observed m/z"))
    highest_mz = as_float(spectrum.get("highest observed m/z"))
    if len(mz_array):
        if lowest_mz is None:
            lowest_mz = float(np.nanmin(mz_array))
        if highest_mz is None:
            highest_mz = float(np.nanmax(mz_array))

    raw_spectrum_ref = (
        str(precursor.get("spectrumRef"))
        if isinstance(precursor, dict) and precursor.get("spectrumRef") is not None
        else None
    )
    precursor_scan_number = parse_scan_number(raw_spectrum_ref)

    native_id_override = None
    if not constants.get("native_id_template") or constants[
        "native_id_template"
    ].format(scan_number=scan_number) != native_id:
        native_id_override = native_id or None

    spectrum_ref_override = None
    spectrum_ref_template = constants.get("spectrum_ref_template")
    if raw_spectrum_ref and (
        precursor_scan_number is None
        or not spectrum_ref_template
        or spectrum_ref_template.format(
            precursor_scan_number=precursor_scan_number
        )
        != raw_spectrum_ref
    ):
        spectrum_ref_override = raw_spectrum_ref

    filter_string = (
        str(scan.get("filter string"))
        if isinstance(scan, dict) and scan.get("filter string") is not None
        else None
    )

    record = {
        "scan_number": scan_number,
        "source_index": source_index,
        "instrument_configuration_ref": (
            str(scan.get("instrumentConfigurationRef"))
            if isinstance(scan, dict)
            and scan.get("instrumentConfigurationRef") is not None
            else None
        ),
        "native_id": native_id_override,
        "ms_level": ms_level,
        "rt": rt,
        "precursor_mz": selected_mz if selected_mz is not None else isolation_target,
        "precursor_charge": as_int(selected_ion.get("charge state")),
        "precursor_intensity": as_float(selected_ion.get("peak intensity")),
        "collision_energy": as_float(activation.get("collision energy")),
        "activation_type": activation_type,
        "isolation_window_target": isolation_target,
        "isolation_window_lower": as_float(
            isolation.get("isolation window lower offset")
        ),
        "isolation_window_upper": as_float(
            isolation.get("isolation window upper offset")
        ),
        "precursor_scan_number": precursor_scan_number,
        "base_peak_mz": as_float(spectrum.get("base peak m/z")),
        "base_peak_intensity": as_float(spectrum.get("base peak intensity")),
        "tic": as_float(spectrum.get("total ion current")),
        "lowest_mz": lowest_mz,
        "highest_mz": highest_mz,
        "filter_string": filter_string,
        "ion_injection_time": ion_injection_time,
        "monoisotopic_mz": as_float(
            scan.get("[Thermo Trailer Extra]Monoisotopic M/Z:")
        )
        if isinstance(scan, dict)
        else None,
        "scan_window_lower": as_float(
            first_nested(
                scan,
                "scanWindowList",
                "scanWindow",
                0,
                "scan window lower limit",
            )
        ),
        "scan_window_upper": as_float(
            first_nested(
                scan,
                "scanWindowList",
                "scanWindow",
                0,
                "scan window upper limit",
            )
        ),
    }

    text_overrides = text_override_rows(
        scan_number,
        native_id=record["native_id"],
        spectrum_ref=spectrum_ref_override,
        filter_string=filter_string if ms_level >= 2 else None,
    )
    extra_params = extract_extra_params(
        scan_number,
        spectrum,
        scan,
        precursor,
        selected_ion,
        activation,
        isolation,
    )
    return record, mz_array, intensity_array, text_overrides, extra_params, warnings


def text_override_rows(scan_number, *, native_id=None, spectrum_ref=None, filter_string=None):
    rows = []
    for field_name, value in (
        ("native_id", native_id),
        ("spectrum_ref", spectrum_ref),
        ("filter_string", filter_string),
    ):
        if value is None:
            continue
        rows.append(
            {
                "scan_number": int(scan_number),
                "field_name": field_name,
                "value": str(value),
            }
        )
    return rows


def ms1_row(record, mz_array, intensity_array):
    row = {column: record[column] for column in MS1_COLUMNS if column in record}
    row["mz_array"] = mz_array
    row["intensity_array"] = intensity_array
    return row


def ms2_row(record, mz_array, intensity_array):
    row = {column: record[column] for column in MS2_COLUMNS if column in record}
    row["mz_array"] = mz_array
    row["intensity_array"] = intensity_array
    return row


def msn_row(record, mz_array, intensity_array):
    row = {column: record[column] for column in MSN_COLUMNS if column in record}
    row["mz_array"] = mz_array
    row["intensity_array"] = intensity_array
    return row


def resolve_scan_number(spectrum, native_id, source_index):
    scan_number = parse_scan_number(native_id)
    if scan_number is None:
        scan_number = as_int(spectrum.get("index"))
    if scan_number is None:
        scan_number = source_index + 1
    return scan_number


def spectrum_polarity(spectrum):
    if "positive scan" in spectrum:
        return "positive"
    if "negative scan" in spectrum:
        return "negative"
    return None


def spectrum_centroided(spectrum):
    if "centroid spectrum" in spectrum:
        return True
    if "profile spectrum" in spectrum:
        return False
    return None


def activation_type_from_dict(activation):
    if not isinstance(activation, dict):
        return None
    for key in activation:
        if key in NON_ACTIVATION_KEYS:
            continue
        return ACTIVATION_MAP.get(str(key), str(key))
    return None


def numpy_dtype_for_storage(storage_type):
    if storage_type == "FLOAT":
        return np.float32
    if storage_type == "DOUBLE":
        return np.float64
    raise ValueError(f"Unsupported storage type: {storage_type!r}")


def pyarrow_value_type_for_storage(storage_type):
    if storage_type == "FLOAT":
        return pa.float32()
    if storage_type == "DOUBLE":
        return pa.float64()
    raise ValueError(f"Unsupported storage type: {storage_type!r}")


class TableBatch:
    def __init__(
        self,
        conn,
        table_name,
        columns,
        *,
        batch_size,
        mz_storage_type,
        intensity_storage_type,
    ):
        self.conn = conn
        self.table_name = table_name
        self.columns = list(columns)
        self.batch_size = batch_size
        self.mz_storage_type = mz_storage_type
        self.intensity_storage_type = intensity_storage_type
        self.rows = []

    def append(self, row):
        self.rows.append(row)
        if len(self.rows) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.rows:
            return
        data = {}
        for column in self.columns:
            if column == "mz_array":
                data[column] = make_list_array(
                    [row[column] for row in self.rows],
                    pyarrow_value_type_for_storage(self.mz_storage_type),
                    numpy_dtype_for_storage(self.mz_storage_type),
                )
            elif column == "intensity_array":
                data[column] = make_list_array(
                    [row[column] for row in self.rows],
                    pyarrow_value_type_for_storage(self.intensity_storage_type),
                    numpy_dtype_for_storage(self.intensity_storage_type),
                )
            else:
                data[column] = pa.array(
                    [row.get(column) for row in self.rows],
                    type=SCALAR_ARROW_TYPES[column],
                )
        table = pa.table({column: data[column] for column in self.columns})
        view_name = f"_mzduck_{self.table_name}_batch"
        self.conn.register(view_name, table)
        try:
            self.conn.execute(f"INSERT INTO {self.table_name} SELECT * FROM {view_name}")
        finally:
            self.conn.unregister(view_name)
        self.rows.clear()


def build_batches(
    *,
    conn,
    include_ms1,
    include_ms2,
    msn_levels,
    batch_size,
    mz_storage_type,
    intensity_storage_type,
    include_text_overrides,
    include_extra_params,
):
    batches = {}
    if include_ms1:
        batches["ms1_spectra"] = TableBatch(
            conn,
            "ms1_spectra",
            MS1_COLUMNS,
            batch_size=batch_size,
            mz_storage_type=mz_storage_type,
            intensity_storage_type=intensity_storage_type,
        )
    if include_ms2:
        batches["ms2_spectra"] = TableBatch(
            conn,
            "ms2_spectra",
            MS2_COLUMNS,
            batch_size=batch_size,
            mz_storage_type=mz_storage_type,
            intensity_storage_type=intensity_storage_type,
        )
    for level in msn_levels:
        table_name = msn_table_name(level)
        batches[table_name] = TableBatch(
            conn,
            table_name,
            MSN_COLUMNS,
            batch_size=batch_size,
            mz_storage_type=mz_storage_type,
            intensity_storage_type=intensity_storage_type,
        )
    if include_text_overrides:
        batches["spectrum_text_overrides"] = TableBatch(
            conn,
            "spectrum_text_overrides",
            TEXT_OVERRIDE_COLUMNS,
            batch_size=batch_size,
            mz_storage_type=mz_storage_type,
            intensity_storage_type=intensity_storage_type,
        )
    if include_extra_params:
        batches["spectrum_extra_params"] = TableBatch(
            conn,
            "spectrum_extra_params",
            EXTRA_PARAM_COLUMNS,
            batch_size=batch_size,
            mz_storage_type=mz_storage_type,
            intensity_storage_type=intensity_storage_type,
        )
    return batches


def make_list_array(arrays, value_type, numpy_dtype):
    offsets = np.empty(len(arrays) + 1, dtype=np.int32)
    offsets[0] = 0
    position = 0
    for i, array in enumerate(arrays, start=1):
        position += len(array)
        offsets[i] = position
    if position:
        values = np.concatenate(arrays).astype(numpy_dtype, copy=False)
    else:
        values = np.asarray([], dtype=numpy_dtype)
    return pa.ListArray.from_arrays(
        pa.array(offsets, type=pa.int32()),
        pa.array(values, type=value_type),
    )


def validate_import(
    conn,
    *,
    expected_counts,
    expected_peak_counts,
    include_ms1,
    include_ms2,
    msn_levels,
):
    if include_ms1:
        validate_array_table(
            conn,
            "ms1_spectra",
            expected_counts.get(1, 0),
            expected_peak_counts.get(1, 0),
        )
    if include_ms2:
        validate_array_table(
            conn,
            "ms2_spectra",
            expected_counts.get(2, 0),
            expected_peak_counts.get(2, 0),
        )
        duplicates = conn.execute(
            "SELECT COUNT(*) - COUNT(DISTINCT scan_number) FROM ms2_spectra"
        ).fetchone()[0]
        if duplicates:
            raise ValueError(
                f"{duplicates} duplicate scan_number values in ms2_spectra"
            )
        mgf_count = conn.execute("SELECT COUNT(*) FROM mgf").fetchone()[0]
        if mgf_count != expected_counts.get(2, 0):
            raise ValueError(
                f"mgf compatibility view count mismatch: {mgf_count} != "
                f"{expected_counts.get(2, 0)}"
            )
    for level in msn_levels:
        table_name = msn_table_name(level)
        validate_array_table(
            conn,
            table_name,
            expected_counts.get(level, 0),
            expected_peak_counts.get(level, 0),
        )


def validate_array_table(conn, table_name, expected_spectrum_count, expected_peak_count):
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if count != expected_spectrum_count:
        raise ValueError(
            f"Imported {table_name} count mismatch: {count} != "
            f"{expected_spectrum_count}"
        )
    peak_count = conn.execute(
        f"SELECT COALESCE(SUM(len(mz_array)), 0) FROM {table_name}"
    ).fetchone()[0]
    if peak_count != expected_peak_count:
        raise ValueError(
            f"Imported {table_name} peak count mismatch: {peak_count} != "
            f"{expected_peak_count}"
        )
    mismatched = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE len(mz_array) != len(intensity_array)
        """
    ).fetchone()[0]
    if mismatched:
        raise ValueError(f"{mismatched} spectra in {table_name} have mismatched arrays")


def build_summary_metadata(
    conn,
    *,
    inserted_counts,
    inserted_peak_counts,
    warnings,
    filter_encoding,
):
    total_spectra = sum(inserted_counts.values())
    total_peaks = sum(inserted_peak_counts.values())
    metadata = {
        "total_spectrum_count": str(total_spectra),
        "total_peak_count": str(total_peaks),
        "spectrum_count": str(total_spectra),
        "peak_count": str(total_peaks),
        "mgf_spectrum_count": str(table_count(conn, "mgf")),
        "mgf_peak_count": str(table_peak_count(conn, "mgf")),
        "table_registry": metadata_json(table_registry(conn)),
        "conversion_warnings": json.dumps(warnings, sort_keys=True),
        "filter_string_encoding": filter_encoding,
    }
    for level in sorted(inserted_counts):
        metadata[f"ms{level}_spectrum_count"] = str(inserted_counts[level])
        metadata[f"ms{level}_peak_count"] = str(inserted_peak_counts[level])
    if 1 not in inserted_counts:
        metadata["ms1_spectrum_count"] = "0"
        metadata["ms1_peak_count"] = "0"
    if 2 not in inserted_counts:
        metadata["ms2_spectrum_count"] = "0"
        metadata["ms2_peak_count"] = "0"
    return metadata


def extract_extra_params(
    scan_number,
    spectrum,
    scan,
    precursor,
    selected_ion,
    activation,
    isolation,
):
    rows = []
    rows.extend(
        param_rows_from_mapping(
            scan_number,
            "spectrum",
            spectrum,
            TOP_LEVEL_PARAM_SKIP,
            ordinal_start=0,
        )
    )
    rows.extend(
        param_rows_from_mapping(
            scan_number,
            "scan",
            scan,
            SCAN_PARAM_SKIP,
            ordinal_start=len(rows),
        )
    )
    rows.extend(
        param_rows_from_mapping(
            scan_number,
            "precursor",
            precursor,
            PRECURSOR_PARAM_SKIP,
            ordinal_start=len(rows),
        )
    )
    rows.extend(
        param_rows_from_mapping(
            scan_number,
            "selected_ion",
            selected_ion,
            SELECTED_ION_PARAM_SKIP,
            ordinal_start=len(rows),
        )
    )
    activation_skip = set(NON_ACTIVATION_KEYS)
    if isinstance(activation, dict):
        for key in activation:
            if key in NON_ACTIVATION_KEYS:
                continue
            activation_skip.add(str(key))
            break
    rows.extend(
        param_rows_from_mapping(
            scan_number,
            "activation",
            activation,
            activation_skip,
            ordinal_start=len(rows),
        )
    )
    rows.extend(
        param_rows_from_mapping(
            scan_number,
            "isolation_window",
            isolation,
            ISOLATION_PARAM_SKIP,
            ordinal_start=len(rows),
        )
    )
    return rows


def param_rows_from_mapping(scan_number, scope, mapping, skip_keys, *, ordinal_start):
    if not isinstance(mapping, dict):
        return []
    rows = []
    ordinal = ordinal_start
    for key, value in mapping.items():
        name = str(key)
        if name in skip_keys:
            continue
        if isinstance(value, (dict, list, tuple, np.ndarray)):
            continue
        rows.append(
            {
                "scan_number": int(scan_number),
                "scope": scope,
                "ordinal": ordinal,
                "accession": getattr(key, "accession", None),
                "name": name,
                "value": serialize_param_value(value),
                "unit_accession": getattr(value, "unit_accession", None),
                "unit_name": unit_of(value),
                "cv_ref": accession_cv_ref(getattr(key, "accession", None)),
            }
        )
        ordinal += 1
    return rows


def serialize_param_value(value):
    if value is None:
        return None
    if isinstance(value, (str, bytes)):
        text = value.decode() if isinstance(value, bytes) else value
        return text if text != "" else None
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    return str(value)


def accession_cv_ref(accession):
    if not accession or ":" not in accession:
        return None
    return accession.split(":", 1)[0]
