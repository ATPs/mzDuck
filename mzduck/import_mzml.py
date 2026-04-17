"""mzML to mzDuck conversion."""

from __future__ import annotations

import json
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
)
from .schema import (
    SPECTRA_COLUMNS,
    create_scan_number_index,
    create_schema,
    upsert_metadata,
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
    "spectrum_ref": pa.string(),
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
}


def convert_mzml_to_mzduck(
    mzml_path,
    output_path,
    *,
    overwrite=False,
    batch_size=5000,
    compression="zstd",
    compression_level=6,
    index_scan_number=False,
    compute_sha256=True,
):
    """Convert a centroid MS2 mzML file into a v1 mzDuck database."""
    source = Path(mzml_path)
    output = Path(output_path)
    validate_input_paths(source, output, overwrite=overwrite)
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    compression = validate_compression(compression)
    compression_level = validate_compression_level(compression_level)

    pre_scan = pre_scan_mzml(source)
    warnings = list(pre_scan["warnings"])

    conn = duckdb.connect(str(output))
    try:
        warnings.extend(apply_compression(conn, compression, compression_level))
        create_schema(
            conn,
            mz_array_type=pre_scan["mz_array_storage_dtype"],
            intensity_array_type=pre_scan["intensity_array_storage_dtype"],
        )

        metadata = provenance_metadata(
            source,
            creator="mzduck convert",
            mzduck_version=__version__,
            compute_sha256=compute_sha256,
        )
        metadata.update(extract_header_metadata(source))
        metadata.update(
            {
                "rt_unit": pre_scan["rt_unit"],
                "polarity": pre_scan["polarity"],
                "centroided": "true" if pre_scan["centroided"] else "false",
                "ion_injection_time_unit": pre_scan["ion_injection_time_unit"],
                "native_id_template": pre_scan["native_id_template"],
                "mz_array_dtype": ",".join(pre_scan["mz_array_dtypes"]),
                "intensity_array_dtype": ",".join(pre_scan["intensity_array_dtypes"]),
                "mz_array_storage_dtype": pre_scan["mz_array_storage_dtype"],
                "intensity_array_storage_dtype": pre_scan[
                    "intensity_array_storage_dtype"
                ],
                "compression": compression,
                "compression_level": str(compression_level),
                "index_scan_number": "true" if index_scan_number else "false",
            }
        )
        upsert_metadata(conn, metadata)

        rows: list[dict] = []
        mz_arrays: list[np.ndarray] = []
        intensity_arrays: list[np.ndarray] = []
        spectrum_count = 0
        peak_count = 0

        with mzml.MzML(str(source)) as reader:
            for source_order, spectrum in enumerate(reader):
                row, mz_array, intensity_array, row_warnings = spectrum_to_row(
                    spectrum,
                    source_order=source_order,
                    constants=pre_scan,
                )
                warnings.extend(row_warnings)
                rows.append(row)
                mz_arrays.append(mz_array)
                intensity_arrays.append(intensity_array)
                spectrum_count += 1
                peak_count += len(mz_array)
                if len(rows) >= batch_size:
                    flush_batches(
                        conn,
                        rows,
                        mz_arrays,
                        intensity_arrays,
                        pre_scan["mz_array_storage_dtype"],
                        pre_scan["intensity_array_storage_dtype"],
                    )

        flush_batches(
            conn,
            rows,
            mz_arrays,
            intensity_arrays,
            pre_scan["mz_array_storage_dtype"],
            pre_scan["intensity_array_storage_dtype"],
        )

        upsert_metadata(
            conn,
            {
                "spectrum_count": str(spectrum_count),
                "peak_count": str(peak_count),
                "conversion_warnings": json.dumps(warnings, sort_keys=True),
            },
        )
        if index_scan_number:
            create_scan_number_index(conn)
        validate_import(conn, spectrum_count, peak_count)
        conn.execute("CHECKPOINT")
    except Exception:
        conn.close()
        if output.exists():
            output.unlink()
        raise
    else:
        conn.close()
    return output


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
        raise ValueError(f"compression_level must be a non-negative integer: {value!r}") from exc
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


def pre_scan_mzml(source: Path) -> dict:
    rt_units = set()
    polarities = set()
    centroided_values = set()
    ion_injection_units = set()
    mz_dtypes = set()
    intensity_dtypes = set()
    id_scan_pairs = []
    warnings = []

    with mzml.MzML(str(source)) as reader:
        for source_order, spectrum in enumerate(reader):
            ms_level = as_int(spectrum.get("ms level"))
            if ms_level != 2:
                raise ValueError(
                    f"Unsupported spectrum ms level at source index {source_order}: "
                    f"{ms_level}"
                )
            if "m/z array" not in spectrum or "intensity array" not in spectrum:
                raise ValueError(f"Spectrum {source_order} is missing required arrays")

            mz_array = np.asarray(spectrum["m/z array"])
            intensity_array = np.asarray(spectrum["intensity array"])
            if len(mz_array) != len(intensity_array):
                raise ValueError(
                    f"Spectrum {source_order} has mismatched m/z and intensity "
                    f"lengths: {len(mz_array)} != {len(intensity_array)}"
                )
            mz_dtypes.add(str(mz_array.dtype))
            intensity_dtypes.add(str(intensity_array.dtype))

            scan = first_nested(spectrum, "scanList", "scan", 0)
            if scan is None or "scan start time" not in scan:
                raise ValueError(f"Spectrum {source_order} is missing scan start time")
            rt, rt_unit = numeric_with_unit(scan["scan start time"], default_unit="minute")
            if rt is None:
                raise ValueError(f"Spectrum {source_order} has non-numeric scan start time")
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

            native_id = str(spectrum.get("id") or "")
            scan_number = resolve_scan_number(spectrum, native_id, source_order)
            id_scan_pairs.append((native_id, scan_number))

    if len(rt_units) != 1:
        raise ValueError(f"Mixed or missing retention time units in mzML: {sorted(rt_units)}")
    if len(polarities) > 1:
        raise ValueError(f"Mixed polarity values in mzML: {sorted(polarities)}")
    if False in centroided_values:
        raise ValueError("v1 mzDuck only supports centroid spectra")
    if True not in centroided_values:
        raise ValueError("No centroid spectrum indicator found in mzML")
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

    return {
        "rt_unit": next(iter(rt_units)),
        "polarity": next(iter(polarities)) if polarities else "",
        "centroided": True,
        "ion_injection_time_unit": (
            next(iter(ion_injection_units)) if ion_injection_units else "millisecond"
        ),
        "native_id_template": infer_native_id_template(id_scan_pairs),
        "mz_array_dtypes": sorted(mz_dtypes),
        "intensity_array_dtypes": sorted(intensity_dtypes),
        "mz_array_storage_dtype": mz_storage,
        "intensity_array_storage_dtype": intensity_storage,
        "warnings": warnings,
    }


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


def infer_native_id_template(id_scan_pairs):
    if not id_scan_pairs or any(not native_id for native_id, _ in id_scan_pairs):
        return None
    first_id, first_scan_number = id_scan_pairs[0]
    token = f"scan={first_scan_number}"
    if token not in first_id:
        return None
    candidate = first_id.replace(token, "scan={scan_number}", 1)
    for native_id, scan_number in id_scan_pairs:
        if candidate.format(scan_number=scan_number) != native_id:
            return None
    return candidate


def spectrum_to_row(spectrum, *, source_order: int, constants: dict):
    warnings: list[str] = []
    native_id = str(spectrum.get("id") or "")
    scan_number = resolve_scan_number(spectrum, native_id, source_order)
    ms_level = as_int(spectrum.get("ms level"))
    if ms_level != 2:
        raise ValueError(
            f"Unsupported spectrum ms level at source index {source_order}: {ms_level}"
        )

    mz_array = np.asarray(spectrum["m/z array"])
    intensity_array = np.asarray(spectrum["intensity array"])
    if len(mz_array) != len(intensity_array):
        raise ValueError(
            f"Spectrum {source_order} has mismatched m/z and intensity lengths: "
            f"{len(mz_array)} != {len(intensity_array)}"
        )
    mz_array = mz_array.astype(numpy_dtype_for_storage(constants["mz_array_storage_dtype"]), copy=False)
    intensity_array = intensity_array.astype(
        numpy_dtype_for_storage(constants["intensity_array_storage_dtype"]),
        copy=False,
    )

    scan = first_nested(spectrum, "scanList", "scan", 0)
    if scan is None or "scan start time" not in scan:
        raise ValueError(f"Spectrum {source_order} is missing scan start time")
    rt, rt_unit = numeric_with_unit(scan["scan start time"], default_unit="minute")
    rt_unit = normalize_unit(rt_unit, default="minute")
    if rt_unit != constants["rt_unit"]:
        raise ValueError(
            f"Spectrum {source_order} has RT unit {rt_unit!r}; expected "
            f"{constants['rt_unit']!r}"
        )
    if rt is None:
        raise ValueError(f"Spectrum {source_order} has non-numeric scan start time")

    polarity = spectrum_polarity(spectrum)
    if polarity and constants["polarity"] and polarity != constants["polarity"]:
        raise ValueError(
            f"Spectrum {source_order} has polarity {polarity!r}; expected "
            f"{constants['polarity']!r}"
        )
    centroided = spectrum_centroided(spectrum)
    if centroided is False:
        raise ValueError("v1 mzDuck only supports centroid spectra")

    precursor = first_nested(spectrum, "precursorList", "precursor", 0, default={})
    precursor_list = first_nested(spectrum, "precursorList", "precursor", default=[])
    if isinstance(precursor_list, list) and len(precursor_list) > 1:
        warnings.append(
            f"Spectrum {source_order} has multiple precursors; stored first precursor"
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
            f"Spectrum {source_order} has unknown activation term: {activation_type}"
        )

    ion_injection_time = None
    if "ion injection time" in scan:
        ion_injection_time, injection_unit = numeric_with_unit(
            scan["ion injection time"], default_unit="millisecond"
        )
        injection_unit = normalize_unit(injection_unit, default="millisecond")
        if injection_unit != constants["ion_injection_time_unit"]:
            raise ValueError(
                f"Spectrum {source_order} has ion injection unit {injection_unit!r}; "
                f"expected {constants['ion_injection_time_unit']!r}"
            )

    lowest_mz = as_float(spectrum.get("lowest observed m/z"))
    highest_mz = as_float(spectrum.get("highest observed m/z"))
    if len(mz_array):
        if lowest_mz is None:
            lowest_mz = float(np.nanmin(mz_array))
        if highest_mz is None:
            highest_mz = float(np.nanmax(mz_array))

    template = constants.get("native_id_template")
    stored_native_id = None
    if not template or template.format(scan_number=scan_number) != native_id:
        stored_native_id = native_id or None

    row = {
        "scan_number": scan_number,
        "native_id": stored_native_id,
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
        "spectrum_ref": precursor.get("spectrumRef") if isinstance(precursor, dict) else None,
        "base_peak_mz": as_float(spectrum.get("base peak m/z")),
        "base_peak_intensity": as_float(spectrum.get("base peak intensity")),
        "tic": as_float(spectrum.get("total ion current")),
        "lowest_mz": lowest_mz,
        "highest_mz": highest_mz,
        "filter_string": scan.get("filter string") if isinstance(scan, dict) else None,
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
    return row, mz_array, intensity_array, warnings


def resolve_scan_number(spectrum, native_id, source_order):
    scan_number = parse_scan_number(native_id)
    if scan_number is None:
        scan_number = as_int(spectrum.get("index"))
    if scan_number is None:
        raise ValueError(
            "Cannot resolve scan_number for spectrum at source index "
            f"{source_order}"
        )
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
        return ACTIVATION_MAP.get(key, key)
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


def flush_batches(
    conn,
    rows,
    mz_arrays,
    intensity_arrays,
    mz_storage_type,
    intensity_storage_type,
):
    if not rows:
        return
    data = {
        column: pa.array([row[column] for row in rows], type=SCALAR_ARROW_TYPES[column])
        for column in SPECTRA_COLUMNS
        if column not in {"mz_array", "intensity_array"}
    }
    data["mz_array"] = make_list_array(
        mz_arrays,
        pyarrow_value_type_for_storage(mz_storage_type),
        numpy_dtype_for_storage(mz_storage_type),
    )
    data["intensity_array"] = make_list_array(
        intensity_arrays,
        pyarrow_value_type_for_storage(intensity_storage_type),
        numpy_dtype_for_storage(intensity_storage_type),
    )
    table = pa.table({column: data[column] for column in SPECTRA_COLUMNS})
    conn.register("_mzduck_spectra_batch", table)
    try:
        conn.execute("INSERT INTO spectra SELECT * FROM _mzduck_spectra_batch")
    finally:
        conn.unregister("_mzduck_spectra_batch")
    rows.clear()
    mz_arrays.clear()
    intensity_arrays.clear()


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


def validate_import(conn, expected_spectrum_count, expected_peak_count):
    spectrum_count = conn.execute("SELECT COUNT(*) FROM spectra").fetchone()[0]
    if spectrum_count != expected_spectrum_count:
        raise ValueError(
            f"Imported spectrum count mismatch: {spectrum_count} != "
            f"{expected_spectrum_count}"
        )
    peak_count = conn.execute(
        "SELECT COALESCE(SUM(len(mz_array)), 0) FROM spectra"
    ).fetchone()[0]
    if peak_count != expected_peak_count:
        raise ValueError(
            f"Imported peak count mismatch: {peak_count} != {expected_peak_count}"
        )
    mismatched = conn.execute(
        """
        SELECT COUNT(*)
        FROM spectra
        WHERE len(mz_array) != len(intensity_array)
        """
    ).fetchone()[0]
    if mismatched:
        raise ValueError(f"{mismatched} spectra have mismatched array lengths")
    duplicate_scan_numbers = conn.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT scan_number) FROM spectra"
    ).fetchone()[0]
    if duplicate_scan_numbers:
        raise ValueError(f"{duplicate_scan_numbers} duplicate scan_number values")
    null_arrays = conn.execute(
        """
        SELECT COUNT(*)
        FROM spectra
        WHERE mz_array IS NULL OR intensity_array IS NULL
        """
    ).fetchone()[0]
    if null_arrays:
        raise ValueError(f"{null_arrays} spectra have NULL peak arrays")
