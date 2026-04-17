"""mzML to mzDuck conversion."""

from __future__ import annotations

import gc
import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
from pyteomics import mzml, mzmlb

from . import __version__
from .metadata import (
    as_float,
    as_int,
    dumps_json,
    extract_header_metadata,
    first_nested,
    normalize_unit,
    numeric_with_unit,
    parse_scan_number,
    provenance_metadata,
)
from .schema import INDEX_SQL, SPECTRA_COLUMNS, VIEW_SQL, create_schema, upsert_metadata

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


def convert_mzml_to_mzduck(
    mzml_path,
    output_path,
    *,
    overwrite=False,
    batch_size=5000,
    compute_sha256=True,
):
    """Convert a centroid MS2 mzML file into a v1 mzDuck database."""
    source = Path(mzml_path)
    output = Path(output_path)
    source_format = detect_source_format(source)
    if not source.exists():
        raise FileNotFoundError(f"Input mzML/mzMLb does not exist: {source}")
    if not source.is_file():
        raise ValueError(f"Input mzML/mzMLb is not a file: {source}")
    if output.exists():
        if not overwrite:
            raise FileExistsError(f"Output already exists: {output}")
        output.unlink()
    if output.parent and not output.parent.exists():
        raise FileNotFoundError(f"Output directory does not exist: {output.parent}")
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")

    conn = duckdb.connect(str(output))
    try:
        create_schema(conn)
        metadata = provenance_metadata(
            source,
            creator="mzduck convert",
            mzduck_version=__version__,
            compute_sha256=compute_sha256,
            source_format=source_format,
        )
        metadata.update(extract_header_metadata(source))
        upsert_metadata(conn, metadata)

        warnings: list[str] = []
        spectra_batch: list[dict] = []
        peak_scan_ids: list[np.ndarray] = []
        peak_indices: list[np.ndarray] = []
        peak_mzs: list[np.ndarray] = []
        peak_intensities: list[np.ndarray] = []
        rt_units = Counter()
        mz_dtypes = set()
        intensity_dtypes = set()
        spectrum_count = 0
        peak_count = 0

        with open_spectrum_reader(source) as reader:
            for source_order, spectrum in enumerate(reader):
                row, peaks, row_warnings = spectrum_to_row(
                    spectrum, scan_id=spectrum_count, source_order=source_order
                )
                if row_warnings:
                    warnings.extend(row_warnings)

                spectra_batch.append(row)
                rt_units.update([row["rt_unit"]])
                mz_array, intensity_array = peaks
                mz_dtypes.add(str(mz_array.dtype))
                intensity_dtypes.add(str(intensity_array.dtype))
                n_peaks = len(mz_array)
                if n_peaks:
                    peak_scan_ids.append(np.full(n_peaks, spectrum_count, dtype=np.int64))
                    peak_indices.append(np.arange(n_peaks, dtype=np.int64))
                    peak_mzs.append(mz_array.astype(np.float64, copy=False))
                    peak_intensities.append(intensity_array.astype(np.float32, copy=False))
                spectrum_count += 1
                peak_count += n_peaks

                if len(spectra_batch) >= batch_size:
                    flush_batches(
                        conn,
                        spectra_batch,
                        peak_scan_ids,
                        peak_indices,
                        peak_mzs,
                        peak_intensities,
                    )

        flush_batches(
            conn,
            spectra_batch,
            peak_scan_ids,
            peak_indices,
            peak_mzs,
            peak_intensities,
        )

        dominant_rt_unit = rt_units.most_common(1)[0][0] if rt_units else ""
        final_metadata = {
            "rt_unit": dominant_rt_unit,
            "spectrum_count": str(spectrum_count),
            "peak_count": str(peak_count),
            "mz_array_dtype": ",".join(sorted(mz_dtypes)),
            "intensity_array_dtype": ",".join(sorted(intensity_dtypes)),
            "conversion_warnings": json.dumps(warnings, sort_keys=True),
        }
        upsert_metadata(conn, final_metadata)
        validate_import(conn)
        conn.close()
        conn = None
        gc.collect()

        create_indexes_in_subprocess(output)
    except Exception:
        if conn is not None:
            conn.close()
        if output.exists():
            output.unlink()
        raise
    else:
        if conn is not None:
            conn.close()
    return output


def detect_source_format(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".mzml":
        return "mzML"
    if suffix == ".mzmlb":
        return "mzMLb"
    raise ValueError(f"Unsupported input format for mzDuck conversion: {path}")


def open_spectrum_reader(path: Path):
    source_format = detect_source_format(path)
    if source_format == "mzMLb":
        return mzmlb.MzMLb(str(path))
    return mzml.MzML(str(path))


def create_indexes_in_subprocess(output_path):
    """Create indexes in a fresh process after large bulk imports."""
    code = f"""
import duckdb
import sys

path = sys.argv[1]
index_sql = {INDEX_SQL!r}
view_sql = {VIEW_SQL!r}
con = duckdb.connect(path)
try:
    existing_indexes = {{
        row[0] for row in con.execute("SELECT index_name FROM duckdb_indexes()").fetchall()
    }}
    for statement in index_sql:
        index_name = statement.split()[2]
        if index_name not in existing_indexes:
            con.execute(statement)
    existing_views = {{
        row[0] for row in con.execute("SELECT view_name FROM duckdb_views()").fetchall()
    }}
    if "spectrum_peaks" not in existing_views:
        con.execute(view_sql)
finally:
    con.close()
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(output_path)],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        details = "\n".join(part for part in (result.stdout, result.stderr) if part)
        raise RuntimeError(f"Failed to create mzDuck indexes: {details}")


def spectrum_to_row(spectrum, *, scan_id: int, source_order: int):
    warnings: list[str] = []
    ms_level = as_int(spectrum.get("ms level"))
    if ms_level != 2:
        raise ValueError(
            f"Unsupported spectrum ms level at source index {source_order}: {ms_level}"
        )

    if "m/z array" not in spectrum or "intensity array" not in spectrum:
        raise ValueError(f"Spectrum {source_order} is missing required arrays")
    mz_array = np.asarray(spectrum["m/z array"])
    intensity_array = np.asarray(spectrum["intensity array"])
    if len(mz_array) != len(intensity_array):
        raise ValueError(
            f"Spectrum {source_order} has mismatched m/z and intensity lengths: "
            f"{len(mz_array)} != {len(intensity_array)}"
        )

    scan = first_nested(spectrum, "scanList", "scan", 0)
    if scan is None or "scan start time" not in scan:
        raise ValueError(f"Spectrum {source_order} is missing scan start time")
    rt, rt_unit = numeric_with_unit(scan["scan start time"], default_unit="minute")
    rt_unit = normalize_unit(rt_unit, default="minute")
    if rt is None:
        raise ValueError(f"Spectrum {source_order} has non-numeric scan start time")

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

    selected_ion_mz = as_float(selected_ion.get("selected ion m/z"))
    selected_ion_intensity = as_float(selected_ion.get("peak intensity"))
    selected_ion_charge = as_int(selected_ion.get("charge state"))
    isolation_target = as_float(isolation.get("isolation window target m/z"))
    activation_type, activation_cv = activation_info(activation)
    if activation_type and activation_type == next(iter(activation_cv.keys()), None):
        warnings.append(f"Spectrum {source_order} has unknown activation term: {activation_type}")

    precursor_mz = selected_ion_mz if selected_ion_mz is not None else isolation_target
    precursor_charge = selected_ion_charge
    precursor_intensity = selected_ion_intensity
    injection_time, injection_unit = numeric_with_unit(
        scan.get("ion injection time"), default_unit="millisecond"
    )
    if injection_time is None:
        injection_unit = None

    native_id = str(spectrum.get("id", f"scan={scan_id}"))
    centroided = None
    if "centroid spectrum" in spectrum:
        centroided = True
    elif "profile spectrum" in spectrum:
        centroided = False

    polarity = None
    if "positive scan" in spectrum:
        polarity = "positive"
    elif "negative scan" in spectrum:
        polarity = "negative"

    lowest_mz = as_float(spectrum.get("lowest observed m/z"))
    highest_mz = as_float(spectrum.get("highest observed m/z"))
    if len(mz_array):
        if lowest_mz is None:
            lowest_mz = float(np.nanmin(mz_array))
        if highest_mz is None:
            highest_mz = float(np.nanmax(mz_array))

    source_index = as_int(spectrum.get("index"))
    if source_index is None:
        source_index = source_order

    row = {
        "scan_id": scan_id,
        "source_index": source_index,
        "native_id": native_id,
        "scan_number": parse_scan_number(native_id),
        "ms_level": ms_level,
        "rt": rt,
        "rt_unit": rt_unit,
        "precursor_mz": precursor_mz,
        "precursor_charge": precursor_charge,
        "precursor_intensity": precursor_intensity,
        "selected_ion_mz": selected_ion_mz,
        "selected_ion_intensity": selected_ion_intensity,
        "selected_ion_charge": selected_ion_charge,
        "collision_energy": as_float(activation.get("collision energy")),
        "activation_type": activation_type,
        "activation_cv": dumps_json(activation_cv) if activation_cv else None,
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
        "num_peaks": int(len(mz_array)),
        "polarity": polarity,
        "centroided": centroided,
        "filter_string": scan.get("filter string"),
        "ion_injection_time": injection_time,
        "ion_injection_time_unit": injection_unit,
        "monoisotopic_mz": as_float(
            scan.get("[Thermo Trailer Extra]Monoisotopic M/Z:")
        ),
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
    return row, (mz_array, intensity_array), warnings


def activation_info(activation):
    if not isinstance(activation, dict) or not activation:
        return None, {}
    activation_cv = {str(k): v for k, v in activation.items()}
    for key in activation:
        lowered = str(key).lower()
        if lowered in NON_ACTIVATION_KEYS:
            continue
        if lowered in ACTIVATION_MAP:
            return ACTIVATION_MAP[lowered], activation_cv
        return str(key), activation_cv
    return None, activation_cv


def flush_batches(
    conn,
    spectra_batch,
    peak_scan_ids,
    peak_indices,
    peak_mzs,
    peak_intensities,
):
    if spectra_batch:
        spectra_df = pd.DataFrame(spectra_batch, columns=SPECTRA_COLUMNS)
        conn.register("_mzduck_spectra_batch", spectra_df)
        try:
            conn.execute("INSERT INTO spectra SELECT * FROM _mzduck_spectra_batch")
        finally:
            conn.unregister("_mzduck_spectra_batch")
        spectra_batch.clear()

    if peak_scan_ids:
        peaks_table = pa.table(
            {
                "scan_id": pa.array(np.concatenate(peak_scan_ids), type=pa.int64()),
                "peak_index": pa.array(np.concatenate(peak_indices), type=pa.int64()),
                "mz": pa.array(np.concatenate(peak_mzs), type=pa.float64()),
                "intensity": pa.array(
                    np.concatenate(peak_intensities), type=pa.float32()
                ),
            }
        )
        conn.register("_mzduck_peaks_batch", peaks_table)
        try:
            conn.execute("INSERT INTO peaks SELECT * FROM _mzduck_peaks_batch")
        finally:
            conn.unregister("_mzduck_peaks_batch")
        peak_scan_ids.clear()
        peak_indices.clear()
        peak_mzs.clear()
        peak_intensities.clear()


def validate_import(conn):
    mismatched_counts = conn.execute(
        """
        SELECT COUNT(*)
        FROM spectra s
        LEFT JOIN (
            SELECT scan_id, COUNT(*) AS n
            FROM peaks
            GROUP BY scan_id
        ) p ON p.scan_id = s.scan_id
        WHERE s.num_peaks != COALESCE(p.n, 0)
        """
    ).fetchone()[0]
    if mismatched_counts:
        raise ValueError(f"{mismatched_counts} spectra have mismatched peak counts")

    min_scan, max_scan, n_spectra = conn.execute(
        "SELECT MIN(scan_id), MAX(scan_id), COUNT(*) FROM spectra"
    ).fetchone()
    if n_spectra and (min_scan != 0 or max_scan != n_spectra - 1):
        raise ValueError("scan_id values are not contiguous from 0")

    bad_peak_indexes = conn.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT scan_id, COUNT(*) AS n, MAX(peak_index) AS max_peak_index
            FROM peaks
            GROUP BY scan_id
            HAVING n != max_peak_index + 1
        )
        """
    ).fetchone()[0]
    if bad_peak_indexes:
        raise ValueError(f"{bad_peak_indexes} spectra have non-contiguous peak_index values")
