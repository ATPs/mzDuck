"""Public mzDuck file API."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import numpy as np

from .export_mgf import export_mgf
from .export_mzml import export_mzml
from .import_mzml import convert_mzml_to_mzduck
from .schema import msn_levels_present, msn_table_name, table_exists, validate_required_schema


class MzDuckFile:
    """Read/write access to a .mzduck file."""

    def __init__(self, path, conn, *, read_only=True):
        self.path = Path(path)
        self.conn = conn
        self.read_only = read_only

    @classmethod
    def from_mzml(
        cls,
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
    ) -> "MzDuckFile":
        output = convert_mzml_to_mzduck(
            mzml_path,
            output_path,
            overwrite=overwrite,
            batch_size=batch_size,
            compression=compression,
            compression_level=compression_level,
            index_scan=index_scan,
            index_scan_number=index_scan_number,
            compute_sha256=compute_sha256,
            ms2_mgf_only=ms2_mgf_only,
            no_ms1=no_ms1,
            ms2_only=ms2_only,
            ms1_only=ms1_only,
            start_scan=start_scan,
            end_scan=end_scan,
        )
        return cls.open(output, read_only=False)

    @classmethod
    def open(cls, path, read_only=True) -> "MzDuckFile":
        db_path = Path(path)
        if not db_path.exists():
            raise FileNotFoundError(f"mzDuck file does not exist: {db_path}")
        conn = duckdb.connect(str(db_path), read_only=read_only)
        try:
            validate_required_schema(conn)
        except Exception:
            conn.close()
            raise
        return cls(db_path, conn, read_only=read_only)

    def to_mgf(self, output_path, *, overwrite=False):
        """Export the stored MGF contract table to MGF format."""
        return export_mgf(self.conn, output_path, overwrite=overwrite)

    def to_mzml(
        self,
        output_path,
        *,
        overwrite=False,
        mz_precision=None,
        intensity_precision=None,
    ):
        """Export to mzML format using psims."""
        return export_mzml(
            self.conn,
            output_path,
            overwrite=overwrite,
            mz_precision=mz_precision,
            intensity_precision=intensity_precision,
        )

    def get_spectrum(self, scan_number) -> dict:
        """Get one spectrum by mzML scan number."""
        scan_number = int(scan_number)
        meta = self.metadata()

        if table_exists(self.conn, "mgf"):
            result = self._get_ms2_spectrum(scan_number)
            if result is not None:
                return finalize_spectrum(result, meta)

        if table_exists(self.conn, "ms1_spectra"):
            result = self._fetch_one("ms1_spectra", scan_number)
            if result is not None:
                return finalize_spectrum(result, meta)

        for level in msn_levels_present(self.conn):
            result = self._fetch_one(msn_table_name(level), scan_number)
            if result is not None:
                return finalize_spectrum(result, meta)

        raise KeyError(f"No spectrum with scan_number={scan_number}")

    def _get_ms2_spectrum(self, scan_number):
        if table_exists(self.conn, "ms2_spectra"):
            cursor = self.conn.execute(
                """
                SELECT
                    m.scan_number,
                    m.title,
                    2 AS ms_level,
                    COALESCE(d.rt, m.rt) AS rt,
                    d.source_index,
                    d.native_id,
                    m.precursor_mz,
                    m.precursor_charge,
                    m.precursor_intensity,
                    d.collision_energy,
                    d.activation_type,
                    d.isolation_window_target,
                    d.isolation_window_lower,
                    d.isolation_window_upper,
                    d.spectrum_ref,
                    d.base_peak_mz,
                    d.base_peak_intensity,
                    d.tic,
                    d.lowest_mz,
                    d.highest_mz,
                    d.filter_string,
                    d.ion_injection_time,
                    d.monoisotopic_mz,
                    d.scan_window_lower,
                    d.scan_window_upper,
                    m.mz_array,
                    m.intensity_array
                FROM mgf m
                LEFT JOIN ms2_spectra d USING (scan_number)
                WHERE m.scan_number = ?
                """,
                [scan_number],
            )
        else:
            cursor = self.conn.execute(
                """
                SELECT
                    scan_number,
                    title,
                    2 AS ms_level,
                    rt,
                    CAST(NULL AS INTEGER) AS source_index,
                    CAST(NULL AS VARCHAR) AS native_id,
                    precursor_mz,
                    precursor_charge,
                    precursor_intensity,
                    CAST(NULL AS FLOAT) AS collision_energy,
                    CAST(NULL AS VARCHAR) AS activation_type,
                    precursor_mz AS isolation_window_target,
                    CAST(NULL AS FLOAT) AS isolation_window_lower,
                    CAST(NULL AS FLOAT) AS isolation_window_upper,
                    CAST(NULL AS VARCHAR) AS spectrum_ref,
                    CAST(NULL AS FLOAT) AS base_peak_mz,
                    CAST(NULL AS FLOAT) AS base_peak_intensity,
                    CAST(NULL AS FLOAT) AS tic,
                    CAST(NULL AS FLOAT) AS lowest_mz,
                    CAST(NULL AS FLOAT) AS highest_mz,
                    CAST(NULL AS VARCHAR) AS filter_string,
                    CAST(NULL AS FLOAT) AS ion_injection_time,
                    CAST(NULL AS DOUBLE) AS monoisotopic_mz,
                    CAST(NULL AS FLOAT) AS scan_window_lower,
                    CAST(NULL AS FLOAT) AS scan_window_upper,
                    mz_array,
                    intensity_array
                FROM mgf
                WHERE scan_number = ?
                """,
                [scan_number],
            )
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [item[0] for item in cursor.description]
        return dict(zip(columns, row))

    def _fetch_one(self, table_name, scan_number):
        cursor = self.conn.execute(
            f"SELECT * FROM {table_name} WHERE scan_number = ?",
            [scan_number],
        )
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [item[0] for item in cursor.description]
        return dict(zip(columns, row))

    def query(self, sql, parameters=None):
        """Run arbitrary SQL against the database."""
        return self.conn.execute(sql, parameters or [])

    def metadata(self) -> dict[str, str | None]:
        rows = self.conn.execute(
            "SELECT key, value FROM run_metadata ORDER BY key"
        ).fetchall()
        return {key: value for key, value in rows}

    def inspect(self) -> dict:
        meta = self.metadata()
        registry = json.loads(meta.get("table_registry") or "[]")
        scan_range, rt_range = self._scan_and_rt_ranges()
        charge_distribution = {}
        precursor_mz_range = [None, None]
        if table_exists(self.conn, "mgf"):
            charge_distribution = dict(
                self.conn.execute(
                    """
                    SELECT COALESCE(CAST(precursor_charge AS VARCHAR), 'NULL'), COUNT(*)
                    FROM mgf
                    GROUP BY precursor_charge
                    ORDER BY precursor_charge
                    """
                ).fetchall()
            )
            precursor_mz_range = list(
                self.conn.execute(
                    "SELECT MIN(precursor_mz), MAX(precursor_mz) FROM mgf"
                ).fetchone()
            )
        return {
            "schema_version": meta.get("schema_version"),
            "source_filename": meta.get("source_filename"),
            "run_id": meta.get("run_id"),
            "import_mode": meta.get("import_mode"),
            "total_spectrum_count": int(meta.get("total_spectrum_count") or 0),
            "total_peak_count": int(meta.get("total_peak_count") or 0),
            "spectrum_count": int(meta.get("spectrum_count") or 0),
            "peak_count": int(meta.get("peak_count") or 0),
            "ms1_spectrum_count": int(meta.get("ms1_spectrum_count") or 0),
            "ms1_peak_count": int(meta.get("ms1_peak_count") or 0),
            "ms2_spectrum_count": int(meta.get("ms2_spectrum_count") or 0),
            "ms2_peak_count": int(meta.get("ms2_peak_count") or 0),
            "mgf_spectrum_count": int(meta.get("mgf_spectrum_count") or 0),
            "mgf_peak_count": int(meta.get("mgf_peak_count") or 0),
            "scan_number_range": scan_range,
            "scan_numbers_contiguous": scan_numbers_contiguous(scan_range, meta),
            "rt_range": rt_range,
            "precursor_mz_range": precursor_mz_range,
            "charge_distribution": charge_distribution,
            "rt_unit": meta.get("rt_unit"),
            "polarity": meta.get("polarity"),
            "centroided": meta.get("centroided"),
            "ion_injection_time_unit": meta.get("ion_injection_time_unit"),
            "native_id_template": meta.get("native_id_template"),
            "mgf_title_template": meta.get("mgf_title_template"),
            "compression": meta.get("compression"),
            "compression_level": meta.get("compression_level"),
            "index_scan": meta.get("index_scan"),
            "mz_array_storage_dtype": meta.get("mz_array_storage_dtype"),
            "intensity_array_storage_dtype": meta.get(
                "intensity_array_storage_dtype"
            ),
            "tables": registry,
            "file_size": self.path.stat().st_size,
        }

    def _scan_and_rt_ranges(self):
        if table_exists(self.conn, "spectrum_summary"):
            row = self.conn.execute(
                "SELECT MIN(scan_number), MAX(scan_number), MIN(rt), MAX(rt) FROM spectrum_summary"
            ).fetchone()
        elif table_exists(self.conn, "mgf"):
            row = self.conn.execute(
                "SELECT MIN(scan_number), MAX(scan_number), MIN(rt), MAX(rt) FROM mgf"
            ).fetchone()
        elif table_exists(self.conn, "ms1_spectra"):
            row = self.conn.execute(
                "SELECT MIN(scan_number), MAX(scan_number), MIN(rt), MAX(rt) FROM ms1_spectra"
            ).fetchone()
        else:
            return [None, None], [None, None]
        return [row[0], row[1]], [row[2], row[3]]

    def close(self):
        """Close the database connection."""
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


def finalize_spectrum(result, metadata):
    result = dict(result)
    result["native_id"] = reconstruct_native_id(result, metadata)
    result["rt_unit"] = metadata.get("rt_unit")
    result["polarity"] = metadata.get("polarity") or None
    result["centroided"] = str(metadata.get("centroided", "")).lower() == "true"
    result["ion_injection_time_unit"] = metadata.get("ion_injection_time_unit")
    if "mz_array" in result:
        result["mz"] = np.asarray(
            result["mz_array"],
            dtype=numpy_dtype_for_storage(metadata.get("mz_array_storage_dtype")),
        )
    if "intensity_array" in result:
        result["intensity"] = np.asarray(
            result["intensity_array"],
            dtype=numpy_dtype_for_storage(metadata.get("intensity_array_storage_dtype")),
        )
    return result


def reconstruct_native_id(row, metadata):
    native_id = row.get("native_id")
    if native_id:
        return native_id
    template = metadata.get("native_id_template")
    if template:
        return template.format(scan_number=row["scan_number"])
    return f"scan={row['scan_number']}"


def numpy_dtype_for_storage(storage_dtype):
    if storage_dtype == "FLOAT":
        return np.float32
    if storage_dtype == "DOUBLE":
        return np.float64
    return None


def scan_numbers_contiguous(scan_range, metadata):
    low, high = scan_range
    if low is None or high is None:
        return False
    total = int(metadata.get("total_spectrum_count") or metadata.get("spectrum_count") or 0)
    return high - low + 1 == total
