"""Public mzDuck file API."""

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np

from .export_mgf import export_mgf
from .export_mzml import export_mzml
from .import_mzml import convert_mzml_to_mzduck
from .schema import validate_required_schema


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
        index_scan_number=False,
        compute_sha256=True,
    ) -> "MzDuckFile":
        output = convert_mzml_to_mzduck(
            mzml_path,
            output_path,
            overwrite=overwrite,
            batch_size=batch_size,
            compression=compression,
            compression_level=compression_level,
            index_scan_number=index_scan_number,
            compute_sha256=compute_sha256,
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
        """Export to MGF format."""
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
        """Get a single spectrum by mzML scan number."""
        cursor = self.conn.execute(
            "SELECT * FROM spectra WHERE scan_number = ?",
            [scan_number],
        )
        row = cursor.fetchone()
        if row is None:
            raise KeyError(f"No spectrum with scan_number={scan_number}")
        columns = [item[0] for item in cursor.description]
        result = dict(zip(columns, row))
        meta = self.metadata()
        result["native_id"] = reconstruct_native_id(result, meta)
        result["rt_unit"] = meta.get("rt_unit")
        result["polarity"] = meta.get("polarity") or None
        result["centroided"] = str(meta.get("centroided", "")).lower() == "true"
        result["ion_injection_time_unit"] = meta.get("ion_injection_time_unit")
        result["mz"] = np.asarray(
            result["mz_array"],
            dtype=numpy_dtype_for_storage(meta.get("mz_array_storage_dtype")),
        )
        result["intensity"] = np.asarray(
            result["intensity_array"],
            dtype=numpy_dtype_for_storage(meta.get("intensity_array_storage_dtype")),
        )
        return result

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
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS spectrum_count,
                COALESCE(SUM(len(mz_array)), 0) AS peak_count,
                MIN(scan_number) AS scan_number_min,
                MAX(scan_number) AS scan_number_max,
                MIN(rt) AS rt_min,
                MAX(rt) AS rt_max,
                MIN(precursor_mz) AS precursor_mz_min,
                MAX(precursor_mz) AS precursor_mz_max
            FROM spectra
            """
        ).fetchone()
        charge_distribution = dict(
            self.conn.execute(
                """
                SELECT COALESCE(CAST(precursor_charge AS VARCHAR), 'NULL'), COUNT(*)
                FROM spectra
                GROUP BY precursor_charge
                ORDER BY precursor_charge
                """
            ).fetchall()
        )
        activation_distribution = dict(
            self.conn.execute(
                """
                SELECT COALESCE(activation_type, 'NULL'), COUNT(*)
                FROM spectra
                GROUP BY activation_type
                ORDER BY COUNT(*) DESC, activation_type
                """
            ).fetchall()
        )
        scan_min = row[2]
        scan_max = row[3]
        spectrum_count = int(row[0])
        scan_numbers_contiguous = (
            scan_min is not None
            and scan_max is not None
            and scan_max - scan_min + 1 == spectrum_count
        )
        return {
            "schema_version": meta.get("schema_version"),
            "source_filename": meta.get("source_filename"),
            "run_id": meta.get("run_id"),
            "spectrum_count": spectrum_count,
            "peak_count": int(row[1]),
            "scan_number_range": [scan_min, scan_max],
            "scan_numbers_contiguous": scan_numbers_contiguous,
            "rt_range": [row[4], row[5]],
            "precursor_mz_range": [row[6], row[7]],
            "charge_distribution": charge_distribution,
            "activation_type_distribution": activation_distribution,
            "rt_unit": meta.get("rt_unit"),
            "polarity": meta.get("polarity"),
            "centroided": meta.get("centroided"),
            "ion_injection_time_unit": meta.get("ion_injection_time_unit"),
            "native_id_template": meta.get("native_id_template"),
            "compression": meta.get("compression"),
            "compression_level": meta.get("compression_level"),
            "index_scan_number": meta.get("index_scan_number"),
            "mz_array_storage_dtype": meta.get("mz_array_storage_dtype"),
            "intensity_array_storage_dtype": meta.get(
                "intensity_array_storage_dtype"
            ),
            "file_size": self.path.stat().st_size,
        }

    def close(self):
        """Close the database connection."""
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


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
