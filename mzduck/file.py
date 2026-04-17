"""Public mzDuck file API."""

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np

from .export_mgf import export_mgf
from .export_mzml import export_mzml, export_mzmlb
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
        compute_sha256=True,
    ) -> "MzDuckFile":
        output = convert_mzml_to_mzduck(
            mzml_path,
            output_path,
            overwrite=overwrite,
            batch_size=batch_size,
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

    def to_mzml(self, output_path, *, overwrite=False):
        """Export to mzML format using psims."""
        return export_mzml(self.conn, output_path, overwrite=overwrite)

    def to_mzmlb(self, output_path, *, overwrite=False):
        """Export to mzMLb format using psims."""
        return export_mzmlb(self.conn, output_path, overwrite=overwrite)

    def get_spectrum(self, scan_id) -> dict:
        """Get a single spectrum by scan ID."""
        cursor = self.conn.execute(
            "SELECT * FROM spectra WHERE scan_id = ?",
            [scan_id],
        )
        row = cursor.fetchone()
        if row is None:
            raise KeyError(f"No spectrum with scan_id={scan_id}")
        columns = [item[0] for item in cursor.description]
        result = dict(zip(columns, row))
        peak_rows = self.conn.execute(
            """
            SELECT mz, intensity
            FROM peaks
            WHERE scan_id = ?
            ORDER BY peak_index
            """,
            [scan_id],
        ).fetchall()
        result["mz"] = np.asarray([peak[0] for peak in peak_rows], dtype=np.float64)
        result["intensity"] = np.asarray(
            [peak[1] for peak in peak_rows], dtype=np.float32
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
                COALESCE(SUM(num_peaks), 0) AS peak_count,
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
        return {
            "schema_version": meta.get("schema_version"),
            "source_filename": meta.get("source_filename"),
            "run_id": meta.get("run_id"),
            "spectrum_count": int(row[0]),
            "peak_count": int(row[1]),
            "rt_range": [row[2], row[3]],
            "precursor_mz_range": [row[4], row[5]],
            "charge_distribution": charge_distribution,
            "activation_type_distribution": activation_distribution,
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
