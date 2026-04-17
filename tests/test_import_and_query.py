from __future__ import annotations

import numpy as np

from mzduck import MzDuckFile


def test_import_creates_schema_and_metadata(tiny_mzduck):
    with MzDuckFile.open(tiny_mzduck, read_only=True) as db:
        metadata = db.metadata()
        assert metadata["schema_version"] == "1"
        assert metadata["format_name"] == "mzDuck"
        assert metadata["source_filename"] == "tiny.mzML"
        assert metadata["run_id"] == "tiny_run"
        assert metadata["spectrum_count"] == "2"
        assert metadata["peak_count"] == "5"

        assert db.query("SELECT COUNT(*) FROM spectra").fetchone()[0] == 2
        assert db.query("SELECT COUNT(*) FROM peaks").fetchone()[0] == 5
        indexes = {
            row[0] for row in db.query("SELECT index_name FROM duckdb_indexes()").fetchall()
        }
        assert "idx_peaks_scan_id" in indexes
        assert "idx_spectra_precursor_mz" in indexes


def test_get_spectrum_preserves_peak_order(tiny_mzduck):
    with MzDuckFile.open(tiny_mzduck, read_only=True) as db:
        spectrum = db.get_spectrum(1)
        assert spectrum["scan_id"] == 1
        assert spectrum["native_id"] == "controllerType=0 controllerNumber=1 scan=2"
        assert spectrum["scan_number"] == 2
        assert spectrum["rt"] == 2.0
        assert spectrum["rt_unit"] == "minute"
        assert spectrum["precursor_charge"] == 3
        assert spectrum["activation_type"] == "CID"
        np.testing.assert_allclose(spectrum["mz"], np.asarray([300.0, 250.0]))
        np.testing.assert_allclose(spectrum["intensity"], np.asarray([5.0, 15.0]))


def test_sql_queries(tiny_mzduck):
    with MzDuckFile.open(tiny_mzduck, read_only=True) as db:
        precursor_hits = db.query(
            """
            SELECT native_id
            FROM spectra
            WHERE precursor_mz BETWEEN ? AND ?
            """,
            [445.0, 446.0],
        ).fetchall()
        assert precursor_hits == [("controllerType=0 controllerNumber=1 scan=1",)]

        xic = db.query(
            """
            SELECT s.rt, SUM(p.intensity) AS xic
            FROM spectra s
            JOIN peaks p ON p.scan_id = s.scan_id
            WHERE p.mz BETWEEN 149.0 AND 151.0
            GROUP BY s.rt
            ORDER BY s.rt
            """
        ).fetchall()
        assert xic == [(1.5, 50.0)]


def test_inspect_summary(tiny_mzduck):
    with MzDuckFile.open(tiny_mzduck, read_only=True) as db:
        summary = db.inspect()
    assert summary["schema_version"] == "1"
    assert summary["source_filename"] == "tiny.mzML"
    assert summary["spectrum_count"] == 2
    assert summary["peak_count"] == 5
    assert summary["charge_distribution"] == {"2": 1, "3": 1}
