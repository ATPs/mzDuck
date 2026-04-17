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
        assert metadata["rt_unit"] == "minute"
        assert metadata["polarity"] == "positive"
        assert metadata["centroided"] == "true"
        assert metadata["native_id_template"] == (
            "controllerType=0 controllerNumber=1 scan={scan_number}"
        )
        assert metadata["index_scan_number"] == "false"

        assert db.query("SELECT COUNT(*) FROM spectra").fetchone()[0] == 2
        assert db.query("SELECT SUM(len(mz_array)) FROM spectra").fetchone()[0] == 5
        assert db.query("SELECT COUNT(*) FROM peaks").fetchone()[0] == 5
        indexes = {
            row[0] for row in db.query("SELECT index_name FROM duckdb_indexes()").fetchall()
        }
        assert indexes == set()


def test_get_spectrum_preserves_peak_order(tiny_mzduck):
    with MzDuckFile.open(tiny_mzduck, read_only=True) as db:
        spectrum = db.get_spectrum(2)
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
            SELECT scan_number
            FROM spectra
            WHERE precursor_mz BETWEEN ? AND ?
            """,
            [445.0, 446.0],
        ).fetchall()
        assert precursor_hits == [(1,)]

        xic = db.query(
            """
            SELECT rt, SUM(p.intensity) AS xic
            FROM spectra s
            JOIN peaks p ON p.scan_number = s.scan_number
            WHERE p.mz BETWEEN 149.0 AND 151.0
            GROUP BY rt
            ORDER BY rt
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
    assert summary["scan_number_range"] == [1, 2]
    assert summary["scan_numbers_contiguous"] is True
    assert summary["charge_distribution"] == {"2": 1, "3": 1}


def test_optional_scan_number_index(tiny_mzml, tmp_path):
    path = tmp_path / "indexed.mzduck"
    handle = MzDuckFile.from_mzml(
        tiny_mzml,
        path,
        overwrite=True,
        batch_size=1,
        index_scan_number=True,
        compute_sha256=False,
    )
    handle.close()

    with MzDuckFile.open(path, read_only=True) as db:
        metadata = db.metadata()
        indexes = {
            row[0]
            for row in db.query("SELECT index_name FROM duckdb_indexes()").fetchall()
        }
        assert metadata["index_scan_number"] == "true"
        assert "idx_spectra_scan_number" in indexes
