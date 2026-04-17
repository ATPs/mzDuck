from __future__ import annotations

import numpy as np
import pytest
from pyteomics import mzml

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
        assert metadata["mgf_title_template"] == (
            "{mgf_title_source}.{scan_number}.{scan_number}.{precursor_charge}"
        )
        assert "<fileDescription" in metadata["file_description_xml"]
        assert "<softwareList" in metadata["software_list_xml"]
        assert "<instrumentConfigurationList" in metadata["instrument_configuration_xml"]
        assert "<dataProcessingList" in metadata["data_processing_xml"]
        assert metadata["ms1_spectrum_count"] == "0"
        assert metadata["ms2_spectrum_count"] == "2"
        assert metadata["ms2_peak_count"] == "5"

        assert db.query("SELECT COUNT(*) FROM mgf").fetchone()[0] == 2
        assert db.query("SELECT SUM(len(mz_array)) FROM mgf").fetchone()[0] == 5
        assert db.query("SELECT COUNT(*) FROM ms2_spectra").fetchone()[0] == 2
        assert db.query("SELECT COUNT(*) FROM spectrum_summary").fetchone()[0] == 2
        assert {row[0] for row in db.query("SHOW TABLES").fetchall()} == {
            "mgf",
            "ms2_spectra",
            "run_metadata",
            "spectrum_summary",
        }
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
        assert spectrum["title"] == "tiny.2.2.3"
        np.testing.assert_allclose(spectrum["mz"], np.asarray([300.0, 250.0]))
        np.testing.assert_allclose(spectrum["intensity"], np.asarray([5.0, 15.0]))


def test_sql_queries(tiny_mzduck):
    with MzDuckFile.open(tiny_mzduck, read_only=True) as db:
        precursor_hits = db.query(
            """
            SELECT scan_number
            FROM mgf
            WHERE precursor_mz BETWEEN ? AND ?
            """,
            [445.0, 446.0],
        ).fetchall()
        assert precursor_hits == [(1,)]

        xic = db.query(
            """
            SELECT rt, SUM(p.intensity) AS xic
            FROM (
                SELECT
                    rt,
                    UNNEST(mz_array) AS mz,
                    UNNEST(intensity_array) AS intensity
                FROM mgf
            ) p
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
    assert summary["ms1_spectrum_count"] == 0
    assert summary["ms2_spectrum_count"] == 2
    assert summary["mgf_spectrum_count"] == 2
    assert summary["scan_number_range"] == [1, 2]
    assert summary["scan_numbers_contiguous"] is True
    assert summary["charge_distribution"] == {"2": 1, "3": 1}
    assert [item["table"] for item in summary["tables"]] == [
        "mgf",
        "ms2_spectra",
        "spectrum_summary",
    ]


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
        assert "idx_mgf_scan_number" in indexes


def test_ms2_mgf_only_mode_has_only_mgf_contract(tiny_mzml, tmp_path):
    path = tmp_path / "mgf-only.mzduck"
    handle = MzDuckFile.from_mzml(
        tiny_mzml,
        path,
        overwrite=True,
        batch_size=1,
        ms2_mgf_only=True,
        compute_sha256=False,
    )
    handle.close()

    with MzDuckFile.open(path, read_only=True) as db:
        tables = {row[0] for row in db.query("SHOW TABLES").fetchall()}
        assert tables == {"mgf", "run_metadata"}
        assert db.metadata()["import_mode"] == "ms2_mgf_only"
        spectrum = db.get_spectrum(1)
        assert spectrum["title"] == "mgf-only.1.1.2"
        assert spectrum["activation_type"] is None

        mzml_path = tmp_path / "mgf-only-roundtrip.mzML"
        db.to_mzml(mzml_path)

    spectra = list(mzml.MzML(str(mzml_path)))
    assert len(spectra) == 2
    assert spectra[0]["ms level"] == 2


def test_ms1_default_and_ms1_only_modes(tiny_with_ms1_mzml, tmp_path):
    default_path = tmp_path / "with-ms1.mzduck"
    handle = MzDuckFile.from_mzml(
        tiny_with_ms1_mzml,
        default_path,
        overwrite=True,
        batch_size=1,
        compute_sha256=False,
    )
    handle.close()

    with MzDuckFile.open(default_path, read_only=True) as db:
        tables = {row[0] for row in db.query("SHOW TABLES").fetchall()}
        assert {"mgf", "ms1_spectra", "ms2_spectra", "spectrum_summary"} <= tables
        assert db.inspect()["ms1_spectrum_count"] == 2
        assert db.inspect()["ms2_spectrum_count"] == 1
        ms1 = db.get_spectrum(1)
        assert ms1["ms_level"] == 1
        np.testing.assert_allclose(ms1["mz"], np.asarray([400.0, 500.0]))

    ms1_only_path = tmp_path / "ms1-only.mzduck"
    handle = MzDuckFile.from_mzml(
        tiny_with_ms1_mzml,
        ms1_only_path,
        overwrite=True,
        batch_size=1,
        ms1_only=True,
        compute_sha256=False,
    )
    handle.close()

    with MzDuckFile.open(ms1_only_path, read_only=True) as db:
        tables = {row[0] for row in db.query("SHOW TABLES").fetchall()}
        assert tables == {"ms1_spectra", "run_metadata", "spectrum_summary"}
        assert db.inspect()["ms1_spectrum_count"] == 2
        assert db.inspect()["ms2_spectrum_count"] == 0
        with pytest.raises(ValueError, match="mgf table"):
            db.to_mgf(tmp_path / "should-not-exist.mgf")


def test_start_end_scan_subset(tiny_with_ms1_mzml, tmp_path):
    path = tmp_path / "subset.mzduck"
    handle = MzDuckFile.from_mzml(
        tiny_with_ms1_mzml,
        path,
        overwrite=True,
        batch_size=1,
        start_scan=2,
        end_scan=2,
        compute_sha256=False,
    )
    handle.close()

    with MzDuckFile.open(path, read_only=True) as db:
        assert db.inspect()["total_spectrum_count"] == 1
        assert db.inspect()["ms2_spectrum_count"] == 1
        assert db.query("SELECT title FROM mgf").fetchone()[0] == "subset.2.2.2"
