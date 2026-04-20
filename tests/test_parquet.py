from __future__ import annotations

import zipfile

import duckdb

from mzduck.cli import main


def parquet_scalar(path, sql):
    conn = duckdb.connect()
    try:
        return conn.execute(sql.format(path=str(path))).fetchone()[0]
    finally:
        conn.close()


def test_cli_convert_parquet_directory_omits_empty_tables(tiny_mzml, tmp_path):
    output = tmp_path / "tiny-parquet"

    assert main(
        [
            "convert",
            str(tiny_mzml),
            "-o",
            str(output),
            "--parquet",
            "--overwrite",
            "--batch-size",
            "1",
            "--no-sha256",
        ]
    ) == 0

    members = {path.name for path in output.iterdir()}
    assert members == {"run_metadata.parquet", "mgf.parquet", "ms2_spectra.parquet"}
    assert parquet_scalar(
        output / "mgf.parquet",
        "SELECT COUNT(*) FROM read_parquet('{path}')",
    ) == 2
    assert parquet_scalar(
        output / "run_metadata.parquet",
        "SELECT value FROM read_parquet('{path}') WHERE key = 'container_format'",
    ) == "parquet"


def test_cli_convert_parquet_zip_uses_stored_members(tiny_mzml, tmp_path):
    output = tmp_path / "tiny.parquet.zip"

    assert main(
        [
            "convert",
            str(tiny_mzml),
            "-o",
            str(output),
            "--parquet-zip",
            "--overwrite",
            "--batch-size",
            "1",
            "--no-sha256",
        ]
    ) == 0

    with zipfile.ZipFile(output) as archive:
        members = {info.filename for info in archive.infolist()}
        assert members == {"run_metadata.parquet", "mgf.parquet", "ms2_spectra.parquet"}
        assert all(info.compress_type == zipfile.ZIP_STORED for info in archive.infolist())
        archive.extract("mgf.parquet", path=tmp_path)

    assert parquet_scalar(
        tmp_path / "mgf.parquet",
        "SELECT COUNT(*) FROM read_parquet('{path}')",
    ) == 2


def test_parquet_keeps_non_empty_override_tables(tiny_raw_filter_mzml, tmp_path):
    output = tmp_path / "raw-filter-parquet"

    assert main(
        [
            "convert",
            str(tiny_raw_filter_mzml),
            "-o",
            str(output),
            "--parquet",
            "--overwrite",
            "--batch-size",
            "1",
            "--no-sha256",
        ]
    ) == 0

    members = {path.name for path in output.iterdir()}
    assert members == {
        "run_metadata.parquet",
        "mgf.parquet",
        "ms2_spectra.parquet",
        "spectrum_text_overrides.parquet",
    }
    assert parquet_scalar(
        output / "spectrum_text_overrides.parquet",
        "SELECT COUNT(*) FROM read_parquet('{path}') WHERE field_name = 'filter_string'",
    ) == 1
