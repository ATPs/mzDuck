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


def parquet_columns(path):
    conn = duckdb.connect()
    try:
        rows = conn.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [str(path)],
        ).fetchall()
        return [row[0] for row in rows]
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


def test_cli_mzml_mgf_writes_single_self_describing_parquet(tiny_mzml, tmp_path):
    output = tmp_path / "tiny.mgf.parquet"

    assert main(
        [
            "mzml-mgf",
            str(tiny_mzml),
            "-o",
            str(output),
            "--overwrite",
            "--batch-size",
            "1",
        ]
    ) == 0

    assert output.exists()
    assert parquet_columns(output) == [
        "scan_number",
        "source_index",
        "rt",
        "precursor_mz",
        "precursor_intensity",
        "precursor_charge",
        "title",
        "rt_unit",
        "rt_seconds",
        "mz_array",
        "intensity_array",
    ]
    assert parquet_scalar(
        output,
        "SELECT COUNT(*) FROM read_parquet('{path}')",
    ) == 2
    assert parquet_scalar(
        output,
        "SELECT title FROM read_parquet('{path}') WHERE scan_number = 1",
    ) == "tiny"
    assert parquet_scalar(
        output,
        "SELECT title FROM read_parquet('{path}') WHERE scan_number = 2",
    ) == "tiny"
    assert parquet_scalar(
        output,
        "SELECT rt_unit FROM read_parquet('{path}') LIMIT 1",
    ) == "minute"
    assert parquet_scalar(
        output,
        "SELECT rt_seconds FROM read_parquet('{path}') WHERE scan_number = 1",
    ) == 90.0


def test_cli_mzml_mgf_supports_gzip_input_and_scan_windows(tiny_mzml_gz, tmp_path):
    output = tmp_path / "window.mgf.parquet"

    assert main(
        [
            "mzml-mgf",
            str(tiny_mzml_gz),
            "-o",
            str(output),
            "--overwrite",
            "--start-scan",
            "2",
            "--end-scan",
            "2",
        ]
    ) == 0

    assert parquet_scalar(
        output,
        "SELECT COUNT(*) FROM read_parquet('{path}')",
    ) == 1
    assert parquet_scalar(
        output,
        "SELECT scan_number FROM read_parquet('{path}')",
    ) == 2
    assert parquet_scalar(
        output,
        "SELECT title FROM read_parquet('{path}')",
    ) == "window"


def test_cli_mzml_mgf_fails_when_selected_scans_have_no_ms2(
    tiny_with_ms1_mzml, tmp_path, capsys
):
    output = tmp_path / "ms1-only.mgf.parquet"

    assert main(
        [
            "mzml-mgf",
            str(tiny_with_ms1_mzml),
            "-o",
            str(output),
            "--start-scan",
            "1",
            "--end-scan",
            "1",
        ]
    ) == 1

    captured = capsys.readouterr()
    assert "do not contain any MS2 spectra" in captured.err
    assert not output.exists()
