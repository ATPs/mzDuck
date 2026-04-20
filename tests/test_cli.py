from __future__ import annotations

import json

import pytest

from mzduck.cli import main


def test_cli_help_includes_examples_and_precision_flags(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--help"])

    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "Examples:" in captured.out
    assert "mzduck convert input.mzML -o output.mzduck" in captured.out
    assert "input.mzML.gz" in captured.out
    assert "--parquet" in captured.out
    assert "mzml-mgf" in captured.out
    assert "physical `mgf`" in captured.out
    assert "--mz64" in captured.out
    assert "--inten32" in captured.out


def test_convert_help_mentions_parquet_and_mode_split(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["convert", "--help"])

    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "--parquet" in captured.out
    assert "--parquet-zip" in captured.out
    assert "run_metadata plus physical mgf only" in captured.out


def test_mzml_mgf_help_mentions_self_describing_output(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["mzml-mgf", "--help"])

    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "self-describing" in captured.out
    assert "rt_seconds" in captured.out
    assert "convert --parquet" in captured.out


def test_export_mgf_help_mentions_parquet_input(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["export-mgf", "--help"])

    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "input.mgf.parquet" in captured.out
    assert "self-describing" in captured.out
    assert "mzduck mzml-mgf" in captured.out


def test_cli_convert_inspect_and_export_mgf(tiny_mzml, tmp_path, capsys):
    db_path = tmp_path / "cli.mzduck"
    mgf_path = tmp_path / "cli.mgf"

    assert main(
        [
            "convert",
            str(tiny_mzml),
            "-o",
            str(db_path),
            "--overwrite",
            "--batch-size",
            "1",
            "--no-sha256",
        ]
    ) == 0
    assert db_path.exists()

    assert main(["inspect", str(db_path), "--json"]) == 0
    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["spectrum_count"] == 2
    assert summary["peak_count"] == 5

    assert main(["export-mgf", str(db_path), str(mgf_path)]) == 0
    assert mgf_path.exists()


def test_cli_export_mzml_with_out_and_precision_flags(tiny_mzduck, tmp_path):
    output = tmp_path / "cli.mzML"

    assert main(
        [
            "export-mzml",
            str(tiny_mzduck),
            "--out",
            str(output),
            "--32",
            "--mz64",
        ]
    ) == 0
    assert output.exists()
    text = output.read_text()
    assert 'name="64-bit float"' in text
    assert 'name="32-bit float"' in text
