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
    assert "--mz64" in captured.out
    assert "--inten32" in captured.out


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
