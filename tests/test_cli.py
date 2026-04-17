from __future__ import annotations

import json

from mzduck.cli import main


def test_cli_convert_inspect_and_export_mgf(tiny_mzml, tmp_path, capsys):
    db_path = tmp_path / "cli.mzduck"
    mgf_path = tmp_path / "cli.mgf"
    mzmlb_path = tmp_path / "cli.mzMLb"

    assert main(
        [
            "convert",
            str(tiny_mzml),
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

    assert main(["export-mzmlb", str(db_path), str(mzmlb_path)]) == 0
    assert mzmlb_path.exists()
