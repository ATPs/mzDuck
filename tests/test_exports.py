from __future__ import annotations

from pyteomics import mgf, mzml, mzmlb

from mzduck import MzDuckFile


def test_export_mgf(tiny_mzduck, tmp_path):
    output = tmp_path / "tiny.mgf"
    with MzDuckFile.open(tiny_mzduck, read_only=True) as db:
        db.to_mgf(output)

    text = output.read_text()
    assert "TITLE=controllerType=0 controllerNumber=1 scan=1" in text
    assert "PEPMASS=445.34 1200" in text
    assert "CHARGE=2+" in text
    assert "RTINSECONDS=90" in text
    spectra = list(mgf.read(str(output)))
    assert len(spectra) == 2
    assert int(spectra[0]["params"]["charge"][0]) == 2


def test_export_mzml_is_parseable(tiny_mzduck, tmp_path):
    output = tmp_path / "roundtrip.mzML"
    with MzDuckFile.open(tiny_mzduck, read_only=True) as db:
        db.to_mzml(output)

    spectra = list(mzml.MzML(str(output)))
    assert len(spectra) == 2
    assert spectra[0]["ms level"] == 2
    assert spectra[0]["id"] == "controllerType=0 controllerNumber=1 scan=1"
    assert spectra[0]["scanList"]["scan"][0]["scan start time"] == 1.5
    assert list(spectra[1]["m/z array"]) == [300.0, 250.0]


def test_export_mzmlb_is_parseable(tiny_mzduck, tmp_path):
    output = tmp_path / "roundtrip.mzMLb"
    with MzDuckFile.open(tiny_mzduck, read_only=True) as db:
        db.to_mzmlb(output)

    spectra = list(mzmlb.MzMLb(str(output)))
    assert len(spectra) == 2
    assert spectra[0]["ms level"] == 2
    assert spectra[0]["id"] == "controllerType=0 controllerNumber=1 scan=1"
    assert spectra[0]["scanList"]["scan"][0]["scan start time"] == 1.5
    assert list(spectra[1]["m/z array"]) == [300.0, 250.0]
