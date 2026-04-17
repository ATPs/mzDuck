from __future__ import annotations

from lxml import etree
from pyteomics import mgf, mzml

from mzduck import MzDuckFile


NS = {"mz": "http://psi.hupo.org/ms/mzml"}


def array_precision_terms(path):
    tree = etree.parse(str(path))
    terms = []
    for binary_array in tree.xpath(".//mz:binaryDataArray", namespaces=NS):
        names = [
            item.get("name")
            for item in binary_array.xpath("./mz:cvParam", namespaces=NS)
        ]
        if "m/z array" in names:
            array_type = "mz"
        elif "intensity array" in names:
            array_type = "intensity"
        else:
            continue
        if "32-bit float" in names:
            precision = 32
        elif "64-bit float" in names:
            precision = 64
        else:
            precision = None
        terms.append((array_type, precision))
    return terms


def test_export_mgf(tiny_mzduck, tmp_path):
    output = tmp_path / "tiny.mgf"
    with MzDuckFile.open(tiny_mzduck, read_only=True) as db:
        db.to_mgf(output)

    text = output.read_text()
    assert "TITLE=tiny.1.1.2" in text
    assert "TITLE=tiny.2.2.3" in text
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


def test_export_mzml_precision_flags(tiny_mzduck, tmp_path):
    output = tmp_path / "roundtrip-precision.mzML"
    with MzDuckFile.open(tiny_mzduck, read_only=True) as db:
        db.to_mzml(output, mz_precision=32, intensity_precision=64)

    assert array_precision_terms(output) == [
        ("mz", 32),
        ("intensity", 64),
        ("mz", 32),
        ("intensity", 64),
    ]
    spectra = list(mzml.MzML(str(output)))
    assert len(spectra) == 2
