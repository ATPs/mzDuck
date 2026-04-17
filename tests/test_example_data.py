from __future__ import annotations

from pyteomics import mgf, mzml

from mzduck import MzDuckFile, example_data_path


def test_bundled_example_data_files_are_readable():
    mzml_path = example_data_path("tiny.mzML")
    mzduck_path = example_data_path("tiny.mzduck")
    mgf_path = example_data_path("tiny.mgf")

    assert mzml_path.is_file()
    assert mzduck_path.is_file()
    assert mgf_path.is_file()

    with mzml.MzML(str(mzml_path)) as reader:
        spectra = list(reader)
    assert len(spectra) == 2
    assert sum(len(spectrum["m/z array"]) for spectrum in spectra) == 5

    with MzDuckFile.open(mzduck_path) as db:
        summary = db.inspect()
        assert summary["spectrum_count"] == 2
        assert summary["peak_count"] == 5
        assert db.get_spectrum(1)["native_id"].endswith("scan=1")

    with mgf.read(str(mgf_path)) as reader:
        spectra = list(reader)
    assert len(spectra) == 2
    assert sum(len(spectrum["m/z array"]) for spectrum in spectra) == 5
