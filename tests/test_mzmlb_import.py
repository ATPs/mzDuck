from __future__ import annotations

from pyteomics import mzmlb

from mzduck import MzDuckFile, example_data_path


def test_convert_tiny_mzmlb_to_mzduck(tmp_path):
    source = example_data_path("tiny.mzMLb")
    output = tmp_path / "tiny-from-mzmlb.mzduck"

    handle = MzDuckFile.from_mzml(
        source,
        output,
        overwrite=True,
        batch_size=1,
        compute_sha256=False,
    )
    try:
        metadata = handle.metadata()
        assert metadata["source_format"] == "mzMLb"
        assert metadata["source_filename"] == "tiny.mzMLb"
        assert metadata["spectrum_count"] == "2"
        assert metadata["peak_count"] == "5"

        spectrum = handle.get_spectrum(0)
        assert spectrum["native_id"].endswith("scan=1")
        assert spectrum["precursor_mz"] == 445.34
        assert spectrum["activation_type"] == "HCD"
    finally:
        handle.close()

    with mzmlb.MzMLb(str(source)) as reader:
        source_spectra = list(reader)
    with MzDuckFile.open(output) as db:
        assert db.query("SELECT COUNT(*) FROM spectra").fetchone()[0] == len(source_spectra)
