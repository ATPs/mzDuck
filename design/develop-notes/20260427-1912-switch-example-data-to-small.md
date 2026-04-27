Date: 2026-04-27 19:12

Plan:
- replace the too-small `tiny.*` example set under `examples/data/`
- use `/data/p/xiaolong/mzPeak/mzPeak/small.mzML` as the source requested by the user
- if the source is not directly importable by mzDuck, normalize it first with `msconvert`
- keep the same example format coverage: mzML, mzDuck, MGF, and MGF parquet

Findings:
- the source `small.mzML` is profile data, so direct `mzduck convert` fails with
  `mzDuck only supports centroid spectra`
- `msconvert --mzML --filter "peakPicking true 1-"` produced a usable centroid
  `examples/data/small.mzML`

Changes made:
- removed the temporary `tiny.*` files from `examples/data/`
- generated `examples/data/small.mzML` via `msconvert` peak picking from the requested source file
- generated `examples/data/small.mzduck` with `python -m mzduck convert`
- generated `examples/data/small.mzduck.mgf` with `python -m mzduck export-mgf`
- generated `examples/data/small.mgf.parquet` with `python -m mzduck mzml-mgf`
- generated `examples/data/small.mgf` with `msconvert --mgf`
- updated `examples/README.md` to describe the new `small.*` dataset

Verification:
- confirmed `examples/data/` contains `small.mzML`, `small.mzduck`, `small.mgf`, `small.mzduck.mgf`, and `small.mgf.parquet`
- verified `small.mzML` has 48 centroid-tagged spectra
- verified `small.mgf.parquet` has 34 rows
- verified `small.mzduck` contains 34 `mgf` rows and 14 `ms1_spectra` rows
- verified both MGF files contain 34 spectra and parse with `pyteomics.mgf.read(...)`
