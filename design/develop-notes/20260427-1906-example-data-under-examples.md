Date: 2026-04-27 19:06

Plan:
- place a small ready-to-use example dataset under `examples/`
- include the formats the user asked for: `.mzML`, `.mzduck`, `.mgf`, and `.mgf.parquet`
- use existing tiny example content so the files stay small and predictable
- validate that the generated example files are readable

Changes made:
- created `examples/data/`
- copied `mzduck/example_data/tiny.mzML` to `examples/data/tiny.mzML`
- generated `examples/data/tiny.mzduck` with `python -m mzduck convert`
- generated `examples/data/tiny.mzduck.mgf` with `python -m mzduck export-mgf`
- generated `examples/data/tiny.mgf.parquet` with `python -m mzduck mzml-mgf`
- generated `examples/data/tiny.mgf` with `msconvert --mgf`
- updated `examples/README.md` to describe the new sample files

Verification:
- confirmed `examples/data/` contains `tiny.mzML`, `tiny.mzduck`, `tiny.mgf`, `tiny.mzduck.mgf`, and `tiny.mgf.parquet`
- verified `tiny.mgf.parquet` has 2 rows with DuckDB
- verified `tiny.mzduck` contains 2 spectra and 5 peaks by querying `mgf`
- verified both `tiny.mgf` and `tiny.mzduck.mgf` contain 2 `BEGIN IONS` blocks and parse with `pyteomics.mgf.read(...)`
