# Development Notes

Date: 2026-04-20 16:05 Asia/Shanghai

## Plan implemented

- extend `mzduck export-mgf` to accept self-describing `*.mgf.parquet`
- keep the public Python API unchanged
- change `mzduck mzml-mgf` parquet `title` values from per-spectrum TITLE text to the file-name title source only
- keep text MGF export behavior unchanged by reconstructing full per-spectrum `TITLE` from title source, `scan_number`, and charge

## Code changes made

### Export path

- added parquet-input support in `mzduck/export_mgf.py`
- kept `.mzduck -> .mgf` export behavior unchanged
- added validation that parquet input must have the self-describing `mzml-mgf` columns
- added shared MGF record writing so `.mzduck` and `.mgf.parquet` use the same text formatting
- made parquet export reconstruct per-spectrum TITLE from the stored title source
- added compatibility handling so older parquet files that already stored full TITLE still export correctly

### `mzml-mgf` parquet schema semantics

- changed `mzduck/import_mzml.py` so the `title` column stores only the file-name title source
- kept `rt_unit` and `rt_seconds` unchanged
- left the rest of the single-file parquet schema unchanged

### CLI, tests, and docs

- updated `export-mgf` help text to accept `.mzduck` and self-describing `.mgf.parquet`
- updated parquet tests to expect source-only `title`
- added export tests for `mgf.parquet -> .mgf`
- added a negative test that rejects physical `convert --parquet` `mgf.parquet`
- updated `README.md`, `docs/usage.md`, and `design/design.md`

## Validation

- `/data/p/anaconda3/bin/python -m pytest -q`
- full suite result: `31 passed, 1 warning`

## Real-file validation

- regenerated:
  `/data2/pub/proteome/PRIDE/protinsight/2019/07/PXD010154/temp/temp/1556259.mgf.parquet`
- confirmed parquet `title` now has one distinct value: `1556259`
- exported:
  `/data2/pub/proteome/PRIDE/protinsight/2019/07/PXD010154/temp/temp/1556259.from-mgf-parquet.mgf`
- verified:
  - parquet row count = `55814`
  - `BEGIN IONS` count = `55814`
  - `pyteomics` parsed spectrum count = `55814`
  - first five reconstructed `TITLE=` values matched parquet-derived expectations
