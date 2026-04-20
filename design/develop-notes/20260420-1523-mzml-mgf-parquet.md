# Development Notes

Date: 2026-04-20 15:23 Asia/Shanghai

## Plan implemented

- add `mzduck mzml-mgf` as a CLI-only conversion command
- write one self-describing `mgf.parquet` file from `.mzML` or `.mzML.gz`
- keep `convert --parquet` unchanged as the physical-relation parquet container export
- make the single-file parquet standalone by adding derived `title`, `rt_unit`, and `rt_seconds`
- keep the public Python API unchanged

## Code changes made

### CLI

- added `mzml-mgf` subcommand to `mzduck/cli.py`
- exposed only `-o/--out`, `--overwrite`, `--batch-size`, `--compression`, `--compression-level`, `--start-scan`, and `--end-scan`
- updated top-level help text to distinguish `mzml-mgf` from `convert --parquet`

### Import and parquet export

- added `convert_mzml_to_mgf_parquet()` in `mzduck/import_mzml.py`
- implemented it by creating a temporary `--ms2-mgf-only` `.mzduck` file with `compute_sha256=False`
- added `copy_query_to_parquet()` so derived parquet projections can be written directly
- added a derived MGF parquet projection with columns:
  `scan_number`, `source_index`, `rt`, `precursor_mz`, `precursor_intensity`, `precursor_charge`, `title`, `rt_unit`, `rt_seconds`, `mz_array`, `intensity_array`
- made `rt_seconds` follow the same conversion rule as `export_mgf.rt_to_seconds()`
- made `title` self-contained by deriving `mgf_title_source` from the final output name, with special handling for the recommended `*.mgf.parquet` suffix
- converted the no-selected-MS2 case into a clear `mzml-mgf`-specific error

### Tests and docs

- added CLI help coverage for `mzml-mgf`
- added parquet tests for plain mzML, gzip mzML, scan-window export, schema checks, derived fields, and no-MS2 error handling
- updated `README.md`, `docs/usage.md`, and `design/design.md`

## Validation

- `/data/p/anaconda3/bin/python -m pytest tests/test_cli.py tests/test_parquet.py -q`
- `/data/p/anaconda3/bin/python -m pytest -q`
- full suite result: `28 passed, 1 warning`
