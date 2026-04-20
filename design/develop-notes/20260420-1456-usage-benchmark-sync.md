# Development Notes

Date: 2026-04-20 14:56 Asia/Shanghai

## Plan

- refresh the stale user-facing usage guide so it matches the current v2 storage model
- regenerate bundled example data so the checked-in `tiny.mzduck` file is v2 instead of v1
- add a regression assertion so example data drift is caught by tests
- close the open-ended benchmark caveat by recording the April 17 validation sizes in the main docs

## Changes made

### Documentation sync

- rewrote `docs/usage.md` around v2 behavior
- removed stale references to `schema_version = 1`, physical `spectrum_summary`, and the old table layout
- documented `.mzML.gz`, `--parquet`, `--parquet-zip`, derived `TITLE`, and exact-or-raw text reconstruction
- updated `mzduck/example_data/README.md` and `examples/make_example_data.py` to use `python` in the regenerate command

### Example data sync

- regenerated `mzduck/example_data/tiny.mzML`
- regenerated `mzduck/example_data/tiny.mzduck`
- verified the bundled `tiny.mzduck` now reports:
  - `schema_version = 2`
  - `container_format = duckdb`
  - tables `mgf` and `ms2_spectra`

### Benchmark closure

- added an observed validation snapshot to `README.md`
- added the same benchmark summary to `design/design.md`
- recorded the fixed `1556259` benchmark result:
  - previous default: 120.01 MiB
  - previous `--ms2-mgf-only`: 135.01 MiB
  - current v2 default: 90.26 MiB
  - current v2 `--ms2-mgf-only`: 90.26 MiB
  - matching mzMLb: 83.26 MiB
- recorded the fixed 10-file `.mzML.gz` validation-set summary:
  - default DuckDB output averaged 97.2% of source gzip size
  - observed range was 89.9% to 102.3%
- recorded three-file compact-mode sample sizes for `--ms2-mgf-only` and `--parquet-zip`
- updated the April 17 note so its old `Pending validation` section now points to this follow-up instead of leaving the caveat open

### Test coverage

- strengthened `tests/test_example_data.py` to require bundled example data to be v2
- asserted the bundled example file exposes only `mgf` and `ms2_spectra`

## Verification

- regenerated example data with:
  - `python examples/make_example_data.py`
- ran full tests:
  - `python -m pytest`
- result:
  - `24 passed, 1 warning`

## Remaining optional work

- broader benchmarking across additional PRIDE projects can still be done later if we want more coverage, but the original documentation and size-analysis caveats are now closed
