Date: 2026-04-21 11:57

Plan:
- reproduce the failure on `1555583.mzML.gz` and identify the exact field/value
- implement the smallest shared-import fix so malformed precursor-charge values do not abort conversion
- follow the user's preference: skip the bad spectrum and print a warning
- verify with targeted pytest coverage and a real-file rerun

Findings:
- the failure was not a missing charge
- `scan=26926` in `1555583.mzML.gz` carries `selectedIon/charge state = 1294941225`
- that value overflows the Arrow `int8` column used for `precursor_charge`, causing
  `pyarrow.lib.ArrowInvalid: Value 1294941225 too large to fit in C integer type`

Changes made:
- added precursor-charge range validation in `mzduck/import_mzml.py`
- if precursor charge is outside the signed `TINYINT` range, the importer now skips that spectrum
- skip events are printed to stderr as `mzduck warning: ...`
- skip messages are also recorded in `conversion_warnings`
- summary metadata now rewrites `included_ms_level_counts` and `included_ms_level_peak_counts` from the spectra actually inserted, so skip cases stay consistent
- added a synthetic test fixture with an invalid precursor charge
- added parquet/export regression tests for skip behavior
- updated `design/design.md` to record the malformed-charge import behavior

Verification:
- `/data/p/anaconda3/bin/python -m pytest tests/test_parquet.py -q`
- `/data/p/anaconda3/bin/python -m pytest tests/test_exports.py -q`
- `/data/p/anaconda3/bin/python -m pytest tests/test_import_and_query.py -q`
- real file rerun:
  `/data/p/anaconda3/bin/python -m mzduck mzml-mgf /data2/pub/proteome/PRIDE/mzML/2019/07/PXD010154/1555583.mzML.gz -o /tmp/1555583.skip-test.mgf.parquet --overwrite`
  result: warning printed for `scan 26926`, parquet row count `48123`, `scan 26926` absent, temp file cleaned
