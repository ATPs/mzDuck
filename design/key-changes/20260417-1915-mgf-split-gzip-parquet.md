# Key Changes

Date: 2026-04-17 19:15 Asia/Shanghai

- `mgf` is physical again and stores the MS2 payload.
- `ms2_spectra` is now detail-only and keyed by `scan_number`.
- `--ms2-mgf-only` now writes only `run_metadata` plus `mgf`.
- empty optional tables are dropped before final output.
- `.mzML.gz` is supported.
- gzip imports now disable pyteomics indexed seeking and use metadata-only pre-scan decoding.
- `mzduck convert` now supports `--parquet` and `--parquet-zip`.
- parquet outputs include only physical relations and omit empty optional relations.
- `TITLE` remains derived and is no longer promised as `mgf.title`.
- DuckDB remains the only format opened by `MzDuckFile.open()` and returned by the Python conversion API.
- compaction runs in a fresh process and falls back to table-copy compaction if `COPY FROM DATABASE` fails on a large run.
- README, `design/design.md`, and the dated design snapshots were updated to describe this architecture.
