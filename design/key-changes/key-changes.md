# Key Changes

This is the single rolling key-change log for the repository. Append new dated sections to this file instead of creating a new file in this directory.

## 2026-04-17 19:15 Asia/Shanghai

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

## 2026-04-17 20:35 Asia/Shanghai

- `README.md` now uses `python` instead of a machine-local interpreter path.
- machine-local PRIDE validation paths were removed from the public README.
- the manual validation section now uses generic example commands.

## 2026-04-20 14:56 Asia/Shanghai

- `docs/usage.md` now matches the v2 schema and mode behavior.
- bundled `mzduck/example_data/tiny.mzduck` was regenerated and is now a v2 file.
- `tests/test_example_data.py` now guards against example-data drift back to v1.
- `README.md` and `design/design.md` now include the April 17 benchmark summary.
- the old April 17 development note now points to this follow-up instead of leaving size analysis as an open caveat.

## 2026-04-20 15:23 Asia/Shanghai

- added `mzduck mzml-mgf` for direct `.mzML` and `.mzML.gz` to single-file MGF parquet export.
- the new output is self-describing and includes derived `title`, `rt_unit`, and `rt_seconds`.
- `convert --parquet` is unchanged and still exports physical relations as-is.
- `mzml-mgf` is CLI-only and does not change the public Python API.
- scan-window filtering is supported on the new command.
- selecting no MS2 spectra now returns a clear `mzml-mgf`-specific error.
- README, usage docs, and `design/design.md` now document the difference between single-file MGF parquet and parquet containers.

## 2026-04-20 16:05 Asia/Shanghai

- `mzduck export-mgf` now accepts self-describing `*.mgf.parquet` input from `mzduck mzml-mgf`.
- parquet input support remains limited to the single-file self-describing schema, not generic parquet containers.
- `mzduck mzml-mgf` now stores only the file-name title source in the parquet `title` column.
- `export-mgf` reconstructs the full per-spectrum TITLE from title source, `scan_number`, and charge.
- older parquet files that already stored full TITLE are still exported correctly.
- docs and design notes now describe the source-only `title` semantics and the new parquet-to-MGF export path.

## 2026-04-21 11:30 Asia/Shanghai

- mzDuck now declares an optional `numpress` extra that installs `pynumpress`.
- README installation docs now explain when `pynumpress` is needed.
- design docs now state that MS-Numpress-compressed mzML requires `pynumpress` to be installed and loadable.

## 2026-04-21 11:37 Asia/Shanghai

- `mzduck mzml-mgf --help` now shows how to convert the generated `.mgf.parquet` file to plain `.mgf`.
- CLI help coverage now checks for the paired `export-mgf` example.
- README, usage docs, and design docs now describe the same `mzml-mgf -> export-mgf` workflow.

## 2026-04-21 11:43 Asia/Shanghai

- consolidated the dated key-change entries in `design/key-changes/` into this single rolling file.
- `AGENTS.md` now directs future key-change updates to append to `design/key-changes/key-changes.md`.
- `design/design.md` now documents the single-file key-change workflow.
