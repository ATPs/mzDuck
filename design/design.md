# mzDuck v2 Design

Date: 2026-04-17

## Purpose

mzDuck v2 is a compact relational representation of centroid mzML that aims to keep most of the size win of mzMLb while still preserving the structured information needed for useful mzML round-trip export.

## Main storage idea

The current design splits MS2 storage into two layers:

- `mgf`
  physical MS2 payload table for the MGF-native contract
- `ms2_spectra`
  physical detail-only MS2 table keyed by `scan_number`

This removes the worst MS2 duplication while still letting full/default output keep important mzML detail.

## Physical relations

### Always present

- `run_metadata`

### Present when MS2 is included

- `mgf`

### Present when richer MS2 detail is included

- `ms2_spectra`

### Optional

- `ms1_spectra`
- `ms3_spectra`, `ms4_spectra`, ...
- `spectrum_text_overrides`
- `spectrum_extra_params`

Empty optional tables are dropped before final output.

## Canonical columns

### `mgf`

Stored columns:

- `scan_number`
- `source_index`
- `rt`
- `precursor_mz`
- `precursor_intensity`
- `precursor_charge`
- `mz_array`
- `intensity_array`

Not stored:

- `title`

`TITLE` is derived from run metadata plus scan number and charge.

### `ms2_spectra`

Stored columns:

- `scan_number`
- `instrument_configuration_ref`
- `collision_energy`
- `activation_type`
- `isolation_window_target`
- `isolation_window_lower`
- `isolation_window_upper`
- `precursor_scan_number`
- `base_peak_mz`
- `base_peak_intensity`
- `tic`
- `lowest_mz`
- `highest_mz`
- `ion_injection_time`
- `monoisotopic_mz`
- `scan_window_lower`
- `scan_window_upper`

Not stored here:

- RT
- precursor payload
- arrays
- `title`
- row-wise `native_id`
- row-wise `spectrum_ref`
- row-wise `filter_string` when exact reconstruction exists

## Mode semantics

### Default

Stores:

- `run_metadata`
- `mgf`
- `ms2_spectra`
- optional MS1 and higher MSn tables
- non-empty fallback tables

### `--ms2-only`

Stores:

- `run_metadata`
- `mgf`
- `ms2_spectra`
- non-empty fallback tables

### `--ms2-mgf-only`

Stores:

- `run_metadata`
- `mgf`

This mode is intentionally smaller and does not preserve the richer mzML detail layer.

### `--ms1-only`

Stores:

- `run_metadata`
- `ms1_spectra`

## Reconstruction policy

### Exact-or-raw rule

If a field can be reconstructed exactly for the full selected run, store the rule once.
If not, store the original exact value.

This policy drives:

- `native_id`
- `spectrum_ref`
- `filter_string`

### Templates

Run-level templates are stored in `run_metadata`:

- `native_id_template`
- `spectrum_ref_template`

Only mismatching rows go into `spectrum_text_overrides`.

### Filter strings

Filter-string handling is run-specific.

- rules are implemented as encoder candidates
- candidates must match the full selected run exactly
- accepted rule id is written to `run_metadata`
- if no rule matches exactly, mzDuck stores the original strings

## Extra mzML params

`spectrum_extra_params` is reserved for genuine extras that do not already have:

- a clean typed column
- an exact run-level template

The design prefers promoting repeated structural mzML information into typed fields when that reduces size and improves export fidelity.

## Input formats

Supported input files:

- `.mzML`
- `.mzML.gz`

Runtime note:

- ordinary mzML input does not need extra compression helpers
- MS-Numpress-compressed mzML requires the optional `pynumpress` package to be installed and loadable in the active Python environment

`run_metadata` records:

- `source_format = mzML`
- `source_compression = none|gzip`

## Output containers

### DuckDB

Default output is one `.mzduck` file.

- writer uses staging plus a fresh final database copy
- `COPY FROM DATABASE` is attempted first
- if DuckDB fails on a large run during that step, mzDuck falls back to table-by-table copy into the fresh final database
- final file is validated before rename

### Parquet folder

`mzduck convert --parquet`

- writes one parquet file per physical relation
- omits derived views
- omits empty optional relations

### Parquet zip

`mzduck convert --parquet-zip`

- writes the same parquet members into one zip archive
- zip-level compression is disabled

### Single-file MGF parquet

`mzduck mzml-mgf`

- writes one self-describing parquet file for MS2 MGF export
- keeps the physical `mgf` payload columns
- adds derived `title`, `rt_unit`, and `rt_seconds`
- stores only the file-name title source in `title`
- is intended to pair with `mzduck export-mgf input.mgf.parquet -o output.mgf`
- is CLI-only and is not opened by `MzDuckFile.open()`
- remains distinct from `mzduck convert --parquet`, which exports physical relations as-is

## Reader/export behavior

### `MzDuckFile.get_spectrum()`

- joins `mgf` with `ms2_spectra` when detail exists
- returns MGF-native spectra plus derived `title` in `--ms2-mgf-only`
- reconstructs `native_id`, `spectrum_ref`, and `filter_string` from templates and overrides

### `export_mgf()`

- reads physical `mgf`
- derives `TITLE`
- for self-describing `*.mgf.parquet`, reconstructs the full TITLE from the
  stored title source plus `scan_number` and charge

### `export_mzml()`

- uses `mgf` for arrays, RT, precursor payload, and source order
- uses `ms2_spectra` when present for richer detail reconstruction
- merges text overrides and extra params

## Compatibility

- new writes default to `schema_version = 2`
- readers still support v1 and v2
- top-level Python API remains importable:
  `MzDuckFile`, `from_mzml`, `open`, `to_mgf`, `to_mzml`

## Validation snapshot

Observed size results from the April 17 validation run:

- `1556259.v2.mzduck` is 90.26 MiB, down from the previous 120.01 MiB default file
- `1556259.v2.ms2-mgf-only.mzduck` is 90.26 MiB, down from the previous 135.01 MiB `--ms2-mgf-only` file
- the matching `1556259.mzMLb` benchmark is 83.26 MiB, so the current v2 default output is within about 7 MiB
- across the fixed 10-file `.mzML.gz` PRIDE validation set, default DuckDB output averaged 97.2% of source gzip size, with a range of 89.9% to 102.3%
- on the three-file compact-mode sample set, `--ms2-mgf-only` measured 144.26 MiB for `708040`, 8.26 MiB for `1802513`, and 68.76 MiB for `1861786`
- on the same sample set, `--parquet-zip` tracked default physical-table size closely at 281.47 MiB, 80.39 MiB, and 161.99 MiB

This closes the original open-ended size-analysis note for the April 17 redesign work. Broader cross-project benchmarking can still be done later, but it is no longer a missing deliverable for the v2 rollout.

## Validation priorities

The current acceptance focus is:

- exact scan order
- exact `native_id`
- exact `spectrumRef`
- exact filter-string recovery when a run-level rule exists
- correct raw fallback when no exact rule exists
- structurally different outputs for full/default vs `--ms2-mgf-only`
- no empty optional tables in final outputs
- correct `.mzML.gz` handling
- correct parquet/parquet-zip physical exports

## Project note workflow

- detailed work logs continue to use timestamped files in `design/develop-notes/`
- key change summaries are now kept in the single rolling file `design/key-changes/key-changes.md`
