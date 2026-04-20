# mzDuck

mzDuck stores centroid mzML runs in a compact relational container.

The current v2 design tries to keep two promises at the same time:

1. keep the final file close to mzMLb size in practice;
2. keep enough structured information to export the important mzML content back faithfully.

The storage model is now built around a physical `mgf` payload table for MS2, an optional `ms2_spectra` detail table for richer mzML metadata, sparse exact fallback tables only when needed, and a compact-copy final write step for DuckDB output.

## Current v2 model

### Core relations

- `run_metadata`
  stores provenance, original header fragments, templates, encoder policy, counts, table registry, `container_format`, and `source_compression`.
- `mgf`
  physical MS2 payload table. This is the MGF-native contract: `scan_number`, `source_index`, RT, precursor payload, and peak arrays.
- `ms2_spectra`
  physical detail-only MS2 table keyed by `scan_number`. This stores richer mzML detail that is not part of the MGF-native payload.
- `ms1_spectra`
  physical MS1 table when MS1 is included.
- `ms3_spectra`, `ms4_spectra`, ...
  physical higher-order MSn tables when present.
- `spectrum_text_overrides`
  sparse exact text fallbacks for `native_id`, `spectrum_ref`, and `filter_string`.
- `spectrum_extra_params`
  unmapped mzML parameters that still need exact round-trip preservation.

### What changed in this version

- `mgf` is a real stored table again, not a compatibility view.
- `ms2_spectra` no longer duplicates RT, precursor payload, or peak arrays.
- `TITLE` is still derived, but it is not stored in `mgf`.
- empty optional tables are dropped before the final `.mzduck` file is written.
- `.mzML.gz` is supported end-to-end.
- `mzduck convert` can now write DuckDB, parquet folders, or parquet zip archives.
- `mzduck mzml-mgf` can write one self-describing single-file MGF parquet.

## Mode behavior

### Default

Stores:

- `run_metadata`
- `mgf`
- `ms2_spectra`
- `ms1_spectra` when present
- `msN_spectra` when present
- non-empty `spectrum_text_overrides`
- non-empty `spectrum_extra_params`

### `--ms2-only`

Stores:

- `run_metadata`
- `mgf`
- `ms2_spectra`
- non-empty `spectrum_text_overrides`
- non-empty `spectrum_extra_params`

### `--ms2-mgf-only`

Stores:

- `run_metadata`
- `mgf`

This mode intentionally preserves the MGF-native contract only. It is smaller because it does not keep the richer mzML detail layer.

### `--ms1-only`

Stores:

- `run_metadata`
- `ms1_spectra`

## Reconstructed vs stored fields

### `TITLE`

`TITLE` is always derived from:

```text
{mgf_title_source}.{scan_number}.{scan_number}.{precursor_charge}
```

It is available through the Python API and MGF export behavior, but not as a physical `mgf.title` column.

### `native_id`

If every selected spectrum matches one exact run-level template such as:

```text
controllerType=0 controllerNumber=1 scan={scan_number}
```

then mzDuck stores the template once in `run_metadata`.

If not, mzDuck stores only the exact exception rows in `spectrum_text_overrides`.

### `spectrum_ref`

If every precursor reference matches one exact template such as:

```text
controllerType=0 controllerNumber=1 scan={precursor_scan_number}
```

then mzDuck stores that template in `run_metadata`.

If not, mzDuck stores the original exact strings in `spectrum_text_overrides`.

### `filter_string`

Filter-string reconstruction is conservative and per-run.

- mzDuck tests encoder candidates against the full selected run.
- a candidate rule is accepted only if it reproduces every selected value exactly.
- the accepted id is written to `run_metadata`, for example `thermo_ms2_v1`.
- if no exact rule exists for that file, mzDuck sets `filter_string_encoding = raw` and stores the original strings.

This avoids inventing approximate Thermo strings for files that do not fit the rule exactly.

## Container formats

### DuckDB `.mzduck`

This is the primary container.

- `MzDuckFile.open()` supports only this format.
- `MzDuckFile.from_mzml()` and the top-level `from_mzml()` convenience API still write this format only.
- writes use a staging file and a fresh final database compaction step.
- mzDuck tries `COPY FROM DATABASE` first.
- if DuckDB crashes on a large run during that step, mzDuck falls back to a fresh-process table copy into the final database.

### `--parquet`

`mzduck convert --parquet` writes one parquet file per physical relation into an output folder.

- only physical relations are written
- no derived compatibility views are written
- empty optional relations are omitted
- parquet compression reuses `--compression` and `--compression-level`

### `--parquet-zip`

`mzduck convert --parquet-zip` writes the same parquet members into one zip archive.

- zip-level compression is disabled
- each member stays a plain parquet file
- empty optional relations are omitted here too

### `mzml-mgf`

`mzduck mzml-mgf` writes one self-describing parquet file for MS2 MGF export.

- output is a single file, not a parquet container
- it keeps the MGF payload columns and adds `title`, `rt_unit`, and `rt_seconds`
- `title` stores only the file-name title source, not the full per-spectrum TITLE
- recommended output naming is `*.mgf.parquet`
- `mzduck convert --parquet` still exports physical relations as-is

## Reference size targets

Reference benchmarking during development used representative PRIDE data,
including public accession `PXD010154`.

Baseline size references discussed during the redesign:

| File | Size |
| --- | ---: |
| `1556259.mzML` | 409 MiB |
| `1556259.mzMLb` | 84 MiB |
| previous `1556259.mzduck` | 121 MiB |
| previous `1556259.ms2-mgf-only.mzduck` | 136 MiB |

The current v2 split is designed so that:

- full/default output and `--ms2-mgf-only` are structurally different again
- the full/default run can preserve the mzML detail layer
- `--ms2-mgf-only` can stay smaller by keeping only the MGF-native payload

### Observed validation snapshot

The fixed `1556259` benchmark now lands at:

| Artifact | Size |
| --- | ---: |
| `1556259.mzML` | 408.44 MiB |
| `1556259.mzMLb` | 83.26 MiB |
| previous `1556259.mzduck` | 120.01 MiB |
| previous `1556259.ms2-mgf-only.mzduck` | 135.01 MiB |
| `1556259.v2.mzduck` | 90.26 MiB |
| `1556259.v2.ms2-mgf-only.mzduck` | 90.26 MiB |

That means the current v2 default output is about 29.75 MiB smaller than the
previous default file, the current v2 `--ms2-mgf-only` output is about 44.75
MiB smaller than the previous `--ms2-mgf-only` file, and the v2 default output
is within about 7 MiB of the matching mzMLb benchmark.

Across the fixed 10-file `.mzML.gz` PRIDE validation set used during the April
17 validation pass, default DuckDB output averaged 97.2% of the source gzip
size, with an observed range of 89.9% to 102.3%.

Sample compact-mode sizes from the same validation run:

| Stem | Default | `--ms2-mgf-only` | `--parquet-zip` |
| --- | ---: | ---: | ---: |
| `708040` | 279.01 MiB | 144.26 MiB | 281.47 MiB |
| `1802513` | 84.51 MiB | 8.26 MiB | 80.39 MiB |
| `1861786` | 159.76 MiB | 68.76 MiB | 161.99 MiB |

## Installation

From this repository:

```bash
python -m pip install -e .
```

Top-level imports remain stable:

```python
from mzduck import MzDuckFile, from_mzml, open, to_mgf, to_mzml
```

## Command line

Landing help:

```bash
python -m mzduck --help
```

Focused conversion help:

```bash
python -m mzduck convert --help
```

Focused MGF parquet help:

```bash
python -m mzduck mzml-mgf --help
```

### Common conversions

DuckDB:

```bash
python -m mzduck convert \
  input.mzML \
  -o output.mzduck \
  --overwrite
```

Gzipped mzML:

```bash
python -m mzduck convert \
  input.mzML.gz \
  -o output.mzduck \
  --overwrite
```

MS2 MGF-only:

```bash
python -m mzduck convert \
  input.mzML.gz \
  -o output.ms2-mgf-only.mzduck \
  --ms2-mgf-only \
  --overwrite
```

Parquet folder:

```bash
python -m mzduck convert \
  input.mzML.gz \
  -o output.parquet.dir \
  --parquet \
  --overwrite
```

Parquet zip:

```bash
python -m mzduck convert \
  input.mzML.gz \
  -o output.parquet.zip \
  --parquet-zip \
  --overwrite
```

Single-file MGF parquet:

```bash
python -m mzduck mzml-mgf \
  input.mzML.gz \
  -o output.mgf.parquet \
  --overwrite
```

Inspect:

```bash
python -m mzduck inspect output.mzduck --json
```

Export MGF:

```bash
python -m mzduck export-mgf output.mzduck -o output.mgf --overwrite
python -m mzduck export-mgf output.mgf.parquet -o output.mgf --overwrite
```

For self-describing `*.mgf.parquet` input, `export-mgf` reconstructs the full
per-spectrum `TITLE` from the stored `title` source plus `scan_number` and
charge.

Export mzML:

```bash
python -m mzduck export-mzml output.mzduck -o roundtrip.mzML --overwrite
```

Precision control for mzML export:

```bash
python -m mzduck export-mzml output.mzduck -o out.mzML --32 --overwrite
python -m mzduck export-mzml output.mzduck -o out.mzML --mz64 --inten32 --overwrite
```

## Python usage

Create a new `.mzduck` file:

```python
from mzduck import MzDuckFile

db = MzDuckFile.from_mzml(
    "input.mzML.gz",
    "output.mzduck",
    overwrite=True,
    compute_sha256=False,
)

try:
    print(db.inspect())
    spectrum = db.get_spectrum(2)
    print(spectrum["title"])
    print(spectrum["native_id"])
    print(spectrum["mz"])
    print(spectrum["intensity"])
finally:
    db.close()
```

Open an existing file:

```python
from mzduck import MzDuckFile

with MzDuckFile.open("output.mzduck") as db:
    print(db.metadata()["schema_version"])
    print(db.get_spectrum(1)["title"])
```

Top-level convenience functions:

```python
from mzduck import from_mzml, to_mgf, to_mzml

from_mzml("input.mzML.gz", "output.mzduck", overwrite=True, compute_sha256=False)
to_mgf("output.mzduck", "output.mgf")
to_mzml("output.mzduck", "output.mzML")
```

## SQL examples

### MGF-native MS2 payload

```sql
SELECT
  scan_number,
  source_index,
  rt,
  precursor_mz,
  precursor_charge
FROM mgf
ORDER BY source_index;
```

### MS2 payload plus detail layer

```sql
SELECT
  m.scan_number,
  m.rt,
  m.precursor_mz,
  d.precursor_scan_number,
  d.collision_energy,
  d.activation_type
FROM mgf AS m
LEFT JOIN ms2_spectra AS d USING (scan_number)
ORDER BY m.source_index;
```

### Extract an ion chromatogram from stored arrays

```sql
SELECT rt, SUM(p.intensity) AS xic
FROM (
  SELECT
    rt,
    UNNEST(mz_array) AS mz,
    UNNEST(intensity_array) AS intensity
  FROM mgf
) AS p
WHERE p.mz BETWEEN 499.5 AND 500.5
GROUP BY rt
ORDER BY rt;
```

### Find raw text fallbacks that had to be stored

```sql
SELECT scan_number, field_name, value
FROM spectrum_text_overrides
ORDER BY scan_number, field_name;
```

### Inspect unmapped mzML params

```sql
SELECT scan_number, scope, ordinal, name, value
FROM spectrum_extra_params
ORDER BY scan_number, scope, ordinal;
```

## Manual validation ideas

For larger validation runs, compare the same representative `.mzML.gz` inputs
across:

- default DuckDB output
- `--ms2-mgf-only`
- `--parquet`
- `--parquet-zip`

Example commands:

```bash
python -m mzduck convert \
  input.mzML.gz \
  -o output.default.mzduck \
  --overwrite \
  --no-sha256

python -m mzduck convert \
  input.mzML.gz \
  -o output.ms2-mgf-only.mzduck \
  --ms2-mgf-only \
  --overwrite \
  --no-sha256

python -m mzduck convert \
  input.mzML.gz \
  -o output.parquet.dir \
  --parquet \
  --overwrite \
  --no-sha256

python -m mzduck convert \
  input.mzML.gz \
  -o output.parquet.zip \
  --parquet-zip \
  --overwrite \
  --no-sha256
```

## Design notes

The main design doc is:

- `design/design.md`

Supporting design snapshots:

- `design/20260416design.codex.md`
- `design/20260416design.claude.md`

Development notes are recorded in:

- `design/develop-notes/`
- `design/key-changes/`
