# mzDuck

mzDuck is a DuckDB-backed container for centroid mzML runs.

The v2 layout is designed around two goals that pull in opposite directions:

1. keep the file close to mzMLb size in practice;
2. keep enough information to reconstruct the important mzML fields exactly.

The core idea is to store one canonical MS2 table, derive the public MGF shape
as a view, hoist repeatable text into run-level templates, keep sparse exact
fallback rows only when reconstruction is not exact, and finish every write
with a compact copy into a fresh DuckDB file.

## What V2 Stores

`schema_version = 2` uses these relations:

- `run_metadata`
  run-level provenance, original mzML header fragments, templates, encoder
  choices, counts, ranges, and table registry.
- `ms1_spectra`
  physical MS1 spectra and arrays when MS1 is included.
- `ms2_spectra`
  canonical physical MS2 spectra and arrays.
- `mgf`
  compatibility view derived from `ms2_spectra`. `TITLE` is computed, not
  stored.
- `spectrum_text_overrides`
  sparse exact fallback rows for `native_id`, `spectrum_ref`, and
  `filter_string`.
- `spectrum_extra_params`
  mzML params that do not map to typed columns.
- `ms3_spectra`, `ms4_spectra`, ...
  higher MSn tables when present.

There is no physical `spectrum_summary` table in v2. Summary counts live in
`run_metadata`, and `inspect()` derives the report from metadata plus the
present tables.

## PRIDE Acceptance Run

Reference data:

- source mzML:
  `/data2/pub/proteome/PRIDE/protinsight/2019/07/PXD010154/temp/temp/1556259.mzML`
- reference mzMLb:
  `/data2/pub/proteome/PRIDE/protinsight/2019/07/PXD010154/temp/temp/1556259.mzMLb`

Observed sizes on 2026-04-17 with the current v2 implementation:

| File | Size |
| --- | ---: |
| `1556259.mzML` | 408.44 MiB |
| `1556259.mzMLb` | 83.26 MiB |
| old `1556259.mzduck` | 120.01 MiB |
| v2 `1556259.v2.mzduck` | 90.26 MiB |
| old `1556259.ms2-mgf-only.mzduck` | 135.01 MiB |
| v2 `1556259.v2.ms2-mgf-only.mzduck` | 90.26 MiB |

For this PRIDE run, v2 validated:

- exact scan order recovery;
- exact `native_id` recovery;
- exact `spectrumRef` recovery;
- exact Thermo `filter string` recovery;
- exact per-scan `instrumentConfigurationRef` recovery.

The PRIDE file now lands much closer to mzMLb while preserving the important
MS2 fields exactly.

## Installation

From this repository:

```bash
/data/p/anaconda3/bin/python -m pip install -e .
```

Top-level imports remain stable:

```python
from mzduck import MzDuckFile, from_mzml, open, to_mgf, to_mzml
```

## Command Line

Detailed landing help:

```bash
/data/p/anaconda3/bin/python -m mzduck --help
```

Focused convert help:

```bash
/data/p/anaconda3/bin/python -m mzduck convert --help
```

Quick examples:

```bash
/data/p/anaconda3/bin/python -m mzduck convert \
  mzduck/example_data/tiny.mzML \
  -o /tmp/tiny.mzduck \
  --overwrite \
  --no-sha256

/data/p/anaconda3/bin/python -m mzduck inspect /tmp/tiny.mzduck --json

/data/p/anaconda3/bin/python -m mzduck export-mgf /tmp/tiny.mzduck -o /tmp/tiny.mgf --overwrite

/data/p/anaconda3/bin/python -m mzduck export-mzml /tmp/tiny.mzduck -o /tmp/tiny.roundtrip.mzML --overwrite
```

MS level selection:

```bash
/data/p/anaconda3/bin/python -m mzduck convert input.mzML -o out.mzduck --ms2-mgf-only
/data/p/anaconda3/bin/python -m mzduck convert input.mzML -o out.mzduck --ms2-only
/data/p/anaconda3/bin/python -m mzduck convert input.withMS1.mzML -o out.mzduck --ms1-only
/data/p/anaconda3/bin/python -m mzduck convert input.withMS1.mzML -o out.mzduck --no-ms1
```

Precision control for mzML export:

```bash
/data/p/anaconda3/bin/python -m mzduck export-mzml in.mzduck -o out.mzML --32 --overwrite
/data/p/anaconda3/bin/python -m mzduck export-mzml in.mzduck -o out.mzML --mz64 --inten32 --overwrite
```

## Python Usage

Create a new file:

```python
from mzduck import MzDuckFile

db = MzDuckFile.from_mzml(
    "mzduck/example_data/tiny.mzML",
    "/tmp/tiny.mzduck",
    overwrite=True,
    compute_sha256=False,
)

try:
    print(db.inspect())
    spectrum = db.get_spectrum(2)
    print(spectrum["title"])
    print(spectrum["native_id"])
    print(spectrum["spectrum_ref"])
    print(spectrum["filter_string"])
    print(spectrum["mz"])
    print(spectrum["intensity"])
finally:
    db.close()
```

Open an existing file:

```python
from mzduck import MzDuckFile, example_data_path

with MzDuckFile.open(example_data_path("tiny.mzduck")) as db:
    print(db.metadata()["schema_version"])
    print(db.get_spectrum(1)["title"])
```

Top-level convenience functions:

```python
from mzduck import from_mzml, open, to_mgf, to_mzml

from_mzml("input.mzML", "output.mzduck", overwrite=True, compute_sha256=False)
to_mgf("output.mzduck", "output.mgf")
to_mzml("output.mzduck", "output.mzML")
```

## Reconstruction Policy

The public API and export code reconstruct several fields instead of storing
them redundantly.

### MGF title

`mgf.title` is always derived from:

```text
{mgf_title_source}.{scan_number}.{scan_number}.{precursor_charge}
```

The title stays publicly queryable through the `mgf` view and Python API, but
it is not physically stored in v2.

### `native_id`

If every selected spectrum matches one exact run-level template such as:

```text
controllerType=0 controllerNumber=1 scan={scan_number}
```

then the template is stored once in `run_metadata` and no per-row text is kept.
If any row does not match, only those rows go into `spectrum_text_overrides`.

### `spectrum_ref`

If every selected precursor reference matches one exact template such as:

```text
controllerType=0 controllerNumber=1 scan={precursor_scan_number}
```

then the template is stored once in `run_metadata`. Otherwise, exact raw values
go into `spectrum_text_overrides`.

### Filter strings

Filter-string reconstruction is run-specific, not a global Thermo assumption.

- mzDuck keeps a registry of encoder candidates.
- A candidate is accepted only if it reproduces every selected row exactly.
- The accepted encoder id is stored in `run_metadata`, for example
  `thermo_ms2_v1`.
- If no candidate matches the whole run exactly, mzDuck sets
  `filter_string_encoding = raw` and stores the original strings.
- Sparse per-row overrides remain possible even when a run-level encoder exists.

### Extra mzML params

Anything that is not mapped to a typed field is kept in
`spectrum_extra_params` instead of being dropped.

In the PRIDE acceptance run, the only repeated non-typed field was a
scan-level `instrumentConfigurationRef`. That field is now stored as a typed
column instead of inflating `spectrum_extra_params`.

## Compaction

DuckDB `VACUUM` was not enough to reach the target size on the PRIDE run.
The v2 writer therefore always uses this flow:

1. write the import into a staging DuckDB file;
2. validate the staging schema;
3. copy into a fresh database with `COPY FROM DATABASE`;
4. validate the compacted file;
5. atomically rename the compacted file into place.

This compact-copy step is part of normal v2 output, not an optional cleanup.

## SQL Examples

### Canonical MS2 table

```sql
SELECT
  scan_number,
  source_index,
  rt,
  precursor_mz,
  precursor_charge,
  collision_energy,
  activation_type
FROM ms2_spectra
ORDER BY source_index
LIMIT 10;
```

### Public MGF compatibility view

```sql
SELECT
  scan_number,
  title,
  rt,
  precursor_mz,
  precursor_charge
FROM mgf
ORDER BY scan_number
LIMIT 10;
```

### Peak extraction from the MGF view

```sql
SELECT
  scan_number,
  generate_subscripts(mz_array, 1) - 1 AS peak_index,
  UNNEST(mz_array) AS mz,
  UNNEST(intensity_array) AS intensity
FROM mgf
WHERE scan_number = 1
ORDER BY peak_index;
```

### Product-ion chromatogram

```sql
SELECT rt, SUM(intensity) AS xic
FROM (
  SELECT
    rt,
    UNNEST(mz_array) AS mz,
    UNNEST(intensity_array) AS intensity
  FROM mgf
) peaks
WHERE mz BETWEEN 149.0 AND 151.0
GROUP BY rt
ORDER BY rt;
```

### Exact fallback rows

```sql
SELECT scan_number, field_name, value
FROM spectrum_text_overrides
ORDER BY scan_number, field_name;
```

### Additional mzML params not covered by typed columns

```sql
SELECT scan_number, scope, name, value
FROM spectrum_extra_params
ORDER BY scan_number, scope, ordinal;
```

## Large-File Validation Commands

Full PRIDE conversion:

```bash
/data/p/anaconda3/bin/python -m mzduck convert \
  /data2/pub/proteome/PRIDE/protinsight/2019/07/PXD010154/temp/temp/1556259.mzML \
  -o /data2/pub/proteome/PRIDE/protinsight/2019/07/PXD010154/temp/temp/1556259.v2.mzduck \
  --overwrite \
  --no-sha256
```

MS2-only compact conversion:

```bash
/data/p/anaconda3/bin/python -m mzduck convert \
  /data2/pub/proteome/PRIDE/protinsight/2019/07/PXD010154/temp/temp/1556259.mzML \
  -o /data2/pub/proteome/PRIDE/protinsight/2019/07/PXD010154/temp/temp/1556259.v2.ms2-mgf-only.mzduck \
  --overwrite \
  --no-sha256 \
  --ms2-mgf-only
```

Inspect the result:

```bash
/data/p/anaconda3/bin/python -m mzduck inspect \
  /data2/pub/proteome/PRIDE/protinsight/2019/07/PXD010154/temp/temp/1556259.v2.mzduck \
  --json
```

One exact-recovery validation pass:

```bash
/data/p/anaconda3/bin/python - <<'PY'
from pyteomics import mzml
from mzduck import MzDuckFile
from mzduck.export_mzml import iter_export_spectra, load_extra_params, load_text_overrides
from mzduck.metadata import parse_scan_number

source_path = "/data2/pub/proteome/PRIDE/protinsight/2019/07/PXD010154/temp/temp/1556259.mzML"
db_path = "/data2/pub/proteome/PRIDE/protinsight/2019/07/PXD010154/temp/temp/1556259.v2.mzduck"

with MzDuckFile.open(db_path, read_only=True) as db:
    metadata = db.metadata()
    rebuilt = iter_export_spectra(
        db.conn,
        metadata=metadata,
        text_overrides=load_text_overrides(db.conn),
        extra_params=load_extra_params(db.conn),
    )
    mismatches = 0
    with mzml.MzML(source_path) as reader:
        for source_spectrum in reader:
            rebuilt_spectrum = next(rebuilt)
            source_scan = source_spectrum["scanList"]["scan"][0]
            source_precursor = source_spectrum["precursorList"]["precursor"][0]
            checks = [
                rebuilt_spectrum["scan_number"] == parse_scan_number(source_spectrum["id"]),
                rebuilt_spectrum["spectrum_ref"] == source_precursor.get("spectrumRef"),
                rebuilt_spectrum["filter_string"] == source_scan.get("filter string"),
                rebuilt_spectrum["instrument_configuration_ref"] == source_scan.get("instrumentConfigurationRef"),
            ]
            if not all(checks):
                mismatches += 1
                break
    print({"mismatches": mismatches})
PY
```

## Tests

Run the test suite:

```bash
/data/p/anaconda3/bin/python -m pytest -q
```

The fast regression set covers:

- v2 schema validation;
- `mgf` compatibility view behavior;
- absence of physical `spectrum_summary`;
- `get_spectrum()`, `export_mgf()`, and `export_mzml()`;
- top-level package imports;
- detailed top-level CLI help;
- exact filter-string encoder detection and raw fallback;
- mzML header restoration and scan-level instrument configuration export.
