# mzDuck

mzDuck is a Python package for storing centroid mzML spectra in a single
DuckDB database file. It keeps the MGF export content in a narrow `mgf` table,
stores MS1/MS2/MSn detail tables separately, and records mzML header fragments
plus table summaries in `run_metadata`.

The current v1 scope is intentionally focused:

- input: centroid mzML;
- storage: one `.mzduck` DuckDB database per source run;
- output: `.mgf` and `.mzML`;
- API: Python class plus `mzduck` command-line tool.

## Repository Contents

```text
mzduck/                     Python package
mzduck/example_data/         tiny bundled mzML, mzDuck, and MGF examples
examples/                    runnable example scripts
docs/usage.md                detailed usage guide
tests/                       pytest suite
design/20260416design.codex.md
                             v1 format and package specification
```

## Installation

From this repository:

```bash
/data/p/anaconda3/bin/python -m pip install -e .
```

Check that the CLI is available:

```bash
mzduck --help
```

## Bundled Example Data

The package includes a tiny two-spectrum example set:

```text
mzduck/example_data/tiny.mzML
mzduck/example_data/tiny.mzduck
mzduck/example_data/tiny.mgf
```

The tiny mzML file has two centroid MS2 spectra and five total peaks. It is
useful for smoke tests, docs, and quick manual inspection.

Regenerate the example data with:

```bash
/data/p/anaconda3/bin/python examples/make_example_data.py
```

## Command-Line Quick Start

Convert mzML to mzDuck:

```bash
mzduck convert \
  mzduck/example_data/tiny.mzML \
  -o /tmp/tiny.mzduck \
  --overwrite \
  --no-sha256 \
  --compression zstd
```

Convert only the MGF-relevant MS2 data:

```bash
mzduck convert input.mzML -o /tmp/ms2-mgf-only.mzduck --ms2-mgf-only --overwrite
```

Convert a scan window or select MS levels:

```bash
mzduck convert input.withMS1.mzML -o /tmp/window.mzduck --start-scan 1000 --end-scan 2000
mzduck convert input.withMS1.mzML -o /tmp/ms1-only.mzduck --ms1-only
mzduck convert input.withMS1.mzML -o /tmp/ms2-only.mzduck --ms2-only
mzduck convert input.withMS1.mzML -o /tmp/no-ms1.mzduck --no-ms1
```

Create the optional scan lookup index. The index is only created on
`mgf(scan_number)`:

```bash
mzduck convert input.mzML -o /tmp/indexed.mzduck --index-scan --overwrite
```

Inspect the mzDuck file:

```bash
mzduck inspect /tmp/tiny.mzduck --json
```

Export MGF:

```bash
mzduck export-mgf /tmp/tiny.mzduck -o /tmp/tiny.mgf --overwrite
```

Export mzML:

```bash
mzduck export-mzml /tmp/tiny.mzduck -o /tmp/tiny.roundtrip.mzML --overwrite
```

Control mzML binary array precision:

```bash
# Both arrays as 32-bit floats.
mzduck export-mzml /tmp/tiny.mzduck -o /tmp/tiny.32.mzML --32 --overwrite

# Per-array control.
mzduck export-mzml /tmp/tiny.mzduck -o /tmp/tiny.mz64-int32.mzML --mz64 --inten32 --overwrite

# Mixed precision is allowed.
mzduck export-mzml /tmp/tiny.mzduck -o /tmp/tiny.mz32-int64.mzML --mz32 --inten64 --overwrite
```

Commands that write files accept either a positional output path or `-o/--out`.
For example, `mzduck convert input.mzML output.mzduck` and
`mzduck convert input.mzML -o output.mzduck` are equivalent.

## Python Quick Start

```python
from mzduck import MzDuckFile

db = MzDuckFile.from_mzml(
    "mzduck/example_data/tiny.mzML",
    "/tmp/tiny.mzduck",
    overwrite=True,
    batch_size=1,
    compute_sha256=False,
)

try:
    print(db.inspect())

    spectrum = db.get_spectrum(1)
    print(spectrum["native_id"])
    print(spectrum["mz"])
    print(spectrum["intensity"])

    rows = db.query(
        """
        SELECT scan_number, rt, precursor_mz, precursor_charge
        FROM mgf
        WHERE precursor_mz BETWEEN ? AND ?
        ORDER BY rt
        """,
        [440.0, 450.0],
    ).fetchall()
    print(rows)

    db.to_mgf("/tmp/tiny.mgf", overwrite=True)
    db.to_mzml("/tmp/tiny.roundtrip.mzML", overwrite=True)
finally:
    db.close()
```

Open an existing file read-only:

```python
from mzduck import MzDuckFile, example_data_path

with MzDuckFile.open(example_data_path("tiny.mzduck")) as db:
    print(db.get_spectrum(1))
```

## SQL Examples

Find precursor spectra in a mass window:

```sql
SELECT scan_number, rt, precursor_mz, precursor_charge
FROM mgf
WHERE precursor_mz BETWEEN 440.0 AND 450.0
ORDER BY rt;
```

Get peaks for one spectrum in source order:

```sql
SELECT
  generate_subscripts(mz_array, 1) - 1 AS peak_index,
  UNNEST(mz_array) AS mz,
  UNNEST(intensity_array) AS intensity
FROM mgf
WHERE scan_number = 1
ORDER BY peak_index;
```

Extract a product-ion chromatogram:

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

## Example Scripts

Run the query example:

```bash
/data/p/anaconda3/bin/python examples/query_example.py
```

Run a full conversion/export workflow:

```bash
/data/p/anaconda3/bin/python examples/convert_example.py --outdir /tmp/mzduck-example
```

## Schema Summary

The v1 database always has `run_metadata` plus the selected data tables:

- `run_metadata`: schema version, provenance, mzML header XML fragments,
  source counts, included counts, dtype/compression settings, and a JSON
  `table_registry` describing every table in the DuckDB file.
- `mgf`: one row per exported MS2 MGF block. It stores only MGF content:
  `title`, `rt`, precursor fields, and peak arrays.
- `ms2_spectra`: non-MGF MS2 metadata used for richer mzML export, including
  native id, activation, collision energy, isolation window, scan windows, TIC,
  base peak, and filter string.
- `ms1_spectra`: MS1 metadata plus MS1 peak arrays when MS1 is included.
- `msN_spectra`: separate higher-level tables such as `ms3_spectra` when MSn
  spectra are present in default mode.
- `spectrum_summary`: one row per included spectrum in all modes except
  `--ms2-mgf-only`.

The schema creates no secondary indexes by default. Use
`mzduck convert --index-scan` when repeated exact scan-number lookups need an
ART index on `mgf(scan_number)`.

See `design/20260416design.codex.md` for the complete v1 specification.

## Development Tests

Run the test suite:

```bash
/data/p/anaconda3/bin/python -m pytest -q
```

The tests generate a tiny mzML fixture, convert it to mzDuck, export MGF/mzML,
and validate round-trip parsing.

## Notes

- mzML export uses `psims`.
- v1 mzML round-trip means semantic equivalence for supported fields, not
  byte-identical XML.
- The full-size validation fixture from the design is much larger and is not
  bundled with the package.
