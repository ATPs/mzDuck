# mzDuck

mzDuck is a Python package for storing centroid MS2 mzML or mzMLb data in a single
DuckDB database file. It keeps spectra and peaks queryable with ordinary SQL
while preserving enough metadata for MGF export and semantic mzML/mzMLb export.

The current v1 scope is intentionally focused:

- input: centroid MS2 mzML or mzMLb;
- storage: one `.mzduck` DuckDB database per source run;
- output: `.mgf`, `.mzML`, and `.mzMLb`;
- API: Python class plus `mzduck` command-line tool.

## Repository Contents

```text
mzduck/                     Python package
mzduck/example_data/         tiny bundled mzML, mzMLb, mzDuck, and MGF examples
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
mzduck/example_data/tiny.mzMLb
mzduck/example_data/tiny.mzduck
mzduck/example_data/tiny.mgf
```

The tiny mzML and mzMLb files have two centroid MS2 spectra and five total
peaks. They are useful for smoke tests, docs, and quick manual inspection.

Regenerate the example data with:

```bash
/data/p/anaconda3/bin/python examples/make_example_data.py
```

## Command-Line Quick Start

Convert mzML to mzDuck:

```bash
mzduck convert \
  mzduck/example_data/tiny.mzML \
  /tmp/tiny.mzduck \
  --overwrite \
  --no-sha256
```

Convert mzMLb to mzDuck in the same way:

```bash
mzduck convert input.mzMLb output.mzduck --overwrite --no-sha256
```

Inspect the mzDuck file:

```bash
mzduck inspect /tmp/tiny.mzduck --json
```

Export MGF:

```bash
mzduck export-mgf /tmp/tiny.mzduck /tmp/tiny.mgf --overwrite
```

Export mzML:

```bash
mzduck export-mzml /tmp/tiny.mzduck /tmp/tiny.roundtrip.mzML --overwrite
```

Export mzMLb:

```bash
mzduck export-mzmlb /tmp/tiny.mzduck /tmp/tiny.roundtrip.mzMLb --overwrite
```

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

    spectrum = db.get_spectrum(0)
    print(spectrum["native_id"])
    print(spectrum["mz"])
    print(spectrum["intensity"])

    rows = db.query(
        """
        SELECT native_id, rt, precursor_mz, precursor_charge
        FROM spectra
        WHERE precursor_mz BETWEEN ? AND ?
        ORDER BY rt
        """,
        [440.0, 450.0],
    ).fetchall()
    print(rows)

    db.to_mgf("/tmp/tiny.mgf", overwrite=True)
    db.to_mzml("/tmp/tiny.roundtrip.mzML", overwrite=True)
    db.to_mzmlb("/tmp/tiny.roundtrip.mzMLb", overwrite=True)
finally:
    db.close()
```

Open an existing file read-only:

```python
from mzduck import MzDuckFile, example_data_path

with MzDuckFile.open(example_data_path("tiny.mzduck")) as db:
    print(db.get_spectrum(0))
```

## SQL Examples

Find precursor spectra in a mass window:

```sql
SELECT native_id, rt, precursor_mz, precursor_charge
FROM spectra
WHERE precursor_mz BETWEEN 440.0 AND 450.0
ORDER BY rt;
```

Get peaks for one spectrum in source order:

```sql
SELECT mz, intensity
FROM peaks
WHERE scan_id = 0
ORDER BY peak_index;
```

Extract a product-ion chromatogram:

```sql
SELECT s.rt, SUM(p.intensity) AS xic
FROM spectra s
JOIN peaks p ON p.scan_id = s.scan_id
WHERE p.mz BETWEEN 149.0 AND 151.0
GROUP BY s.rt
ORDER BY s.rt;
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

The v1 database has three required tables:

- `spectra`: one row per imported MS2 spectrum with retention time, precursor
  metadata, scan metadata, activation type, scan windows, TIC, base peak, and
  source identifiers.
- `peaks`: one row per peak with `scan_id`, `peak_index`, `mz`, and
  `intensity`. `peak_index` preserves source array order.
- `run_metadata`: schema version, provenance, source metadata, counts, dtypes,
  and mzML header snapshots.

The schema also creates indexes for spectrum lookup, precursor m/z search,
fragment m/z search, retention-time windows, native ids, and scan numbers.

See `design/20260416design.codex.md` for the complete v1 specification.

## Development Tests

Run the test suite:

```bash
/data/p/anaconda3/bin/python -m pytest -q
```

The tests generate a tiny mzML fixture, convert it to mzDuck, export
MGF/mzML/mzMLb, and validate round-trip parsing.

## Notes

- mzML and mzMLb export use `psims`. If `hdf5plugin` is not installed, psims
  writes mzMLb with gzip compression instead of Blosc.
- v1 mzML round-trip means semantic equivalence for supported MS2 fields, not
  byte-identical XML.
- The full-size validation fixture from the design is much larger and is not
  bundled with the package.
