# mzDuck Usage Guide

This guide uses the bundled tiny example data:

```text
mzduck/example_data/tiny.mzML
mzduck/example_data/tiny.mzduck
mzduck/example_data/tiny.mgf
```

The tiny mzML file contains two centroid MS2 spectra. It is intentionally small
so examples run quickly and so users can inspect the text output by eye.

## Install

From the repository root:

```bash
/data/p/anaconda3/bin/python -m pip install -e .
```

After installation, the `mzduck` command should be available:

```bash
mzduck --help
```

## Convert mzML to mzDuck

```bash
mzduck convert \
  mzduck/example_data/tiny.mzML \
  -o /tmp/tiny.mzduck \
  --overwrite \
  --no-sha256 \
  --compression zstd
```

Commands that write files accept either a positional output path or `-o/--out`:

```bash
mzduck convert mzduck/example_data/tiny.mzML /tmp/tiny.mzduck --overwrite
mzduck convert mzduck/example_data/tiny.mzML --out /tmp/tiny.mzduck --overwrite
```

Inspect the result:

```bash
mzduck inspect /tmp/tiny.mzduck --json
```

Expected key values:

```json
{
  "spectrum_count": 2,
  "peak_count": 5,
  "schema_version": "1"
}
```

## Table Modes

Default conversion stores:

- `run_metadata`
- `mgf`
- `ms2_spectra`
- `ms1_spectra` when the input has MS1 and MS1 is not disabled
- higher MSn tables such as `ms3_spectra` when present
- `spectrum_summary`

Mode examples:

```bash
# Only run_metadata and the MS2 MGF contract table.
mzduck convert input.mzML -o /tmp/ms2-mgf-only.mzduck --ms2-mgf-only --overwrite

# Keep MS2 MGF rows and MS2 metadata, skip all other MS levels.
mzduck convert input.withMS1.mzML -o /tmp/ms2-only.mzduck --ms2-only --overwrite

# Keep only MS1 spectra and MS1 peak arrays.
mzduck convert input.withMS1.mzML -o /tmp/ms1-only.mzduck --ms1-only --overwrite

# Default mode, but skip MS1 spectra.
mzduck convert input.withMS1.mzML -o /tmp/no-ms1.mzduck --no-ms1 --overwrite

# Inclusive scan-number subset.
mzduck convert input.mzML -o /tmp/window.mzduck --start-scan 1000 --end-scan 2000 --overwrite

# Index exact scan-number lookups on the MGF table only.
mzduck convert input.mzML -o /tmp/indexed.mzduck --index-scan --overwrite
```

## Export mzDuck to MGF

```bash
mzduck export-mgf /tmp/tiny.mzduck -o /tmp/tiny.mgf --overwrite
```

The MGF output contains one `BEGIN IONS` block per MS2 spectrum.

## Export mzDuck to mzML

```bash
mzduck export-mzml /tmp/tiny.mzduck -o /tmp/tiny.roundtrip.mzML --overwrite
```

The mzML output is semantic round-trip output written through `psims`. It is not
expected to be byte-identical to the original mzML.

Precision flags control the mzML binary array value types:

```bash
# Both m/z and intensity arrays as 32-bit floats.
mzduck export-mzml /tmp/tiny.mzduck -o /tmp/tiny.32.mzML --32 --overwrite

# Both arrays as 64-bit floats.
mzduck export-mzml /tmp/tiny.mzduck -o /tmp/tiny.64.mzML --64 --overwrite

# Per-array control.
mzduck export-mzml /tmp/tiny.mzduck -o /tmp/tiny.mz64-int32.mzML --mz64 --inten32 --overwrite

# Mixed precision in the other direction.
mzduck export-mzml /tmp/tiny.mzduck -o /tmp/tiny.mz32-int64.mzML --mz32 --inten64 --overwrite
```

## Query With Python

```python
from mzduck import MzDuckFile, example_data_path

with MzDuckFile.open(example_data_path("tiny.mzduck")) as db:
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
        """,
        [440.0, 450.0],
    ).fetchall()
    print(rows)
```

## Useful SQL Queries

Find precursor spectra in a mass window:

```sql
SELECT scan_number, rt, precursor_mz, precursor_charge
FROM mgf
WHERE precursor_mz BETWEEN 440.0 AND 450.0
ORDER BY rt;
```

Get the peaks for one spectrum in source order:

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

## Metadata

`run_metadata` stores provenance, mzML header XML fragments, counts by MS level,
array dtype choices, compression settings, and a JSON `table_registry`. The
registry is useful for deciding whether a file can export to MGF, whether MS1
peaks are present, and which table has a scan-number index.

## Regenerate Example Data

The example files are generated from `examples/make_example_data.py`:

```bash
/data/p/anaconda3/bin/python examples/make_example_data.py
```

This rewrites the files under `mzduck/example_data/`.
