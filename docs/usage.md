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
  --no-sha256
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

# Per-array control. This is also the default.
mzduck export-mzml /tmp/tiny.mzduck -o /tmp/tiny.mz64-int32.mzML --mz64 --inten32 --overwrite

# Mixed precision in the other direction.
mzduck export-mzml /tmp/tiny.mzduck -o /tmp/tiny.mz32-int64.mzML --mz32 --inten64 --overwrite
```

## Query With Python

```python
from mzduck import MzDuckFile, example_data_path

with MzDuckFile.open(example_data_path("tiny.mzduck")) as db:
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
        """,
        [440.0, 450.0],
    ).fetchall()
    print(rows)
```

## Useful SQL Queries

Find precursor spectra in a mass window:

```sql
SELECT native_id, rt, precursor_mz, precursor_charge
FROM spectra
WHERE precursor_mz BETWEEN 440.0 AND 450.0
ORDER BY rt;
```

Get the peaks for one spectrum in source order:

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

## Regenerate Example Data

The example files are generated from `examples/make_example_data.py`:

```bash
/data/p/anaconda3/bin/python examples/make_example_data.py
```

This rewrites the files under `mzduck/example_data/`.
