# mzDuck Usage Guide

This guide uses the bundled tiny example data:

```text
mzduck/example_data/tiny.mzML
mzduck/example_data/tiny.mzMLb
mzduck/example_data/tiny.mzduck
mzduck/example_data/tiny.mgf
```

The tiny mzML and mzMLb files contain two centroid MS2 spectra. They are
intentionally small so examples run quickly and so users can inspect the text
output by eye.

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
  /tmp/tiny.mzduck \
  --overwrite \
  --no-sha256
```

The same command accepts mzMLb input:

```bash
mzduck convert input.mzMLb /tmp/from_mzmlb.mzduck --overwrite --no-sha256
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
mzduck export-mgf /tmp/tiny.mzduck /tmp/tiny.mgf --overwrite
```

The MGF output contains one `BEGIN IONS` block per MS2 spectrum.

## Export mzDuck to mzML

```bash
mzduck export-mzml /tmp/tiny.mzduck /tmp/tiny.roundtrip.mzML --overwrite
```

The mzML output is semantic round-trip output written through `psims`. It is not
expected to be byte-identical to the original mzML.

## Export mzDuck to mzMLb

```bash
mzduck export-mzmlb /tmp/tiny.mzduck /tmp/tiny.roundtrip.mzMLb --overwrite
```

The mzMLb output uses `psims.mzmlb.MzMLbWriter`. If `hdf5plugin` is unavailable,
psims falls back to gzip compression.

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
