# mzDuck Usage Guide

This guide uses the bundled tiny example data:

```text
mzduck/example_data/tiny.mzML
mzduck/example_data/tiny.mzduck
mzduck/example_data/tiny.mgf
```

The tiny example is a v2 DuckDB-backed `.mzduck` file with two centroid MS2
spectra and five total peaks. It is intentionally small so examples run
quickly and the stored arrays can be inspected by eye.

## Install

From the repository root:

```bash
python -m pip install -e .
```

After installation, either of these should work:

```bash
mzduck --help
python -m mzduck --help
```

## Convert mzML to mzDuck

Default DuckDB output:

```bash
mzduck convert \
  mzduck/example_data/tiny.mzML \
  -o /tmp/tiny.mzduck \
  --overwrite \
  --no-sha256 \
  --compression zstd
```

Gzipped mzML input is supported too:

```bash
mzduck convert \
  input.mzML.gz \
  -o /tmp/tiny-from-gzip.mzduck \
  --overwrite
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

Expected key values for the bundled tiny example:

```json
{
  "schema_version": "2",
  "container_format": "duckdb",
  "spectrum_count": 2,
  "peak_count": 5
}
```

## Storage Model

Default v2 conversion stores:

- `run_metadata`
- `mgf`
- `ms2_spectra`
- `ms1_spectra` when the input includes MS1 and MS1 is not disabled
- higher `msN_spectra` tables when present
- non-empty `spectrum_text_overrides`
- non-empty `spectrum_extra_params`

Important v2 behavior:

- `mgf` is a physical MS2 payload table, not a derived view
- `ms2_spectra` is detail-only and keyed by `scan_number`
- `TITLE` is derived and is not stored as `mgf.title`
- empty optional tables are omitted from the final `.mzduck` file

## Table Modes

Mode examples:

```bash
# Only run_metadata plus the MGF-native MS2 payload.
mzduck convert input.mzML -o /tmp/ms2-mgf-only.mzduck --ms2-mgf-only --overwrite

# Keep MS2 payload and the richer MS2 detail table, skip other MS levels.
mzduck convert input.withMS1.mzML -o /tmp/ms2-only.mzduck --ms2-only --overwrite

# Keep only MS1 spectra and MS1 peak arrays.
mzduck convert input.withMS1.mzML -o /tmp/ms1-only.mzduck --ms1-only --overwrite

# Default mode, but skip MS1 spectra.
mzduck convert input.withMS1.mzML -o /tmp/no-ms1.mzduck --no-ms1 --overwrite

# Inclusive scan-number subset.
mzduck convert input.mzML -o /tmp/window.mzduck --start-scan 1000 --end-scan 2000 --overwrite

# Index exact scan-number lookups on the physical mgf table.
mzduck convert input.mzML -o /tmp/indexed.mzduck --index-scan --overwrite
```

## Alternate Containers

Parquet folder:

```bash
mzduck convert input.mzML.gz -o /tmp/out-parquet --parquet --overwrite
```

Parquet zip:

```bash
mzduck convert input.mzML.gz -o /tmp/out.parquet.zip --parquet-zip --overwrite
```

These modes write the same physical relations as the DuckDB output for the
selected mode, omit empty optional tables, and leave `MzDuckFile.open()`
reserved for DuckDB `.mzduck` files.

Single-file MGF parquet:

```bash
mzduck mzml-mgf input.mzML.gz -o /tmp/out.mgf.parquet --overwrite
```

This command is different from `mzduck convert --parquet`:

- it writes one parquet file, not a parquet container
- it keeps the MGF payload columns and adds derived `title`, `rt_unit`, and
  `rt_seconds`
- `title` stores only the file-name title source, not the full per-spectrum
  TITLE
- it is intended for standalone downstream MGF-style processing

## Reconstructed Fields

`MzDuckFile.get_spectrum()`, `export-mgf`, and `export-mzml` reconstruct some
text fields instead of storing them row-by-row when an exact run-level rule is
available:

- `TITLE` is derived from the run metadata template plus scan number and charge
- `native_id` can use `native_id_template`
- `spectrum_ref` can use `spectrum_ref_template`
- `filter_string` uses exact per-run encoder detection and falls back to raw
  stored strings when no exact rule exists

If any row does not fit the run-level rule exactly, mzDuck stores only the
exception rows in `spectrum_text_overrides`.

## Export mzDuck to MGF

```bash
mzduck export-mgf /tmp/tiny.mzduck -o /tmp/tiny.mgf --overwrite
mzduck export-mgf /tmp/out.mgf.parquet -o /tmp/from-parquet.mgf --overwrite
```

The MGF output contains one `BEGIN IONS` block per MS2 spectrum. `TITLE` is
always derived during export. For self-describing `*.mgf.parquet` input,
`export-mgf` reconstructs the full per-spectrum `TITLE` from the stored
`title` source plus `scan_number` and charge.

## Export mzDuck to mzML

```bash
mzduck export-mzml /tmp/tiny.mzduck -o /tmp/tiny.roundtrip.mzML --overwrite
```

The mzML output is a semantic round-trip written through `psims`. It is not
expected to be byte-identical to the original mzML XML.

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
    print(spectrum["title"])
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

Join MS2 payload with optional detail:

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

Inspect exact text fallbacks:

```sql
SELECT scan_number, field_name, value
FROM spectrum_text_overrides
ORDER BY scan_number, field_name;
```

## Metadata

`run_metadata` stores provenance, mzML header XML fragments, counts by MS
level, array dtype choices, compaction settings, templates, filter-string
policy, `source_compression`, `container_format`, and a JSON table registry.

## Regenerate Example Data

The bundled example files are generated from `examples/make_example_data.py`:

```bash
python examples/make_example_data.py
```

This rewrites the files under `mzduck/example_data/`.
