# mzDuck

mzDuck is a DuckDB-backed storage format, command-line tool, and Python package for centroid mzML data.
It converts `.mzML` and `.mzML.gz` files into a compact relational container that is easy to query with SQL, easy to export to MGF, and able to round-trip back to semantic mzML.

The project is intentionally practical: a single-file primary container, a Python-first install story, direct DuckDB access, sparse exact metadata fallbacks, and export paths that match common downstream workflows.

## Why mzDuck?

- Single-file primary container: the default output is one DuckDB-backed `.mzduck` file.
- Python package and CLI: install with `pip`, run `mzduck ...`, or `import mzduck` directly.
- SQL-native workflow: open the file in DuckDB and query real tables such as `mgf` and `ms2_spectra`.
- Focused scope: designed for centroid mzML conversion, inspection, export, and analysis.
- Practical round-trip story: export MGF or semantic mzML from the same stored file.
- Compact storage: v2 uses sparse exact text fallbacks, structure-aware metadata reconstruction, and a compact-copy final write step to keep files small.

## mzDuck vs mzPeak

`mzPeak` is a broader file-format prototyping project. It explores multiple Parquet layouts, profile-oriented encodings, chromatograms, and a Rust-first implementation. `mzDuck` takes a narrower path.

| If you care most about... | Why `mzDuck` is often the better fit |
| --- | --- |
| A simple end-user workflow | `mzDuck` is built around one direct task: convert centroid mzML into a compact, queryable file and export it again when needed. |
| Python-first usage | `mzDuck` installs and imports as a normal Python package, with a small public API and a matching CLI. |
| A single-file queryable container | The default output is one DuckDB file instead of an archive containing multiple member files. |
| Straightforward SQL analysis | The MS2 payload lives in a physical `mgf` table, so common precursor/fragment workflows map cleanly to SQL. |
| MGF-oriented downstream work | `mzDuck` has an explicit MGF-native contract, MGF export, and a single-file self-describing `*.mgf.parquet` mode. |
| Centroid mzML fidelity with minimal duplication | Run-level templates plus sparse exact overrides avoid storing repetitive text fields row-by-row. |
| Fewer user-facing encoding decisions | `mzDuck` hides most storage-layout complexity behind a small set of modes and defaults. |

Use `mzDuck` when you want a practical centroid mzML container for Python, SQL, and export workflows.
Use `mzPeak` when you need the broader format R&D surface, profile/chromatogram work, or the Rust-centered ecosystem.

## Current Scope

`mzDuck` currently targets:

- centroid `.mzML` and `.mzML.gz` input
- one input run per command invocation
- DuckDB-backed `.mzduck` as the primary container
- optional Parquet directory and Parquet ZIP exports of physical relations
- a single-file self-describing MGF Parquet export for MS2 workflows
- MGF export and semantic mzML export

Important scope notes:

- exported mzML is intended to be semantically faithful, not byte-identical XML
- `MzDuckFile.open()` supports DuckDB `.mzduck` files, not Parquet containers
- `--ms2-mgf-only` preserves the MGF-native contract only, not the full mzML detail layer

## Installation

### Requirements

- Python 3.10 or newer
- standard runtime dependencies are installed automatically from `pyproject.toml`
- `pynumpress` is only needed when importing mzML files whose binary arrays use MS-Numpress compression

### Install from source

```bash
python -m pip install .
```

For development:

```bash
python -m pip install -e .
```

Optional Numpress support:

```bash
python -m pip install ".[numpress]"
```

After installation, both of these should work:

```bash
mzduck --help
python -m mzduck --help
```

## Python Package Usage

`mzDuck` can be used either as a CLI or as an importable Python package.
The public top-level API is:

```python
from mzduck import MzDuckFile, example_data_path, from_mzml, open, to_mgf, to_mzml
```

### Convert and inspect from Python

```python
from mzduck import from_mzml

handle = from_mzml(
    "input.mzML.gz",
    "output.mzduck",
    overwrite=True,
    compute_sha256=False,
)

try:
    print(handle.inspect())
    spectrum = handle.get_spectrum(1)
    print(spectrum["title"])
    print(spectrum["native_id"])
    print(spectrum["mz"])
    print(spectrum["intensity"])
finally:
    handle.close()
```

### Open an existing file and run SQL

```python
from mzduck import MzDuckFile

with MzDuckFile.open("output.mzduck") as db:
    print(db.metadata()["schema_version"])

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
```

### Export from Python

```python
from mzduck import to_mgf, to_mzml

to_mgf("output.mzduck", "output.mgf")
to_mzml("output.mzduck", "roundtrip.mzML")
```

## Quick Start

Convert one file to the default `.mzduck` container:

```bash
mzduck convert input.mzML.gz -o output.mzduck --overwrite
```

Inspect the result:

```bash
mzduck inspect output.mzduck --json
```

Export MGF:

```bash
mzduck export-mgf output.mzduck -o output.mgf --overwrite
```

Export semantic mzML:

```bash
mzduck export-mzml output.mzduck -o roundtrip.mzML --overwrite
```

Create a single-file MGF Parquet artifact:

```bash
mzduck mzml-mgf input.mzML.gz -o output.mgf.parquet --overwrite
```

## Command Overview

`mzduck` provides five subcommands:

- `convert`: convert one centroid mzML file into `.mzduck`, Parquet directory, or Parquet ZIP
- `mzml-mgf`: convert one centroid mzML file into one self-describing `*.mgf.parquet`
- `export-mgf`: export `.mzduck` or self-describing `*.mgf.parquet` to `.mgf`
- `export-mzml`: export `.mzduck` back to semantic `.mzML`
- `inspect`: print a summary of a `.mzduck` file

## Command Reference

### `mzduck convert`

Convert one centroid `.mzML` or `.mzML.gz` file into a compact `.mzduck` file or a Parquet-based container.

```bash
mzduck convert input.mzML.gz -o output.mzduck --overwrite
```

Common examples:

```bash
mzduck convert input.mzML -o output.mzduck --overwrite
mzduck convert input.mzML.gz -o output.mzduck --overwrite
mzduck convert input.mzML.gz -o output.parquet.dir --parquet --overwrite
mzduck convert input.mzML.gz -o output.parquet.zip --parquet-zip --overwrite
mzduck convert input.mzML.gz -o output.ms2.mzduck --ms2-only --overwrite
mzduck convert input.mzML.gz -o output.mgf-only.mzduck --ms2-mgf-only --overwrite
mzduck convert input.withMS1.mzML -o output.ms1.mzduck --ms1-only --overwrite
mzduck convert input.withMS1.mzML -o output.window.mzduck --start-scan 1000 --end-scan 2000 --overwrite
```

#### Parameters

| Parameter | Meaning |
| --- | --- |
| `input.mzML[.gz]` | Input centroid mzML file. `.mzML` and `.mzML.gz` are supported. |
| `output` | Positional output path. Use this or `-o/--out`. |
| `-o, --out` | Named output path. Use this or the positional output path. |
| `--overwrite` | Replace an existing output file or folder. |
| `--batch-size` | Number of spectra inserted per batch. Larger values may improve throughput; smaller values may reduce peak memory. Default: `5000`. |
| `--no-sha256` | Skip SHA-256 hashing of the source file in provenance metadata. |
| `--compression {zstd,auto,uncompressed}` | Compression policy for DuckDB output and Parquet members. Default: `zstd`. |
| `--compression-level` | Requested Zstandard compression level when `zstd` is used. Default: `6`. |
| `--index-scan` | Create `idx_mgf_scan_number` on `mgf(scan_number)` for faster exact scan lookup. |
| `--index-scan-number` | Deprecated alias for `--index-scan`. |
| `--parquet` | Write one Parquet file per physical relation into an output directory instead of a `.mzduck` file. |
| `--parquet-zip` | Write the same physical Parquet members into one ZIP archive with no ZIP-level compression. |
| `--ms2-mgf-only` | Keep only `run_metadata` and the physical `mgf` table. This is the smallest MS2-focused mode. |
| `--no-ms1` | Skip MS1 spectra in the default mode. |
| `--ms2-only` | Keep `run_metadata`, `mgf`, `ms2_spectra`, and any non-empty fallback tables. |
| `--ms1-only` | Keep only `run_metadata` and `ms1_spectra`. |
| `--start-scan` | Inclusive lower `scan_number` bound for subsetting. |
| `--end-scan` | Inclusive upper `scan_number` bound for subsetting. |

#### Notes

- Use either the positional output path or `-o/--out`, not both.
- `--parquet` and `--parquet-zip` are mutually exclusive.
- `--ms2-mgf-only`, `--ms2-only`, and `--ms1-only` are mutually exclusive.
- `--no-ms1` cannot be combined with `--ms1-only`.
- `--start-scan` cannot be greater than `--end-scan`.
- In default mode, `mzDuck` keeps `run_metadata`, `mgf`, `ms2_spectra`, MS1 when present, higher MSn tables when present, and non-empty fallback tables.

### `mzduck mzml-mgf`

Convert one centroid `.mzML` or `.mzML.gz` file into one self-describing `*.mgf.parquet` file for MGF-style downstream processing.

```bash
mzduck mzml-mgf input.mzML.gz -o output.mgf.parquet --overwrite
```

#### Parameters

| Parameter | Meaning |
| --- | --- |
| `input.mzML[.gz]` | Input centroid mzML file. |
| `output.mgf.parquet` | Positional output path. Use this or `-o/--out`. |
| `-o, --out` | Named output path. Use this or the positional output path. |
| `--overwrite` | Replace an existing output file. |
| `--batch-size` | Number of spectra inserted per batch. Default: `5000`. |
| `--compression {zstd,auto,uncompressed}` | Compression policy for the Parquet file. Default: `zstd`. |
| `--compression-level` | Requested Zstandard compression level when `zstd` is used. Default: `6`. |
| `--start-scan` | Inclusive lower `scan_number` bound. |
| `--end-scan` | Inclusive upper `scan_number` bound. |

#### Notes

- This command writes one Parquet file, not a Parquet container.
- The output includes the MGF payload plus `title`, `rt_unit`, and `rt_seconds`.
- The stored `title` column contains only the file-name title source; `export-mgf` reconstructs the full per-spectrum `TITLE` from `title + scan_number + charge`.

### `mzduck export-mgf`

Export spectra from `.mzduck` or self-describing `*.mgf.parquet` to Mascot Generic Format.

```bash
mzduck export-mgf input.mzduck -o output.mgf --overwrite
mzduck export-mgf input.mgf.parquet -o output.mgf --overwrite
```

#### Parameters

| Parameter | Meaning |
| --- | --- |
| `input` | Input `.mzduck` file or self-describing `*.mgf.parquet` file. |
| `output.mgf` | Positional output path. Use this or `-o/--out`. |
| `-o, --out` | Named output path. Use this or the positional output path. |
| `--overwrite` | Replace an existing output file. |

#### Notes

- `.mzduck` export uses the physical `mgf` table and reconstructs `TITLE` from run metadata.
- Parquet input is supported only for the self-describing `*.mgf.parquet` files produced by `mzduck mzml-mgf`.

### `mzduck export-mzml`

Export a `.mzduck` file back to semantic mzML using `psims`.

```bash
mzduck export-mzml input.mzduck -o output.mzML --overwrite
```

Precision examples:

```bash
mzduck export-mzml input.mzduck -o output.float32.mzML --32 --overwrite
mzduck export-mzml input.mzduck -o output.float64.mzML --64 --overwrite
mzduck export-mzml input.mzduck -o output.mz64-int32.mzML --mz64 --inten32 --overwrite
mzduck export-mzml input.mzduck -o output.mz32-int64.mzML --mz32 --inten64 --overwrite
```

#### Parameters

| Parameter | Meaning |
| --- | --- |
| `input.mzduck` | Input `.mzduck` file. |
| `output.mzML` | Positional output path. Use this or `-o/--out`. |
| `-o, --out` | Named output path. Use this or the positional output path. |
| `--overwrite` | Replace an existing output file. |
| `--32` | Write both m/z and intensity arrays as 32-bit floats. |
| `--64` | Write both m/z and intensity arrays as 64-bit floats. |
| `--mz32` | Write only m/z arrays as 32-bit floats. |
| `--mz64` | Write only m/z arrays as 64-bit floats. |
| `--inten32` | Write only intensity arrays as 32-bit floats. |
| `--inten64` | Write only intensity arrays as 64-bit floats. |

#### Notes

- `--32` and `--64` cannot be used together.
- `--mz32` and `--mz64` cannot be used together.
- `--inten32` and `--inten64` cannot be used together.
- By default, mzML export keeps the stored or source precision when possible.
- The export is semantic rather than byte-identical XML.

### `mzduck inspect`

Print a summary of counts, ranges, storage metadata, reconstruction policy, and table registry details for a `.mzduck` file.

```bash
mzduck inspect input.mzduck
mzduck inspect input.mzduck --json
```

#### Parameters

| Parameter | Meaning |
| --- | --- |
| `input.mzduck` | Input `.mzduck` file to inspect. |
| `--json` | Print machine-readable JSON instead of plain text. |

## Storage Model

The v2 layout is centered on a small number of physical relations:

- `run_metadata`: provenance, templates, encoder policy, counts, table registry, `container_format`, and `source_compression`
- `mgf`: the physical MS2 payload table with scan number, source order, RT, precursor payload, and peak arrays
- `ms2_spectra`: a detail-only MS2 table keyed by `scan_number`
- `ms1_spectra`: physical MS1 spectra when MS1 is included
- `ms3_spectra`, `ms4_spectra`, ...: higher-order MSn tables when present
- `spectrum_text_overrides`: sparse exact fallbacks for `native_id`, `spectrum_ref`, and `filter_string`
- `spectrum_extra_params`: unmapped mzML parameters that still need exact preservation

Important v2 behavior:

- `mgf` is a real stored table, not a compatibility view
- `ms2_spectra` no longer duplicates RT, precursor payload, or peak arrays
- `TITLE` is derived, not stored as `mgf.title`
- empty optional tables are dropped before the final `.mzduck` file is written

## Reconstruction Model

`mzDuck` tries to avoid storing repetitive text fields row-by-row when one exact run-level rule is enough.

### `TITLE`

`TITLE` is always derived as:

```text
{mgf_title_source}.{scan_number}.{scan_number}.{precursor_charge}
```

### `native_id` and `spectrum_ref`

If all selected spectra match one exact run-level template, the template is stored once in `run_metadata`.
If not, `mzDuck` stores only the exact exception rows in `spectrum_text_overrides`.

### `filter_string`

Filter-string reconstruction is conservative.
An encoder is accepted only when it reproduces every selected value exactly for that run.
If no exact rule exists, `mzDuck` stores the original strings instead of inventing an approximate rule.

## SQL Examples

### Inspect the MGF-native MS2 payload

```sql
SELECT scan_number, source_index, rt, precursor_mz, precursor_charge
FROM mgf
ORDER BY source_index;
```

### Join MS2 payload with the detail layer

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

### Build a simple extracted ion chromatogram

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

### Find stored exact text fallbacks

```sql
SELECT scan_number, field_name, value
FROM spectrum_text_overrides
ORDER BY scan_number, field_name;
```

## Size Notes

Internal validation during v2 development used representative public PRIDE data, including accession `PXD010154`.
On one benchmark file, a `408.44 MiB` mzML converted to a `90.26 MiB` v2 `.mzduck`, compared with `83.26 MiB` for the matching `mzMLb` file.
Across a fixed 10-file `.mzML.gz` validation set, default DuckDB output averaged `97.2%` of source gzip size, with an observed range of `89.9%` to `102.3%`.

## Examples and Additional Docs

- [docs/usage.md](docs/usage.md): more usage examples
- [examples/README.md](examples/README.md): example scripts
- [mzduck/example_data/README.md](mzduck/example_data/README.md): bundled tiny example data
- [design/design.md](design/design.md): design notes

## Limitations

- current import support is intentionally focused on centroid mzML
- the public API is centered on `.mzduck` as the primary container
- Parquet container outputs expose physical relations only; they do not recreate DuckDB-only convenience behavior
- `mzduck export-mzml` aims for semantic fidelity, not source XML identity

## License

`mzDuck` is distributed under the terms of the [LICENSE](LICENSE) file.
