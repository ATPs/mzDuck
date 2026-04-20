# Key Changes

Date: 2026-04-20 16:05 Asia/Shanghai

- `mzduck export-mgf` now accepts self-describing `*.mgf.parquet` input from `mzduck mzml-mgf`
- parquet input support remains limited to the single-file self-describing schema, not generic parquet containers
- `mzduck mzml-mgf` now stores only the file-name title source in the parquet `title` column
- `export-mgf` reconstructs the full per-spectrum TITLE from title source, `scan_number`, and charge
- older parquet files that already stored full TITLE are still exported correctly
- docs and design notes now describe the source-only `title` semantics and the new parquet-to-MGF export path
