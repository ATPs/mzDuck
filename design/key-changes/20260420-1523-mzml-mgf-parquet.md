# Key Changes

Date: 2026-04-20 15:23 Asia/Shanghai

- added `mzduck mzml-mgf` for direct `.mzML` and `.mzML.gz` to single-file MGF parquet export
- the new output is self-describing and includes derived `title`, `rt_unit`, and `rt_seconds`
- `convert --parquet` is unchanged and still exports physical relations as-is
- `mzml-mgf` is CLI-only and does not change the public Python API
- scan-window filtering is supported on the new command
- selecting no MS2 spectra now returns a clear `mzml-mgf`-specific error
- README, usage docs, and `design/design.md` now document the difference between single-file MGF parquet and parquet containers
