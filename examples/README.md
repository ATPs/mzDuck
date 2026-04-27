# mzDuck Examples

These examples assume you are running from the repository root after installing
the package in editable mode:

```bash
/data/p/anaconda3/bin/python -m pip install -e .
```

## Files

- `make_example_data.py`: regenerate the bundled tiny mzML, mzDuck, and MGF
  files under `mzduck/example_data/`.
- `convert_example.py`: convert the bundled mzML example to mzDuck and export
  MGF/mzML files in an output directory.
- `query_example.py`: open the bundled mzDuck file and run common SQL queries.

## Example Data

`examples/data/` now includes a ready-to-use sample set built from a larger
development mzML example after `msconvert` peak picking:

- `small.mzML`: centroid mzML with 48 spectra total: 14 MS1 and 34 MS2.
- `small.mzduck`: mzDuck output converted from `small.mzML`.
- `small.mgf`: plain MGF generated from `small.mzML` with `msconvert`.
- `small.mzduck.mgf`: plain MGF exported from `small.mzduck` with
  `mzduck export-mgf`.
- `small.mgf.parquet`: self-describing parquet generated from `small.mzML`
  with `mzduck mzml-mgf`.

## Quick Run

```bash
/data/p/anaconda3/bin/python examples/query_example.py
/data/p/anaconda3/bin/python examples/convert_example.py --outdir /tmp/mzduck-example
```

The bundled `tiny.mzduck` uses the default table layout for an MS2-only file:
`run_metadata`, `mgf`, `ms2_spectra`, and `spectrum_summary`.
