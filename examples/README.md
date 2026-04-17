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

## Quick Run

```bash
/data/p/anaconda3/bin/python examples/query_example.py
/data/p/anaconda3/bin/python examples/convert_example.py --outdir /tmp/mzduck-example
```

The bundled `tiny.mzduck` uses the default table layout for an MS2-only file:
`run_metadata`, `mgf`, `ms2_spectra`, and `spectrum_summary`.
