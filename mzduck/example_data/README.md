# mzDuck Example Data

This directory contains a tiny centroid-MS2 example set:

- `tiny.mzML`: source mzML with two MS2 spectra and five total peaks.
- `tiny.mzduck`: v2 mzDuck conversion of `tiny.mzML`.
- `tiny.mgf`: MGF export from `tiny.mzduck`.

Regenerate these files from the repository root with:

```bash
python examples/make_example_data.py
```

The files are intentionally tiny and are meant for examples, smoke tests, and
quick inspection. They are not a benchmark dataset.
