Date: 2026-04-21 11:37

Plan:
- add an `export-mgf` example to `mzduck mzml-mgf --help`
- keep the same workflow visible in README and usage docs
- add CLI help coverage for the new example

Changes made:
- updated `mzduck/cli.py` so `mzduck mzml-mgf --help` now includes an example for converting the generated `.mgf.parquet` back to plain `.mgf`
- extended `tests/test_cli.py` to assert that example is present
- updated `README.md`, `docs/usage.md`, and `design/design.md` so the paired workflow stays documented consistently
