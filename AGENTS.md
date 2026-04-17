# Repository Guidelines

## Project Structure & Module Organization
`mzduck/` contains the package code. `file.py` is the main storage API, `import_mzml.py` and `export_*.py` handle format conversion, `schema.py` defines the DuckDB layout, and `cli.py` exposes the `mzduck` command. Bundled sample inputs and outputs live in `mzduck/example_data/`.

`tests/` holds the pytest suite, split by behavior: CLI, import/query, exports, bundled example data, and public exports. `docs/usage.md` and `README.md` are the user-facing references. `examples/` contains runnable scripts; `design/` contains working design notes and should not be treated as stable API documentation.

## Build, Test, and Development Commands
Use the repo’s configured Python:

```bash
/data/p/anaconda3/bin/python -m pip install -e .
/data/p/anaconda3/bin/python -m pytest
/data/p/anaconda3/bin/python -m pytest tests/test_cli.py -q
/data/p/anaconda3/bin/python -m mzduck --help
/data/p/anaconda3/bin/python examples/make_example_data.py
```

The first command installs the package in editable mode. Run the full test suite before submitting changes; use focused pytest targets for faster iteration. Regenerate bundled tiny fixtures only when example data or expected outputs change.

## Coding Style & Naming Conventions
Target Python 3.10+ and follow the existing style: 4-space indentation, `snake_case` for modules/functions/tests, `CamelCase` for classes, and concise docstrings. The codebase already uses `from __future__ import annotations`; keep that pattern in new Python modules. Prefer explicit keyword arguments for public APIs and keep feature patches narrowly scoped.

## Testing Guidelines
Write tests with `pytest` in files named `tests/test_*.py`. Add regression coverage alongside the affected area: CLI behavior in `tests/test_cli.py`, format round-trips in `tests/test_exports.py`, and import/query semantics in `tests/test_import_and_query.py`. Reuse fixtures from `tests/conftest.py` and write temporary outputs under `tmp_path`.

## Commit & Pull Request Guidelines
Recent history uses conventional prefixes such as `feat:` and `docs:`. Keep commit subjects short, imperative, and scoped, for example `feat: validate indexed scan lookups`. Pull requests should summarize the behavior change, list the test commands you ran, and call out any CLI, schema, or bundled example-data updates. Avoid committing ad hoc outputs from `/tmp`; only curated files under `mzduck/example_data/` belong in the repo.

## notes
each time after making a plan or make changes, save a notes in folder `mzDuck/design/develop-notes` create a file with date and time, and write down the plan and changes you made. also write down the key points of the plan and changes you made by creating a file in folder `mzDuck/design/key-changes`. if making changes, may need to read `mzDuck/design/key-changes`. file `mzDuck/design/design.md` is the main design document and need to be updated when making changes, this file is for understanding the important points of the tool.

