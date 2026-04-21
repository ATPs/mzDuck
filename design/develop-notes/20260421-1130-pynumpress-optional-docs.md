Date: 2026-04-21 11:30

Plan:
- document `pynumpress` as an optional dependency in package metadata
- explain in the README that Numpress-compressed mzML inputs require it at runtime
- record the same runtime requirement in the main design document

Changes made:
- added a `numpress` optional dependency extra in `pyproject.toml`
- updated the README installation section with `pip install -e .[numpress]`
- clarified that ordinary `.mzML` and `.mzML.gz` inputs do not need `pynumpress`, but MS-Numpress-compressed inputs do
- updated `design/design.md` with the runtime note for Numpress-compressed mzML files
