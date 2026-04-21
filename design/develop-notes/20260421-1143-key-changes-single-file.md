# Develop Notes

Date: 2026-04-21 11:43 Asia/Shanghai

## Plan

- consolidate the existing dated entries in `design/key-changes/` into one rolling markdown file
- update `AGENTS.md` so future key-change updates append to that single file
- record the new note workflow in `design/design.md`

## Changes

- created `design/key-changes/key-changes.md` and migrated the existing dated key-change summaries into dated sections
- removed the old per-change files from `design/key-changes/` so the folder now keeps one key-changes file
- updated `AGENTS.md` to direct future work to append key changes to the rolling file
- added a short workflow note to `design/design.md`
- left unrelated working tree changes in `README.md`, `docs/usage.md`, `mzduck/cli.py`, and `tests/test_cli.py` untouched
- no code or tests changed, so no test run was needed
