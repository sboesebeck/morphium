# CLAUDE.md

Guidance for Claude Code in the Morphium repository. Build, test and architecture notes live in
`DEVELOPMENT_README.md` and `docs/`; the workspace-level `CLAUDE.md` one directory up applies as well.

## Task ledger

- Tasks live in `docs/tasks/ledger.yaml` (ids `MOR-NNN`). That file is the single source of truth.
- Use the `ledger` skill or the `/ledger` command to add, update or close tasks.
- `docs/tasks/ledger.md` is generated (`python3 scripts/ledger/ledger_md.py`). **Never edit it by hand.**
  `python3 scripts/ledger/ledger_md.py --check` fails when it is out of date.
- Research notes belong in `docs/research/`. Details: "Task ledger" in `DEVELOPMENT_README.md`.
