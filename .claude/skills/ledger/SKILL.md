---
name: ledger
description: Use when creating, updating, splitting or closing tasks in docs/tasks/ledger.yaml, or when asked what is open. Keeps the yaml and the generated markdown in sync.
---
# Ledger

Source of truth: `docs/tasks/ledger.yaml`. The readable view `docs/tasks/ledger.md` is generated:
`python3 scripts/ledger/ledger_md.py`. Never edit the markdown by hand.

## New task
1. `git pull --no-rebase` and `git status`.
2. Next free id: `python3 scripts/ledger/ledger_md.py --next-id` (format `MOR-NNN`, never reused).
3. Fields: `id`, `title`, `type`, `priority` (1 first, 3 later), `status`, `effort` (S/M/L), `description`. For
   implementation add `acceptance` (observable, testable), optionally `depends_on`, `subtasks`, `source`, `ticket`.
4. Write titles and texts in English. Reference files with their path.
5. `python3 scripts/ledger/ledger_md.py` and `python3 -m unittest scripts/ledger/test_ledger_md.py`.
   The script validates the ledger (duplicate ids, unknown references, missing evidence) and stops with a list.
6. Show the diff of both files. Commit only when the user asks for it.

## Work on a task
- Set `status: in_progress` when work starts. Set `blocked` only together with `blocked_by`.
- A large task is an epic with `subtasks`; the epic stays `in_progress` until all its subtasks are done.

## Close a task
- `status: done`, `evidence:` with a measured value (commit, test count, timing) and `commits:` with the hash.
- The script refuses `done` without evidence.

## Rules
- Statuses: `open`, `in_progress`, `blocked`, `done`. Types: analysis, implementation, defect, cleanup,
  clarification, verification, infrastructure, documentation.
- `docs/tasks/ledger.md` is never edited by hand. `python3 scripts/ledger/ledger_md.py --check` exits 1 when it
  is out of date.
