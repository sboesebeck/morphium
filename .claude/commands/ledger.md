---
description: Show, add or update tasks in the task ledger (docs/tasks/ledger.yaml)
argument-hint: <what to do, e.g. "show open", "add: ...", "close MOR-001 with evidence ...">
---
Use the skill `ledger` for: $ARGUMENTS

- Without arguments: regenerate `docs/tasks/ledger.md`, then list the open tasks with priority 1 (id, title, status, effort) and ask which one to work on.
- For a change: edit `docs/tasks/ledger.yaml`, run `python3 scripts/ledger/ledger_md.py` and the tests in `scripts/ledger/`, and show the diff of both files. Do not commit without an explicit request.
