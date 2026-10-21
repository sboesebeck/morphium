#!/usr/bin/env python3
"""Generates docs/tasks/ledger.md from docs/tasks/ledger.yaml.

Usage (from the repository root):
    python3 scripts/ledger/ledger_md.py           write docs/tasks/ledger.md
    python3 scripts/ledger/ledger_md.py --check   write nothing, exit 1 if ledger.md is out of date
    python3 scripts/ledger/ledger_md.py --next-id print the next free task id

The YAML file is the source of truth. The Markdown file is generated and must not be edited by hand.
The script validates the ledger first and exits with 2 and a list of problems if it is inconsistent.
Encoding and line endings are fixed (UTF-8, LF). Needs PyYAML (python3 -m pip install pyyaml).
"""
import pathlib
import re
import sys

STATUSES = ("open", "in_progress", "blocked", "done")
TYPES = ("analysis", "implementation", "defect", "cleanup", "clarification", "verification",
         "infrastructure", "documentation")
ID_PATTERN = re.compile(r"^MOR-\d{3,}$")
REQUIRED = ("id", "title", "type", "status", "priority")

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
SOURCE = ROOT / "docs" / "tasks" / "ledger.yaml"
TARGET = ROOT / "docs" / "tasks" / "ledger.md"


def load_yaml(text):
    try:
        import yaml
    except ImportError:
        sys.exit("ledger_md: PyYAML is missing. Install it with: python3 -m pip install pyyaml")
    return yaml.safe_load(text)


def as_text(value):
    return ", ".join(str(v) for v in value) if isinstance(value, list) else str(value)


def validate(tasks):
    """Returns a list of problems, empty when the ledger is consistent."""
    problems = []
    ids = [t.get("id") for t in tasks]
    for task_id in {i for i in ids if ids.count(i) > 1}:
        problems.append(f"duplicate id {task_id}")
    known = set(ids)
    for t in tasks:
        tid = t.get("id", "<no id>")
        for field in REQUIRED:
            if t.get(field) in (None, ""):
                problems.append(f"{tid}: field '{field}' is missing")
        if t.get("id") and not ID_PATTERN.match(str(t["id"])):
            problems.append(f"{tid}: id does not match MOR-NNN")
        if t.get("status") not in STATUSES:
            problems.append(f"{tid}: status '{t.get('status')}' is not one of {', '.join(STATUSES)}")
        if t.get("type") not in TYPES:
            problems.append(f"{tid}: type '{t.get('type')}' is not one of {', '.join(TYPES)}")
        if t.get("priority") not in (1, 2, 3):
            problems.append(f"{tid}: priority must be 1, 2 or 3")
        for link in ("depends_on", "subtasks"):
            for ref in t.get(link, []) or []:
                if ref not in known:
                    problems.append(f"{tid}: {link} refers to unknown id {ref}")
        if t.get("status") == "done" and not t.get("evidence"):
            problems.append(f"{tid}: status done needs 'evidence'")
        if t.get("status") == "blocked" and not t.get("blocked_by"):
            problems.append(f"{tid}: status blocked needs 'blocked_by'")
    return problems


def render(data):
    tasks = data["tasks"]
    counts = {}
    for t in tasks:
        counts[t["status"]] = counts.get(t["status"], 0) + 1
    out = [f"# {data.get('title', 'Task ledger')}", "",
           f"As of {data['as_of']}. Generated from `docs/tasks/ledger.yaml` with "
           "`python3 scripts/ledger/ledger_md.py`, do not edit by hand.", ""]
    if data.get("sources"):
        out += ["Sources: " + ", ".join(f"[{k}]({v})" for k, v in data["sources"].items()), ""]
    out += ["Status: " + ", ".join(f"{s} {counts[s]}" for s in STATUSES if s in counts), ""]
    out += ["## Overview", "",
            "| ID | Status | Prio | Type | Title | Effort | Ticket | Depends on |",
            "|---|---|---|---|---|---|---|---|"]
    for t in sorted(tasks, key=lambda t: (t["status"] == "done", t["priority"], t["id"])):
        out.append(f"| {t['id']} | {t['status']} | {t['priority']} | {t['type']} | {t['title']} | "
                   f"{t.get('effort', '')} | {t.get('ticket', '')} | {as_text(t.get('depends_on', ''))} |")
    out += ["", "## Details", ""]
    for t in tasks:
        head = f"Status **{t['status']}**, priority {t['priority']}, type {t['type']}"
        if t.get("effort"):
            head += f", effort {t['effort']}"
        if t.get("owner"):
            head += f", owner {t['owner']}"
        out += [f"### {t['id']} {t['title']}", "", head, ""]
        for key in ("description", "blocked_by", "result", "evidence"):
            if t.get(key):
                label = {"blocked_by": "Blocked by: ", "result": "Result: ", "evidence": "Evidence: "}.get(key, "")
                out += [label + str(t[key]).strip(), ""]
        if t.get("acceptance"):
            out += ["Acceptance:", ""] + [f"- {x}" for x in t["acceptance"]] + [""]
        for key, label in (("depends_on", "Depends on"), ("subtasks", "Subtasks"), ("source", "Source"),
                           ("ticket", "Ticket"), ("merge_request", "Merge request"), ("commits", "Commits")):
            if t.get(key):
                out += [f"{label}: {as_text(t[key])}", ""]
    return "\n".join(out) + "\n"


def main(argv):
    data = load_yaml(SOURCE.read_text(encoding="utf-8"))
    problems = validate(data["tasks"])
    if problems:
        print("ledger_md: the ledger is inconsistent:", file=sys.stderr)
        for p in problems:
            print("  - " + p, file=sys.stderr)
        return 2
    if "--next-id" in argv:
        numbers = [int(t["id"].split("-")[1]) for t in data["tasks"]]
        print(f"MOR-{max(numbers, default=0) + 1:03d}")
        return 0
    text = render(data)
    if "--check" in argv:
        current = TARGET.read_text(encoding="utf-8").replace("\r\n", "\n") if TARGET.exists() else ""
        if current != text:
            print("ledger_md: docs/tasks/ledger.md is out of date. Run: python3 scripts/ledger/ledger_md.py",
                  file=sys.stderr)
            return 1
        return 0
    with open(TARGET, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
    print(f"{len(data['tasks'])} tasks")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
