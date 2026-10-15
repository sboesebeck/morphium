"""Tests of ledger_md.py. Run from the repository root: python3 -m unittest scripts/ledger/test_ledger_md.py"""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import ledger_md  # noqa: E402


def task(**overrides):
    base = {"id": "MOR-001", "title": "A task", "type": "implementation", "priority": 2, "status": "open"}
    base.update(overrides)
    return base


class ValidateTest(unittest.TestCase):
    def test_should_accept_a_minimal_task(self):
        self.assertEqual(ledger_md.validate([task()]), [])

    def test_should_report_duplicate_ids(self):
        problems = ledger_md.validate([task(), task(title="Other")])
        self.assertTrue(any("duplicate id MOR-001" in p for p in problems))

    def test_should_report_unknown_references(self):
        problems = ledger_md.validate([task(depends_on=["MOR-999"]), task(id="MOR-002", subtasks=["MOR-998"])])
        self.assertTrue(any("depends_on refers to unknown id MOR-999" in p for p in problems))
        self.assertTrue(any("subtasks refers to unknown id MOR-998" in p for p in problems))

    def test_should_require_evidence_for_done_and_reason_for_blocked(self):
        problems = ledger_md.validate([task(status="done"), task(id="MOR-002", status="blocked")])
        self.assertTrue(any("MOR-001: status done needs 'evidence'" in p for p in problems))
        self.assertTrue(any("MOR-002: status blocked needs 'blocked_by'" in p for p in problems))

    def test_should_reject_invalid_values(self):
        problems = ledger_md.validate([task(status="finished", type="misc", priority=5, id="X-1")])
        self.assertEqual(len(problems), 4)


class RenderTest(unittest.TestCase):
    DATA = {"title": "Ledger", "as_of": "2026-10-13", "sources": {"a": "docs/a.md"},
            "tasks": [task(id="MOR-001", status="done", priority=1, evidence="ok"),
                      task(id="MOR-002", priority=2), task(id="MOR-003", priority=1, depends_on=["MOR-002"])]}

    def test_should_sort_open_before_done_and_by_priority(self):
        text = ledger_md.render(self.DATA)
        overview = text.split("## Details")[0]
        self.assertLess(overview.index("MOR-003"), overview.index("MOR-002"))
        self.assertLess(overview.index("MOR-002"), overview.index("MOR-001"))

    def test_should_count_statuses_and_end_with_a_newline(self):
        text = ledger_md.render(self.DATA)
        self.assertIn("Status: open 2, done 1", text)
        self.assertTrue(text.endswith("\n"))

    def test_should_show_dependencies_and_evidence(self):
        text = ledger_md.render(self.DATA)
        self.assertIn("Depends on: MOR-002", text)
        self.assertIn("Evidence: ok", text)


class RepositoryLedgerTest(unittest.TestCase):
    def test_should_have_a_consistent_ledger_and_an_up_to_date_markdown(self):
        if not ledger_md.SOURCE.exists():
            return
        data = ledger_md.load_yaml(ledger_md.SOURCE.read_text(encoding="utf-8"))
        self.assertEqual(ledger_md.validate(data["tasks"]), [])
        self.assertEqual(ledger_md.TARGET.read_text(encoding="utf-8"), ledger_md.render(data))
