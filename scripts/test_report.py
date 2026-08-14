#!/usr/bin/env python3
"""Aggregate test-results records for a target commit into a markdown report.

Rules (spec 2026-08-13-test-results-store-design.md):
- only scope.complete records count;
- per (phase) the record with the newest timestamp wins among records whose
  commit *qualifies* for the target commit;
- commit C qualifies for target R iff C == R, or C is an ancestor of R and
  every path in `git diff C..R` matches the allowlist below.

This tool only *reports*: it aggregates and renders, it never decides whether
a release should proceed. Its exit code is a signal, not a gate - it is the
caller's business whether to treat exit 1 (gaps/broken tests) as fatal, a
warning, or something to ignore entirely. Exit codes: 0 = all REQUIRED_PHASES
covered and broken == 0 everywhere; 1 = gaps or broken tests found; 3 =
infra/fetch failure (store unreachable) - distinct from 1 because it says
nothing about test health.
"""
import argparse
import fnmatch
import json
import os
import subprocess
import sys

REQUIRED_PHASES = ["inmem", "mongodb_rs", "poppydb_rs",
                   "mongodb_single", "poppydb_single"]

# Marks the report section for callers that splice it into a larger document
# (updateReleaseReport.sh replaces everything between these markers in a
# GitHub release body on every re-publish - the "living report"). Keep the
# text stable: it is matched verbatim, not parsed.
MARK_START = "<!-- morphium-test-report:start -->"
MARK_END = "<!-- morphium-test-report:end -->"

# paths that do not change the released artifact
ALLOW = ["docs/*", "*.md", "branding/*", "mkdocs.yml", "LICENSE",
         ".gitignore", "scripts/*", "runtests.sh", "badges/*"]
# allowed too, but the report must say so ("test-only changes since")
ALLOW_ANNOTATE = ["*/src/test/*"]


def sh(*cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


def load_records():
    if sh("git", "fetch", "-q", "origin", "test-results").returncode != 0:
        print("error: cannot fetch origin/test-results", file=sys.stderr)
        sys.exit(3)
    ls = sh("git", "ls-tree", "-r", "--name-only", "FETCH_HEAD")
    records = []
    for name in ls.stdout.split():
        if not name.endswith(".json"):
            continue
        blob = sh("git", "show", "FETCH_HEAD:%s" % name)
        try:
            records.append(json.loads(blob.stdout))
        except ValueError:
            print("warning: skipping unparsable %s" % name, file=sys.stderr)
    return records


def classify_diff(commit, target):
    """'' if identical, 'clean'/'tests' if allowlisted diff, None otherwise."""
    if sh("git", "merge-base", "--is-ancestor", commit, target).returncode != 0:
        return None
    diff = sh("git", "diff", "--name-only", "%s..%s" % (commit, target))
    if diff.returncode != 0:
        return None
    files = [f for f in diff.stdout.splitlines() if f.strip()]
    if not files:
        return ""
    verdict = "clean"
    for f in files:
        if any(fnmatch.fnmatch(f, p) for p in ALLOW):
            continue
        if any(fnmatch.fnmatch(f, p) for p in ALLOW_ANNOTATE):
            verdict = "tests"
            continue
        return None
    return verdict


def aggregate(records, target):
    chosen = {}          # phase -> (record, phase_stats, diff_class)
    for rec in records:
        if not rec.get("scope", {}).get("complete"):
            continue
        diff_class = classify_diff(rec["commit"], target)
        if diff_class is None:
            continue
        for phase, stats in rec["phases"].items():
            cur = chosen.get(phase)
            if cur is None or rec["timestamp"] > cur[0]["timestamp"]:
                chosen[phase] = (rec, stats, diff_class)
    return chosen


def render_markdown(chosen, target):
    lines = ["## Test results", "",
             "| Phase | Tests | Passed | Flaky | Broken | Runner | Tested commit | When (UTC) |",
             "|---|---|---|---|---|---|---|---|"]
    annotate = False
    for phase in REQUIRED_PHASES:
        if phase not in chosen:
            lines.append("| %s | — | — | — | — | *missing* | | |" % phase)
            continue
        rec, st, diff_class = chosen[phase]
        if diff_class == "tests":
            annotate = True
        lines.append("| %s | %d | %d | %d | %d | %s | %s | %s |" % (
            phase, st["methods"], st["passed"], st.get("flaky", 0),
            st["broken"], rec["runner"], rec["commit"][:8], rec["timestamp"]))
    # extension-module phases (jakarta-data, quarkus, ...): report-only, never gate-relevant
    for phase in sorted(p for p in chosen if p not in REQUIRED_PHASES):
        rec, st, diff_class = chosen[phase]
        if diff_class == "tests":
            annotate = True
        lines.append("| %s *(optional)* | %d | %d | %d | %d | %s | %s | %s |" % (
            phase, st["methods"], st["passed"], st.get("flaky", 0),
            st["broken"], rec["runner"], rec["commit"][:8], rec["timestamp"]))
    cov = None
    for phase in REQUIRED_PHASES:
        if phase in chosen and chosen[phase][0].get("coverage"):
            c = chosen[phase][0]
            if cov is None or c["timestamp"] > cov["timestamp"]:
                cov = c
    if cov:
        lines += ["", "**Coverage** (JaCoCo, merged over the full matrix): " +
                  ", ".join("`%s` %.1f%% line / %.1f%% branch" %
                            (m, v.get("line", 0), v.get("branch", 0))
                            for m, v in sorted(cov["coverage"].items()))]
    if annotate:
        lines += ["", "_Some results were produced on an earlier commit; only "
                  "test/doc/tooling files changed since (released artifact identical)._"]
    body = "\n".join(lines) + "\n"
    return MARK_START + "\n" + body + MARK_END + "\n", cov


def write_badges(chosen, cov, badges_dir):
    os.makedirs(badges_dir, exist_ok=True)
    covered = [p for p in REQUIRED_PHASES if p in chosen]
    broken = sum(chosen[p][1]["broken"] for p in covered)
    ok = len(covered) == len(REQUIRED_PHASES) and broken == 0
    passed = sum(chosen[p][1]["passed"] for p in covered)
    with open(os.path.join(badges_dir, "tests.json"), "w") as fh:
        json.dump({"schemaVersion": 1, "label": "tests",
                   "message": "%d/%d phases, %d passed" %
                              (len(covered), len(REQUIRED_PHASES), passed),
                   "color": "brightgreen" if ok else "red"}, fh)
    if cov:
        lines_pct = [v.get("line", 0) for v in cov["coverage"].values()]
        avg = sum(lines_pct) / len(lines_pct)
        color = "brightgreen" if avg >= 75 else "yellow" if avg >= 60 else "orange"
        with open(os.path.join(badges_dir, "coverage.json"), "w") as fh:
            json.dump({"schemaVersion": 1, "label": "coverage",
                       "message": "%.0f%% line" % avg, "color": color}, fh)


def selftest():
    rec = {"schema": 1, "commit": "a" * 40, "branch": "develop",
           "timestamp": "2026-08-13T20:00:00Z", "runner": "t",
           "scope": {"complete": True, "tags": None, "testPattern": None},
           "phases": {"inmem": {"classes": 1, "methods": 10, "passed": 10,
                                 "skipped": 0, "broken": 0, "flaky": 0,
                                 "duration_s": 5}}}
    newer = json.loads(json.dumps(rec))
    newer["timestamp"] = "2026-08-13T21:00:00Z"
    newer["phases"]["inmem"]["broken"] = 1
    import unittest.mock as mock
    with mock.patch(__name__ + ".classify_diff", return_value=""):
        chosen = aggregate([rec, newer], "a" * 40)
    assert chosen["inmem"][1]["broken"] == 1, "newest must win, even when red"
    partial = json.loads(json.dumps(rec))
    partial["scope"]["complete"] = False
    with mock.patch(__name__ + ".classify_diff", return_value=""):
        chosen = aggregate([partial], "a" * 40)
    assert chosen == {}, "incomplete records must never qualify"
    md, cov = render_markdown({}, "a" * 40)
    assert "*missing*" in md
    # Verify annotation only fires for "tests" diffs, not "clean" diffs
    with mock.patch(__name__ + ".classify_diff", return_value="clean"):
        chosen = aggregate([rec], "a" * 40)
    md_clean, _ = render_markdown(chosen, "a" * 40)
    assert "test/doc/tooling files changed" not in md_clean, \
        "annotation must NOT fire for clean diffs (docs-only)"
    with mock.patch(__name__ + ".classify_diff", return_value="tests"):
        chosen = aggregate([rec], "a" * 40)
    md_tests, _ = render_markdown(chosen, "a" * 40)
    assert "test/doc/tooling files changed" in md_tests, \
        "annotation MUST fire for test-only diffs"
    # A gap-state (missing phases) must still write badges - the tool only
    # reports, it never withholds output because the news is bad.
    import tempfile
    with mock.patch(__name__ + ".classify_diff", return_value=""):
        gap_chosen = aggregate([rec], "a" * 40)  # only "inmem" present, 4 missing
    with tempfile.TemporaryDirectory() as tmp:
        write_badges(gap_chosen, None, tmp)
        with open(os.path.join(tmp, "tests.json")) as fh:
            badge = json.load(fh)
    assert badge["color"] == "red", "gap-state badge must be red"
    assert "1/5" in badge["message"], "gap-state badge must show the shortfall"
    # Marker section: updateReleaseReport.sh splices on these markers verbatim,
    # so both must appear, and appear exactly once, in the rendered markdown.
    assert md.count(MARK_START) == 1, "start marker must appear exactly once"
    assert md.count(MARK_END) == 1, "end marker must appear exactly once"
    assert md.index(MARK_START) < md.index(MARK_END), "start marker must precede end marker"
    print("selftest OK")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--target-commit")
    ap.add_argument("--markdown-out")
    ap.add_argument("--badges-dir")
    args = ap.parse_args()
    if args.selftest:
        selftest()
        return
    if not args.target_commit:
        ap.error("--target-commit is required")
    records = load_records()
    chosen = aggregate(records, args.target_commit)
    md, cov = render_markdown(chosen, args.target_commit)
    missing = [p for p in REQUIRED_PHASES if p not in chosen]
    # "broken" looks at required phases only — a red optional (extension-module)
    # phase is reported but does not affect the exit code
    broken = sum(chosen[p][1]["broken"] for p in chosen if p in REQUIRED_PHASES)
    has_gaps = missing or broken
    print(md)
    if args.markdown_out:
        with open(args.markdown_out, "w") as fh:
            fh.write(md)
    # Badges are written whenever records were loadable at all (exit 0 or 1) -
    # a red badge honestly reflects a gap-state, it's not withheld to keep the
    # working tree clean. Only exit 3 (store unreachable) skips them, since
    # there is nothing to render.
    if args.badges_dir:
        write_badges(chosen, cov, args.badges_dir)
    if has_gaps:
        print("REPORT: gaps found - missing=%s broken=%d" % (missing, broken),
              file=sys.stderr)
        sys.exit(1)
    else:
        print("REPORT: all required phases complete and green", file=sys.stderr)


if __name__ == "__main__":
    main()
