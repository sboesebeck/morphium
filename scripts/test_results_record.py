#!/usr/bin/env python3
"""Build one test-results record (JSON) from a finished runtests.sh log directory.

Part of the decoupled test-results store (see docs/superpowers/specs/
2026-08-13-test-results-store-design.md). stdlib only, bash-3.2-friendly CLI.
"""
import argparse
import datetime
import json
import os
import re
import sys

SUMMARY_RE = re.compile(
    r"Tests run: (\d+), Failures: (\d+), Errors: (\d+), Skipped: (\d+),"
    r" Time elapsed: ([\d.,]+) s.*- in ([\w.$]+)")
# report-level totals are the LAST counter of each type in a jacoco XML
# (module -> package -> class counters come first, report totals last).
# Regex instead of xml.etree: no XXE surface, and jacoco's DOCTYPE would
# trip the stdlib parser anyway.
COUNTER_RE = re.compile(
    r'<counter type="(LINE|BRANCH)" missed="(\d+)" covered="(\d+)"/>')


def parse_logdir(logdir):
    try:
        names = sorted(os.listdir(logdir))
    except (FileNotFoundError, NotADirectoryError) as e:
        print("error: no parsable class logs in %s" % logdir, file=sys.stderr)
        sys.exit(2)
    classes = methods = method_failed = class_broken = skipped = 0
    duration = 0.0
    for name in names:
        if not name.endswith(".log") or name == "failed.txt":
            continue
        path = os.path.join(logdir, name)
        last = None
        with open(path, errors="replace") as fh:
            for line in fh:
                m = SUMMARY_RE.search(line)
                if m:
                    last = m
        classes += 1
        if last is None:
            class_broken += 1  # no summary line at all: build/setup failure of the class
            continue
        run, fails, errs, skip, elapsed, _fqcn = last.groups()
        methods += int(run)
        method_failed += int(fails) + int(errs)
        skipped += int(skip)
        duration += float(elapsed.replace(",", ""))
    if classes == 0:
        return None
    return {"classes": classes, "methods": methods,
            "passed": methods - method_failed - skipped, "skipped": skipped,
            "broken": method_failed + class_broken, "duration_s": int(duration)}


def parse_coverage(pairs):
    cov = {}
    for module, path in pairs:
        entry = {}
        with open(path, errors="replace") as fh:
            for ctype, missed, covered in COUNTER_RE.findall(fh.read()):
                total = int(missed) + int(covered)
                # keep overwriting: the last counter per type is the report total
                entry[ctype.lower()] = (
                    round(int(covered) * 100.0 / total, 1) if total else 0.0)
        cov[module] = entry
    return cov or None


def build(args):
    phase_stats = parse_logdir(args.logdir)
    if phase_stats is None:
        print("error: no parsable class logs in %s" % args.logdir, file=sys.stderr)
        sys.exit(2)
    if args.flaky:
        phase_stats["flaky"] = args.flaky
        # flaky tests ended green after retries; they are counted broken by the
        # last-line rule only when they stayed red, so no correction needed here.
    else:
        phase_stats["flaky"] = 0
    complete = not (args.tags or args.test_pattern)
    record = {
        "schema": 1,
        "commit": args.commit,
        "branch": args.branch,
        "timestamp": datetime.datetime.now(datetime.timezone.utc)
                     .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "runner": args.runner,
        "scope": {"complete": complete,
                  "tags": args.tags or None,
                  "testPattern": args.test_pattern or None},
        "phases": {args.phase: phase_stats},
    }
    if args.duration_s:
        record["phases"][args.phase]["duration_s"] = args.duration_s
    cov = parse_coverage(args.coverage_xml)
    if cov:
        record["coverage"] = cov
    return record


SELFTEST_LOG = """\
some noise
[INFO] Tests run: 9, Failures: 1, Errors: 0, Skipped: 1, Time elapsed: 117.69 s <<< FAILURE! - in de.caluga.test.Foo
retry noise
[INFO] Tests run: 9, Failures: 0, Errors: 0, Skipped: 1, Time elapsed: 90.10 s - in de.caluga.test.Foo
"""

SELFTEST_COV = """<?xml version="1.0"?>
<report name="x"><counter type="LINE" missed="258" covered="742"/>
<counter type="BRANCH" missed="382" covered="618"/></report>
"""


def selftest():
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        with open(os.path.join(td, "de.caluga.test.Foo.log"), "w") as fh:
            fh.write(SELFTEST_LOG)
        with open(os.path.join(td, "de.caluga.test.Broken.log"), "w") as fh:
            fh.write("compile error, no summary line\n")
        stats = parse_logdir(td)
        assert stats == {"classes": 2, "methods": 9, "passed": 8, "skipped": 1,
                         "broken": 1, "duration_s": 90}, stats
        covf = os.path.join(td, "cov.xml")
        with open(covf, "w") as fh:
            fh.write(SELFTEST_COV)
        cov = parse_coverage([("morphium-core", covf)])
        assert cov == {"morphium-core": {"line": 74.2, "branch": 61.8}}, cov
    print("selftest OK")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--logdir")
    ap.add_argument("--phase")
    ap.add_argument("--runner")
    ap.add_argument("--commit")
    ap.add_argument("--branch")
    ap.add_argument("--tags")
    ap.add_argument("--test-pattern")
    ap.add_argument("--flaky", type=int, default=0)
    ap.add_argument("--duration-s", type=int, default=0)
    ap.add_argument("--coverage-xml", action="append", default=[],
                    type=lambda s: tuple(s.split("=", 1)))
    args = ap.parse_args()
    if args.selftest:
        selftest()
        return
    for req in ("logdir", "phase", "runner", "commit", "branch"):
        if not getattr(args, req):
            ap.error("--%s is required" % req)
    json.dump(build(args), sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()
