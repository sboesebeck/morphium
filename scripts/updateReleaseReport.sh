#!/bin/bash
# Refresh a GitHub release's test-results section and the shields.io badges
# from the append-only `test-results` store - the "living report": whenever
# new results are published (see runtests.sh publish_test_results()), the
# release notes for the *previously released* tag should reflect them, not
# stay frozen at whatever the matrix looked like at release time. Mirrors the
# style of publishTestResults.sh (bash 3.2 compatible: no associative arrays,
# no `local` outside functions where avoidable).
#
# Badges are published FIRST and unconditionally (they only need git push
# rights to `origin`, not `gh`) - CI runners publishing results typically have
# git push but no gh auth, and the badges must still refresh in that case. The
# GitHub release notes section is a separate, independently-guarded step after
# it: it needs `gh` installed and authenticated, and an existing release for
# the tag; missing any of those only skips the notes step, never the badges.
#
# Entirely best-effort: every failure path warns to stderr and exits 0 - this
# is called opportunistically after every publish (runtests.sh) and must
# never turn a successful test-results publish into a failing script.
#
# Usage: updateReleaseReport.sh [--tag vX.Y.Z] [--dry-run]
set -eo pipefail

REMOTE=origin
BRANCH=test-results
DRY_RUN=0
TAG=""

while [ $# -ne 0 ]; do
  case "$1" in
  --tag) TAG="$2"; shift 2 ;;
  --dry-run) DRY_RUN=1; shift ;;
  *) echo "unknown option: $1" >&2; exit 1 ;;
  esac
done

# Resolve the real repo root from this script's own location rather than the
# caller's CWD. Callers in CI phase workdirs (/tmp/morphium-phase-workdir-*)
# invoke this script through a symlink sitting in a non-git symlink farm -
# `dirname "$0"`/`pwd` alone stays inside that farm (pwd doesn't dereference
# symlinks in the path it walked through), so it has to be dereferenced
# (python3 os.path.realpath; bash 3.2 has no readlink -f) back to where the
# script file actually lives, i.e. inside the real checkout.
REPO_ROOT=$(python3 -c 'import os,sys; print(os.path.dirname(os.path.dirname(os.path.realpath(sys.argv[1]))))' "$0")
git -C "$REPO_ROOT" rev-parse --git-dir >/dev/null 2>&1 || { echo "error: cannot resolve real repo root from $0 (resolved: $REPO_ROOT)" >&2; exit 1; }
# All subsequent git calls in this script, AND test_report.py's subprocess
# git calls (which inherit the CWD, not just argv), must run against the real
# repo regardless of where the caller invoked us from - so cd there now.
cd "$REPO_ROOT"

if [ -z "$TAG" ]; then
  # pipefail-safe: grep finding no v* tag must not abort the script via set -e
  TAG=$(git tag --sort=-creatordate | { grep '^v' || true; } | head -1)
fi
if [ -z "$TAG" ]; then
  echo "warning: no v* tag found - nothing to update" >&2
  exit 0
fi

TAG_COMMIT=$(git rev-list -n 1 "$TAG" 2>/dev/null) || TAG_COMMIT=""
if [ -z "$TAG_COMMIT" ]; then
  echo "warning: cannot resolve commit for tag $TAG - skipping" >&2
  exit 0
fi

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/morphium-releasereport.XXXXXX")
trap 'rm -rf "$WORKDIR"' EXIT

REPORT_MD="$WORKDIR/report.md"
BADGES_TMP="$WORKDIR/badges"

report_status=0
python3 "$REPO_ROOT/scripts/test_report.py" --target-commit "$TAG_COMMIT" \
  --markdown-out "$REPORT_MD" --badges-dir "$BADGES_TMP" >/dev/null 2>&1 || report_status=$?

if [ "$report_status" -eq 3 ]; then
  echo "warning: test-results store unreachable - skipping release report update" >&2
  exit 0
elif [ "$report_status" -ne 0 ] && [ "$report_status" -ne 1 ]; then
  echo "warning: test_report.py failed unexpectedly (exit $report_status) - skipping" >&2
  exit 0
fi
# exit 0 or 1 both fine here - the report is informational, not a gate.

# --- Badges (first, unconditional): publish badges/tests.json +
# badges/coverage.json into the test-results store branch, so the README
# badges (which point at raw.githubusercontent.com/.../test-results/badges/*)
# stay live. This needs only `git push` rights to $REMOTE, not `gh` - it must
# not be gated behind gh availability/auth. Same clone+push-retry pattern as
# publishTestResults.sh.
if [ ! -f "$BADGES_TMP/tests.json" ]; then
  echo "warning: no tests.json badge produced - skipping badge publish" >&2
else
  if [ "$DRY_RUN" -eq 1 ]; then
    echo "dry-run: would publish badges/tests.json to $REMOTE/$BRANCH:"
    cat "$BADGES_TMP/tests.json"
    echo
    if [ -f "$BADGES_TMP/coverage.json" ]; then
      echo "dry-run: would publish badges/coverage.json to $REMOTE/$BRANCH:"
      cat "$BADGES_TMP/coverage.json"
      echo
    fi
  else
    BADGE_WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/morphium-badges.XXXXXX")
    trap 'rm -rf "$WORKDIR" "$BADGE_WORKDIR"' EXIT
    REMOTE_URL=$(git remote get-url "$REMOTE")

    if git ls-remote --exit-code --heads "$REMOTE_URL" "$BRANCH" >/dev/null 2>&1; then
      git clone -q --depth 1 --branch "$BRANCH" "$REMOTE_URL" "$BADGE_WORKDIR/store"
    else
      git init -q "$BADGE_WORKDIR/store"
      (cd "$BADGE_WORKDIR/store" \
        && git checkout -q --orphan "$BRANCH" \
        && git remote add "$REMOTE" "$REMOTE_URL" \
        && printf '%s\n' "# Morphium test results" "" \
           "Append-only store of test-run records. One JSON file per run, written by" \
           "scripts/publishTestResults.sh (see docs in the main branches). Do not edit." \
           > README.md \
        && git add README.md \
        && git -c user.name="${MORPHIUM_RESULTS_GIT_NAME:-morphium-test-results}" \
                -c user.email="${MORPHIUM_RESULTS_GIT_EMAIL:-test-results@morphium.invalid}" \
                commit -q -m "chore: bootstrap test-results store")
    fi

    (
      cd "$BADGE_WORKDIR/store"
      mkdir -p badges
      cp "$BADGES_TMP/tests.json" badges/tests.json
      git add badges/tests.json
      if [ -f "$BADGES_TMP/coverage.json" ]; then
        cp "$BADGES_TMP/coverage.json" badges/coverage.json
        git add badges/coverage.json
      fi

      if git diff --cached --quiet; then
        echo "badges unchanged - nothing to publish"
      else
        # Build the success message from what was actually staged (`git add`
        # above), not a hardcoded list - coverage.json is optional (only
        # written when a phase record carries coverage data), so claiming it
        # was published when it wasn't would be a lie in the log.
        PUBLISHED_FILES=$(git diff --cached --name-only -- badges/ | paste -sd+ -)
        # -c user.name/-c user.email give this commit an identity even on a
        # fresh machine with no git user.* configured (hit for real on the CI
        # testrunner: "fatal: empty ident name" silently dropped a publish) -
        # env vars let a runner customize it, default is a neutral bot identity.
        git -c user.name="${MORPHIUM_RESULTS_GIT_NAME:-morphium-test-results}" \
            -c user.email="${MORPHIUM_RESULTS_GIT_EMAIL:-test-results@morphium.invalid}" \
            commit -q -m "badges: update for $TAG"

        n=0
        while ! git push -q "$REMOTE" "HEAD:refs/heads/$BRANCH" 2>/dev/null; do
          n=$((n + 1))
          if [ "$n" -gt 5 ]; then
            echo "warning: badge push failed after 5 retries" >&2
            exit 0
          fi
          # non-fast-forward: someone else pushed (a results record); rebase
          # our badge commit on top and retry
          git fetch -q "$REMOTE" "$BRANCH"
          git rebase -q "FETCH_HEAD" || { git rebase --abort; echo "warning: badge rebase failed" >&2; exit 0; }
        done
        echo "published $PUBLISHED_FILES to $REMOTE/$BRANCH for $TAG"
      fi
    )
  fi
fi

# --- Owner-confirmed guard: if the aggregation found ZERO qualifying records
# for the tag's commit, the rendered table is all "*missing*" rows - don't
# touch the release notes in that case. Rationale: releases predating the
# test-results store (or any tag nobody has published results for yet) would
# otherwise get decorated with a permanently empty results table forever;
# better to leave the notes untouched and let the first real entry appear
# organically once qualifying runs actually exist. Badges are exempt from
# this guard (see above) since they reflect *current* state, not history.
#
# Detection: inspect the rendered markdown table rather than parsing
# test_report.py's stderr/exit code - exit 1 also fires for "gaps" where some
# (not all) required phases are missing, which must still update the notes,
# so the exit code alone can't distinguish "zero results" from "partial
# results". The markdown is the one artifact both this script and
# test_report.py agree on the shape of (see render_markdown()/selftest() in
# test_report.py), so it's the more robust signal. Exclude the header ("|
# Phase | ...") and the separator ("|---|...") structurally by their fixed
# literal prefixes rather than by the first data cell's letter case - phase
# names are free-form for optional/extension-module phases (e.g. a future
# "Jakarta-Data" row) and may start with an uppercase letter or digit, so a
# character-class match on the first cell would misclassify those as
# non-data rows. Any surviving "| ...|" row not containing "*missing*" means
# at least one phase qualified.
QUALIFYING_ROWS=$(grep '^| ' "$REPORT_MD" | grep -v '^| Phase ' | grep -v '^|---' | { grep -v '\*missing\*' || true; })
if [ -z "$QUALIFYING_ROWS" ]; then
  echo "info: no qualifying test results for $TAG - leaving release notes untouched" >&2
  exit 0
fi

# --- GitHub release notes (independently guarded): needs gh installed,
# authenticated, and an existing release for $TAG. Any of these missing only
# skips this section - the badges above have already been refreshed.
if ! command -v gh >/dev/null 2>&1; then
  echo "warning: gh CLI not found - skipping release notes update" >&2
  exit 0
fi
if ! gh auth status >/dev/null 2>&1; then
  echo "warning: gh CLI not authenticated - skipping release notes update" >&2
  exit 0
fi

if ! gh release view "$TAG" >/dev/null 2>&1; then
  echo "warning: no GitHub release for $TAG - skipping (release.sh creates it at release time)" >&2
  exit 0
fi

EXISTING_BODY=$(gh release view "$TAG" --json body -q .body) || {
  echo "warning: failed to read existing release body for $TAG - skipping" >&2
  exit 0
}
printf '%s' "$EXISTING_BODY" >"$WORKDIR/existing_body.txt"

# Splice the marked section into the existing body: replace it in place if the
# markers are already present (re-publish - keeps the notes "living" without
# ever duplicating the section), otherwise append it. A safe python helper
# instead of sed because release notes are multiline and may contain
# characters sed would choke on.
NEW_BODY=$(python3 - "$WORKDIR/existing_body.txt" "$REPORT_MD" <<'PYEOF'
import sys

MARK_START = "<!-- morphium-test-report:start -->"
MARK_END = "<!-- morphium-test-report:end -->"

existing_path, section_path = sys.argv[1], sys.argv[2]
with open(existing_path) as fh:
    existing = fh.read()
with open(section_path) as fh:
    section = fh.read().rstrip("\n")

start = existing.find(MARK_START)
end = existing.find(MARK_END)
if start != -1 and end != -1 and end > start:
    end += len(MARK_END)
    new_body = existing[:start] + section + existing[end:]
else:
    existing_stripped = existing.rstrip("\n")
    new_body = existing_stripped + "\n\n" + section if existing_stripped else section

if not new_body.endswith("\n"):
    new_body += "\n"
sys.stdout.write(new_body)
PYEOF
)

if [ "$DRY_RUN" -eq 1 ]; then
  echo "dry-run: would update GitHub release notes for $TAG with:"
  printf '%s' "$NEW_BODY"
else
  if ! printf '%s' "$NEW_BODY" | gh release edit "$TAG" --notes-file -; then
    echo "warning: failed to update GitHub release notes for $TAG" >&2
    exit 0
  fi
  echo "updated release notes for $TAG"
fi
