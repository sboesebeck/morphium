#!/bin/bash
# Publish a test-results record (JSON on stdin) to the append-only orphan
# branch `test-results`. Unique filenames make conflicts impossible; concurrent
# pushers only ever need a fetch+retry. bash 3.2 compatible.
set -eo pipefail

# Resolve the real repo root from this script's own location rather than the
# caller's CWD. Callers in CI phase workdirs (/tmp/morphium-phase-workdir-*)
# invoke this script through a symlink sitting in a non-git symlink farm -
# `dirname "$0"` alone stays inside that farm, so it has to be dereferenced
# (python3 os.path.realpath; bash 3.2 has no readlink -f) back to where the
# script file actually lives, i.e. inside the real checkout.
REPO_DIR=$(python3 -c 'import os,sys; print(os.path.dirname(os.path.dirname(os.path.realpath(sys.argv[1]))))' "$0")
git -C "$REPO_DIR" rev-parse --git-dir >/dev/null 2>&1 || { echo "error: cannot resolve real repo root from $0 (resolved: $REPO_DIR)" >&2; exit 1; }

REMOTE=origin
DRY_RUN=0
BRANCH=test-results
while [ $# -ne 0 ]; do
  case "$1" in
  --dry-run) DRY_RUN=1; shift ;;
  --remote) REMOTE="$2"; shift 2 ;;
  *) echo "unknown option: $1" >&2; exit 1 ;;
  esac
done

RECORD=$(cat)
# filename fields straight from the record so file and content cannot diverge
FILE=$(printf '%s' "$RECORD" | python3 -c '
import json, re, sys
try:
    r = json.load(sys.stdin)
    ts = r["timestamp"].replace(":", "-")
    commit8 = r["commit"][:8]
    runner = re.sub(r"[^A-Za-z0-9_-]", "", r["runner"].split(".")[0]) or "unknown"
    scope = "full" if r["scope"]["complete"] else "partial"
    phases = "-".join(sorted(r["phases"]))
    for field in (ts, commit8, phases):
        if not re.fullmatch(r"[A-Za-z0-9._-]+", field):
            raise ValueError("unsafe field content: %r" % field)
    print("%s_%s_%s_%s-%s.json" % (ts, commit8, runner, scope, phases))
except Exception as e:
    print("error: invalid record: %s" % e, file=sys.stderr)
    sys.exit(1)
') || { echo "error: refusing to publish invalid record" >&2; exit 1; }
case "$FILE" in *[!A-Za-z0-9._-]*|"") echo "error: unsafe filename: $FILE" >&2; exit 1 ;; esac

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/morphium-testresults.XXXXXX")
trap 'rm -rf "$WORKDIR"' EXIT
REMOTE_URL=$(git -C "$REPO_DIR" remote get-url "$REMOTE")

if git ls-remote --exit-code --heads "$REMOTE_URL" "$BRANCH" >/dev/null 2>&1; then
  git clone -q --depth 1 --branch "$BRANCH" "$REMOTE_URL" "$WORKDIR/store"
else
  git init -q "$WORKDIR/store"
  (cd "$WORKDIR/store" \
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

cd "$WORKDIR/store"
printf '%s\n' "$RECORD" > "$FILE"
git add "$FILE"
# -c user.name/-c user.email give this commit an identity even on a fresh
# machine with no git user.* configured (hit for real on the CI testrunner:
# "fatal: empty ident name" silently dropped a publish) - env vars let a
# runner customize it, the default is a neutral bot identity either way.
git -c user.name="${MORPHIUM_RESULTS_GIT_NAME:-morphium-test-results}" \
    -c user.email="${MORPHIUM_RESULTS_GIT_EMAIL:-test-results@morphium.invalid}" \
    commit -q -m "results: $FILE"

if [ "$DRY_RUN" -eq 1 ]; then
  echo "dry-run: would push $FILE to $REMOTE/$BRANCH"
  exit 0
fi

n=0
while ! git push -q "$REMOTE" "HEAD:refs/heads/$BRANCH" 2>/dev/null; do
  n=$((n + 1))
  if [ "$n" -gt 5 ]; then
    echo "error: push failed after 5 retries" >&2
    exit 1
  fi
  # non-fast-forward: someone else pushed; replay our unique file on top
  git fetch -q "$REMOTE" "$BRANCH"
  git rebase -q "FETCH_HEAD" || { git rebase --abort; exit 1; }
done
echo "published $FILE to $REMOTE/$BRANCH"
