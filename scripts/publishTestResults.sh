#!/bin/bash
# Publish a test-results record (JSON on stdin) to the append-only orphan
# branch `test-results`. Unique filenames make conflicts impossible; concurrent
# pushers only ever need a fetch+retry. bash 3.2 compatible.
set -eo pipefail

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
eval "$(printf '%s' "$RECORD" | python3 -c '
import json,sys
r=json.load(sys.stdin)
ts=r["timestamp"].replace(":","-")
scope="full" if r["scope"]["complete"] else "partial"
phases="-".join(sorted(r["phases"]))
print("TS=%s COMMIT8=%s RUNNER=%s SCOPE=%s_%s" %
      (ts, r["commit"][:8], r["runner"].split(".")[0], scope, phases))
')"
FILE="${TS}_${COMMIT8}_${RUNNER}_${SCOPE}.json"

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/morphium-testresults.XXXXXX")
trap 'rm -rf "$WORKDIR"' EXIT
REMOTE_URL=$(git remote get-url "$REMOTE")

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
    && git commit -q -m "chore: bootstrap test-results store")
fi

cd "$WORKDIR/store"
printf '%s\n' "$RECORD" > "$FILE"
git add "$FILE"
git commit -q -m "results: $FILE"

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
