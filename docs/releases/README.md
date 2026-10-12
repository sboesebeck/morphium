# Release Documentation

## Current process

Every release lives in two places:

- **[`CHANGELOG.md`](https://github.com/sboesebeck/morphium/blob/develop/CHANGELOG.md)** at the repo root — the single source of truth,
  [Keep a Changelog](https://keepachangelog.com/) format. Entries are added under
  `[Unreleased]` as changes land, not written retroactively at release time.
- **[GitHub Releases](https://github.com/sboesebeck/morphium/releases)** — `release.sh`
  rolls the `[Unreleased]` section into the new version, then builds the release body from
  that CHANGELOG section plus the test report.

There is no per-version file created in this directory for a release; see `release.sh`'s
own comments (search `CHANGELOG helpers`) for exactly what it automates.

## `release.sh`

```bash
# Patch release (default): 6.1.9 → 6.1.10
./release.sh

# Minor release: 6.1.9 → 6.2.0
./release.sh --minor

# Major release: 6.1.9 → 7.0.0
./release.sh --major

# Test the release process without uploading
./release.sh --minor --dry-run

# Roll back a failed release
./release.sh --rollback

# Fix broken state after aborted release
./release.sh --reset
```

The script handles version calculation (from last git tag), Maven release:prepare,
artifact signing, Sonatype upload, git operations (tag, merge to master), and rolling the
CHANGELOG into the GitHub release as described above.

**Multi-module:** the release creates a single Sonatype bundle containing
morphium-parent, morphium (core), and poppydb.

## What's in this directory

`CHANGELOG-6.0.1.md` and `RELEASE-NOTES-6.0.1.md` are a one-off from Morphium 6.0.1: a
two-file format (a detailed technical changelog plus a short user-facing summary) that was
tried once and not carried forward — every release since has used the single-CHANGELOG
process above instead. Kept here for historical reference, not as a template to repeat.
