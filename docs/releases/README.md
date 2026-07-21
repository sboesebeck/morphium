# Release Documentation

This directory contains detailed release notes for each Morphium version.

## Documentation Structure

**Root Level:**
- `CHANGELOG.md` - Single changelog file with all releases (standard format, [Keep a Changelog](https://keepachangelog.com/))

**This Directory (`docs/releases/`):**
- `CHANGELOG-X.Y.Z.md` - Comprehensive technical changelog with implementation details
- `RELEASE-NOTES-X.Y.Z.md` - Quick summary and migration guide for users

## Why This Structure?

- **Single CHANGELOG.md**: Industry standard, easy to browse all versions
- **Detailed docs**: Technical teams need deep dive documentation
- **Quick notes**: Users need fast migration info without technical details
- **No clutter**: Root directory stays clean with just one changelog file

## Release Process

Releases are managed via `release.sh` in the project root:

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
artifact signing, Sonatype upload, and git operations (tag, merge to master).

**Multi-module:** The release creates a single Sonatype bundle containing
morphium-parent, morphium (core), and poppydb.

## For Future Releases

When creating a new release:

1. Run `./release.sh --minor` (or `--patch`/`--major`)
2. Add entry to root `CHANGELOG.md` (concise)
3. Create `docs/releases/CHANGELOG-X.Y.Z.md` (detailed)
4. Create `docs/releases/RELEASE-NOTES-X.Y.Z.md` (user-facing)
5. Update this README with the new release

## Available Releases

### [6.0.1](CHANGELOG-6.0.1.md) - TBD
Bugfix release with enhanced null handling and connection stability
- [Detailed Changelog](CHANGELOG-6.0.1.md)
- [Quick Release Notes](RELEASE-NOTES-6.0.1.md)

**Highlights:**
- Bidirectional @UseIfNull behavior (protection from null contamination)
- Socket timeout retry logic
- Annotation rename: @UseIfnull → @UseIfNull
