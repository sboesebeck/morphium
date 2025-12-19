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

## For Future Releases

When creating a new release:

1. Add entry to root `CHANGELOG.md` (concise)
2. Create `docs/releases/CHANGELOG-X.Y.Z.md` (detailed)
3. Create `docs/releases/RELEASE-NOTES-X.Y.Z.md` (user-facing)
4. Update this README with the new release

## Available Releases

### [6.0.1](CHANGELOG-6.0.1.md) - TBD
Bugfix release with enhanced null handling and connection stability
- [Detailed Changelog](CHANGELOG-6.0.1.md)
- [Quick Release Notes](RELEASE-NOTES-6.0.1.md)

**Highlights:**
- Bidirectional @UseIfNull behavior (protection from null contamination)
- Socket timeout retry logic
- Annotation rename: @UseIfnull → @UseIfNull
