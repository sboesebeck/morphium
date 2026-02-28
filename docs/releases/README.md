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

Releases are listed newest-first. For the full changelog, see [CHANGELOG.md](../../CHANGELOG.md).

### [6.1.8](RELEASE-NOTES-6.1.8.md)
Stability release with breaking `MorphiumDriverException` change and critical connection pool fixes.
- [Quick Release Notes](RELEASE-NOTES-6.1.8.md)

**Highlights:**
- `MorphiumDriverException` is now unchecked (`RuntimeException`)
- Connection pool exhaustion fixes (hostname case, counter drift, heartbeat leak)
- Messaging lock TTL fix, ChangeStreamMonitor auto-recovery

---

### [6.1.0](RELEASE-NOTES-6.1.0.md)
Major feature release: MorphiumServer replica sets, SSL/TLS, persistence, InMemoryDriver improvements.
- [Quick Release Notes](RELEASE-NOTES-6.1.0.md)

**Highlights:**
- MorphiumServer: replica set support, SSL/TLS, snapshot persistence, standalone CLI
- InMemoryDriver: shared databases, `$text` queries, performance optimizations
- Test infrastructure overhaul (MultiDriverTestBase, parameterized tests)

---

### 6.0.3 - 2025-11-28
Bugfix: NPE in MultiCollectionMessaging `getLockCollectionName()`.

---

### 6.0.2 - 2025-10-16
Bugfix: NPE in `Query.set()` and `Msg.preStore()`.

---

### [6.0.1](CHANGELOG-6.0.1.md)
Bugfix release with enhanced null handling and connection stability.
- [Detailed Changelog](CHANGELOG-6.0.1.md)
- [Quick Release Notes](RELEASE-NOTES-6.0.1.md)

**Highlights:**
- New `@IgnoreNullFromDB` annotation (replaces deprecated `@UseIfNull`)
- Default null handling aligned with standard ORMs (Hibernate, JPA)
- Socket timeout retry logic, bulk operation statistics

---

### 6.0.0
Major release: Java 21+, own wire-protocol driver, SSL/TLS support, virtual threads.
