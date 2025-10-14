# Changelog

All notable changes to Morphium will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [6.0.1] - TBD

> 📖 **Detailed release notes**: [docs/releases/CHANGELOG-6.0.1.md](docs/releases/CHANGELOG-6.0.1.md)
> 📝 **Quick summary**: [docs/releases/RELEASE-NOTES-6.0.1.md](docs/releases/RELEASE-NOTES-6.0.1.md)

### Breaking Changes
- **Annotation Rename**: `@UseIfnull` renamed to `@UseIfNull` for consistency with Java naming conventions
  - Migration required: Find and replace all instances in your code
  - Update imports: `de.caluga.morphium.annotations.UseIfNull`

### Added
- **Bidirectional @UseIfNull Behavior**: Annotation now protects fields during both serialization and deserialization
  - Fields WITHOUT `@UseIfNull` are now protected from null values in the database
  - Prevents null contamination from data migrations, manual edits, or external systems
  - Improves data integrity when documents are modified outside the application
- Comprehensive test suites for @UseIfNull behavior (9 new tests)
- Enhanced documentation for @UseIfNull with detailed examples and use cases

### Changed
- **@UseIfNull deserialization behavior**: Fields without annotation now reject null values from database, preserving default values
  - Previous: null in DB always overwrites field value
  - New: null in DB is rejected unless field has @UseIfNull annotation
  - This is a behavioral change but improves data integrity

### Fixed
- Socket timeout handling in `SingleMongoConnection` - automatic retry on timeout exceptions
- Better timeout detection in watch operations
- Multi-collection messaging error handling and lock release
- Connection management in message rejection handler
- **Bulk operations now return proper operation counts**: `runBulk()` now returns statistics including `num_inserted`, `num_matched`, `num_modified`, `num_deleted`, `num_upserts`, and `upsertedIds`

### Performance
- Added collection name caching to reduce reflection overhead

## [6.0.0] - 2024-XX-XX

### Major Release
- Java 21+ requirement
- Significant architectural improvements
- Enhanced driver support
- Improved documentation

---

For detailed release notes, see individual release documentation in [docs/releases/](docs/releases/).
