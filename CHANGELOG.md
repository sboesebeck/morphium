# Changelog

All notable changes to Morphium will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [6.0.1] - TBD

> 📖 **Detailed release notes**: [docs/releases/CHANGELOG-6.0.1.md](docs/releases/CHANGELOG-6.0.1.md)
> 📝 **Quick summary**: [docs/releases/RELEASE-NOTES-6.0.1.md](docs/releases/RELEASE-NOTES-6.0.1.md)

### Breaking Changes
- **Null Handling Behavior Change**: Default behavior now matches standard ORM conventions
  - **Previous behavior**: Null values were NOT stored in the database by default (fields omitted)
  - **New behavior**: Null values ARE stored as explicit nulls in the database by default
  - Fields WITHOUT annotation: Accept and store null values (standard ORM behavior)
  - Fields WITH `@IgnoreNullFromDB`: Reject nulls, field omitted when null
  - **Migration impact**: Existing code that relies on null values being omitted by default may need to add `@IgnoreNullFromDB` to those fields

- **@UseIfNull Deprecated**: Replaced with `@IgnoreNullFromDB` for clearer semantics
  - Old annotation had inverted logic that was confusing
  - `@UseIfNull` is now deprecated but still functional
  - Migration: Replace `@UseIfNull` with `@IgnoreNullFromDB` and remove the annotation (behavior is inverted)

### Added
- **New `@IgnoreNullFromDB` annotation**: Protects fields from null contamination
  - Prevents null values from being stored during serialization (field omitted)
  - Rejects null values during deserialization (preserves default value)
  - Distinguishes between "field missing from DB" vs "field present with null value"
  - Special handling for `@Id` fields: NEVER stored when null (MongoDB auto-generates)
  - Comprehensive documentation with behavior matrix and use cases
- Comprehensive test suites for null handling behavior
- Enhanced documentation for null handling with detailed examples

### Changed
- **Default null handling now matches standard ORMs**:
  - Serialization: Null values stored as explicit null in database
  - Deserialization: Null values from database accepted and set to null
  - This aligns with Hibernate, JPA, and other standard ORMs
- **@Id field handling**: Fields annotated with `@Id` are NEVER stored when null
  - Ensures MongoDB can auto-generate unique `_id` values
  - Prevents E11000 duplicate key errors from null `_id` values

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
