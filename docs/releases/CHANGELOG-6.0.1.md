# Morphium 6.0.1 Release Notes

**Release Date:** TBD
**Version:** 6.0.1

## Overview

This is a bugfix release for Morphium 6.0, focusing on improving null value handling, connection stability, and fixing annotation naming inconsistencies.

---

## Breaking Changes

### ⚠️ Annotation Rename: @UseIfnull → @UseIfNull

The `@UseIfnull` annotation has been renamed to `@UseIfNull` for consistency with Java naming conventions.

**Migration Required:**
```java
// OLD (v6.0.0 and earlier)
@UseIfnull
private Integer nullValue;

// NEW (v6.0.1 and later)
@UseIfNull
private Integer nullValue;
```

**Action Required:** Update all `@UseIfnull` annotations to `@UseIfNull` in your code.

**Files Changed:**
- `de.caluga.morphium.annotations.UseIfnull` → `de.caluga.morphium.annotations.UseIfNull`
- All internal usages updated

---

## Bug Fixes

### Connection Stability

**Socket Timeout Handling Improvements** (`SingleMongoConnection.java`)
- Added automatic retry logic for socket timeout exceptions
- Improved timeout detection in watch operations
- Enhanced error handling to prevent premature connection closure
- Connection now retries on `SocketTimeoutException` instead of failing immediately

**Impact:** Significantly improved stability for long-running operations and change streams.

### Null Value Handling

**🎯 Major Enhancement: Bidirectional @UseIfNull Behavior** (`ObjectMapperImpl.java`)

The `@UseIfNull` annotation now works **bidirectionally**, providing protection during both serialization (write) and deserialization (read).

**Previous Behavior (v6.0.0):**
- Annotation only affected writing to database
- Fields could receive null values from DB regardless of annotation
- No protection from "null contamination"

**New Behavior (v6.0.1):**
- Annotation affects both writing AND reading
- Fields WITHOUT @UseIfNull are **protected from null values in the database**
- Provides data integrity when documents are modified outside the application

**Detailed Behavior:**

**Serialization (Writing to DB):**
- **Without @UseIfNull:** null fields omitted from database document
- **With @UseIfNull:** null values stored explicitly in database

**Deserialization (Reading from DB):**
- **Without @UseIfNull:**
  - Field missing from DB → default value preserved
  - **Field present as null in DB → null ignored, default value preserved** ✨ NEW
- **With @UseIfNull:**
  - Field missing from DB → default value preserved
  - Field present as null in DB → null accepted, overrides default

**Impact:** Protects fields from unwanted null values during data migrations, manual database edits, or when integrating with external systems.

**Example:**
```java
@Entity
public class MyEntity {
    private Integer counter = 42;  // No @UseIfNull

    @UseIfNull
    private Integer nullCounter = 99;
}

// MongoDB document: { counter: null, nullCounter: null }
// After deserialization:
// counter = 42        (protected from null!)
// nullCounter = null  (accepted null)
```

### Multi-Collection Messaging

**Fixed Handling for Multi-Collection Messaging** (`MessageRejectedException.java`)
- Improved error handling when rejecting messages
- Fixed exclusive message lock release
- Enhanced connection management in rejection handler

### Bulk Operations

**Bulk Operations Now Return Proper Statistics**
- Fixed `MorphiumBulkContext.runBulk()` to return operation statistics instead of null/empty results
- All three driver implementations now properly collect and aggregate operation counts:
  - `InMemoryDriver` - Fixed inline bulk context implementation
  - `SingleMongoConnectDriver` - Fixed inline bulk context implementation
  - `PooledDriver` - Fixed inline bulk context implementation

**Returned Statistics:**
```java
Map<String, Object> result = bulkContext.runBulk();
// Returns:
// {
//   "num_inserted": <count>,
//   "num_matched": <count>,
//   "num_modified": <count>,
//   "num_deleted": <count>,
//   "num_upserts": <count>,
//   "upsertedIds": [<list of IDs>]  // Only if upserts occurred
// }
```

**Impact:** Applications can now track bulk operation results for monitoring, logging, and validation purposes.

**Test Coverage:** Added comprehensive test `BulkOperationTest.bulkTestReturnCounts()` verifying all count returns.

### Performance Improvements

**Collection Name Caching**
- Added caching mechanism for collection names to reduce reflection overhead
- Improves performance for repeated entity operations

---

## Improvements

### Documentation

**Enhanced @UseIfNull Documentation**
- Added comprehensive JavaDoc with behavior explanations
- Included code examples for common use cases
- Documented use cases: sharding keys, distinguishing "not set" vs "explicitly null", sparse indexes

**General Documentation Fixes**
- Fixed various documentation inconsistencies
- Updated code comments for clarity

### Test Coverage

**New Test Suites:**

1. **UseIfNullTest** - Basic @UseIfNull annotation behavior
   - Verifies serialization and deserialization correctness
   - Tests default value preservation
   - Validates null value storage

2. **UseIfNullDistinctionTest** - Bidirectional behavior verification
   - Tests distinction between field missing vs. field with null value
   - Verifies protection from null contamination
   - Validates bidirectional annotation behavior
   - Tests mixed scenarios and edge cases

---

## Technical Details

### Modified Files

**Core:**
- `src/main/java/de/caluga/morphium/Morphium.java` - Import updates
- `src/main/java/de/caluga/morphium/ObjectMapperImpl.java` - UseIfNull usage
- `src/main/java/de/caluga/morphium/annotations/UseIfNull.java` - Renamed and documented

**Driver:**
- `src/main/java/de/caluga/morphium/driver/wire/SingleMongoConnection.java` - Timeout handling
- `src/main/java/de/caluga/morphium/driver/inmem/InMemoryDriver.java` - Bulk operations return counts
- `src/main/java/de/caluga/morphium/driver/wire/SingleMongoConnectDriver.java` - Bulk operations return counts
- `src/main/java/de/caluga/morphium/driver/wire/PooledDriver.java` - Bulk operations return counts

**Messaging:**
- `src/main/java/de/caluga/morphium/messaging/Msg.java` - UseIfNull usage
- `src/main/java/de/caluga/morphium/messaging/MessageRejectedException.java` - Error handling

**Tests:**
- `src/test/java/de/caluga/test/mongo/suite/data/ComplexObject.java` - UseIfNull usage
- `src/test/java/de/caluga/test/mongo/suite/base/UseIfNullTest.java` - New test suite (5 tests)
- `src/test/java/de/caluga/test/mongo/suite/base/UseIfNullDistinctionTest.java` - New bidirectional tests (4 tests)
- `src/test/java/de/caluga/test/mongo/suite/base/BulkOperationTest.java` - Added bulkTestReturnCounts() test

### Compatibility

**Minimum Requirements:** (unchanged from 6.0.0)
- Java 21+
- MongoDB 4.0+

**Dependencies:** No changes to external dependencies

---

## Migration Guide

### Upgrading from 6.0.0 to 6.0.1

1. **Update @UseIfnull annotations:**
   ```bash
   # Search for usage in your codebase
   grep -r "@UseIfnull" src/

   # Replace with @UseIfNull
   find src/ -type f -name "*.java" -exec sed -i '' 's/@UseIfnull/@UseIfNull/g' {} +
   ```

2. **Update imports (if any):**
   ```java
   // OLD
   import de.caluga.morphium.annotations.UseIfnull;

   // NEW
   import de.caluga.morphium.annotations.UseIfNull;
   ```

3. **Review null value handling (IMPORTANT):**
   - **Fields without @UseIfNull are now protected from null values in the database**
   - If you have documents with null values that you expect to read as null, add @UseIfNull to those fields
   - Fields with default values will now keep those defaults even if DB contains null
   - This is generally the desired behavior and improves data integrity

4. **Rebuild and test:**
   ```bash
   mvn clean install
   ```

5. **Verify behavior in your tests:**
   - Test scenarios where database contains null values
   - Ensure fields behave as expected with/without @UseIfNull
   - Review the enhanced @UseIfNull documentation for complete behavior details

### No Migration Needed If:

- You don't use the `@UseIfnull` annotation
- You only use Morphium's core features without custom null handling

---

## Known Issues

None identified in this release.

---

## Contributors

- Stephan Bösebeck (@sboesebeck) - Core development and bug fixes

---

## Links

- **Repository:** https://github.com/sboesebeck/morphium
- **Documentation:** http://caluga.de
- **Issues:** https://github.com/sboesebeck/morphium/issues

---

## Next Release

Features planned for 6.1.0:
- Additional performance optimizations
- Enhanced aggregation pipeline support
- Improved transaction handling

---

**Full Changelog:** https://github.com/sboesebeck/morphium/compare/v6.0.0...v6.0.1
