# Morphium 6.0.1 Release Notes

**Release Date:** TBD
**Version:** 6.0.1

## Overview

This is a bugfix release for Morphium 6.0, focusing on improving null value handling, connection stability, and fixing annotation naming inconsistencies.

---

## Breaking Changes

### ⚠️ Null Handling Behavior Change

The default null handling behavior has been **inverted** to match standard ORM conventions (Hibernate, JPA, etc.).

**Previous Behavior (v6.0.0 and earlier):**
- Null values were NOT stored in the database by default (fields omitted)
- Null values from DB were ignored during deserialization
- Required `@UseIfNull` annotation to accept/store null values

**New Behavior (v6.0.1 and later):**
- **Null values ARE stored as explicit nulls in the database by default**
- Null values from DB are accepted and set during deserialization
- New `@IgnoreNullFromDB` annotation to protect fields from nulls

**Migration Required:**

```java
// OLD (v6.0.0) - Fields protected from nulls by default
private Integer counter = 42;  // Null not stored, default preserved

@UseIfNull
private Integer nullableField;  // Null stored and accepted

// NEW (v6.0.1) - Fields accept nulls by default
private Integer counter = 42;  // Null stored and accepted (loses default!)

@IgnoreNullFromDB
private Integer protectedField = 42;  // Protected from nulls (keeps default)
```

**Action Required:**
1. **Review all entity fields** - Fields that should preserve defaults need `@IgnoreNullFromDB`
2. **Remove `@UseIfNull`** - Annotation is deprecated; default behavior now matches its old purpose
3. **Add `@IgnoreNullFromDB`** to fields that need protection from null contamination
4. **Test thoroughly** - Behavior change affects all entities

### ⚠️ @UseIfNull Deprecated

The `@UseIfNull` annotation is **deprecated** and replaced with `@IgnoreNullFromDB` for clearer semantics.

**Why the change?**
- The old annotation name was confusing (inverse logic)
- New name clearly expresses intent: "ignore null values from the database"
- Aligns with standard ORM conventions where nulls are accepted by default

**Migration:**
```java
// OLD - Confusing inverse logic
private Integer field1;  // Protected from nulls
@UseIfNull
private Integer field2;  // Accepts nulls

// NEW - Clear, intuitive semantics
private Integer field1;  // Accepts nulls (standard ORM)
@IgnoreNullFromDB
private Integer field2;  // Protected from nulls
```

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

**🎯 Major Enhancement: New @IgnoreNullFromDB Annotation** (`ObjectMapperImpl.java`)

The new `@IgnoreNullFromDB` annotation provides **bidirectional protection** from null contamination during both serialization and deserialization.

**Key Improvements:**
- Default behavior now matches standard ORMs (Hibernate, JPA)
- Clear separation: "field missing" vs "field with null value"
- Special handling for `@Id` fields (never stored when null)
- Comprehensive documentation with behavior matrix

**Detailed Behavior:**

**Without @IgnoreNullFromDB (Default - Standard ORM Behavior):**

*Serialization (Writing to DB):*
- Null values stored as explicit null in database

*Deserialization (Reading from DB):*
- Field missing from DB → default value preserved
- Field present as null in DB → null accepted, field set to null

**With @IgnoreNullFromDB (Protected from Null Contamination):**

*Serialization (Writing to DB):*
- Null values NOT stored (field omitted from document)

*Deserialization (Reading from DB):*
- Field missing from DB → default value preserved
- **Field present as null in DB → null ignored, default value preserved** ✨ NEW

**Special @Id Field Handling:**
- Fields annotated with `@Id` are NEVER stored when null
- Allows MongoDB to auto-generate unique `_id` values
- Prevents E11000 duplicate key errors

**Impact:**
- Standard behavior aligns with other ORMs
- Optional protection from null contamination
- Better data integrity for migrations and external edits

**Example:**
```java
@Entity
public class MyEntity {
    @Id
    private MorphiumId id;  // Never stored when null (auto-generated)

    private Integer counter = 42;  // Accepts nulls (standard ORM)

    @IgnoreNullFromDB
    private Integer protectedCounter = 99;  // Protected from nulls
}

// MongoDB document: { counter: null, protectedCounter: null }
// After deserialization:
// counter = null              (accepted null - standard)
// protectedCounter = 99       (protected from null!)
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

**Enhanced Null Handling Documentation**
- Added comprehensive JavaDoc for `@IgnoreNullFromDB` with behavior matrix
- Included detailed examples for common use cases
- Documented distinction between "field missing" vs "field with null value"
- Added migration guide from `@UseIfNull` to `@IgnoreNullFromDB`
- Documented special `@Id` field handling

**General Documentation Fixes**
- Updated CHANGELOG.md with breaking changes
- Updated detailed release notes
- Fixed various documentation inconsistencies
- Updated code comments for clarity

### Test Coverage

**Updated Test Suites:**

1. **UseIfNullTest** - Updated for new @IgnoreNullFromDB behavior
   - Verifies serialization and deserialization correctness
   - Tests default value preservation
   - Validates null protection with @IgnoreNullFromDB
   - Tests standard null acceptance without annotation

2. **UseIfNullDistinctionTest** - Bidirectional behavior verification
   - Tests distinction between field missing vs. field with null value
   - Verifies protection from null contamination
   - Validates bidirectional annotation behavior
   - Tests mixed scenarios and edge cases
   - Includes comprehensive behavior matrix documentation

3. **ObjectMapperTest & ObjectMapperImplTest** - Updated for new null behavior
   - Updated 6 test assertions to expect null fields in serialized output
   - Verifies null values are stored as explicit nulls by default
   - Tests pass with both inmem and pooled drivers

4. **DefaultValuesTest** - Updated to remove deprecated @UseIfNull
   - Tests default value behavior without annotation
   - Verifies default behavior accepts nulls

---

## Technical Details

### Modified Files

**Core - Annotations:**
- `src/main/java/de/caluga/morphium/annotations/IgnoreNullFromDB.java` - NEW annotation with comprehensive documentation
- `src/main/java/de/caluga/morphium/annotations/UseIfNull.java` - Deprecated with migration guide

**Core - Implementation:**
- `src/main/java/de/caluga/morphium/ObjectMapperImpl.java` - Updated null handling logic:
  - Line 559-568: Serialization - omit nulls only for @IgnoreNullFromDB fields
  - Line 560-563: Special @Id field handling - never store null IDs
  - Line 1001-1013: Deserialization - accept nulls unless @IgnoreNullFromDB
  - Uses `objectMap.containsKey()` to distinguish "missing" vs "null"

**Driver:**
- `src/main/java/de/caluga/morphium/driver/wire/SingleMongoConnection.java` - Timeout handling
- `src/main/java/de/caluga/morphium/driver/inmem/InMemoryDriver.java` - Bulk operations return counts
- `src/main/java/de/caluga/morphium/driver/wire/SingleMongoConnectDriver.java` - Bulk operations return counts
- `src/main/java/de/caluga/morphium/driver/wire/PooledDriver.java` - Bulk operations return counts

**Messaging:**
- `src/main/java/de/caluga/morphium/messaging/Msg.java` - Removed @UseIfNull (now default behavior)
- `src/main/java/de/caluga/morphium/messaging/MessageRejectedException.java` - Error handling

**Test Data Classes:**
- `src/test/java/de/caluga/test/mongo/suite/data/ComplexObject.java` - Removed @UseIfNull
- `src/test/java/de/caluga/test/mongo/suite/data/UncachedObject.java` - Added @IgnoreNullFromDB to boolData

**Tests - Updated for new behavior:**
- `src/test/java/de/caluga/test/mongo/suite/base/UseIfNullTest.java` - Updated to test @IgnoreNullFromDB
- `src/test/java/de/caluga/test/mongo/suite/base/UseIfNullDistinctionTest.java` - Updated field names and behavior
- `src/test/java/de/caluga/test/mongo/suite/base/DefaultValuesTest.java` - Removed @UseIfNull
- `src/test/java/de/caluga/test/mongo/suite/base/ObjectMapperTest.java` - Updated 3 test assertions
- `src/test/java/de/caluga/test/mongo/suite/base/ObjectMapperImplTest.java` - Updated 3 test assertions
- `src/test/java/de/caluga/test/mongo/suite/base/BulkOperationTest.java` - Added bulkTestReturnCounts() test

**Documentation:**
- `CHANGELOG.md` - Updated with breaking changes and migration guide
- `docs/releases/CHANGELOG-6.0.1.md` - Comprehensive release notes

### Compatibility

**Minimum Requirements:** (unchanged from 6.0.0)
- Java 21+
- MongoDB 4.0+

**Dependencies:** No changes to external dependencies

---

## Migration Guide

### Upgrading from 6.0.0 to 6.0.1

⚠️ **CRITICAL**: This release contains a **breaking change** in null handling behavior that affects ALL entities.

#### Step 1: Understand the Behavior Change

**Old Behavior (6.0.0):**
- Null values NOT stored by default (fields omitted)
- `@UseIfNull` required to accept/store nulls

**New Behavior (6.0.1):**
- **Null values ARE stored by default** (standard ORM behavior)
- `@IgnoreNullFromDB` required to protect from nulls

#### Step 2: Review All Entity Classes

```bash
# Find all entity classes
grep -r "@Entity" src/ --include="*.java"

# Review each entity for null handling requirements
```

For each entity field, decide:
- **Accept nulls** (standard): No annotation needed (NEW default)
- **Protect from nulls**: Add `@IgnoreNullFromDB`

#### Step 3: Update Annotations

```java
// SCENARIO 1: Field currently has no annotation
// OLD behavior: Protected from nulls
// NEW behavior: Accepts nulls
private Integer counter = 42;

// ACTION: If you want OLD behavior, add @IgnoreNullFromDB:
@IgnoreNullFromDB
private Integer counter = 42;

// ACTION: If NEW behavior is OK, no change needed


// SCENARIO 2: Field currently has @UseIfNull
@UseIfNull
private Integer nullableField;

// ACTION: Remove @UseIfNull (deprecated, now default behavior)
private Integer nullableField;


// SCENARIO 3: Field has default value you want to preserve
private String status = "PENDING";

// ACTION: Add @IgnoreNullFromDB to prevent null contamination:
@IgnoreNullFromDB
private String status = "PENDING";
```

#### Step 4: Update Imports

```bash
# Add import for new annotation where needed
import de.caluga.morphium.annotations.IgnoreNullFromDB;

# Remove @UseIfNull usages
find src/ -type f -name "*.java" -exec sed -i '' '/@UseIfNull/d' {} +
```

#### Step 5: Test Thoroughly

```bash
# Run full test suite
mvn clean test

# Pay special attention to:
# - Tests that expect null values to be stored/retrieved
# - Tests that expect default values to be preserved
# - Tests involving data migrations or external DB modifications
```

#### Step 6: Update Database (If Needed)

If you have existing documents with fields that should NOT be null:

```javascript
// MongoDB shell - Example: Remove null values
db.myCollection.updateMany(
  { fieldName: null },
  { $unset: { fieldName: "" } }
);

// Or set to default value
db.myCollection.updateMany(
  { fieldName: null },
  { $set: { fieldName: 42 } }
);
```

### Common Migration Patterns

**Pattern 1: Fields with meaningful defaults**
```java
// Add @IgnoreNullFromDB to preserve defaults
@IgnoreNullFromDB
private Integer retryCount = 0;

@IgnoreNullFromDB
private String status = "PENDING";

@IgnoreNullFromDB
private Boolean enabled = true;
```

**Pattern 2: Optional fields that can be null**
```java
// No annotation needed (now default behavior)
private String optionalDescription;
private Integer optionalValue;
private Date optionalTimestamp;
```

**Pattern 3: Fields used in sparse indexes**
```java
// Add @IgnoreNullFromDB to keep fields out of DB when null
@Index
@IgnoreNullFromDB
private String uniqueCode;  // Only indexed when present
```

### Validation Checklist

- [ ] All entity classes reviewed
- [ ] `@UseIfNull` annotations removed
- [ ] `@IgnoreNullFromDB` added where needed
- [ ] Imports updated
- [ ] Tests updated and passing
- [ ] Database migration plan created (if needed)
- [ ] Staging environment tested
- [ ] Documentation updated

### No Migration Needed If:

- Your application doesn't use Morphium entities (unlikely)
- You're starting a new project (use new behavior from start)

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
