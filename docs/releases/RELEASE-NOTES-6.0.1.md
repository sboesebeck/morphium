# Morphium 6.0.1 - Release Summary

## Quick Summary

Bugfix release focusing on null value handling improvements, connection stability, and annotation naming consistency.

## ⚠️ Breaking Change

**@UseIfnull renamed to @UseIfNull**

All instances of `@UseIfnull` must be changed to `@UseIfNull`. This is a simple find-and-replace operation.

## Key Improvements

### 1. 🎯 Bidirectional @UseIfNull Behavior (Major Enhancement)
- **NEW**: Annotation now protects fields during BOTH write AND read operations
- Fields WITHOUT `@UseIfNull` are **protected from null contamination** in the database
- Prevents unwanted nulls from data migrations, manual edits, or external systems
- **Impact**: Improved data integrity and more predictable behavior

**Example:**
```java
private Integer counter = 42;  // Protected from nulls in DB!

@UseIfNull
private Integer nullable = 99;  // Accepts nulls from DB
```

### 2. Enhanced Connection Stability
- Socket timeout exceptions now trigger automatic retries instead of closing connections
- Better handling of long-running operations and change streams
- Improved error detection in watch operations

### 3. Bug Fixes
- Multi-collection messaging error handling
- Collection name caching for improved performance
- Various documentation improvements

## Migration Steps

1. Replace `@UseIfnull` with `@UseIfNull` in your code:
   ```bash
   find src/ -type f -name "*.java" -exec sed -i 's/@UseIfnull/@UseIfNull/g' {} +
   ```

2. Update imports if needed:
   ```java
   import de.caluga.morphium.annotations.UseIfNull;  // correct
   ```

3. Rebuild and test:
   ```bash
   mvn clean install
   ```

## Testing Recommendations

**Important**: The bidirectional behavior may affect existing code:

```java
@Entity
public class TestEntity {
    // Without @UseIfNull: NOW PROTECTED from null in DB
    private Integer counter = 42;  // Missing in DB: stays 42
                                   // null in DB: stays 42 (NEW!)

    // With @UseIfNull: accepts nulls
    @UseIfNull
    private Integer nullable = 42;  // Missing in DB: stays 42
                                    // null in DB: becomes null
}
```

**Action**: If you have fields that should accept null from the database, add `@UseIfNull`.

## Dependencies

- No external dependency changes
- Requires Java 21+
- Supports MongoDB 4.0+

## What's Next

Version 6.1.0 will focus on:
- Performance optimizations
- Enhanced aggregation support
- Improved transaction handling

---

For detailed information, see [CHANGELOG-6.0.1.md](CHANGELOG-6.0.1.md)
