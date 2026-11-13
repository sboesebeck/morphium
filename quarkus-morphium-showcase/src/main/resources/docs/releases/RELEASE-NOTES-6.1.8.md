# Morphium 6.1.8 - Release Summary

## Quick Summary

Stability release focused on connection pool reliability, test stabilization, and documentation improvements.

## Key Fixes

### Connection Pool

- **Counter drift / pool exhaustion**: Fixed incorrect borrowed counter decrement under topology changes
- **Heartbeat connection leak**: Connections are now properly closed in `finally` blocks when `getHelloResult()` or `connect()` throws during heartbeat
- **ReadPreference fall-through**: Documented intentional `NEAREST` -> `PRIMARY_PREFERRED` -> `SECONDARY` degradation path

### Test Infrastructure

- Split long-running `QueryTest` and `ObjectMapperTest` into focused classes for better maintainability
- Tuned timeouts for more resilient test execution under load
- Improved test scripts (rg stdin fallback, log file handling)

### Documentation

- Added SSL/TLS documentation and benchmark results
- Added v5 vs v6 performance comparison
- Fixed messaging comparison (v5 also had ChangeStream)

## Requirements

- Java 21+
- MongoDB 5.0+ (for production deployments)

---

For detailed changelog, see the [6.1.8 section in CHANGELOG.md](../../CHANGELOG.md).
