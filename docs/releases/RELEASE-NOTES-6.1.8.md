# Morphium 6.1.8 - Release Summary

## Quick Summary

Stability and reliability release with a breaking change to `MorphiumDriverException`, critical connection pool fixes, and improved messaging resilience.

## Breaking Change

### MorphiumDriverException is now unchecked (`extends RuntimeException`)

`MorphiumDriverException` now extends `RuntimeException` instead of `Exception`, aligning with every major Java database framework (MongoDB Driver, JPA/Hibernate, Spring Data, jOOQ).

**Action required** if your code catches a `RuntimeException` and inspects the cause:

```java
// BROKEN after this release -- instanceof check now always returns false
catch (RuntimeException e) {
    if (e.getCause() instanceof MorphiumDriverException) { ... }
}

// Correct -- catch it directly
catch (MorphiumDriverException e) {
    handleDbError(e);
}
```

This is a **silent behavioral change** -- no compile error, the `instanceof` check simply returns `false`.

## Key Fixes

### Connection Pool

- **Counter drift / pool exhaustion**: Fixed incorrect borrowed counter decrement under topology changes
- **Hostname case mismatch**: Fixed pool exhaustion when MongoDB reports hostnames with different casing than the seed list (all hostname operations now normalize to lowercase)
- **Heartbeat connection leak**: Connections are now properly closed in `finally` blocks
- **Parallel connection creation**: Changed from sequential to parallel (up to 10 virtual threads) for burst scenarios

### Messaging

- **Lock TTL bug**: Locks no longer expire immediately when messages have `timingOut=false` (uses 7-day fallback TTL)
- **ChangeStreamMonitor stability**: Auto-recovery on "connection closed" instead of permanent stop; resume token tracking prevents duplicate events
- **ChangeStreamHistoryLost**: Graceful recovery by discarding stale resume tokens

### MorphiumServer

- **Write concern with partial replica sets**: Fast-fail (100ms) when no secondaries are registered instead of blocking for full `wtimeout`
- **Replication**: Extended to handle `drop`, `dropDatabase`, `replace`, and `rename` operations
- **killCursors handler**: Prevents virtual thread accumulation from leaked watch cursors

## Requirements

- Java 21+
- MongoDB 5.0+ (for production deployments)

---

For detailed changelog, see the [6.1.8 section in CHANGELOG.md](../../CHANGELOG.md).
