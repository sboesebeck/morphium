# Morphium 6.1.0 - Release Summary

## Quick Summary

Major feature release introducing MorphiumServer with replica set support, SSL/TLS for the server, snapshot persistence, significant InMemoryDriver improvements, and a comprehensive test infrastructure overhaul.

## Highlights

### MorphiumServer: Standalone MongoDB-Compatible Server

MorphiumServer is now a true **drop-in replacement** for MongoDB in development and testing:

- **Replica set support** with automatic primary election and failover (Raft-inspired protocol)
- **SSL/TLS encrypted connections** via Netty
- **Snapshot persistence** (`--dump-dir`, `--dump-interval`) for data survival across restarts
- **Standalone CLI** (`morphium-server-cli.jar`) for running from the command line
- Any MongoDB client (Java, Python, Node.js, Go, mongosh, Compass) can connect

```bash
# Start with persistence and SSL
java -jar target/morphium-6.2.0-SNAPSHOT-server-cli.jar \
  --port 27017 --dump-dir ./data --dump-interval 300 \
  --ssl --sslKeystore server.jks --sslKeystorePassword changeit
```

### InMemoryDriver Enhancements

- **Shared databases**: Multiple Morphium instances can share the same InMemory database (`setShareInMemoryDatabase(true)`)
- **Full `$text` query support**: MongoDB-compatible text search with phrase matching, negation, and case sensitivity
- **Tailable cursor support**
- **Performance**: Removed global synchronization, optimized `find()` deep copy, improved index lookups, `$in` O(n+m) via HashSet

### Messaging Improvements

- **Topic Registry / Network Registry** for discovering messaging topics across the network
- **MessagingSettings** configuration class
- Various stability fixes for connection handling, message processing, and shutdown

### Test Infrastructure Overhaul

- **MultiDriverTestBase**: 72 test classes migrated, 356+ methods now parameterized across drivers
- **Driver selection**: `./runtests.sh --driver inmem|pooled|all`
- **MorphiumServer backend**: `./runtests.sh --morphium-server`

## Migration from 6.0.x

No breaking API changes. Key points:

1. MorphiumServer CLI JAR is a new build artifact (`morphium-X.Y.Z-server-cli.jar`)
2. Messaging settings moved to `MorphiumConfig.messagingSettings()`
3. InMemory shared databases are opt-in via `DriverSettings.setShareInMemoryDatabase()`

## Requirements

- Java 21+
- MongoDB 5.0+ (for production deployments)

## Test Results

| Backend | Tests Run | Passed | Errors | Skipped |
|---------|-----------|--------|--------|---------|
| InMemory Driver | 1046 | 929 | 0 | 105 |
| MongoDB (Replicaset) | 1046 | 933 | 0 | 105 |
| MorphiumServer (Replicaset) | 1024 | 1024 | 0 | 92 |

---

For detailed changelog, see the [6.1.0 section in CHANGELOG.md](../../CHANGELOG.md).
