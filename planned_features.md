# Planned Features

## InMemoryDriver Index Refactoring

**Current limitation:** The index system uses hash-based buckets (`hashCode()` of field values), which only supports exact equality matches efficiently. Range queries (`$gt`, `$lt`, `$in`, etc.) cannot use the index and fall back to full collection scans.

**Proposed improvements:**

### 1. TreeMap-based indexes for range queries
- Use `TreeMap` instead of `HashMap` for index storage
- Enables efficient range scans via `subMap()`, `headMap()`, `tailMap()`
- Supports `$gt`, `$lt`, `$gte`, `$lte` operators

### 2. Multiple index types
- **Hash index**: Fast O(1) equality lookups (similar to current)
- **B-tree index**: Sorted structure for range queries and ordering
- **Text index**: For `$text` search queries (see Text Index Support below)

### 3. Compound index prefix matching
- Current implementation builds compound bucket IDs but can't do prefix matching
- Should support queries on leading fields of compound index (e.g., index on `{a, b, c}` should support queries on just `{a}` or `{a, b}`)

### 4. Index selection / query planning
- Analyze query shape to choose the best available index
- Consider selectivity and index coverage
- Currently just uses first matching index

### 5. Index-assisted sorting
- Use sorted indexes to avoid in-memory sort operations
- Return results in index order when sort matches index

**Files to modify:**
- `src/main/java/de/caluga/morphium/driver/inmem/InMemoryDriver.java`
  - `getDataFromIndex()` method
  - `indexDataByDBCollection` structure
  - Index maintenance in `insert()`, `update()`, `delete()`

**Estimated effort:** Medium-high - requires rethinking index data structures and query planning logic.

---

## MorphiumServer Architecture Improvements

**Current limitations:** The server uses blocking I/O with one thread per connection, which limits scalability under high connection counts.

**Proposed improvements:**

### 1. Non-blocking I/O (NIO/NIO2)
- Replace blocking `Socket` with `SocketChannel` and `Selector`
- Single thread can handle multiple connections
- Significantly reduces thread overhead for many concurrent connections

### 2. Netty-based networking (alternative to NIO)
- Use Netty framework for high-performance networking
- Built-in support for connection pooling, backpressure, SSL/TLS
- Well-tested, production-ready solution
- Easier to maintain than raw NIO

### 3. Async replication
- Current replication blocks during sync operations
- Use change streams with async event processing
- Non-blocking write propagation to secondaries

### 4. Connection pooling for replica communication
- Currently creates new socket per heartbeat check
- Maintain persistent connections between replica set members
- Reduces connection setup overhead

### 5. Pre-allocated serialization buffers
- Use thread-local or pooled byte buffers
- Avoid repeated `ByteArrayOutputStream` allocations
- Reduces GC pressure under high throughput

**Files to modify:**
- `src/main/java/de/caluga/morphium/server/MorphiumServer.java`
  - `incoming()` method - main connection handler
  - Heartbeat and replication logic
- `src/main/java/de/caluga/morphium/driver/wireprotocol/*.java`
  - `bytes()` and `getPayload()` methods for buffer pooling

**Estimated effort:** High - significant architectural change, especially for NIO/Netty migration.

---

## InMemoryDriver Cursor Synchronization

**Current limitation:** The InMemoryDriver uses a single global lock (`cursorsMutex`) for cursor operations. This causes contention when multiple concurrent operations access different cursors.

**Proposed improvements:**

### Per-cursor or striped locking
- Replace global `cursorsMutex` with per-cursor synchronization
- Alternative: Use striped locking based on cursor ID hash
- Significantly reduces contention for concurrent cursor operations

**Files to modify:**
- `src/main/java/de/caluga/morphium/driver/inmem/InMemoryDriver.java`
  - `cursorsMutex` field and all synchronized blocks using it
  - Consider using `ConcurrentHashMap.compute()` for atomic cursor operations

**Estimated effort:** Medium - requires careful analysis of cursor lifecycle and concurrent access patterns.

---

## MorphiumServer SSL/TLS Support

**Current limitation:** MorphiumServer only supports unencrypted connections, making it unsuitable for production environments where data in transit must be protected.

**Proposed improvements:**

### 1. SSL/TLS encrypted connections
- Support for `SSLServerSocket` / `SSLSocket` wrapper around existing sockets
- Configurable via server settings (enabled/disabled, port)
- Support for both self-signed and CA-signed certificates

### 2. Certificate configuration
- **Keystore support**: Load server certificate and private key from JKS/PKCS12 keystore
- **PEM file support**: Alternative loading from PEM-encoded certificate/key files
- **Truststore**: For client certificate validation (mutual TLS)

### 3. TLS version and cipher configuration
- Configurable minimum TLS version (TLS 1.2, TLS 1.3)
- Cipher suite selection for compliance requirements
- Option to disable weak ciphers

### 4. Client certificate authentication (mTLS)
- Optional mutual TLS for strong client authentication
- Extract client identity from certificate for authorization

**Configuration example:**
```java
MorphiumServerConfig config = new MorphiumServerConfig();
config.setSslEnabled(true);
config.setSslPort(27018);
config.setKeystorePath("/path/to/keystore.jks");
config.setKeystorePassword("password");
config.setTlsMinVersion("TLSv1.2");
```

**Files to modify:**
- `src/main/java/de/caluga/morphium/server/MorphiumServer.java`
  - Socket creation in `incoming()` method
  - New SSL context initialization
- `src/main/java/de/caluga/morphium/server/MorphiumServerConfig.java`
  - SSL configuration properties

**Estimated effort:** Medium - SSL/TLS APIs are well-documented, main work is configuration and testing.

---

## MorphiumServer Authentication

**Current limitation:** MorphiumServer accepts all connections without authentication, making it unsuitable for multi-tenant or security-sensitive deployments.

**Proposed improvements:**

### 1. SCRAM-SHA-256 authentication
- MongoDB-compatible authentication mechanism
- Salted challenge-response prevents password interception
- Compatible with standard MongoDB drivers

### 2. User management
- Store users in a system collection (`morphium_server.users`)
- Support for creating, updating, deleting users
- Password hashing with bcrypt or PBKDF2

### 3. Role-based access control (RBAC)
- Predefined roles: `read`, `readWrite`, `dbAdmin`, `userAdmin`, `root`
- Per-database role assignments
- Custom role definitions

### 4. Authentication commands
- `authenticate` - authenticate a connection
- `createUser` / `dropUser` - user management
- `grantRolesToUser` / `revokeRolesFromUser` - role management
- `usersInfo` - list users

### 5. Connection state
- Track authenticated user per connection
- Enforce authorization on each command
- Session timeout / re-authentication

**Configuration example:**
```java
MorphiumServerConfig config = new MorphiumServerConfig();
config.setAuthenticationEnabled(true);
config.setAuthMechanism("SCRAM-SHA-256");
config.setAdminUser("admin");
config.setAdminPassword("secure_password");
```

**Files to modify:**
- `src/main/java/de/caluga/morphium/server/MorphiumServer.java`
  - Command interception for auth check
  - Authentication command handlers
- `src/main/java/de/caluga/morphium/server/MorphiumServerConfig.java`
  - Authentication configuration
- New files:
  - `src/main/java/de/caluga/morphium/server/auth/AuthenticationManager.java`
  - `src/main/java/de/caluga/morphium/server/auth/ScramSha256Authenticator.java`
  - `src/main/java/de/caluga/morphium/server/auth/User.java`
  - `src/main/java/de/caluga/morphium/server/auth/Role.java`

**Estimated effort:** High - SCRAM-SHA-256 implementation is complex, RBAC requires careful design.

---

## InMemoryDriver Text Index Support

**Current state:** Text indexes can be created and are properly returned by `listIndexes` with correct metadata (`weights`, `textIndexVersion`). However, the actual `$text` query operator is not implemented - text search queries will not work.

**What works now:**
- ✅ Creating text indexes via `createIndex({field: "text"})`
- ✅ Listing text indexes with correct metadata (weights, textIndexVersion)
- ✅ Index comparison between entity annotations and MongoDB indexes
- ❌ `$text` query operator (queries will fail or return no results)
- ❌ `$meta: "textScore"` for relevance scoring

**Proposed implementation:**

### 1. Basic text search (`$text` operator)
```java
// Support for queries like:
db.collection.find({ $text: { $search: "coffee shop" } })
```

- Tokenize indexed text fields on whitespace and punctuation
- Build inverted index: word → set of document IDs
- Support multi-word searches (AND semantics by default)
- Case-insensitive matching

### 2. Text tokenization
- Split on whitespace and common punctuation
- Normalize to lowercase
- Optional: basic stop word removal (the, a, an, etc.)
- Optional: simple stemming (running → run)

### 3. Search operators
- **Phrase search**: `"coffee shop"` - exact phrase match
- **Negation**: `-word` - exclude documents containing word
- **OR search**: Multiple words without quotes

### 4. Text score (`$meta`)
```java
// Support for relevance scoring
db.collection.find(
    { $text: { $search: "coffee" } },
    { score: { $meta: "textScore" } }
).sort({ score: { $meta: "textScore" } })
```

- Calculate TF-IDF or simple term frequency
- Support sorting by text score

### 5. Weighted fields
```java
// Honor weights from index definition
db.collection.createIndex(
    { title: "text", content: "text" },
    { weights: { title: 10, content: 1 } }
)
```

**Implementation approach:**

1. **Inverted index structure:**
   ```java
   // Per collection: word → Set<ObjectId>
   Map<String, Map<String, Set<Object>>> textIndex;
   ```

2. **Index maintenance:**
   - On insert: tokenize text fields, add doc ID to inverted index
   - On update: remove old tokens, add new tokens
   - On delete: remove doc ID from all token sets

3. **Query execution:**
   - Parse `$text.$search` string into tokens
   - Look up each token in inverted index
   - Intersect result sets (AND) or union (OR)
   - Optionally calculate scores

**Files to modify:**
- `src/main/java/de/caluga/morphium/driver/inmem/InMemoryDriver.java`
  - `createIndex()` - build inverted index for text fields
  - `find()` / query execution - handle `$text` operator
  - `insert()`, `update()`, `delete()` - maintain inverted index

**Estimated effort:** Medium - basic text search is straightforward, advanced features (stemming, scoring) add complexity.
