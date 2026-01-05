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

---

## MorphiumServer Automatic Election and Failover

**Current limitation:** MorphiumServer replicaset uses static primary assignment at startup. When the primary goes down, secondaries cannot take over - all writes fail until the original primary is restarted. This makes MorphiumServer unsuitable for production high-availability deployments.

**Goal:** Enable automatic leader election so that when the primary fails, a secondary can be promoted to primary automatically, enabling rolling updates and uninterrupted service.

---

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Election Protocol                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐                  │
│  │ FOLLOWER │───▶│CANDIDATE │───▶│  LEADER  │                  │
│  └──────────┘    └──────────┘    └──────────┘                  │
│       ▲               │               │                         │
│       │               │               │                         │
│       └───────────────┴───────────────┘                         │
│                   (election timeout / higher term)               │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    Inter-Node Communication                      │
│                                                                  │
│   Node A (Leader)  ◄────heartbeat────►  Node B (Follower)       │
│         │          ◄────heartbeat────►  Node C (Follower)       │
│         │                                                        │
│         └──────── replication events ──────────▶                │
│                                                                  │
│   Vote requests flow: Candidate → All nodes → Vote responses    │
└─────────────────────────────────────────────────────────────────┘
```

---

### Phase 1: Election Protocol Foundation

#### 1.1 Create ElectionState enum and NodeRole management

**File:** `src/main/java/de/caluga/morphium/server/election/ElectionState.java`

```java
public enum ElectionState {
    FOLLOWER,    // Following a leader, cannot accept writes
    CANDIDATE,   // Requesting votes, no leader yet
    LEADER       // Accepted as leader, can accept writes
}
```

**Tasks:**
- [ ] Create `ElectionState` enum with FOLLOWER, CANDIDATE, LEADER states
- [ ] Add `currentTerm` (long) - monotonically increasing election term number
- [ ] Add `votedFor` (String) - who this node voted for in current term (null if none)
- [ ] Add `currentLeader` (String) - address of current known leader
- [ ] Add state transition methods with proper synchronization
- [ ] Persist term/votedFor to disk to survive restarts (optional for first version)

#### 1.2 Create ElectionManager class

**File:** `src/main/java/de/caluga/morphium/server/election/ElectionManager.java`

**Core responsibilities:**
- Manage election state machine
- Handle election timeouts
- Process vote requests and responses
- Coordinate with ReplicationCoordinator

**Tasks:**
- [ ] Create `ElectionManager` class with constructor taking MorphiumServer reference
- [ ] Add configuration: `electionTimeoutMin` (default 150ms), `electionTimeoutMax` (default 300ms)
- [ ] Add configuration: `heartbeatInterval` (default 50ms) - leader sends heartbeats at this rate
- [ ] Add `electionTimer` - randomized timeout that triggers election if no heartbeat received
- [ ] Implement `resetElectionTimer()` - called when heartbeat received from leader
- [ ] Implement `startElection()` - transition to CANDIDATE and request votes
- [ ] Implement `becomeLeader()` - transition to LEADER when majority votes received
- [ ] Implement `becomeFollower(term, leaderId)` - transition to FOLLOWER
- [ ] Add scheduled executor for election timeout and heartbeat threads

#### 1.3 Implement vote request/response protocol

**Wire Protocol Messages:**

```java
// Vote Request (Candidate → All Nodes)
{
    "requestVote": 1,
    "term": <long>,           // Candidate's term
    "candidateId": <string>,  // Candidate's address (host:port)
    "lastLogIndex": <long>,   // Index of candidate's last log entry
    "lastLogTerm": <long>     // Term of candidate's last log entry
}

// Vote Response (Node → Candidate)
{
    "voteGranted": <boolean>,
    "term": <long>            // Responder's current term
}
```

**Tasks:**
- [ ] Create `VoteRequest` class with term, candidateId, lastLogIndex, lastLogTerm
- [ ] Create `VoteResponse` class with voteGranted, term
- [ ] Add `handleVoteRequest(VoteRequest)` method to ElectionManager
  - Grant vote if: term >= currentTerm AND (votedFor is null OR votedFor == candidateId) AND candidate's log is at least as up-to-date
  - Update currentTerm if request term is higher
- [ ] Add `handleVoteResponse(VoteResponse)` method to ElectionManager
  - Count votes, become leader if majority received
  - Step down to follower if response term > currentTerm
- [ ] Register vote request handler in MongoCommandHandler

---

### Phase 2: Heartbeat and Failure Detection

#### 2.1 Leader heartbeat mechanism

**Wire Protocol Messages:**

```java
// Heartbeat (Leader → Followers) - Also serves as AppendEntries RPC
{
    "appendEntries": 1,
    "term": <long>,           // Leader's term
    "leaderId": <string>,     // Leader's address
    "prevLogIndex": <long>,   // Index of log entry immediately preceding new ones
    "prevLogTerm": <long>,    // Term of prevLogIndex entry
    "entries": [],            // Log entries to store (empty for heartbeat)
    "leaderCommit": <long>    // Leader's commit index (last replicated sequence)
}

// Heartbeat Response (Follower → Leader)
{
    "success": <boolean>,
    "term": <long>,
    "matchIndex": <long>      // Follower's last replicated index
}
```

**Tasks:**
- [ ] Implement `sendHeartbeat()` in ElectionManager - leader sends to all followers
- [ ] Schedule heartbeat at `heartbeatInterval` (50ms default) when in LEADER state
- [ ] Implement `handleAppendEntries(AppendEntriesRequest)` in ElectionManager
  - Reset election timer on valid heartbeat
  - Update currentLeader
  - Step down if term > currentTerm
  - Return success=false if term < currentTerm
- [ ] Track follower responses to detect slow/dead followers
- [ ] Register appendEntries handler in MongoCommandHandler

#### 2.2 Election timeout and leader detection

**Tasks:**
- [ ] Implement randomized election timeout (150-300ms default range)
- [ ] On timeout without heartbeat: start election (transition to CANDIDATE)
- [ ] Add `isLeaderAlive()` method - returns true if heartbeat received within timeout
- [ ] Add leader lease mechanism - leader considers itself leader only if majority responded recently
- [ ] Handle network partitions - leader steps down if can't reach majority

---

### Phase 3: State Transitions and Write Routing

#### 3.1 Dynamic primary flag management

**File:** `src/main/java/de/caluga/morphium/server/MorphiumServer.java`

**Current code (static):**
```java
private boolean primary = true;  // Set once at startup
```

**New code (dynamic):**
```java
private volatile boolean primary = false;  // Changed by ElectionManager
private ElectionManager electionManager;

public void setPrimary(boolean isPrimary) {
    boolean wasPrimary = this.primary;
    this.primary = isPrimary;

    if (isPrimary && !wasPrimary) {
        onBecomeLeader();
    } else if (!isPrimary && wasPrimary) {
        onBecomeFollower();
    }
}
```

**Tasks:**
- [ ] Make `primary` field volatile for thread-safe reads
- [ ] Add `setPrimary(boolean)` method callable by ElectionManager
- [ ] Add `onBecomeLeader()` callback:
  - Initialize ReplicationCoordinator
  - Start accepting writes
  - Begin sending heartbeats
  - Log leader transition
- [ ] Add `onBecomeFollower()` callback:
  - Stop ReplicationCoordinator
  - Reject writes with "not master" error
  - Start ReplicationManager to sync from new leader
  - Log follower transition
- [ ] Update `configureReplicaSet()` to initialize ElectionManager instead of static primary assignment

#### 3.2 Write rejection on non-primary

**File:** `src/main/java/de/caluga/morphium/server/netty/MongoCommandHandler.java`

**Tasks:**
- [ ] Add write check in `handleInsert()`, `handleUpdate()`, `handleDelete()`
- [ ] If not primary, return error: `{"ok": 0, "errmsg": "not master", "code": 10107}`
- [ ] Include `primaryHost` in error response so client can redirect
- [ ] Allow reads on secondaries (when readPreference allows)

#### 3.3 Primary discovery for clients

**Tasks:**
- [ ] Update `hello`/`isMaster` response with dynamic primary info:
  ```java
  res.setWritablePrimary(isPrimary());
  res.setPrimary(electionManager.getCurrentLeader());
  res.setSecondary(!isPrimary());
  ```
- [ ] Ensure clients reconnect to new primary automatically (PooledDriver already handles this via heartbeat)

---

### Phase 4: Data Consistency During Failover

#### 4.1 Replication sequence tracking

**Tasks:**
- [ ] Add `lastLogIndex` and `lastLogTerm` to each write operation
- [ ] Store log index in change stream events
- [ ] Only promote candidate if its log is at least as up-to-date as voter's log
- [ ] Implement log comparison: `(lastLogTerm, lastLogIndex)` comparison

#### 4.2 Catch-up replication for new leader

**Tasks:**
- [ ] When becoming leader, wait for majority of followers to acknowledge current sequence
- [ ] New leader sends missing entries to lagging followers
- [ ] Follower requests missing entries on startup or after partition heal

#### 4.3 Handling in-flight writes during failover

**Tasks:**
- [ ] Buffer pending writes during election (short timeout)
- [ ] Retry writes on new leader once elected
- [ ] Return error to client if election takes too long
- [ ] Document behavior: writes may be lost if not acknowledged by majority before failover

---

### Phase 5: Graceful Stepdown for Rolling Updates

#### 5.1 Implement replSetStepDown command

**File:** `src/main/java/de/caluga/morphium/server/netty/MongoCommandHandler.java`

**Tasks:**
- [ ] Handle `replSetStepDown` command:
  ```java
  {
      "replSetStepDown": <seconds>,
      "secondaryCatchUpPeriodSecs": <seconds>,  // Optional
      "force": <boolean>                        // Optional
  }
  ```
- [ ] On stepdown:
  1. Stop accepting new writes
  2. Wait for secondaries to catch up (up to secondaryCatchUpPeriodSecs)
  3. Transition to FOLLOWER state
  4. Refuse to become primary for `stepDownSecs` seconds
  5. Trigger new election
- [ ] Return success response once stepped down

#### 5.2 Pre-election notification

**Tasks:**
- [ ] Add `replSetFreeze` command - prevent node from seeking election for N seconds
- [ ] Add `replSetMaintenance` command - put node in maintenance mode (not eligible for election)
- [ ] Allow admin to prepare for maintenance window

#### 5.3 Rolling update procedure

**Documented procedure for zero-downtime updates:**

1. **Update secondary nodes first:**
   ```bash
   # For each secondary:
   morphium-cli --host secondary1 --eval "db.adminCommand({replSetMaintenance: true})"
   # Stop, update, restart secondary
   morphium-cli --host secondary1 --eval "db.adminCommand({replSetMaintenance: false})"
   # Wait for secondary to sync
   ```

2. **Step down and update primary:**
   ```bash
   # Graceful stepdown (waits for secondaries to catch up)
   morphium-cli --host primary --eval "db.adminCommand({replSetStepDown: 60, secondaryCatchUpPeriodSecs: 30})"
   # Wait for new primary election
   # Stop, update, restart old primary (now secondary)
   ```

3. **Optional: Re-elect original primary:**
   ```bash
   # If preferred primary, step down current leader
   morphium-cli --host newPrimary --eval "db.adminCommand({replSetStepDown: 10})"
   ```

---

### Phase 6: Split-Brain Prevention

#### 6.1 Quorum requirements

**Tasks:**
- [ ] Require majority votes to become leader: `votesReceived > (clusterSize / 2)`
- [ ] Leader must maintain contact with majority to remain leader
- [ ] If leader can't reach majority for `leaderLeaseTimeout`, step down
- [ ] Add configuration: `leaderLeaseTimeout` (default 10 seconds)

#### 6.2 Term-based consistency

**Tasks:**
- [ ] Every message includes sender's term
- [ ] Node receiving higher term immediately becomes follower
- [ ] Reject messages from lower terms
- [ ] Increment term on each election attempt

#### 6.3 Network partition handling

**Scenarios and behavior:**

| Scenario | Behavior |
|----------|----------|
| Leader isolated from minority | Leader steps down, minority can't elect (no quorum) |
| Leader isolated from majority | Leader steps down, majority elects new leader |
| Even split (2-2 in 4-node cluster) | No election possible, all nodes become followers |
| Partition heals | Lower-term leader steps down, cluster converges |

**Tasks:**
- [ ] Implement leader lease checking (periodic majority confirmation)
- [ ] Handle partition healing - detect and resolve split brain
- [ ] Add monitoring/alerting for partition scenarios

---

### Phase 7: Observability and Monitoring

#### 7.1 Election status commands

**Tasks:**
- [ ] Add `replSetGetStatus` command response with election info:
  ```json
  {
      "set": "rs0",
      "myState": 1,  // 1=PRIMARY, 2=SECONDARY
      "term": 42,
      "electionCandidateMetrics": {
          "lastElectionReason": "timeout",
          "lastElectionDate": "2024-01-15T10:30:00Z"
      },
      "members": [
          {"_id": 0, "name": "host1:27017", "state": 1, "stateStr": "PRIMARY"},
          {"_id": 1, "name": "host2:27017", "state": 2, "stateStr": "SECONDARY"},
          {"_id": 2, "name": "host3:27017", "state": 2, "stateStr": "SECONDARY"}
      ]
  }
  ```
- [ ] Add election event logging (term changes, state transitions, vote results)
- [ ] Add metrics: elections_total, election_duration_ms, time_since_last_heartbeat

#### 7.2 Health checks

**Tasks:**
- [ ] Add `/health` endpoint (or command) returning:
  - isLeader
  - currentTerm
  - lastHeartbeat
  - replicationLag
  - electionState
- [ ] Integration with container orchestration (Kubernetes readiness/liveness probes)

---

### Implementation Order and Dependencies

```
Phase 1 ──────────────────────────────────────────────────────────▶
   │  1.1 ElectionState enum
   │  1.2 ElectionManager class
   │  1.3 Vote request/response protocol
   │
   ▼
Phase 2 ──────────────────────────────────────────────────────────▶
   │  2.1 Leader heartbeat mechanism (depends on 1.2)
   │  2.2 Election timeout (depends on 1.2)
   │
   ▼
Phase 3 ──────────────────────────────────────────────────────────▶
   │  3.1 Dynamic primary flag (depends on 1.2, 2.1)
   │  3.2 Write rejection (depends on 3.1)
   │  3.3 Primary discovery (depends on 3.1)
   │
   ▼
Phase 4 ──────────────────────────────────────────────────────────▶
   │  4.1 Replication sequence tracking (depends on existing ReplicationCoordinator)
   │  4.2 Catch-up replication (depends on 4.1)
   │  4.3 In-flight write handling (depends on 4.2)
   │
   ▼
Phase 5 ──────────────────────────────────────────────────────────▶
   │  5.1 replSetStepDown command (depends on 3.1)
   │  5.2 Pre-election notification (depends on 1.2)
   │  5.3 Rolling update docs (depends on 5.1, 5.2)
   │
   ▼
Phase 6 ──────────────────────────────────────────────────────────▶
   │  6.1 Quorum requirements (depends on 1.3)
   │  6.2 Term-based consistency (integrated with all phases)
   │  6.3 Network partition handling (depends on 6.1, 6.2)
   │
   ▼
Phase 7 ──────────────────────────────────────────────────────────▶
      7.1 Status commands (depends on 1.2)
      7.2 Health checks (depends on 1.2)
```

---

### Files to Create

| File | Purpose |
|------|---------|
| `src/main/java/de/caluga/morphium/server/election/ElectionState.java` | Enum for FOLLOWER, CANDIDATE, LEADER |
| `src/main/java/de/caluga/morphium/server/election/ElectionManager.java` | Core election logic and state machine |
| `src/main/java/de/caluga/morphium/server/election/VoteRequest.java` | Vote request message |
| `src/main/java/de/caluga/morphium/server/election/VoteResponse.java` | Vote response message |
| `src/main/java/de/caluga/morphium/server/election/AppendEntriesRequest.java` | Heartbeat/replication message |
| `src/main/java/de/caluga/morphium/server/election/AppendEntriesResponse.java` | Heartbeat response |
| `src/main/java/de/caluga/morphium/server/election/ElectionConfig.java` | Configuration for timeouts, intervals |

### Files to Modify

| File | Changes |
|------|---------|
| `MorphiumServer.java` | Add ElectionManager, dynamic primary, state callbacks |
| `MongoCommandHandler.java` | Handle election commands, write rejection, primary discovery |
| `ReplicationManager.java` | Support dynamic leader changes, catch-up replication |
| `ReplicationCoordinator.java` | Integration with election term tracking |

---

### Configuration Options

```java
public class ElectionConfig {
    // Election timeout range (randomized to prevent split votes)
    private int electionTimeoutMinMs = 150;
    private int electionTimeoutMaxMs = 300;

    // Leader sends heartbeats at this interval
    private int heartbeatIntervalMs = 50;

    // Leader steps down if can't reach majority for this long
    private int leaderLeaseTimeoutMs = 10000;

    // Minimum time to wait for secondaries to catch up on stepdown
    private int stepdownCatchupTimeoutMs = 30000;

    // Priority for this node (higher = more likely to become leader)
    private int electionPriority = 1;

    // If true, this node can never become leader (arbiter-like)
    private boolean canBecomeLeader = true;
}
```

---

### Testing Strategy

#### Unit Tests
- [ ] ElectionState transitions
- [ ] Vote granting logic (term comparison, log comparison)
- [ ] Term increment on election
- [ ] Heartbeat timeout detection

#### Integration Tests
- [ ] 3-node cluster: kill primary, verify new election
- [ ] 3-node cluster: network partition minority, verify no election
- [ ] 3-node cluster: graceful stepdown, verify controlled failover
- [ ] 5-node cluster: kill 2 nodes, verify cluster remains available
- [ ] Rolling update simulation: sequential node restarts

#### Chaos Tests
- [ ] Random node kills during write load
- [ ] Network partition simulation (iptables/tc)
- [ ] Clock skew simulation
- [ ] Slow network (high latency) simulation

---

### Estimated Effort

| Phase | Effort | Notes |
|-------|--------|-------|
| Phase 1: Election Protocol | 3-4 days | Core state machine and vote protocol |
| Phase 2: Heartbeat/Failure Detection | 2-3 days | Timer management, network handling |
| Phase 3: State Transitions | 2-3 days | Integration with existing code |
| Phase 4: Data Consistency | 3-4 days | Most complex - ensure no data loss |
| Phase 5: Graceful Stepdown | 1-2 days | Building on earlier phases |
| Phase 6: Split-Brain Prevention | 2-3 days | Edge cases and partition handling |
| Phase 7: Observability | 1-2 days | Commands and monitoring |

**Total: ~15-20 days of focused development**

---

### Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Split-brain causing data divergence | Strict quorum requirements, term-based fencing |
| Election storms (repeated failed elections) | Randomized timeouts, priority-based tie-breaking |
| Data loss during failover | Require majority acknowledgment before commit |
| Performance impact of election protocol | Efficient heartbeat, minimal overhead in steady state |
| Complexity of distributed consensus | Start simple, iterate; extensive testing |

---

### References

- [Raft Consensus Algorithm](https://raft.github.io/) - Primary inspiration for election protocol
- [MongoDB Replica Set Elections](https://www.mongodb.com/docs/manual/core/replica-set-elections/) - Compatibility reference
- [Viewstamped Replication](http://pmg.csail.mit.edu/papers/vr-revisited.pdf) - Alternative consensus approach
