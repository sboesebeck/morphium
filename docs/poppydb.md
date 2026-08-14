# PoppyDB: Standalone MongoDB-Compatible Server

<p align="center">
  <img class="logo-light" src="../assets/brand/poppydb-logo.svg" alt="PoppyDB" width="480"><img class="logo-dark" src="../assets/brand/poppydb-logo-dark.svg" alt="PoppyDB" width="480"></a>
</p>

PoppyDB is a standalone MongoDB wire protocol-compatible server built on the InMemoryDriver. Introduced in its mature form with **Morphium 6.1**, it allows any MongoDB client (Java, Python, Node.js, Go, etc.) to connect and interact with an in-memory database as a true **drop-in replacement** for MongoDB during development and testing.

**Important:** PoppyDB can be run as a standalone application from a dedicated executable JAR, or used programmatically as part of a Java application.

## Key Features

- ✅ **MongoDB Wire Protocol Compatible** - Works with any MongoDB client library
- ✅ **Multi-Language Support** - Connect from Java, Python, Node.js, Go, C#, etc.
- ✅ **3x Faster Than MongoDB** - Insert 0.74ms vs 4.48ms, Find 0.45ms vs 1.95ms, Update 0.66ms vs 5.19ms (local benchmarks)
- ✅ **Fast Startup** - Starts in ~100-500ms vs ~2-5 seconds for MongoDB
- ✅ **Lightweight** - ~50-100MB RAM vs ~500MB-1GB for MongoDB
- ✅ **No Installation** - Pure Java, runs anywhere
- ✅ **Perfect for CI/CD** - No Docker or MongoDB installation required
- ✅ **Integration Testing** - Test multi-language microservices together
- ✅ **Opt-in Authentication & TLS** - Real SCRAM-SHA-1/-256 auth (`--auth`) and SSL/TLS encrypted connections (`--ssl`)
- ✅ **Configuration File** - `--cfg`/`-f`, systemd/Docker-friendly, keeps secrets off the command line via `*-file` indirection (see [Configuration File](#configuration-file))

## Quick Start

### Running from Command Line

After building the project, you can run the server directly using the PoppyDB CLI JAR.

```bash
# Build the project first if you haven't
mvn clean package -pl poppydb -am -Dmaven.test.skip=true

# Run PoppyDB with default settings (port 17017)
java -jar poppydb/target/poppydb-<version>-cli.jar

# Run on a different port
java -jar poppydb/target/poppydb-<version>-cli.jar --port 27017
```

### Running Programmatically

```java
import de.caluga.poppydb.PoppyDB;

public class MyApp {
    public static void main(String[] args) throws Exception {
        // Start embedded MongoDB-compatible server
        PoppyDB server = new PoppyDB(27017, "0.0.0.0", 100, 10);
        server.start();

        System.out.println("PoppyDB running on port 27017");

        // Keep running
        while (true) {
            Thread.sleep(1000);
        }
    }
}
```

## Configuration

### Configuration File

Instead of (or in addition to) command-line arguments, PoppyDB can read its settings from a
`java.util.Properties`-format file (`key=value`, one per line). This is the preferred way to
configure a production instance under systemd, Docker, or config management — see the
[Production Deployment Playbook](./howtos/poppydb-deployment.md) for a full walkthrough.

**Precedence, for every single setting, no exceptions:** command line argument, if given, always
wins. Otherwise the value from the config file, if present, wins. Otherwise the built-in default
(shown in the [options table](#command-line-arguments) below) applies. `--no-ssl`/`--no-auth`
exist specifically so a config file's `ssl=true`/`auth=true` can still be switched back off from
the command line.

**Search order (first match wins - files are never merged):**

```
1. --cfg <path> / -f <path>                                (explicit; stops the search)
2. $POPPYDB_CONF                                            (env var; stops the search)
3. ${XDG_CONFIG_HOME:-~/.config}/poppydb/config              (user, directory form)
4. ${XDG_CONFIG_HOME:-~/.config}/poppydb.conf                (user, single file)
5. /etc/poppydb/config                                       (system, directory form)
6. /etc/poppydb.conf                                         (system, single file)
```

If neither `--cfg`/`-f` nor `$POPPYDB_CONF` is given, and none of the four default locations
exist, PoppyDB starts exactly as before (command-line arguments and defaults only) — this is not
an error. `--no-config` skips the four default locations entirely (an explicit `--cfg`/`-f` still
applies if given alongside it); `scripts/poppydb.sh` and `scripts/startPoppyDB.sh` always pass
`--no-config` for local test runs, so a stray `~/.config/poppydb/config` on a developer machine
can never silently change what a test run connects to.

There is deliberately **no `#include` and no `conf.d` directory merging** — a "directory form"
search path (e.g. `/etc/poppydb/config`) just means "look for a file literally named `config`
inside that directory"; whichever single file is found is the only one that is read.

**`java.util.Properties` syntax notes** (easy to get wrong):

- The file is always read as **UTF-8** (not the `Properties.load(InputStream)` default of
  ISO-8859-1).
- `#` or `!` start a comment, but **only at the beginning of a line** —
  `root-password=secret#1` is a perfectly valid password.
- `\` is the escape character, so a literal Windows path needs doubled backslashes:
  `ssl-keystore = C:\\keys\\server.jks`.
- Keys are matched **case-insensitively**, and `-`, `_`, `.` are all ignored during matching:
  `max-bson-size`, `maxBsonSize`, `MAX_BSON_SIZE` and `MAX.BSON.SIZE` are all the same key. An
  optional `poppydb.` prefix on any key is stripped before matching (`poppydb.port` ≡ `port`).
  Setting two spellings of the same key in one file (including the prefixed/unprefixed pair) is
  a hard error, not a silent pick.
- Boolean values accept `true|yes|on|1` / `false|no|off|0`, case-insensitive — anything else is
  rejected rather than silently treated as `false`.
- An unknown key (typo) aborts startup with a "did you mean" suggestion instead of being
  silently ignored.

**Keeping secrets out of the main file:** `root-password` and `ssl-keystore-password` each have
a `*-file` counterpart (`root-password-file`, `ssl-keystore-password-file`) that reads the
secret from a separate file instead (UTF-8, one trailing `\r`/`\n` stripped) — this is what lets
you commit/distribute the main config while keeping secrets in a separately-permissioned file
(or behind `systemd`'s `LoadCredential=`, see the
[deployment playbook](./howtos/poppydb-deployment.md)). Setting both the direct value and the
`*-file` indirection for the same secret is a hard error. Whenever a config file directly embeds
a secret, or a `*-file` value points at one, PoppyDB checks its POSIX permissions: readable by
group/others only warns (`chmod 600` recommended), writable by group/others refuses to start (a
world-writable config carrying secrets is a privilege escalation, not a style issue). On
non-POSIX filesystems (Windows) the check is skipped.

There is no live-reload — a config file change needs a restart to take effect.

Example file (copy this as a starting point):

```properties
#
# PoppyDB configuration
#
# Search order (first match wins, no merging across files):
#   1. --cfg <path> / -f <path>
#   2. $POPPYDB_CONF
#   3. ${XDG_CONFIG_HOME:-~/.config}/poppydb/config
#   4. ${XDG_CONFIG_HOME:-~/.config}/poppydb.conf
#   5. /etc/poppydb/config
#   6. /etc/poppydb.conf
#
# Command line arguments always override values from this file.
# Values not set here fall back to the built-in defaults shown in [brackets].
#
# Syntax notes (java.util.Properties):
#   - '#' or '!' start a comment, but only at the beginning of a line
#     ('root-password=secret#1' is a valid password)
#   - backslashes must be escaped: C:\\keys\\server.jks
#   - trailing whitespace is part of the value - watch out with passwords
#   - the file is read as UTF-8
#   - keys are matched case-insensitively, '-', '_' and '.' are ignored:
#     max-bson-size == maxBsonSize == MAX_BSON_SIZE
#   - an optional 'poppydb.' prefix on every key is stripped
#
# If this file contains a password, restrict its permissions:
#   chmod 600 /etc/poppydb/config
#

port = 17017
bind = 0.0.0.0
max-connections = 2000
socket-timeout = 300
compressor = none
log-level = INFO
memory-warn = 75
memory-reject = 90
max-bson-size = 16777216
#rs-name = rs0
#rs-seed = node1:17017,node2:17017,node3:17017
#rs-priorities = 100,50,50
#ssl = true
#ssl-keystore = /etc/poppydb/server.p12
#ssl-keystore-password-file = /run/credentials/poppydb.service/keystore.pw
#auth = true
#root-user = admin
#root-password-file = /etc/poppydb/secrets/root.pw
#users-file = /etc/poppydb/users.json
#dump-dir = /var/lib/poppydb/data
#dump-interval = 300
```

Load it explicitly, or drop it at one of the default search paths:

```bash
java -jar poppydb-cli.jar --cfg /etc/poppydb/config
```

### Inspecting and validating the configuration

`--print-config` prints the *effective* configuration - built-in defaults, the loaded config
file and the command line merged, with the usual precedence (command line > config file >
defaults) - and exits. Every key carries a comment naming where its value came from, secrets
are redacted (`# root-password=***`). The output is itself a valid configuration file, so it
doubles as a starting template:

    java -jar poppydb.jar --no-config --print-config > poppydb.conf.template

`--check-config` validates the effective configuration without starting the server and exits
with code 0 (OK) or 1 (errors) - like `nginx -t`. Beyond syntax and semantic cross-checks
(value ranges, `root-user`/`root-password` pairing, `rs-priorities` count matching `rs-seed`,
`memory-warn` <= `memory-reject`, ...), it performs deep checks: the SSL keystore is actually
loaded (catching wrong keystore passwords), secret files are read, the dump directory is
checked for usability, and — if `users-file` is set — the file is read, permission-checked and
fully parsed/validated exactly like at real startup (see
[Bootstrapping users](#bootstrapping-users-users-file)), so a broken users file is caught before
it can abort a real deployment. Warnings (e.g. `ssl` without a keystore) do not affect the exit code:

    java -jar poppydb.jar --cfg /etc/poppydb/config --check-config

Both flags combine with `--cfg`/`-f`, `--no-config` and any other option, but not with each other:
`--print-config --check-config` exits immediately with an error.

### Command Line Arguments
You can configure the PoppyDB using the following command-line arguments. The **Config Key**
column is the equivalent key for the [configuration file](#configuration-file) above (kebab-case,
case/separator-insensitive) — flags without one are CLI-only (there is nothing to put in a file).

| Argument | Config Key | Description | Default |
|---|---|---|---|
| `-p`, `--port <port>` | `port` | Port to listen on. | `17017` |
| `-b`, `--bind <host>` | `bind` | Host to bind to. | `localhost` |
| `--log-level <level>` | `log-level` | Log verbosity: `ERROR`, `WARN`, `INFO`, `DEBUG` or `TRACE`. See [Logging](#logging). | `INFO` |
| `--memory-warn <percent>` | `memory-warn` | Log a warning when heap occupancy crosses this percentage (100 = off). See [Memory Watermark](#memory-watermark). | `75` |
| `--memory-reject <percent>` | `memory-reject` | Reject document-creating writes above this heap percentage (100 = off). See [Memory Watermark](#memory-watermark). | `90` |
| `--max-bson-size <bytes>` | `max-bson-size` | BSON document size limit, enforced like mongod (0 = off). See [BSON Size Limit](#bson-size-limit). | `16777216` (16MB) |
| `-c`, `--compressor <type>` | `compressor` | Compressor to use for the wire protocol. Can be `none`, `snappy`, `zstd`, or `zlib`. | `none` |
| `--rs-name <name>` | `rs-name` | Name of the replica set. | |
| `--rs-seed <hosts>` | `rs-seed` | Comma-separated list of hosts to seed the replica set. The first host in the list will have the highest priority. | |
| `--rs-priorities <list>` | `rs-priorities` | Comma-separated list of election priorities (0-100) matching seed order. | all `50` |
| `--ssl`, `--tls` | `ssl` | Enable SSL/TLS encrypted connections. | disabled |
| `--no-ssl` | | Force SSL/TLS off, overriding a config file's `ssl=true`. | |
| `--sslKeystore <path>` | `ssl-keystore` | Path to JKS or PKCS12 keystore file containing server certificate. | |
| `--sslKeystorePassword <pw>` | `ssl-keystore-password` | Password for the keystore. `ssl-keystore-password-file` (config-file only) reads it from a separate file instead. | |
| `--auth` | `auth` | Require SCRAM authentication (SCRAM-SHA-1 / SCRAM-SHA-256). Unauthenticated connections may only run the handshake/SASL/ping commands. | disabled |
| `--no-auth` | | Force auth off, overriding a config file's `auth=true`. | |
| `--rootUser <name>` | `root-user` | Initial admin user, created at startup if absent. Required for a fresh `--auth` server — there is no localhost exception. | |
| `--rootPassword <pw>` | `root-password` | Password for the initial admin user. `root-password-file` (config-file only) reads it from a separate file instead. | |
| `--users-file <path>` | `users-file` | JSON file declaring users to provision at startup (idempotent upsert, primary-only apply, optional version gate). See [Bootstrapping users](#bootstrapping-users-users-file). | |
| `-d`, `--dump-dir <path>` | `dump-dir` | Directory for periodic database dumps. Enables persistence. | |
| `--dump-interval <seconds>` | `dump-interval` | Interval between periodic dumps. 0 = only dump on shutdown. | `0` |
| `--max-connections <num>` | `max-connections` | Maximum concurrent connections. | `500` |
| `--socket-timeout <seconds>` | `socket-timeout` | Idle connection timeout in seconds. | `300` |
| `--cfg <path>`, `-f <path>` | | Load settings from this [configuration file](#configuration-file). | |
| `--no-config` | | Skip the configuration file's default search paths (an explicit `--cfg`/`-f` still applies). | |
| `--print-config` | | Print the effective configuration as a reusable config file, then exit. See [Inspecting and validating the configuration](#inspecting-and-validating-the-configuration). | |
| `--check-config` | | Validate the effective configuration without starting the server, exit 0/1. See [Inspecting and validating the configuration](#inspecting-and-validating-the-configuration). | |
| `-h`, `--help` | | Print this help message and exit. | |

Example:
```bash
java -jar poppydb/target/poppydb-<version>-cli.jar -p 27018 -b 0.0.0.0 --rs-name my-rs --rs-seed host1:27017,host2:27018
```

### Replica Set Behavior (experimental)

PoppyDB now performs a lightweight initial sync whenever you start an additional member with the same `--rs-name` / `--rs-seed`:

- The first node that starts without detecting peers becomes primary immediately.
- Any later node that can reach an existing peer demotes itself to secondary, runs an initial sync from the detected primary (or highest-priority reachable host), and only participates in elections after the sync finishes.
- Elections and automatic failover continue to respect the configured host priorities, but a node will not promote itself until it completed the initial copy of data.

Practical tips:

1. Always include all hosts in `--rs-seed` so nodes can find a sync source.
2. Start at least one node, write the test data you need, then bring additional members online—they will clone the existing data automatically.
3. Keep in mind that this is still meant for testing: persistence and durability are unchanged.

**Known limitation - the vote's Raft log check is currently dead code.** Elections compare
candidates' replicated-log state (`lastLogIndex`/`lastLogTerm`) per the Raft paper, so a node
behind on data should lose a vote against a more caught-up peer. In this implementation nothing
ever calls `ElectionManager.updateLogIndex(...)` - `ReplicationManager` reports progress through
a separate hook that nothing wires to it - so that state stays `0` on every node forever and the
check is vacuously true for every candidate. Elections are in practice decided purely by term +
priority + heartbeat timing, never by log freshness. The concrete, currently-accepted consequence
is the users-file version gate's mid-resync caveat below; more generally, a node whose local data
was just wiped for a resync is exactly as electable as a fully caught-up peer for the short window
before its own sync completes. Pre-existing, tracked as a follow-up, not something to rely on
being fixed.

### Programmatic Replica Set Configuration

You can configure a replica set programmatically using the `configureReplicaSet()` method:

```java
PoppyDB primary = new PoppyDB(27017, "localhost", 100, 10);

// Configure as a 2-node replica set with host priorities
var hosts = List.of("localhost:27017", "localhost:27018");
var priorities = Map.of("localhost:27017", 300, "localhost:27018", 100);
primary.configureReplicaSet("myReplicaSet", hosts, priorities);

primary.start();

// Start secondary later
PoppyDB secondary = new PoppyDB(27018, "localhost", 100, 10);
secondary.configureReplicaSet("myReplicaSet", hosts, priorities);
secondary.start();
```

**Write Concern Behavior with Partial Replica Sets:**

When using entities with `@WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES)` or explicit write concerns with `w > 1`, PoppyDB handles the case where not all secondaries are available:

- If no secondaries have connected yet, the server returns a `writeConcernError` after a brief grace period (100ms) instead of waiting for the full `wtimeout`
- This allows you to store documents on the primary before starting secondary nodes
- Once secondaries connect, writes will properly wait for replication acknowledgment

This is particularly useful for testing scenarios where you want to:
1. Start a primary node
2. Store initial test data
3. Start secondary nodes and verify data replication

### Index replication

Secondaries replicate **index definitions** as well as documents (since 6.3.0, #258): the initial
sync copies the primary's `listIndexes` output after the data snapshot, and a periodic diff (every
30s) converges afterwards — indexes created on the primary are created on the secondary with their
full options (unique, TTL, partial, sparse, ...), and indexes dropped on the primary are dropped
locally (the `_id` index is never touched). The periodic diff also covers changes the secondary
missed while disconnected. Change streams carry no index DDL, so index changes can lag up to one
diff interval behind; document replication is unaffected. After a failover, a promoted secondary
therefore enforces the same unique constraints and expires TTL documents like the old primary did.

### Replication buffer sizing

A secondary that falls behind (network partition, GC pause, slow disk) resumes from its
last-applied position once it reconnects — but only if the primary's replay buffer still covers
the gap. If too much has been written while the secondary was disconnected, the primary signals
"resume window lost" and the secondary falls back to a full initial re-sync instead of a cheap
incremental resume.

The rule of thumb: **the replay buffer must be sized to cover the sustainable write rate times the
worst-case sync/reconnect duration** — `buffer size >= write_rate × sync_duration`. A buffer sized
for average load will still force a full re-sync under a burst or a slow reconnect, and if
re-syncs then start overlapping with new bursts faster than they can complete, replication can
enter a state where it never catches up (each re-sync itself takes time, during which more writes
accumulate). This is exactly what `ReplicationManager` now watches for: if a resync happens more
than once within a 10-minute window, it logs a WARN —
*"replication cannot keep up — buffer sizes bound write rate × sync duration"* — because a single
isolated re-sync is a normal recovery from a transient outage, but back-to-back re-syncs are a sign
the buffer (or the sync speed) can no longer absorb the actual write rate.

Use `ReplicationManager.getStats()` (nested under `PoppyDB.getStats()`'s `"replication"` key) to
watch this before it becomes an incident:

| Key | Meaning |
|-----|---------|
| `resyncCount` | How many times this secondary has fallen back to a full re-sync. |
| `lastAppliedSequence` | The secondary's own last-applied change-stream sequence. |
| `eventQueueSize` / `eventQueueCapacity` | Current depth / configured bound of the secondary's local replication event queue — a queue that is consistently near capacity means the batch processor cannot keep up with incoming events. |
| `replicationLagEvents` | The primary's sequence at the most recent watch registration minus `lastAppliedSequence` — an approximation of how many events behind the secondary was at reconnect time. |
| `watchGeneration` | Bumped on every successful watch (re-)registration; a fast-climbing counter indicates a flapping connection. |

### Persistence (Periodic Snapshots)

PoppyDB can periodically dump all databases to disk and restore them on startup. This provides basic persistence for development and testing scenarios.

**How it works:**
- On startup: If dump files exist in the configured directory, they are automatically restored.
- During runtime: If `--dump-interval` is set, databases are dumped periodically.
- On shutdown: A final dump is performed to capture all changes.

**Quick Start with Persistence:**

```bash
# Start with persistence - dumps every 5 minutes
java -jar poppydb/target/poppydb-<version>-cli.jar -p 27017 \
  --dump-dir /var/morphium/data --dump-interval 300

# Start with persistence - dump only on shutdown
java -jar poppydb/target/poppydb-<version>-cli.jar -p 27017 \
  --dump-dir /var/morphium/data
```

**Manual Snapshots:**
You can trigger a manual dump at any time using the `dumpNow()` method programmatically (see below).

**Programmatic Configuration:**
```java
import de.caluga.poppydb.PoppyDB;
import java.io.File;

PoppyDB server = new PoppyDB(27017, "localhost", 100, 10);

// Configure persistence
server.setDumpDirectory(new File("/var/morphium/data"));
server.setDumpIntervalMs(300000); // 5 minutes

// Restore previous state before starting
try {
    int restored = server.restoreFromDump();
    System.out.println("Restored " + restored + " databases");
} catch (Exception e) {
    System.out.println("Starting fresh: " + e.getMessage());
}

server.start();

// Manual dump if needed
server.dumpNow();
```

**Dump File Format:**
- Each database is saved as `<dbname>.morphium.gz` (gzip-compressed JSON)
- Files can be inspected with `zcat <file>.morphium.gz | jq .`

**Limitations:**
- Not a real-time persistence solution (no write-ahead log)
- Data between dump intervals may be lost on crash
- Suitable for development/testing, not production

### Memory Watermark

An in-memory store dies of OOM when producers outrun consumers — and a replica set dies
*completely*, because replication copies the data volume to every node. PoppyDB therefore
guards its heap with two watermarks (percent of the JVM's max heap):

- **Warn** (default 75%): a WARN log line when heap occupancy crosses the threshold
  (logged once per crossing, re-arms 5% below).
- **Reject** (default 90%): document-creating writes (`insert`, replace-style `store`) are
  refused with a mongod-shaped `ExceededMemoryLimit` error (code 146). **Updates, deletes
  and TTL expiry keep working** — the drain paths (messaging processed-marks, lock
  releases, cleanup) must stay open so the system can get back under the watermark
  instead of being stuck above it.

Replication applies and the initial sync bypass the guard: the primary is the gate, and a
secondary refusing to apply what the primary accepted would silently diverge. All nodes of
a replica set stop accepting new data at the same watermark instead of failing together.

Clients receive the rejection as a write error and should treat it as retryable
backpressure. The current state is visible in `db.serverStatus().memoryWatermark`
(`heapUsedPercent`, `heapUsedAfterGcPercent`, thresholds, warn state).

Both stages decide on the **post-GC live set** (`heapUsedAfterGcPercent`, from the JVM's
per-pool collection usage), not on raw heap occupancy: with `-Xms` == `-Xmx` the JVM only
collects when the heap is nearly full, so the raw `used/max` gauge routinely reads above
90% under allocation-heavy load even when the next GC would free most of it. Deciding on
the raw gauge would reject writes on a heap that is one GC away from half empty. The raw
gauge remains as a cheap precheck (the live set can never exceed it) and is what
`heapUsedPercent` reports in `serverStatus`.

```bash
# defaults: warn at 75%, reject at 90%
java -jar poppydb-cli.jar --port 27017

# tighter bound, e.g. when sharing the JVM host with other services
java -Xmx2g -jar poppydb-cli.jar --port 27017 --memory-warn 60 --memory-reject 75

# disable entirely (test setups that intentionally fill the heap)
java -jar poppydb-cli.jar --port 27017 --memory-warn 100 --memory-reject 100
```

Programmatic: `poppyDb.setMemoryWatermarks(warnPercent, rejectPercent)` or
`InMemoryDriver.setMemoryWatermarks(...)` for embedded use.

### BSON Size Limit

PoppyDB enforces MongoDB's per-document BSON size limit (default 16MB) — measured against a
real 8.0.26 server, the behaviour is:

- **Inserts/stores** of a document over the limit fail with `BSONObjectTooLarge` (code
  10334) and mongod's message shape (`BSONObj size: N (0x..) is invalid. Size must be
  between 0 and 16793600(16MB) First element: ...`). In practice, well-behaved drivers
  already refuse to *send* such documents, because the limit is advertised as
  `maxBsonObjectSize` in the `hello` handshake — which PoppyDB now answers with the
  *configured* value instead of a hardcoded one.
- **Updates** (`$set`, `$push`, replacements, upserts) whose **resulting** document would
  exceed the limit fail with the same error, atomically — the stored document is left
  untouched. This is the case client-side checks cannot catch (each individual update
  command is small), so the server-side check is what actually guarantees the bound.
  Like mongod, update results get a 16KB internal margin (`BSONObjMaxInternalSize`).

Do not confuse this with `maxMessageSizeBytes` (48MB): that bounds a single **wire
protocol message** — the envelope — so a bulk insert can carry multiple documents of up
to `maxBsonObjectSize` each in one message. Both sides of that bound are enforced:
Morphium's wire drivers split oversized write payloads (insert documents, update/delete
statements) into several messages under the advertised limit — transparently, with summed
counters and correctly shifted error indices — and PoppyDB caps **reply** batches
(find/getMore/aggregate cursors) at `maxBsonObjectSize` worth of documents per batch like
mongod, handing out the remainder via the cursor.

Configurable via `--max-bson-size <bytes>` (0 disables the check entirely — then `hello`
advertises a permissive 128MB), programmatic via `poppyDb.setMaxBsonObjectSize(bytes)` or
`InMemoryDriver.setMaxBsonObjectSize(bytes)` for embedded use. The embedded InMemoryDriver
enforces the same 16MB default, so tests against it catch oversized documents before a
real MongoDB would.

### SSL/TLS Configuration

PoppyDB supports SSL/TLS encrypted connections for secure communication.

**Quick Start with SSL:**

1. Generate a self-signed certificate:
```bash
keytool -genkeypair -alias morphium -keyalg RSA -keysize 2048 \
  -validity 365 -keystore server.jks -storepass changeit \
  -dname "CN=localhost"
```

2. Start the server with SSL enabled:
```bash
java -jar poppydb/target/poppydb-<version>-cli.jar -p 27018 \
  --ssl --sslKeystore server.jks --sslKeystorePassword changeit
```

3. Connect with mongosh:
```bash
# For self-signed certificates
mongosh "mongodb://localhost:27018" --tls --tlsAllowInvalidCertificates

# With proper certificate verification (export cert first)
keytool -exportcert -alias morphium -keystore server.jks \
  -storepass changeit -rfc -file server-cert.pem
mongosh "mongodb://localhost:27018" --tls --tlsCAFile server-cert.pem
```

**Programmatic SSL Configuration:**
```java
import de.caluga.poppydb.PoppyDB;
import de.caluga.morphium.driver.wire.SslHelper;

PoppyDB server = new PoppyDB(27018, "localhost", 100, 10);

// Load keystore and enable SSL
SSLContext sslContext = SslHelper.createServerSslContext(
    "server.jks", "changeit"
);
server.setSslContext(sslContext);
server.setSslEnabled(true);

server.start();
```

### Authentication (`--auth`)

PoppyDB supports real SCRAM authentication (SCRAM-SHA-1 and SCRAM-SHA-256, RFC 5802/7677) with
users stored mongod-shaped in `admin.system.users`. Enforcement is **strictly opt-in**: without
`--auth` the server stays completely open (unchanged test/dev behavior). With `--auth`, a
connection may only run the handshake, SASL, `logout`, `ping` and `buildInfo` commands until it
completes a SCRAM exchange — everything else is rejected with code 13 `Unauthorized`.

**Quick Start with authentication:**

```bash
# There is no localhost exception - configure the initial admin user at startup:
java -jar poppydb-cli.jar -p 27018 \
  --auth --rootUser admin --rootPassword s3cr3t
```

```bash
# Unauthenticated access is rejected:
mongosh "mongodb://localhost:27018/test" --eval 'db.coll.find()'
# MongoServerError: command find requires authentication

# Standard clients authenticate as against real MongoDB:
mongosh "mongodb://admin:s3cr3t@localhost:27018/test?authSource=admin"

# Create additional users the normal way:
mongosh "mongodb://admin:s3cr3t@localhost:27018/admin?authSource=admin" \
  --eval 'db.createUser({user: "app", pwd: "apppass", roles: []})'
```

Morphium clients simply set credentials as usual (`authDb`/user/password in the connection
settings) — the driver performs the SCRAM handshake automatically on connect.

**Combine with TLS** for encrypted, authenticated deployments:
```bash
java -jar poppydb-cli.jar -p 27018 --auth --rootUser admin --rootPassword s3cr3t \
  --ssl --sslKeystore server.jks --sslKeystorePassword changeit
```

**Programmatic configuration:**
```java
PoppyDB server = new PoppyDB(27018, "localhost", 100, 10);
server.setAuthRequired(true);
server.setRootUser("admin", "s3cr3t");   // created at startup if absent
server.start();
```

**Current limitations:**
- Authorization is authentication-only: a logged-in user may run any command — roles are
  stored (`createUser`'s `roles` field, `createRole` is not implemented) but not evaluated.
- X.509 client-certificate authentication is not supported (fails honestly with code 18).
- Passwords travel SCRAM-hashed, never in the clear — but combine `--auth` with `--ssl` when
  crossing untrusted networks to also encrypt the data itself.

**User replication:** in a replica set, `admin.system.users` is the one system collection that
replicates — users created, updated or removed via `createUser`/`updateUser`/`dropUser` reach
every member, and (like all writes) only the primary accepts these commands; a secondary answers
them with `NotWritablePrimary`. This means logins survive failover: a user created before a leadership
change can still authenticate against the new primary and against every secondary, and a dump
taken on any member — including a priority-0 backup node that never leads — contains the users,
not just the data. Before this change users were node-local, so a backup-node dump silently
omitted them. Two windows are worth knowing about: during a full resync a node's local user set
is briefly empty until the snapshot lands, so SCRAM logins against that node fail in that window
(unsurprising, since the node isn't caught up yet anyway); and right after a failover there is a
brief window before the new primary has (re-)created the root user, during which root logins may
transiently fail until that completes.

For provisioning more than the one initial admin user declaratively, see
[Bootstrapping users (`--users-file`)](#bootstrapping-users-users-file) below — a JSON file of
users applied the same idempotent, primary-only, replication-riding way `--rootUser` is.

**SSL with Docker:**
```dockerfile
FROM openjdk:21-slim
WORKDIR /app

COPY poppydb/target/poppydb-<version>-cli.jar /app/poppydb.jar
COPY server.jks /app/server.jks

EXPOSE 27018

CMD ["java", "-jar", "/app/poppydb.jar", \
     "--port", "27018", "--host", "0.0.0.0", \
     "--ssl", "--sslKeystore", "/app/server.jks", \
     "--sslKeystorePassword", "changeit"]
```

### Bootstrapping users (`--users-file`)

`--rootUser`/`--rootPassword` provision exactly one admin user. For production ("IaC")
provisioning of the application's actual user set, point `--users-file <path>` (config key
`users-file`) at a JSON file and PoppyDB applies it as an idempotent upsert every time a node
becomes primary — no manual `createUser` shell commands, no drift between environments.

**File format** — either a bare JSON array (unversioned), or an object wrapping it with a
`version`:

```json
{ "version": 3,
  "users": [
    { "user": "app",     "db": "mydb",  "pwd": "s3cret",
      "roles": [{ "role": "readWrite", "db": "mydb" }] },
    { "user": "monitor", "pwd": "..." }
  ] }
```

Per entry: `user` and `pwd` are required non-empty strings; `db` defaults to `"admin"`; `roles`
is optional and stored mongod-shaped but **not enforced** (like everywhere else in PoppyDB —
see [Current limitations](#authentication-auth) above); `mechanisms` is optional. Any unknown
field in an entry, or at the top level, is a hard error naming the field (and the entry index)
instead of being silently ignored. Two entries naming the same `(user, db)` pair are a hard error
too — mongod identifies a user by that pair, so both would apply to the same principal; without
this check the file loaded silently with the later entry's password/roles winning, no
diagnostic at all. `version`, if present, must be a positive integer.

```bash
java -jar poppydb-cli.jar --auth --rootUser admin --rootPassword s3cr3t \
  --users-file /etc/poppydb/users.json
```

**Applied by the primary only, at the same point `--rootUser` is:**

- Non-replicated / static-mode primary: once, right after startup (a failure here is fatal —
  it aborts startup, same as any other broken config, so a bad file is caught immediately
  instead of silently leaving users unprovisioned).
- Election-mode replica set: every time this node's leadership hook runs, right after
  `ensureRootUser` — i.e. on every election, not just the first one. This is intentionally
  idempotent: `createUser` on a name that already exists falls back to `updateUser` (password
  and roles from the file replace the stored state; `mechanisms`, when listed in the file,
  replaces the stored set too — but when the file entry OMITS `mechanisms`, an existing user
  keeps whatever mechanism set they already have, mongod's `updateUser` semantics. A user first
  provisioned with `mechanisms: ["SCRAM-SHA-256"]` therefore stays SHA-256-only even if a later
  file version drops the key; to get back to the default pair, list both mechanisms explicitly),
  so repeated leadership changes (flapping, priority takeover) just re-apply harmlessly. A failure here can only be **logged**
  (`ERROR`) — a running server cannot abort mid-failover, so the node keeps serving with
  whatever user state it already had.
- A static-mode **secondary** never applies the file locally, even if `--users-file` is
  configured on it too (PoppyDB logs an INFO line noting that it is ignored there) — it receives
  the result purely through the normal `admin.system.users` replication that already carries
  `createUser`/`updateUser` writes (see [User replication](#authentication-auth) above). The
  file is only ignored for *application* on such a node — it is still parsed and validated at
  startup like everywhere else, so a syntactically broken file fails that node's startup too
  (fail-fast by design, not a live-apply attempt).

**Deploy the identical file to every node.** The version gate below is what makes that safe even
across a failback to a node that still has an older copy on disk.

**Version gate — protects against rollback after a failback:** an unversioned file always
applies in full on every leadership change. A versioned file only applies if the file's
`version` is **greater than** the last version this cluster actually applied — that applied
state is itself a small document, `admin.system.version { _id: "poppydb.usersFile",
appliedVersion: N }`, and it replicates exactly like the users themselves (same collection-scope
rule as `admin.system.users`). Concretely: node A applies version 3 and becomes primary; a
network partition later promotes node B, which still has version 2 of the file on local disk —
B's apply is skipped (`appliedVersion 3 >= file version 2`, logged at INFO) instead of rolling
passwords back to the older file's contents. Re-applying the *same* version is likewise a no-op;
only a strictly higher version triggers a new apply. Known exception: the gate reads the
replicated meta document, so a node elected primary while still mid-resync — after its local
`admin.system.version` has been cleared for the resync but before the primary's snapshot has
landed — sees no meta document and re-applies its own file regardless of version. This is
possible because nothing currently stops such a node from winning the election in the first
place — the vote's Raft log check is dead code (see [Replica Set
Behavior](#replica-set-behavior-experimental) above), so a mid-resync node with an empty local
log is exactly as electable as a fully caught-up peer. Pre-existing, tracked as a follow-up, not
a property of the version gate itself.

**Rotation flow:** edit the file (bump `version`), roll out the new file to every node, then
rolling-restart (or just let the next election re-run the leadership hook, in election mode).
Logins flip cluster-wide the moment a primary holding the new file applies it — there is no
per-node lag beyond ordinary replication.

**Programmatic configuration** (mirrors `setRootUser`):
```java
import de.caluga.poppydb.PoppyDB;
import de.caluga.poppydb.config.UsersFileLoader;

PoppyDB server = new PoppyDB(27018, "localhost", 100, 10);
server.setBootstrapUsers(UsersFileLoader.load("/etc/poppydb/users.json"));
server.start();
```

**Out of scope (by design):** the file has no reconciliation-delete — it only ever adds/updates,
so removing a user means an explicit `dropUser` command against the primary (which replicates
like any other user write; merely deleting the entry from the file does NOT remove the user);
no role *enforcement* (same limitation as
`createUser`'s `roles` field everywhere else); no environment-variable substitution inside the
file; and no file-watching — a changed file only takes effect on the next apply (restart, or the
next leadership change in election mode), never live.

### Constructor Options

```java
// Full constructor
PoppyDB server = new PoppyDB(
    int port,           // Server port
    String host,        // Bind address
    int maxThreads,     // Maximum threads
    int minThreads      // Minimum threads
);

// Default constructor (port 17017, localhost, 100/10 threads)
PoppyDB server = new PoppyDB();
```

## Connecting Clients

### Capabilities document and driver settings

The `hello` reply carries a `poppyCapabilities` document describing what PoppyDB honestly
supports, so clients and tooling can adapt instead of discovering gaps at runtime:

```json
"poppyCapabilities": {
  "version": 1,
  "retryableWrites": false,
  "journal": false,
  "durability": "snapshot",
  "readConcern": "local",
  "transactions": "partial",
  "textSearch": "simplified"
}
```

Practical consequences for non-Morphium drivers:

- **Set `retryWrites=false` in the connection string.** PoppyDB advertises a replica set and
  logical sessions, which makes modern drivers enable retryable writes by default — but
  PoppyDB has no `(lsid, txnNumber)` deduplication yet, so a driver-side retry after a lost
  acknowledgement would apply the write twice. (True retryable-write support is specced in
  issue #293.)
- **`j: true` write concerns fail honestly** with `writeConcernError` code 2 (`BadValue`),
  like mongod running without journaling: PoppyDB persists via periodic snapshots, there is
  no journal to wait for. The write itself is still executed.
- **Reads on a secondary require an explicit read preference.** MongoDB's default read
  preference is `primary`, so a read without `$readPreference` is rejected on a secondary
  with `NotPrimaryNoSecondaryOk` (13435) — the same way mongod treats a direct secondary
  connection without `secondaryOk`. Morphium's own driver always sends a read preference
  (default `primaryPreferred`) and is unaffected.

### Java (Morphium)

```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings()
   .setDatabase("mydb")
   .addHost("localhost", 27017);
cfg.driverSettings()
   .setDriverName("SingleMongoConnectDriver");

Morphium morphium = new Morphium(cfg);
```

### Python (PyMongo)

```python
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client.mydb
collection = db.users

# Works like regular MongoDB!
collection.insert_one({'name': 'Alice', 'age': 30})
user = collection.find_one({'name': 'Alice'})
print(user)
```

### Node.js (mongodb driver)

```javascript
const { MongoClient } = require('mongodb');

async function main() {
    const client = new MongoClient('mongodb://localhost:27017');
    await client.connect();

    const db = client.db('mydb');
    const collection = db.collection('users');

    await collection.insertOne({ name: 'Bob', age: 25 });
    const user = await collection.findOne({ name: 'Bob' });
    console.log(user);
}

main();
```

### Go (mongo-driver)

```go
package main

import (
    "context"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
    client, _ := mongo.Connect(
        context.TODO(),
        options.Client().ApplyURI("mongodb://localhost:27017"),
    )

    collection := client.Database("mydb").Collection("users")
    collection.InsertOne(context.TODO(), bson.D{{"name", "Charlie"}})
}
```

### MongoDB Shell

```bash
mongosh mongodb://localhost:27017/mydb

# Test it
> db.users.insertOne({name: "Alice", age: 30})
> db.users.find()
```

## Use Cases

### 1. CI/CD Pipelines

```yaml
# .github/workflows/test.yml
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Build Morphium
        run: mvn clean package -pl poppydb -am -Dmaven.test.skip=true

      - name: Start PoppyDB
        run: |
          java -jar poppydb/target/poppydb-<version>-cli.jar \
               --port 27017 --host 0.0.0.0 &
          sleep 2

      - name: Run Integration Tests
        run: npm test
        env:
          MONGO_URL: mongodb://localhost:27017
```

### 2. Integration Testing (Multi-Language)

```java
@BeforeAll
static void startServer() throws Exception {
    server = new PoppyDB(27017, "0.0.0.0", 100, 10);
    server.start();
    Thread.sleep(500); // Wait for server to be ready
}

@Test
void testCrossLanguageCompatibility() throws Exception {
    // Insert from Java
    MorphiumConfig cfg = new MorphiumConfig();
    cfg.connectionSettings().setDatabase("test").addHost("localhost", 27017);
    cfg.driverSettings().setDriverName("SingleMongoConnectDriver");

    Morphium morphium = new Morphium(cfg);
    MyEntity entity = new MyEntity();
    entity.setName("test-entity");
    morphium.store(entity);
    morphium.close();

    // Verify from Python script
    ProcessBuilder pb = new ProcessBuilder("python3", "test_read.py");
    pb.environment().put("MONGO_URL", "mongodb://localhost:27017/test");
    Process p = pb.start();
    assertEquals(0, p.waitFor());
}

@AfterAll
static void stopServer() {
    server.terminate();
}
```

### 3. Microservices Development

```bash
# Terminal 1: Start PoppyDB
java -jar poppydb/target/poppydb-<version>-cli.jar --port 27017

# Terminal 2: Start Node.js service
MONGO_URL=mongodb://localhost:27017 npm start

# Terminal 3: Start Python service
MONGO_URL=mongodb://localhost:27017 python app.py

# Terminal 4: Start Java service
MONGO_URL=mongodb://localhost:27017 ./gradlew run
```

### 4. Docker Deployment

**Dockerfile:**
```dockerfile
FROM openjdk:21-slim
WORKDIR /app

# Copy the executable server JAR
COPY poppydb/target/poppydb-<version>-cli.jar /app/poppydb.jar

EXPOSE 27017

CMD ["java", "-jar", "/app/poppydb.jar", \
     "--port", "27017", "--host", "0.0.0.0"]
```

**Docker Compose:**
```yaml
version: '3.8'

services:
  morphium-db:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "27017:27017"

  app:
    image: myapp:latest
    depends_on:
      - morphium-db
    environment:
      - MONGO_URL=mongodb://morphium-db:27017/appdb
```

**Build and Run:**
```bash
docker build -t poppydb .
docker run -p 27017:27017 poppydb

# Or use docker-compose
docker-compose up
```

**Alternative: mounted configuration file instead of CLI flags.** For anything beyond a couple
of flags, a mounted [config file](#configuration-file) reads more clearly than a long `CMD` line
and lets ops change settings without rebuilding the image:

```dockerfile
FROM openjdk:21-slim
WORKDIR /app

COPY poppydb/target/poppydb-<version>-cli.jar /app/poppydb.jar

EXPOSE 27017

# No CLI flags at all - everything comes from the mounted file. --cfg also works with
# --no-config if you want to rule out any other config file location entirely.
CMD ["java", "-jar", "/app/poppydb.jar", "--cfg", "/etc/poppydb/config"]
```

```bash
docker run -p 27017:27017 -v "$PWD/poppydb.conf:/etc/poppydb/config:ro" poppydb
```

Secrets referenced via the `*-file` indirection (`root-password-file`,
`ssl-keystore-password-file`) can be mounted from a Docker/Swarm `secret` file separately from the
rest of the config, keeping them out of the image and out of `docker inspect` environment output.

### 5. Message Broker for Short-Lived Messages (production)

Morphium messaging runs natively against PoppyDB: TTL-based messages, exclusive locks,
answer semantics, and event-driven delivery via change streams. For **ephemeral messages**
— events, cache invalidation, job triggers with sender-side retry — a 3-node replica set
is a lightweight broker: node failures are covered by replication and Raft failover, and
the [memory watermark](#memory-watermark) turns overload into retryable backpressure
instead of an OOM.

**Know the loss model:** there is no write-ahead log. A cluster-wide outage loses all
messages since the last snapshot (if any). Use it where in-flight loss is acceptable and
senders can retry — not for guaranteed delivery.

### 6. Cache / Session Storage (production)

Cache semantics tolerate total loss by definition, which makes PoppyDB a candidate for
memcached/Redis-style roles with two twists neither offers out of the box:

- **Wire compatibility**: every language with a MongoDB driver is a client — no extra
  protocol or library.
- **Push invalidation**: change streams are built-in pub/sub — cache entries invalidate
  by event instead of polling.

Session storage is the textbook case: a TTL index on the last-access field expires
sessions automatically, replica-set failover keeps sessions alive across node restarts
(more than memcached offers), and off-the-shelf MongoDB session backends (e.g. Spring
Session) work unchanged. `$inc` + TTL also cover rate limiting and counters; tiny
config/feature-flag collections get instant propagation via change streams.

For all production use: enable [`--auth`](#authentication-auth) (note that roles are
not evaluated yet — isolate the network segment), size the heap deliberately, monitor
`db.serverStatus().memoryWatermark` and `db.stats()`, and read the loss model above.

## Performance

| Metric | PoppyDB | MongoDB |
|--------|---------------|---------|
| Startup Time | ~100-500ms | ~2-5 seconds |
| Memory (baseline) | ~50-100MB | ~500MB-1GB |
| Inserts/sec | ~50,000 | Varies |
| Queries/sec | ~100,000 | Varies |
| Updates/sec | ~40,000 | Varies |
| Latency (localhost) | 1-5ms | 1-10ms |

### Messaging Round-Trip Latency (measured)

Measured with [morpheus](https://github.com/sboesebeck/morpheus), the CLI tool for
morphium-driven projects: its latency harness sends morphium messages ping-pong style and
records the full round trip (send → receive → answer → answer received). Setup: dedicated
run on a single MacBook Pro, **both** systems as a 3-node replica set, identical morphium
messaging code on both sides.

| Metric (ms) | PoppyDB (3-node RS) | MongoDB (3-node RS) | Factor |
|---|---|---|---|
| avg round trip | 2.64 | 59.5 | ~22x |
| p50 | 2.43 | 59.1 | ~24x |
| p90 | 3.31 | 70.0 | ~21x |
| p99 | 6.70 | 79.8 | ~12x |
| min | 1.31 | 34.8 | ~26x |
| jitter | 0.66 | 6.48 | ~10x |
| message loss | 0% | 0% | — |

**Why the gap is structural, not tuning:** MongoDB change streams only emit
majority-committed events, so every messaging hop pays replication plus journal-commit
latency — that is MongoDB's ~35ms latency *floor* in this setup, and it buys durability.
PoppyDB's change streams emit directly from memory and pay nothing, because there is
nothing to persist — the same trade-off described in the
[use cases](#5-message-broker-for-short-lived-messages-production): you get the latency
because you accepted the loss model. Numbers vary with hardware; re-run the harness on
your target machine for real figures.

## Monitoring

### Built-in Status Monitoring

**All Morphium messaging instances automatically include status monitoring** via the `morphium_status` topic. This works with PoppyDB and any Morphium messaging setup.

Quick example:
```java
MorphiumMessaging sender = morphium.createMessaging();
sender.start();

// Query all instances for status
List<Msg> responses = sender.sendAndAwaitAnswers(
    new Msg(sender.getStatusInfoListenerName(), "status", "ALL"),
    5,      // Wait for up to 5 responses
    2000    // 2 second timeout
);

// Process JVM, messaging, and driver metrics
for (Msg response : responses) {
    Map<String, Object> stats = response.getMapValue();
    System.out.println("Instance: " + response.getSender());
    System.out.println("  Heap Used: " + stats.get("jvm.heap.used"));
    System.out.println("  Messages Processing: " + stats.get("messaging.processing"));
}
```

**For complete documentation on status monitoring**, including:
- All available metrics (JVM, messaging, driver)
- Query levels (PING, MESSAGING_ONLY, MORPHIUM_ONLY, ALL)
- Cross-language monitoring (Python, Node.js, etc.)
- Health checks and periodic monitoring
- Enable/disable controls

See the **[Messaging - Built-in Status Monitoring](./messaging.md#built-in-status-monitoring)** section.

### Connection Count

```java
PoppyDB server = new PoppyDB(27017, "localhost", 100, 10);
server.start();

// Get active connections
int connections = server.getConnectionCount();
System.out.println("Active connections: " + connections);
```

### Logging

The server CLI jar ships its own Logback configuration: root level `INFO`, Netty at `WARN`,
console output. Verbosity can be raised or lowered at startup — the three overrides, in
increasing order of control:

```bash
# CLI option (recommended)
java -jar poppydb-cli.jar --port 27017 --log-level DEBUG

# System property (read by the bundled logback.xml)
java -Dpoppydb.log.level=DEBUG -jar poppydb-cli.jar --port 27017

# Full control: replace the bundled configuration entirely
java -Dlogback.configurationFile=/path/to/my-logback.xml \
     -jar poppydb-cli.jar --port 27017
```

`--log-level` accepts `ERROR`, `WARN`, `INFO`, `DEBUG` and `TRACE`. Both the CLI option and
the system property change the root logger only — Netty stays at `WARN` either way; to see
Netty internals, replace the configuration via `-Dlogback.configurationFile`.

## Supported Admin Commands

PoppyDB implements the following MongoDB admin commands:

| Command | Description |
|---------|-------------|
| `ping` | Basic connectivity test |
| `hello` / `isMaster` / `ismaster` | Server status and topology information |
| `listDatabases` | List all databases with sizes |
| `buildInfo` | Server version information |
| `getCmdLineOpts` | Command line options |
| `getParameter` | Server parameters |
| `getLog` | Server logs |
| `listCommands` | Names of every command this server answers |
| `currentOp` / `$currentOp` stage | Live operations from the server's op registry — `db.currentOp()` works, including `$match` filters |
| `killOp` | Marks an op kill-pending; best-effort thread interrupt (never a Netty event loop — cooperative like mongod) |
| `serverStatus` | Includes real client connection gauges (`connections.current`/`totalCreated` from the Netty channel group) |
| `dbStats` / `collStats` | Real BSON data sizes, estimated index sizes |
| `hostInfo` | Host basics (hostname, cores, memory, OS, JVM) |
| `connectionStatus` | Authenticated user of this connection (empty without `--auth`) |
| `whatsmyuri` | Client address as the server sees it |
| `replSetGetStatus` / `replSetGetConfig` | `rs.status()` and `rs.conf()` (config reconstructed from seeds and priorities) |
| `dbHash` | MD5 per collection + combined hash in canonical document order — compare replica-set members with one command, works on secondaries |
| `validate` | Real data↔index consistency check against the collection's index store (stale/missing index entries, keysPerIndex) |
| `replSetStepDown` | Step down from primary (for replica sets) |
| `startSession` / `endSessions` / `refreshSessions` | Session management |
| `getMore` | Cursor iteration for both regular queries and change streams |

### Standalone Server Behavior

When running PoppyDB as a standalone server (without replica set configuration):

- The server always reports itself as primary (`isWritablePrimary: true`)
- `replSetStepDown` commands are acknowledged but the server immediately becomes primary again
- This ensures compatibility with clients and tests that issue replica set commands

### StepDown / Failover Behavior (Replica Set)

PoppyDB uses **Raft-based leader election**, which behaves differently from MongoDB:

| Behavior | MongoDB | PoppyDB |
|----------|---------|---------|
| StepDown | Old primary steps down, may become primary again after `timeToStepDown` | Old primary steps down, may reclaim leadership via priority takeover |
| New leader | Elected via priority/oplog, often original wins re-election | Elected via Raft, highest priority wins |
| Step-back | Common (original typically returns as primary) | Yes, if the original node has a higher priority (since 6.3) |
| Election time | ~2-10 seconds | ~2-5 seconds |

**Impact on tests/applications:** the original primary only returns if it was configured with a *higher priority* than the node that took over. In a cluster where all nodes share the default priority (50), the new leader keeps leadership — do not wait for a specific node to become primary, verify that *any* node did.

#### Priority Takeover

A leader periodically checks whether a peer with higher priority is available. It hands leadership over once that peer

- answers the leader's heartbeats (it is online and reachable), and
- has acknowledged everything the leader replicated during its term (it is caught up, see `priorityTakeoverMaxLag`).

The yielding leader then refuses re-election for `priorityTakeoverStepDownSecs`, so the higher-priority node — which uses a shorter, priority-adjusted election timeout — wins the resulting election.

| `ElectionConfig` setting | System property | Default | Meaning |
|--------------------------|-----------------|---------|---------|
| `priorityTakeoverEnabled` | `morphiumserver.priorityTakeoverEnabled` | `true` | Enable voluntary step-down to a higher-priority peer |
| `priorityTakeoverCheckIntervalMs` | `morphiumserver.priorityTakeoverCheckIntervalMs` | `2000` | How often the leader looks for a successor |
| `priorityTakeoverMinStabilityMs` | `morphiumserver.priorityTakeoverMinStabilityMs` | `30000` | How long a node must have been leader before it may yield (anti-flapping) |
| `priorityTakeoverMaxLag` | `morphiumserver.priorityTakeoverMaxLag` | `0` | Change stream events the successor may still lag behind |
| `priorityTakeoverStepDownSecs` | `morphiumserver.priorityTakeoverStepDownSecs` | `10` | How long the yielding leader refuses re-election |

Set `priorityTakeoverEnabled` to `false` to keep the pre-6.3 behavior, where a failover is permanent.

### Change Stream Support

PoppyDB fully supports change streams for real-time notifications:

- **Collection-level watches**: Watch changes on a specific collection
- **Database-level watches**: Watch all collections in a database
- **Cluster-level watches**: Watch all databases
- **Resume tokens**: Resume a change stream from a token after reconnection (since v6.2.2)
- **Tailable cursors**: Capped collection tailable cursors with proper `maxTimeMS` polling (since v6.2.2)

Example with mongosh:
```javascript
// Watch a collection
db.users.watch().on('change', console.log);

// Watch entire database
db.watch().on('change', console.log);
```

## Limitations

### Data Persistence
- ✅ **Periodic Snapshots** - Dump/restore to disk (since v6.1.0)
- ❌ **No Real-time Persistence** - No WAL or journaling
- ❌ **Crash Risk** - Data between dumps may be lost on crash
- 💡 **Tip** - Use short dump intervals for important data

### Scalability
- ❌ **No Sharding** - Single instance only
- ✅ **Replica Sets** - Supported with Raft-based leader election (since v6.2.0)
- ❌ **Memory Bound** - Dataset limited by available RAM

### Features
- ✅ **Server-side Cursors** - Batched find queries with `batchSize` and `getMore` (since v6.2.2)
- ✅ **Tailable Cursors** - Capped collection tailable cursors with `maxTimeMS` polling (since v6.2.2)
- ✅ **Change Stream Resume** - Resume-after token support for reliable event delivery (since v6.2.2)
- ❌ **GridFS** - No file storage
- ❌ **Full-Text Search** - Limited $text support
- ❌ **Advanced Geospatial** - Basic queries only
- ❌ **Distributed Transactions** - Single instance only

### Security
- ✅ **TLS/SSL Supported** - Encrypted connections available (since v6.1.0)
- ✅ **Authentication** - Real SCRAM-SHA-1/SHA-256, opt-in via `--auth` (since v6.3.0) - see
  [Authentication](#authentication-auth)
- ⚠️ **Authorization not enforced** - roles are stored (`createUser`'s `roles` field) but not
  evaluated; any authenticated user may run any command. Isolate the network segment if you need
  fine-grained access control.

## When NOT to Use

**Avoid for:**
- Data that must survive a total cluster outage without loss - there is no write-ahead log; see
  the loss model under [Use Cases](#5-message-broker-for-short-lived-messages-production)
- Datasets exceeding available RAM
- Fine-grained, per-role authorization (roles are stored but not evaluated - see Security above)
- MongoDB Atlas-specific features
- GridFS, advanced full-text search/geospatial, sharding, or distributed transactions

**Use Instead:**
- **Durable system-of-record data**: real MongoDB with persistence
- **Large datasets**: MongoDB with disk storage
- **Cloud-managed**: MongoDB Atlas
- **Fine-grained access control**: real MongoDB's role-based access control

Note that PoppyDB *does* support authentication, TLS, and replica-set failover (see above) -
"avoid for production" is not a blanket rule. For ephemeral messaging and cache/session roles it
is explicitly recommended in production, with the caveats in [Use Cases](#use-cases) below.

## Building from Source

```bash
git clone https://github.com/sboesebeck/morphium.git
cd morphium
mvn clean package -pl poppydb -am -Dmaven.test.skip=true

# This creates the executable PoppyDB CLI JAR:
# poppydb/target/poppydb-X.Y.Z-cli.jar

# Run the server:
java -jar poppydb/target/poppydb-<version>-cli.jar --port 27017
```

## Maven Dependency

```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>poppydb</artifactId>
    <version>6.3.0</version> <!-- use the current release -->
</dependency>
```

Then start programmatically:
```java
public static void main(String[] args) throws Exception {
    // Option 1: Call main from the CLI class
    de.caluga.poppydb.PoppyDBCLI.main(
        new String[]{"--port", "27017", "--host", "0.0.0.0"}
    );

    // Option 2: Create instance directly
    PoppyDB server = new PoppyDB(27017, "0.0.0.0", 100, 10);
    server.start();
}
```

## Comparison: PoppyDB vs InMemory Driver

| Feature | PoppyDB | InMemory Driver |
|---------|---------------|-----------------|
| **Network Access** | Yes (wire protocol) | No (embedded) |
| **Multi-Language** | Yes | No (Java only) |
| **Use Case** | Integration tests, microservices | Unit tests |
| **Overhead** | Network latency | In-process |
| **Setup** | Start server | Config setting |
| **Isolation** | Process-level | Per-JVM |

**When to use each:**
- **InMemory Driver**: Java unit tests, embedded apps
- **PoppyDB (server)**: Integration tests, CI/CD, multi-language services

## See Also

- [PoppyDB Production Deployment Playbook](./howtos/poppydb-deployment.md) - step-by-step guide:
  systemd unit, secrets handling, capacity planning, monitoring, backup/restore, upgrades
- [Migrating from MongoDB to PoppyDB](./howtos/migration-mongodb-to-poppydb.md) - data migration,
  validation, cutover and rollback
- [InMemory Driver](./howtos/inmemory-driver.md) - Embedded driver for unit tests
- [Messaging](./messaging.md) - Messaging with Morphium / PoppyDB
- [Configuration Reference](./configuration-reference.md) - All configuration options
- [Architecture Overview](./architecture-overview.md) - How it works internally
