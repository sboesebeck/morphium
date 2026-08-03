# PoppyDB Production Deployment Playbook

This is a task-oriented, step-by-step guide to running PoppyDB in production. For the full
feature reference (all CLI flags, replica-set internals, admin command support, performance
numbers) see **[PoppyDB](../poppydb.md)** — this page only covers *how to get from a bare JAR to
a securely configured, monitored, backed-up production instance*, in order, ending in a
[Best Practices Checklist](#9-best-practices-checklist) to review before launch.

Read this if you're deploying PoppyDB as a message broker or cache/session store (see
[PoppyDB § Use Cases](../poppydb.md#use-cases)). If you only need it for tests or CI, the defaults
already documented in [PoppyDB § Quick Start](../poppydb.md#quick-start) are enough — skip this
page.

## 1. Prerequisites

- Java 21+ on the target host.
- The PoppyDB CLI jar, either built from source or downloaded as a release artifact:
  ```bash
  git clone https://github.com/sboesebeck/morphium.git
  cd morphium
  mvn clean package -pl poppydb -am -Dmaven.test.skip=true
  # produces poppydb/target/poppydb-<version>-cli.jar
  ```
- Decide up front: single node, or a replica set? Production deployments should default to a
  3-node replica set — a single node has no failover, and PoppyDB's loss model (no
  write-ahead log, see [§5](../poppydb.md#5-message-broker-for-short-lived-messages-production))
  means a crashed single node loses everything since the last snapshot.

## 2. Minimal secure configuration

Every production instance should start with **both** of these enabled — neither is on by default,
both are opt-in for backward compatibility with test/dev usage. Use a
[configuration file](../poppydb.md#configuration-file) from the start rather than command-line
flags: it is the only form that keeps `--rootPassword`/`--sslKeystorePassword` off the process
argument list (see [§3](#3-secrets-handling) for why that matters), and it is what every later
step in this playbook (systemd, monitoring, upgrades) assumes.

```bash
# Generate a keystore once (see PoppyDB § SSL/TLS for details)
keytool -genkeypair -alias poppydb -keyalg RSA -keysize 2048 \
  -validity 3650 -keystore /etc/poppydb/server.jks -storepass "$KEYSTORE_PASSWORD" \
  -dname "CN=poppydb.internal.example.com"
```

```properties
# /etc/poppydb/config - identical on all three nodes except 'bind' if it differs per host
port = 27017
bind = 0.0.0.0
auth = true
root-user = admin
root-password-file = /etc/poppydb/secrets/root.pw
ssl = true
ssl-keystore = /etc/poppydb/server.jks
ssl-keystore-password-file = /etc/poppydb/secrets/keystore.pw
rs-name = prod-rs
rs-seed = node1:27017,node2:27017,node3:27017
```

```bash
# chmod 600 the secret files referenced above, then:
java -jar poppydb-cli.jar --cfg /etc/poppydb/config
```

Repeat on the other two nodes with the same config file (`rs-name`/`rs-seed` must be identical
everywhere). See [PoppyDB § Replica Set Behavior](../poppydb.md#replica-set-behavior-experimental)
for how initial sync and elections work.

**Do not skip `auth`/`ssl` in production** — without them, anyone who can reach the port has full
read/write/admin access in cleartext. Authorization is authentication-only right now (roles are
stored but not evaluated, see [PoppyDB § Security](../poppydb.md#security)) — treat every
authenticated user as an admin and control access at the network boundary, not by role.

If you only need a one-off run without a persistent config file (a quick manual test against a
production-shaped setup, say), the equivalent CLI-only form is
`--port 27017 --bind 0.0.0.0 --auth --rootUser admin --rootPassword "$ROOT_PASSWORD" --ssl --sslKeystore ... --sslKeystorePassword "$KEYSTORE_PASSWORD" --rs-name ... --rs-seed ...`
— but see [§3](#3-secrets-handling) for why this isn't the recommended shape for anything
long-running.

## 3. Secrets handling

`--rootPassword` and `--sslKeystorePassword` on the command line are visible to any local user via
`ps aux` or `/proc/<pid>/cmdline` for the life of the process — avoid them for production and use
PoppyDB's [configuration file](../poppydb.md#configuration-file) instead: it keeps secrets out of
the process argument list entirely, and its `root-password-file`/`ssl-keystore-password-file`
keys go one step further by keeping the secrets out of the main config file too:

```properties
# /etc/poppydb/config - world-readable is fine, it contains no secrets itself
port = 27017
bind = 0.0.0.0
auth = true
root-user = admin
root-password-file = /etc/poppydb/secrets/root.pw
ssl = true
ssl-keystore = /etc/poppydb/server.jks
ssl-keystore-password-file = /etc/poppydb/secrets/keystore.pw
rs-name = prod-rs
rs-seed = node1:27017,node2:27017,node3:27017
```

PoppyDB refuses to start if `root-password`/`ssl-keystore-password` and their `*-file`
counterparts are set at the same time, and checks the POSIX permissions of the main file (if it
embeds a secret directly) and of any `*-file`-referenced secret file: group/other-readable only
warns, group/other-writable is a hard error (a writable config with secrets is a privilege
escalation, not a style issue) — see
[PoppyDB § Configuration File](../poppydb.md#configuration-file) for the full syntax and search
path rules.

- **systemd**: reference the secret files via `LoadCredential=` (see the unit file in
  [§5](#5-run-it-as-a-service-systemd) below) so the secrets themselves are never interpolated
  into `ExecStart` or exposed as environment variables — `root-password-file`/
  `ssl-keystore-password-file` point straight at the credential paths systemd exposes.
- **Docker/Compose**: mount the config file (`docker run -v poppydb.conf:/etc/poppydb/config:ro`,
  see [PoppyDB § Docker Deployment](../poppydb.md#4-docker-deployment)) and mount secret files via
  `secrets:` (Swarm) separately, referenced from the config via `*-password-file` — do not bake
  real secrets into a `CMD` line or an image layer (the plain Dockerfile example in
  [PoppyDB § Use Cases](../poppydb.md#4-docker-deployment) is fine for local/CI use only).
- **Kubernetes**: mount a `Secret` as files into the pod (not environment variables) and point
  `root-password-file`/`ssl-keystore-password-file` at the mounted paths.

## 4. Capacity planning

- **Heap sizing**: PoppyDB's entire dataset lives in the JVM heap (`-Xmx`). Size it to your
  expected data volume plus headroom for the [memory watermark](../poppydb.md#memory-watermark)
  (default reject threshold 90% of heap) — a heap that's *exactly* your dataset size will reject
  writes almost immediately.
- **Memory watermark**: tune `--memory-warn`/`--memory-reject` if you share the host with other
  services, or disable (`100`/`100`) only if you have an external hard cap you trust more (rare).
  Watch `db.serverStatus().memoryWatermark` (see [§6](#6-monitoring)).
- **Replication buffer**: for replica sets under sustained write load, read
  [PoppyDB § Replication buffer sizing](../poppydb.md#replication-buffer-sizing) — an undersized
  buffer causes repeated full re-syncs under load, which is itself extra load.
- **Connections**: `--max-connections` (default 500) and `--socket-timeout` (default 300s) bound
  resource usage per client population; size to your actual number of connecting services, not a
  round number.

## 5. Run it as a service (systemd)

Use a [configuration file](../poppydb.md#configuration-file) instead of a long `ExecStart` line
with `${VAR}` interpolation, and `LoadCredential=` to hand secrets to the process without ever
putting them in `ExecStart`, an environment variable, or `ps aux`:

```ini
# /etc/systemd/system/poppydb.service
[Unit]
Description=PoppyDB MongoDB-compatible server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=poppydb
Group=poppydb
LoadCredential=root-password:/etc/poppydb/secrets/root.pw
LoadCredential=keystore-password:/etc/poppydb/secrets/keystore.pw
ExecStart=/usr/bin/java -Xms4g -Xmx4g -jar /opt/poppydb/poppydb-cli.jar --cfg /etc/poppydb/config
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

```properties
# /etc/poppydb/config - no secrets in here, safe to be world-readable
port = 27017
bind = 0.0.0.0
auth = true
root-user = admin
root-password-file = /run/credentials/poppydb.service/root-password
ssl = true
ssl-keystore = /etc/poppydb/server.jks
ssl-keystore-password-file = /run/credentials/poppydb.service/keystore-password
rs-name = prod-rs
rs-seed = node1:27017,node2:27017,node3:27017
dump-dir = /var/lib/poppydb/snapshots
dump-interval = 300
log-level = INFO
```

`LoadCredential=<name>:<path>` makes systemd copy `<path>` into a runtime-only, root-owned
directory readable only by the service (`/run/credentials/<unit>/<name>`) before the process
starts — the source file at `/etc/poppydb/secrets/*.pw` still needs `chmod 600` itself, but the
password is never expanded into `ExecStart` or exposed as an environment variable the way
`EnvironmentFile` + CLI args would. Repeat on the other two replica-set nodes with the same
`rs-name`/`rs-seed` and each node's own `bind`/config.

```bash
sudo chmod 600 /etc/poppydb/secrets/root.pw /etc/poppydb/secrets/keystore.pw
sudo chown root:poppydb /etc/poppydb/secrets/root.pw /etc/poppydb/secrets/keystore.pw
sudo systemctl daemon-reload
sudo systemctl enable --now poppydb
sudo systemctl status poppydb
```

`log-level` (or `--log-level` on the command line) controls verbosity without editing the bundled
Logback config — see [PoppyDB § Logging](../poppydb.md#logging) if you need more (e.g. a rotating
file appender via `-Dlogback.configurationFile`).

## 6. Monitoring

- **Server health**: poll `db.serverStatus()` (connections, `memoryWatermark`) and `db.stats()`
  per the metrics listed in [PoppyDB § Monitoring](../poppydb.md#monitoring). Wire these into the
  same Prometheus/Grafana setup described in the
  **[Monitoring & Metrics Guide](../monitoring-metrics-guide.md)** — that guide's dashboards and
  alert-rule patterns apply to PoppyDB the same way they do to a Morphium client against real
  MongoDB, since PoppyDB answers the same commands.
- **Replication health** (replica sets only): watch `resyncCount` and `replicationLagEvents` from
  `ReplicationManager.getStats()` — see
  [PoppyDB § Replication buffer sizing](../poppydb.md#replication-buffer-sizing) for what a healthy
  vs. degrading secondary looks like.
- **Application-level messaging monitoring**: every Morphium messaging instance answers the
  built-in `morphium_status` topic regardless of backend — see
  [Messaging § Built-in Status Monitoring](../messaging.md#built-in-status-monitoring).
- **Alerting priorities**: alert on `memoryWatermark` crossing warn (backpressure starting) before
  it hits reject (writes actively failing), and on `resyncCount` incrementing more than once in a
  10-minute window (replication falling behind structurally, not transiently).

## 7. Backup and restore

PoppyDB's only persistence is periodic snapshots (`--dump-dir`/`--dump-interval`) — there is no
write-ahead log, so a snapshot is a point-in-time copy, not continuous durability. Treat backups
accordingly:

- **Snapshot cadence**: `--dump-interval` in seconds; a shorter interval bounds your worst-case
  data loss window on a crash, at the cost of periodic I/O pauses while dumping. 300s (5 min) is a
  reasonable starting point for message-broker/cache workloads where some loss is acceptable by
  design (see the loss model in [Use Cases §5](../poppydb.md#5-message-broker-for-short-lived-messages-production)).
- **Manual snapshot before risky operations** (version upgrade, config change): trigger one
  on-demand via the programmatic `dumpNow()` API, or restart the node (a final dump runs on
  shutdown) before the change.
- **Restore**: dump files (`<dbname>.morphium.gz`) in the configured `--dump-dir` are restored
  automatically on startup if present — to restore onto a fresh node, copy the snapshot files into
  its `--dump-dir` before first start.
- **Verify backups periodically**: `zcat <file>.morphium.gz | jq .` to confirm the file is valid
  and non-empty — a snapshot job that silently stopped producing usable dumps is worse than no
  backup, because it hides the gap.
- **Replica sets**: a healthy secondary is not a backup substitute — it replicates the same data
  (and the same accidental deletes) in near-real-time. Snapshots remain your only protection
  against logical corruption or a cluster-wide event.

## 8. Upgrades

- Pin the exact PoppyDB version in your deployment artifacts (same jar for all replica-set
  members) — mixed-version replica sets are not tested.
- Rolling upgrade for a replica set: upgrade secondaries first (they resync from the current
  primary on restart), then step down the primary (`replSetStepDown` or restart it last) so a
  secondary takes over — verify *some* node became primary afterward rather than waiting for a
  specific one (see [PoppyDB § StepDown/Failover Behavior](../poppydb.md#stepdown--failover-behavior-replica-set)
  for why the original primary may not reclaim leadership).
- Take a manual snapshot immediately before upgrading (see §7) regardless of your regular dump
  interval.

## 9. Best Practices Checklist

A condensed summary of the dos and don'ts from this playbook — use it as a pre-launch review, not
a replacement for reading the sections above.

**Do:**

- Run a **3-node replica set**, not a single node — a single node has no failover (see [§1](#1-prerequisites)).
- Configure via a **[configuration file](../poppydb.md#configuration-file)** (`--cfg`), not raw
  CLI flags — it's the only form that keeps secrets out of `ps aux` (see [§2](#2-minimal-secure-configuration)/[§3](#3-secrets-handling)).
- Enable **both `auth` and `ssl`** on every production instance, always together — either alone
  leaves a real gap (cleartext admin access, or encrypted-but-open access, respectively).
- Keep secrets in their **own file** via `root-password-file`/`ssl-keystore-password-file`, not
  inline in the main config — the main config can then be `0644` and live in your config
  management repo; only the secret files need `0600` and careful handling.
- Size the **heap deliberately** for your expected dataset plus watermark headroom, and **watch
  `memoryWatermark`** — don't let "reject" be the first time you learn it's close (see [§4](#4-capacity-planning)).
- **Snapshot before every upgrade**, in addition to your regular `--dump-interval`, and pin the
  exact same version across all replica-set members (see [§8](#8-upgrades)).
- **Verify backups periodically** (`zcat ... | jq .`) — a silently-broken dump job is worse than no
  backup, because it hides the gap until you need it (see [§7](#7-backup-and-restore)).
- Treat every authenticated user as **fully privileged** — authorization/roles are not enforced
  yet (see [PoppyDB § Security](../poppydb.md#security)); control access at the network boundary.

**Don't:**

- Don't put `--rootPassword`/`--sslKeystorePassword` directly in `ExecStart`, a Dockerfile `CMD`,
  or any long-lived process's argument list.
- Don't rely on a replica-set secondary as your backup — it replicates mistakes just as faithfully
  as good data. Only a snapshot protects against logical corruption or a cluster-wide event.
- Don't assume PoppyDB is a persistent system of record — there is no write-ahead log; only use it
  where the [loss model](../poppydb.md#5-message-broker-for-short-lived-messages-production) (loss
  between snapshots is acceptable) actually fits your data.
- Don't wait for "the original primary" to reclaim leadership after a failover — verify *any* node
  became primary instead (see [§8](#8-upgrades) and [PoppyDB § StepDown/Failover Behavior](../poppydb.md#stepdown--failover-behavior-replica-set)).
- Don't skip the config-file **permission warning** — `chmod 600` any file PoppyDB tells you is
  group/other-readable and contains secrets, before it becomes group/other-*writable* and PoppyDB
  refuses to start entirely.

## See Also

- [PoppyDB](../poppydb.md) — full feature reference
- [Monitoring & Metrics Guide](../monitoring-metrics-guide.md)
- [Security Guide](../security-guide.md)
- [Messaging Implementations](./messaging-implementations.md) — if PoppyDB backs Morphium
  messaging, which implementation to choose
