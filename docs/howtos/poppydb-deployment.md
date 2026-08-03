# PoppyDB Production Deployment Playbook

This is a task-oriented, step-by-step guide to running PoppyDB in production. For the full
feature reference (all CLI flags, replica-set internals, admin command support, performance
numbers) see **[PoppyDB](../poppydb.md)** — this page only covers *how to get from a bare JAR to
a securely configured, monitored, backed-up production instance*, in order.

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
both are opt-in for backward compatibility with test/dev usage:

```bash
# Generate a keystore once (see PoppyDB § SSL/TLS for details)
keytool -genkeypair -alias poppydb -keyalg RSA -keysize 2048 \
  -validity 3650 -keystore /etc/poppydb/server.jks -storepass "$KEYSTORE_PASSWORD" \
  -dname "CN=poppydb.internal.example.com"

java -jar poppydb-cli.jar \
  --port 27017 --bind 0.0.0.0 \
  --auth --rootUser admin --rootPassword "$ROOT_PASSWORD" \
  --ssl --sslKeystore /etc/poppydb/server.jks --sslKeystorePassword "$KEYSTORE_PASSWORD" \
  --rs-name prod-rs --rs-seed node1:27017,node2:27017,node3:27017
```

Repeat on the other two nodes with the same `--rs-name`/`--rs-seed` and each node's own bind
address. See [PoppyDB § Replica Set Behavior](../poppydb.md#replica-set-behavior-experimental) for
how initial sync and elections work.

**Do not skip `--auth` or `--ssl` in production** — without them, anyone who can reach the port
has full read/write/admin access in cleartext. Authorization is authentication-only right now
(roles are stored but not evaluated, see [PoppyDB § Security](../poppydb.md#security)) — treat
every authenticated user as an admin and control access at the network boundary, not by role.

## 3. Secrets handling (until config-file support lands)

`--rootPassword` and `--sslKeystorePassword` on the command line are visible to any local user via
`ps aux` or `/proc/<pid>/cmdline` for the life of the process. Until PoppyDB gains file-based
configuration, mitigate this with your process supervisor rather than the command line directly:

- **systemd**: put secrets in an `EnvironmentFile` with `0600` permissions (root-only readable,
  see the unit file in [§5](#5-run-it-as-a-service-systemd) below) and reference them as
  `${ROOT_PASSWORD}` in `ExecStart` — systemd expands the environment before exec, so the
  expanded values never appear in `ps aux` the way literal CLI arguments would.
- **Docker/Compose**: use `secrets:` (Swarm) or an env file excluded from your image/VCS, not
  hardcoded `CMD` arguments (the [Dockerfile example in PoppyDB § Use Cases](../poppydb.md#4-docker-deployment)
  is fine for local/CI use, but bakes the password into the image layer — do not reuse it verbatim
  for a production image with real secrets).
- **Kubernetes**: mount a `Secret` as environment variables into the pod spec, same reasoning.

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
EnvironmentFile=/etc/poppydb/secrets.env
ExecStart=/usr/bin/java -Xms4g -Xmx4g -jar /opt/poppydb/poppydb-cli.jar \
  --port 27017 --bind 0.0.0.0 \
  --auth --rootUser admin --rootPassword ${ROOT_PASSWORD} \
  --ssl --sslKeystore /etc/poppydb/server.jks --sslKeystorePassword ${KEYSTORE_PASSWORD} \
  --rs-name prod-rs --rs-seed node1:27017,node2:27017,node3:27017 \
  --dump-dir /var/lib/poppydb/snapshots --dump-interval 300 \
  --log-level INFO
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

```bash
# /etc/poppydb/secrets.env - chmod 600, owned by root:poppydb
ROOT_PASSWORD=change-me
KEYSTORE_PASSWORD=change-me-too
```

```bash
sudo chmod 600 /etc/poppydb/secrets.env
sudo chown root:poppydb /etc/poppydb/secrets.env
sudo systemctl daemon-reload
sudo systemctl enable --now poppydb
sudo systemctl status poppydb
```

`--log-level` controls verbosity without editing the bundled Logback config — see
[PoppyDB § Logging](../poppydb.md#logging) if you need more (e.g. a rotating file appender via
`-Dlogback.configurationFile`).

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

## See Also

- [PoppyDB](../poppydb.md) — full feature reference
- [Monitoring & Metrics Guide](../monitoring-metrics-guide.md)
- [Security Guide](../security-guide.md)
- [Messaging Implementations](./messaging-implementations.md) — if PoppyDB backs Morphium
  messaging, which implementation to choose
