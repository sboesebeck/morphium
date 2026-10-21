# PoppyDB Admin Handbook

Day-2 operations notes for a PoppyDB instance that is already deployed and running. This page
does **not** repeat initial setup — see the
**[Production Deployment Playbook](./poppydb-deployment.md)** for that (secrets handling, systemd
unit, capacity planning, monitoring, backup/restore, upgrades) and
**[Migrating from MongoDB to PoppyDB](./migration-mongodb-to-poppydb.md)** for moving an existing
workload over. This page covers what tends to come up *after* that: running without systemd,
keeping logs under control, validating a change before you make it, and a handful of field-tested
gotchas that aren't obvious from the reference docs alone.

## 1. Process supervision without systemd

The deployment playbook's [§5](./poppydb-deployment.md#5-run-it-as-a-service-systemd) covers
systemd, which is the recommended default. If a target host can't use systemd (older init system,
a jump-host-managed environment, a container base image without it), use a small pidfile-based
control script instead of `ps`/`pgrep` pattern matching:

```bash
#!/bin/bash
# poppy-ctl.sh: start | stop | restart | status
set -u
BASE=/opt/poppydb
JAR=$BASE/poppydb.jar
CFG=$BASE/conf/poppydb.conf
LOG=$BASE/poppydb.log
PIDFILE=$BASE/poppydb.pid
HEAP=4g

alive() { [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; }

case "${1:-}" in
  start)
    if alive; then echo "already running (PID $(cat "$PIDFILE"))"; exit 0; fi
    cd "$BASE" || exit 1
    setsid nohup java -Xmx$HEAP -jar "$JAR" --cfg "$CFG" >> "$LOG" 2>&1 < /dev/null &
    echo $! > "$PIDFILE"
    sleep 3
    alive && echo "started (PID $(cat "$PIDFILE"))" || { echo "START FAILED - see $LOG"; exit 1; }
    ;;
  stop)
    if ! alive; then echo "not running"; rm -f "$PIDFILE"; exit 0; fi
    PID=$(cat "$PIDFILE")
    kill "$PID"
    # shutdown performs a final dump - give it time before escalating
    for i in $(seq 1 30); do kill -0 "$PID" 2>/dev/null || break; sleep 1; done
    kill -0 "$PID" 2>/dev/null && kill -9 "$PID"
    rm -f "$PIDFILE"
    echo "stopped"
    ;;
  restart) "$0" stop && "$0" start ;;
  status)
    if alive; then echo "running (PID $(cat "$PIDFILE"))"; else echo "stopped"; exit 1; fi
    ;;
  *) echo "Usage: $0 start|stop|restart|status"; exit 2 ;;
esac
```

**Why pidfile, not `pgrep -f "poppydb.jar"`:** a pattern match against the process command line
also matches the command line of whatever *invoked* it — a remote-ops script or an SSH session
that itself contains the string `poppydb.jar` (e.g. because it just built the `start` command) can
match itself and get killed. This has bitten real deployments during scripted rolling restarts.
Match by pidfile, not by pattern, whenever you're driving `start`/`stop` from another script.

## 2. Log rotation

The bundled Logback configuration ([PoppyDB § Logging](../poppydb.md#logging)) controls
*verbosity* (`--log-level`), not *rotation* — left alone, a `>>`-appended log file grows
unbounded. Two ways to bound it:

- **External `logrotate`**, if the process writes to a plain file (as in the `poppy-ctl.sh`
  pattern above, `>> "$LOG"`): use `copytruncate` so the running JVM doesn't need to reopen its
  file handle.

  ```
  # /etc/logrotate.d/poppydb
  /opt/poppydb/poppydb.log {
    copytruncate
    compress
    delaycompress
    missingok
    notifempty
    rotate 7
    daily
    maxsize 500M
  }
  ```

  If a log-shipping agent (Promtail, Filebeat, Fluent Bit, …) reads the same file, grant it read
  access explicitly in a `postrotate` step (e.g. `setfacl -m u:<shipper-user>:r <logfile>`) —
  rotation can otherwise leave the shipper holding a stale file handle pointed at the now-renamed
  file.

- **Full Logback replacement** via `-Dlogback.configurationFile=/path/to/my-logback.xml`
  ([PoppyDB § Logging](../poppydb.md#logging)) with a `RollingFileAppender` — use this if you want
  the JVM itself to own rotation (size- or time-based policies, no `copytruncate` gap) instead of
  an external tool.

**Shipping to a log aggregator:** PoppyDB has no built-in shipper. The straightforward pattern is:
dedicated log directory per instance, `logrotate` (or the Logback appender above) keeping it
bounded, and your existing log-shipping agent's config pointed at that directory — the same way
you'd wire up any other JVM service's logs. There's nothing PoppyDB-specific to configure beyond
making sure the shipper can read the (rotated) file.

## 3. Before you change anything: validate first

Before rolling out a config change or a version upgrade to a replica set:

```bash
java -jar poppydb-cli.jar --cfg conf/poppydb.conf --check-config   # exit 0 = OK, 1 = errors
java -jar poppydb-cli.jar --cfg conf/poppydb.conf --print-config   # effective config, secrets redacted
```

`--check-config` validates syntax, semantic cross-checks, and deep checks (keystore loadable,
dump-dir usable, users-file parses) **without starting the server** — run it against the new
config/JAR before touching a live node. `--print-config` shows the fully merged effective
configuration (defaults + file + CLI, with per-key source annotations) — useful for confirming
what a node will *actually* run with before you restart it, especially when CLI flags and a config
file are both in play (see [Configuration precedence](../poppydb.md#configuration-file)).

Combine with a manual snapshot immediately before the change (see
[Deployment Playbook §8, Upgrades](./poppydb-deployment.md#8-upgrades)) — cheap insurance, and the
one your rollback plan will actually need if something goes wrong.

**Cross-version dump/restore:** dumps are generally forward-readable (see
[Persistence § dumps written before 6.3.2](../poppydb.md#persistence-periodic-snapshots)), but
always verify a restore against the *target* version before relying on it in production — a dump
taken by one build and restored by a materially different build is the one scenario worth testing
explicitly rather than assuming, especially across a version jump you haven't run before.

## 4. Field notes: reconnection after a node replacement or failover

When a node is replaced (a MongoDB→PoppyDB cutover on the same host/port, a version upgrade, or an
ordinary failover), connected clients see their change-stream resume tokens invalidated — this is
**expected**, not a malfunction (see
[StepDown/Failover Behavior](../poppydb.md#stepdown-failover-behavior-replica-set)). A
well-behaved client library reconnects and re-establishes its change stream on its own; you should
**not** need to restart every connected service as a matter of routine.

Treat a coordinated restart of dependent services as a **safety net, not a required step**:
- After a planned node replacement, watch application logs/metrics for successful reconnects
  first.
- Only restart a specific service if it's still failing to reconnect after a reasonable grace
  period — older or misbehaving client library versions occasionally get stuck reporting "no
  primary found" after a failover instead of retrying; a targeted restart of *that* service clears
  it. This is a client-side bug class, not something to work around by restarting the whole fleet
  preemptively.
- If you find yourself routinely needing a full rolling restart after every failover, that's worth
  investigating as a client-library issue rather than accepting it as normal operating procedure.

## See Also

- [PoppyDB Production Deployment Playbook](./poppydb-deployment.md) — initial setup, secrets,
  systemd, capacity planning, monitoring, backup/restore, upgrades
- [PoppyDB](../poppydb.md) — full feature reference
- [Migrating from MongoDB to PoppyDB](./migration-mongodb-to-poppydb.md)
