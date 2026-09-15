#!/usr/bin/env bash
#
# poppy-chaos.sh — local 3-node PoppyDB replica set, steady write load, deliberate failures,
# and a convergence check afterwards.
#
# The point is the last step. "convergence-after-chaos is genuinely untested" (#323): the suite
# checks dbHash equality on healthy failover only, and nothing forces a leader change mid-snapshot
# against a slow primary. This harness produces those situations on purpose and then asks whether
# the three nodes still hold the same data.
#
# It also samples replSetGetStatus once a second throughout, because the second thing being tested
# is whether a node tells the truth about itself while it is unusable (#356) — a rolling restart
# driven off a lying status is how you empty a cluster one node at a time with every check green.
#
# Nothing here touches acceptance or production. Everything runs on loopback in a temp directory.

set -uo pipefail

# ---------------------------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------------------------

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
JAR="${POPPY_JAR:-$REPO_ROOT/poppydb/target/poppydb-6.3.9-SNAPSHOT-cli.jar}"
WORKDIR="${POPPY_CHAOS_DIR:-/tmp/poppy-chaos}"
BASE_PORT="${POPPY_BASE_PORT:-27101}"
RS_NAME="servMsg"
RS_PRIORITIES="100,50,25"   # same shape as acc, so the election behaves the same way
HEAP="${POPPY_HEAP:-2g}"
DUMP_INTERVAL="${POPPY_DUMP_INTERVAL:-30}"   # short on purpose: the dump guard needs a chance to fire
PROFILE="normal"
SCENARIO="all"
KEEP=false
DUMP_INTERVAL_SET=false

DB="chaos"
COLLS=("orders" "events" "audit")
# The same list as a JS array literal, for the mongosh snippets below. Built once: expanding a
# bash array straight into JS silently produces shell quoting, not JavaScript.
COLLS_JS="[$(printf "'%s'," "${COLLS[@]}" | sed 's/,$//')]"

# ---------------------------------------------------------------------------------------------
# Load profiles
#
# Deliberately modest. Acceptance sees less than any of these; the aim is steady, realistic
# traffic during the failures, not a benchmark. Every profile is (writers, docs per batch, pause
# between batches in ms, payload bytes).
# ---------------------------------------------------------------------------------------------

declare -A PROFILES=(
  [idle]="1 1 1000 200"        # a trickle - isolates the failure from the load
  [light]="2 10 200 200"       # ~100 docs/s
  [normal]="4 25 100 500"      # ~1000 docs/s, roughly a busy message bus
  [heavy]="8 50 50 2000"       # ~8000 docs/s and bigger documents - pressure, not a record attempt
)

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

  --profile P     load profile: ${!PROFILES[*]}  (default: $PROFILE)
  --scenario S    all | kill-primary | leader-change-mid-sync | diverge | rolling-restart
                  | dump-guard
                  (default: $SCENARIO)
  --jar PATH      poppydb cli jar (default: \$POPPY_JAR or the built one in this repo)
  --heap SIZE     heap per node (default: $HEAP) - three nodes share this machine, so not 12g
  --dir PATH      work directory (default: $WORKDIR)
  --keep          leave the cluster running and the work directory in place when done
  -h, --help      this

Environment: POPPY_JAR, POPPY_CHAOS_DIR, POPPY_BASE_PORT, POPPY_HEAP, POPPY_DUMP_INTERVAL.

Profiles (writers, docs/batch, pause ms, payload bytes):
$(for p in "${!PROFILES[@]}"; do printf '  %-8s %s\n' "$p" "${PROFILES[$p]}"; done)
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --profile)  PROFILE="$2"; shift 2 ;;
        --scenario) SCENARIO="$2"; shift 2 ;;
        --jar)      JAR="$2"; shift 2 ;;
        --heap)     HEAP="$2"; shift 2 ;;
        --dir)      WORKDIR="$2"; shift 2 ;;
        --dump-interval) DUMP_INTERVAL="$2"; DUMP_INTERVAL_SET=true; shift 2 ;;
        --keep)     KEEP=true; shift ;;
        -h|--help)  usage; exit 0 ;;
        *)          echo "unknown option: $1" >&2; usage; exit 2 ;;
    esac
done

[ -n "${PROFILES[$PROFILE]:-}" ] || { echo "unknown profile: $PROFILE (have: ${!PROFILES[*]})" >&2; exit 2; }
read -r WRITERS BATCH PAUSE_MS PAYLOAD <<<"${PROFILES[$PROFILE]}"

# The dump-guard scenario is pointless with the default interval: the earlier smoke runs never
# had a tick inside a resync window, so "the guard never fired" said nothing about whether it
# works. 2s guarantees it gets the chance.
if [ "$SCENARIO" = "dump-guard" ] && ! $DUMP_INTERVAL_SET; then
    DUMP_INTERVAL=2
fi

PORTS=("$BASE_PORT" "$((BASE_PORT + 1))" "$((BASE_PORT + 2))")
SEED="127.0.0.1:${PORTS[0]},127.0.0.1:${PORTS[1]},127.0.0.1:${PORTS[2]}"
RS_URI="mongodb://$SEED/?replicaSet=$RS_NAME"

RED=$'\033[0;31m'; GRN=$'\033[0;32m'; YEL=$'\033[0;33m'; BLU=$'\033[0;34m'; CL=$'\033[0m'
say()  { echo "${BLU}==>${CL} $*"; }
ok()   { echo "${GRN}  ok${CL} $*"; }
warn() { echo "${YEL}  !!${CL} $*"; }
bad()  { echo "${RED}  XX${CL} $*"; }

FAILURES=0
note_failure() { bad "$*"; FAILURES=$((FAILURES + 1)); }

# ---------------------------------------------------------------------------------------------
# mongosh helpers
# ---------------------------------------------------------------------------------------------

# Direct connection to one node - bypasses replica-set routing, which is what we want when asking
# a specific node about itself.
node_eval() {
    local port="$1" js="$2"
    NO_PROXY=localhost,127.0.0.1 timeout 20 mongosh --quiet \
        --host "127.0.0.1:$port" --eval "$js" 2>/dev/null
}

# Through the replica set, the way a client talks to it.
rs_eval() {
    local js="$1"
    NO_PROXY=localhost,127.0.0.1 timeout 30 mongosh --quiet "$RS_URI" --eval "$js" 2>/dev/null
}

port_of_primary() {
    for p in "${PORTS[@]}"; do
        local st
        st=$(node_eval "$p" 'print(db.adminCommand({replSetGetStatus:1}).myState)' | tail -1)
        [ "$st" = "1" ] && { echo "$p"; return 0; }
    done
    return 1
}

pid_of() { cat "$WORKDIR/node$1.pid" 2>/dev/null; }

# stat's spelling differs between BSD and GNU, and this script should survive being run on the
# build host as well as on a Mac.
mtime_of() {
    [ -f "$1" ] || { echo 0; return; }
    stat -f %m "$1" 2>/dev/null || stat -c %Y "$1" 2>/dev/null || echo 0
}

# ---------------------------------------------------------------------------------------------
# Cluster lifecycle
# ---------------------------------------------------------------------------------------------

start_node() {
    local i="$1" port="${PORTS[$1]}"
    mkdir -p "$WORKDIR/node$i/dumps"
    java -Xms"$HEAP" -Xmx"$HEAP" \
        -Xlog:gc:file="$WORKDIR/node$i/gc.log" \
        -jar "$JAR" \
        --port "$port" --bind 127.0.0.1 \
        --rs-name "$RS_NAME" --rs-seed "$SEED" --rs-priorities "$RS_PRIORITIES" \
        --dump-dir "$WORKDIR/node$i/dumps" --dump-interval "$DUMP_INTERVAL" \
        --log-level INFO \
        > "$WORKDIR/node$i/poppy.log" 2>&1 &
    echo $! > "$WORKDIR/node$i/../node$i.pid"
}

wait_for_primary() {
    local deadline=$((SECONDS + ${1:-60}))
    while [ $SECONDS -lt $deadline ]; do
        local p
        p=$(port_of_primary) && { echo "$p"; return 0; }
        sleep 1
    done
    return 1
}

start_cluster() {
    say "starting 3 nodes on ${PORTS[*]} (heap $HEAP, dump every ${DUMP_INTERVAL}s)"
    for i in 0 1 2; do start_node "$i"; done

    local primary
    primary=$(wait_for_primary 90) || { note_failure "no primary within 90s"; return 1; }
    ok "primary is on port $primary"
}

stop_cluster() {
    for i in 0 1 2; do
        local pid; pid=$(pid_of "$i")
        [ -n "$pid" ] && kill "$pid" 2>/dev/null
    done
    sleep 2
    for i in 0 1 2; do
        local pid; pid=$(pid_of "$i")
        [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null
    done
}

# ---------------------------------------------------------------------------------------------
# Load
#
# Writers go through the replica-set URI so failover is exercised on the client path too. Each
# writer records what it believes it wrote; the count is compared against the cluster afterwards.
# ---------------------------------------------------------------------------------------------

start_writers() {
    say "load profile '$PROFILE': $WRITERS writers x $BATCH docs every ${PAUSE_MS}ms, ${PAYLOAD}B payload"

    for w in $(seq 1 "$WRITERS"); do
        (
            NO_PROXY=localhost,127.0.0.1 mongosh --quiet "$RS_URI" --eval "
              const pad = 'x'.repeat($PAYLOAD);
              const colls = $COLLS_JS;
              const countFile = '$WORKDIR/writer$w.count';
              let acked = 0, failed = 0, i = 0;
              const until = Date.now() + 1000 * 3600;
              while (Date.now() < until) {
                const coll = colls[i % colls.length];
                const docs = [];
                for (let k = 0; k < $BATCH; k++) {
                  docs.push({_id: 'w$w-' + i + '-' + k, w: $w, pad: pad, at: new Date()});
                }
                try {
                  const res = db.getSiblingDB('$DB').getCollection(coll).insertMany(docs, {ordered: false});
                  // Only what the cluster ACKNOWLEDGED counts. An acknowledged write that cannot be
                  // found afterwards is a lost write, and that is the assertion worth making.
                  acked += (res.insertedCount !== undefined ? res.insertedCount : docs.length);
                } catch (e) {
                  failed += docs.length;
                }
                i++;
                // Written every iteration, because the writer is killed rather than allowed to
                // finish - a summary printed at the end would never be printed at all.
                fs.writeFileSync(countFile, acked + ' ' + failed);
                sleep($PAUSE_MS);
              }
            " > "$WORKDIR/writer$w.log" 2>&1
        ) &
        echo $! >> "$WORKDIR/writers.pid"
    done
}

# A reader that talks to ONE node directly and records what it gets. The interesting outcome is
# not an error - a node that is re-syncing SHOULD answer 13436. The interesting outcome is an
# empty successful read, which is what #352 is about: a consistent-looking nothing.
#
# The probe answers exactly one question - data, no data, or an error - at constant cost. It used
# to be countDocuments(), which mongosh sends as an aggregation, which on this driver copies the
# whole collection (#355): at the heavy profile that was ~300MB per node per second and took a
# node down with an OutOfMemoryError (#372). A probe must not perturb what it measures.
start_probe_reader() {
    local port="$1"
    (
        while true; do
            local out
            out=$(node_eval "$port" "
              try {
                const d = db.getSiblingDB('$DB').getCollection('${COLLS[0]}').find({}).limit(1).toArray();
                print(d.length > 0 ? 'DATA' : 'EMPTY');
              } catch (e) {
                print('ERR ' + (e.code || '?') + ' ' + e.codeName);
              }" | tail -1)
            echo "$(date +%H:%M:%S) $out" >> "$WORKDIR/probe-$port.log"
            sleep 1
        done
    ) &
    echo $! >> "$WORKDIR/probes.pid"
}

start_status_sampler() {
    (
        while true; do
            for p in "${PORTS[@]}"; do
                local s
                s=$(node_eval "$p" '
                  const s = db.adminCommand({replSetGetStatus:1});
                  const me = (s.members||[]).find(m => m.self) || {};
                  print(s.myState + " " + (me.stateStr||"?"));' | tail -1)
                echo "$(date +%H:%M:%S) $p $s" >> "$WORKDIR/status.log"
            done
            sleep 1
        done
    ) &
    echo $! >> "$WORKDIR/probes.pid"
}

stop_background() {
    for f in "$WORKDIR/writers.pid" "$WORKDIR/probes.pid"; do
        [ -f "$f" ] || continue
        while read -r pid; do kill "$pid" 2>/dev/null; done < "$f"
        rm -f "$f"
    done
    pkill -f "mongosh --quiet $RS_URI" 2>/dev/null
}

# ---------------------------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------------------------

scenario_kill_primary() {
    say "scenario: kill the primary hard, under load"
    local primary; primary=$(port_of_primary) || { note_failure "no primary to kill"; return 1; }
    local idx=-1
    for i in 0 1 2; do [ "${PORTS[$i]}" = "$primary" ] && idx=$i; done

    warn "SIGKILL on node$idx (port $primary) - not a clean shutdown, so no final dump"
    kill -9 "$(pid_of "$idx")" 2>/dev/null

    local newp
    newp=$(wait_for_primary 60) || { note_failure "no new primary within 60s after kill"; return 1; }
    ok "new primary on port $newp"

    say "bringing node$idx back - it has no final dump, so it must sync from the new primary"
    start_node "$idx"
    sleep 15
}

scenario_leader_change_mid_sync() {
    say "scenario: leader change while a node is mid-snapshot (the #323 trigger)"
    local primary; primary=$(port_of_primary) || return 1
    local victim=-1
    for i in 0 1 2; do [ "${PORTS[$i]}" != "$primary" ] && victim=$i && break; done

    warn "stopping node$victim and wiping its dumps, so its restart is a full initial sync"
    kill -9 "$(pid_of "$victim")" 2>/dev/null
    sleep 2
    rm -rf "$WORKDIR/node$victim/dumps"/*

    say "restarting node$victim; killing the primary while its snapshot is in flight"
    start_node "$victim"
    sleep 3   # long enough to be inside the snapshot, short enough to still be in it
    local pidx=-1
    for i in 0 1 2; do [ "${PORTS[$i]}" = "$primary" ] && pidx=$i; done
    kill -9 "$(pid_of "$pidx")" 2>/dev/null
    warn "primary killed mid-snapshot"

    wait_for_primary 60 >/dev/null || note_failure "no primary within 60s after mid-sync kill"
    say "restarting the old primary"
    start_node "$pidx"
    sleep 20
}

scenario_diverge() {
    say "scenario: force a divergence, then let the node resync"
    local primary; primary=$(port_of_primary) || return 1
    local victim=-1 vport=
    for i in 0 1 2; do
        [ "${PORTS[$i]}" != "$primary" ] && victim=$i && vport="${PORTS[$i]}" && break
    done

    warn "writing a foreign document straight into node$victim's local driver (port $vport)"
    # The write has to carry \$fromPrimary at the COMMAND level - that is where preDispatch looks.
    # insertOne() puts it inside the document, where it means nothing, and the secondary refuses
    # the write with NotWritablePrimary; the scenario then "passed" having diverged nothing (#372).
    local injected
    injected=$(node_eval "$vport" "
      try {
        const res = db.getSiblingDB('$DB').runCommand(
          {insert: '${COLLS[0]}', documents: [{_id: 'divergence-marker'}], \$fromPrimary: true});
        print(res.ok === 1 && res.n === 1 ? 'injected' : 'inject failed: ' + JSON.stringify(res));
      } catch (e) { print('inject failed: ' + (e.codeName || e.message)); }" | tail -1)

    if [ "$injected" != "injected" ]; then
        note_failure "diverge scenario could not inject its marker ($injected) - nothing was diverged, so the resync was never tested"
        return 1
    fi
    ok "marker injected into node$victim"

    # The marker has to SURVIVE the restart, or "gone afterwards" proves nothing: a kill -9 right
    # after the injection leaves it out of every dump, the restart restores a dump without it, and
    # the check passes whether or not the resync cleaned anything up. Persist it on purpose, and
    # prove it is in the file before killing the node.
    local dump="$WORKDIR/node$victim/dumps/$DB.morphium.gz"
    local before; before=$(mtime_of "$dump")
    say "persisting the marker on node$victim (dumpNow) before the restart"
    node_eval "$vport" "print(JSON.stringify(db.adminCommand({dumpNow: 1})))" | tail -1 | sed 's/^/    /'
    local deadline=$((SECONDS + 30))
    while [ $SECONDS -lt $deadline ] && [ "$(mtime_of "$dump")" -le "$before" ]; do sleep 1; done
    if ! gzip -dc "$dump" 2>/dev/null | grep -aq divergence-marker; then
        note_failure "the marker did not make it into node$victim's dump within 30s - the restart would restore a state without it and the check would be vacuous"
        return 1
    fi
    ok "marker is in node$victim's dump - the restart will bring it back"

    say "restarting node$victim so the consistency check runs against the primary"
    kill -9 "$(pid_of "$victim")" 2>/dev/null
    sleep 2
    start_node "$victim"
    sleep 20

    local marker
    marker=$(node_eval "$vport" "
      try {
        print(db.getSiblingDB('$DB').getCollection('${COLLS[0]}').findOne({_id: 'divergence-marker'}) ? 1 : 0);
      } catch (e) { print('unavailable ' + (e.codeName || e.message)); }" | tail -1)

    case "$marker" in
        0) ok "the foreign document is gone - the resync replaced the local state" ;;
        1) note_failure "the foreign document survived the resync - silent divergence" ;;
        *) note_failure "could not check the marker on node$victim after 20s ($marker) - the node is still not serving" ;;
    esac
}

scenario_dump_guard() {
    say "scenario: a node dumping while its resync has the data emptied (#352)"
    say "  dump interval is ${DUMP_INTERVAL}s - short enough that a tick lands inside the window"

    # The guard can only be observed if a dump tick actually falls into the resync. That needs the
    # resync to take longer than the interval, which needs enough data to copy - the earlier smoke
    # runs had 3320 documents and were done before the first tick. Load a burst first.
    say "loading a burst so the resync takes longer than a dump interval"
    rs_eval "
      const pad = 'x'.repeat(2000);
      let inserted = 0;
      for (let b = 0; b < 20; b++) {
        const docs = [];
        for (let k = 0; k < 500; k++) docs.push({_id: 'burst-' + b + '-' + k, pad: pad});
        try {
          inserted += db.getSiblingDB('$DB').getCollection('${COLLS[0]}').insertMany(docs, {ordered: false}).insertedCount;
        } catch (e) {
          // Refused mid-failover: whatever did land is acknowledged, the rest is not.
          inserted += (e.insertedCount || 0);
        }
      }
      // Counted like the writers' acknowledgements, so the summary's 'acknowledged' and 'in
      // cluster' agree instead of the burst showing up as documents nobody wrote (#372).
      fs.writeFileSync('$WORKDIR/burst.count', String(inserted));
      print('burst loaded: ' + inserted + ' acknowledged, '
            + db.getSiblingDB('$DB').getCollection('${COLLS[0]}').estimatedDocumentCount() + ' now in the collection');" | tail -1 | sed 's/^/    /'

    local primary; primary=$(port_of_primary) || { note_failure "no primary"; return 1; }
    local victim=-1
    for i in 0 1 2; do [ "${PORTS[$i]}" != "$primary" ] && victim=$i && break; done

    warn "wiping node$victim's dumps and restarting it - a full initial sync, with dumps ticking"
    kill -9 "$(pid_of "$victim")" 2>/dev/null
    sleep 2
    rm -rf "$WORKDIR/node$victim/dumps"/*
    local dump_before
    dump_before=$(ls -1 "$WORKDIR/node$victim/dumps" 2>/dev/null | wc -l | tr -d ' ')
    start_node "$victim"

    # Watch the node through its sync, so the window is observed rather than assumed.
    local deadline=$((SECONDS + 90)) saw_sync=false
    while [ $SECONDS -lt $deadline ]; do
        local st
        st=$(node_eval "${PORTS[$victim]}" 'print(db.adminCommand({replSetGetStatus:1}).myState)' | tail -1)
        case "$st" in
            3|5) saw_sync=true ;;
            1|2) $saw_sync && break ;;
        esac
        sleep 1
    done

    $saw_sync && ok "node$victim was observed in STARTUP2/RECOVERING during the sync" \
              || warn "the sync was never sampled in a syncing state - it may still have been too fast"

    say "node$victim had $dump_before dump files when it started; letting it settle"
    sleep 10
}

scenario_rolling_restart() {
    say "scenario: rolling restart, the way the deploy pipeline does it"
    for i in 2 1 0; do
        local port="${PORTS[$i]}"
        local st
        st=$(node_eval "$port" 'print(db.adminCommand({replSetGetStatus:1}).myState)' | tail -1)

        if [ "$st" = "1" ]; then
            say "node$i is PRIMARY - stepping down first, as the pipeline does"
            node_eval "$port" 'db.adminCommand({replSetStepDown: 60})' >/dev/null
            sleep 3
        fi

        say "restarting node$i (port $port)"
        kill "$(pid_of "$i")" 2>/dev/null
        sleep 3
        start_node "$i"

        # Wait for the node to say it is usable again - and take it at its word only because
        # replSetGetStatus now distinguishes SECONDARY from STARTUP2/RECOVERING (#356).
        local deadline=$((SECONDS + 60)) ready=false
        while [ $SECONDS -lt $deadline ]; do
            local s
            s=$(node_eval "$port" 'print(db.adminCommand({replSetGetStatus:1}).myState)' | tail -1)
            [ "$s" = "1" ] || [ "$s" = "2" ] && { ready=true; break; }
            sleep 1
        done

        $ready && ok "node$i is back and reports itself usable" \
               || note_failure "node$i did not reach PRIMARY/SECONDARY within 60s"
    done
}

# ---------------------------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------------------------

quiesce() {
    say "quiescing: stopping the load and letting replication settle"
    stop_background
    sleep 15
}

verify_convergence() {
    say "convergence check: dbHash per collection across all three nodes"
    local -a hashes=()

    for p in "${PORTS[@]}"; do
        local h
        h=$(node_eval "$p" "
          const r = db.getSiblingDB('$DB').runCommand({dbHash: 1});
          if (!r.ok) { print('ERR'); } else {
            const c = r.collections || {};
            print(Object.keys(c).sort().map(k => k + '=' + c[k]).join(' '));
          }" | tail -1)
        hashes+=("$h")
        echo "    port $p: ${h:0:110}"
    done

    if [ "${hashes[0]}" = "ERR" ] || [ -z "${hashes[0]}" ]; then
        note_failure "could not read dbHash from the first node"
        return 1
    fi

    local same=true
    for h in "${hashes[@]}"; do [ "$h" = "${hashes[0]}" ] || same=false; done

    $same && ok "all three nodes agree on every collection hash" \
           || note_failure "nodes DISAGREE - this is the divergence #323 is about"
}

verify_no_silent_empty() {
    say "probe check: did any node ever answer a read with a consistent-looking nothing?"
    local found=0

    for f in "$WORKDIR"/probe-*.log; do
        [ -f "$f" ] || continue
        # An EMPTY after the first DATA is the signature: not an error, just nothing.
        local first_data
        first_data=$(grep -an " DATA$" "$f" | head -1 | cut -d: -f1)
        [ -z "$first_data" ] && continue
        local empties
        empties=$(tail -n +"$first_data" "$f" | grep -ac " EMPTY$")
        [ "$empties" -gt 0 ] && { note_failure "$(basename "$f"): $empties empty-but-successful reads after data existed"; found=1; }
    done

    [ "$found" = "0" ] && ok "no empty successful reads - unusable nodes answered with an error instead"

    local rejects
    rejects=$(cat "$WORKDIR"/probe-*.log 2>/dev/null | grep -ac "ERR 13436")
    say "  (the probe was correctly refused with 13436 $rejects times)"
}

verify_dump_guard() {
    say "dump check: did any node dump while it was re-syncing?"
    local skipped
    # grep -a: a control character in a logged document payload makes grep call the log "binary"
    # and print nothing for it, silently skipping that node (#372).
    skipped=$(grep -ah "Skipping periodic dump: this node is re-syncing\|Not dumping: this node is re-syncing" \
              "$WORKDIR"/node*/poppy.log 2>/dev/null | wc -l | tr -d ' ')

    if [ "$skipped" -gt 0 ]; then
        ok "the guard refused $skipped dump(s) during a resync window (#352)"
    elif [ "$SCENARIO" = "dump-guard" ] || [ "$SCENARIO" = "all" ]; then
        # With a ${DUMP_INTERVAL}s interval and a resync that was observed to take longer, a tick
        # must have landed inside the window. Not firing then means the guard is not working -
        # not that it was never asked.
        note_failure "the guard never fired although a dump tick had to fall inside the resync window"
    else
        warn "the guard never fired - no dump coincided with a resync in this scenario"
    fi

    # An empty dump over a good one is the damage this prevents. Any dump file that shrank to
    # near-nothing while the node holds data is suspicious.
    for i in 0 1 2; do
        local d="$WORKDIR/node$i/dumps/$DB.morphium.gz"
        [ -f "$d" ] || continue
        local sz; sz=$(wc -c < "$d" | tr -d ' ')
        [ "$sz" -lt 200 ] && note_failure "node$i's dump of $DB is $sz bytes - suspiciously empty"
    done
}

verify_dumping_resumed() {
    # The mirror image of the dump guard, and the check that was missing: a guard that never lets
    # go is worse than no guard. The flag behind it ("a sync emptied this store") is cleared by
    # exactly one event - a completed sync - so if anything leaves it set, the node stops
    # persisting for the rest of its life and nothing in the earlier checks would notice. That is
    # the shape of a real regression that shipped once already today.
    say "recovery check: does every node dump again once it has finished syncing?"

    local -a before=()
    for i in 0 1 2; do
        local f="$WORKDIR/node$i/dumps/$DB.morphium.gz"
        before+=("$(mtime_of "$f")")
    done

    local wait_s=$((DUMP_INTERVAL + 10))
    say "  waiting ${wait_s}s for at least one dump tick"
    sleep "$wait_s"

    for i in 0 1 2; do
        local f="$WORKDIR/node$i/dumps/$DB.morphium.gz"
        local now_mtime; now_mtime=$(mtime_of "$f")
        local was="${before[$i]}"
        local state
        state=$(node_eval "${PORTS[$i]}" 'print(db.adminCommand({replSetGetStatus:1}).myState)' | tail -1)

        if [ "$state" != "1" ] && [ "$state" != "2" ]; then
            warn "node$i is in state $state - still not usable, so not dumping is correct"
        elif [ "$now_mtime" -gt "$was" ]; then
            ok "node$i dumped again after settling"
        else
            note_failure "node$i has not dumped since before the wait although it reports state $state - the resync guard looks stuck, which disables its persistence for good"
        fi
    done
}

verify_status_honesty() {
    say "status check: what did the nodes say about themselves while they were unusable?"
    local states
    states=$(awk '{print $3, $4}' "$WORKDIR/status.log" 2>/dev/null | sort | uniq -c | sort -rn)
    echo "$states" | sed 's/^/    /'

    if echo "$states" | grep -qE "STARTUP2|RECOVERING"; then
        ok "nodes reported STARTUP2/RECOVERING while syncing instead of claiming SECONDARY (#356)"
    else
        warn "no STARTUP2/RECOVERING was ever sampled - the syncs may have been too fast to catch"
    fi
}

report_errors() {
    say "errors in the node logs"
    local n
    n=$(grep -ahcE "ERROR|OutOfMemory|Exception in thread" "$WORKDIR"/node*/poppy.log 2>/dev/null | paste -sd+ - | bc 2>/dev/null || echo 0)
    if [ "${n:-0}" -gt 0 ]; then
        warn "$n error lines - first few:"
        grep -ahE "ERROR|OutOfMemory|Exception in thread" "$WORKDIR"/node*/poppy.log 2>/dev/null | head -5 | sed 's/^/    /'
    else
        ok "no ERROR lines"
    fi
}

report_writes() {
    say "writes: acknowledged versus actually present"
    local acked=0 failed=0

    # burst.count is what scenario_dump_guard loaded through rs_eval - acknowledged writes like
    # any other, just not from a writer process.
    for f in "$WORKDIR"/writer*.count "$WORKDIR"/burst.count; do
        [ -f "$f" ] || continue
        local a b
        read -r a b < "$f"
        acked=$((acked + ${a:-0}))
        failed=$((failed + ${b:-0}))
    done

    # estimatedDocumentCount() is the count command, which PoppyDB answers from the collection
    # size - exact on this server, and not the collection-copying aggregation (#355, #372).
    local present
    present=$(rs_eval "
      let n = 0;
      $COLLS_JS.forEach(c => n += db.getSiblingDB('$DB').getCollection(c).estimatedDocumentCount());
      print(n);" | tail -1)
    present=${present:-0}

    echo "    acknowledged: $acked"
    echo "    rejected:     $failed  (expected during failover - the client saw an error and knows)"
    echo "    in cluster:   $present"

    if [ "$acked" -gt 0 ] && [ "$present" -lt "$acked" ]; then
        note_failure "$((acked - present)) acknowledged write(s) cannot be found - lost writes"
    elif [ "$acked" -gt 0 ]; then
        ok "every acknowledged write is present"
    else
        warn "no acknowledged writes recorded - did the writers connect?"
    fi
}

# ---------------------------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------------------------

[ -f "$JAR" ] || { echo "jar not found: $JAR (build it: mvn -pl poppydb -am package -DskipTests)" >&2; exit 1; }
command -v mongosh >/dev/null || { echo "mongosh is required" >&2; exit 1; }

say "work directory: $WORKDIR"
rm -rf "$WORKDIR"; mkdir -p "$WORKDIR"

# Logged so a failure of a client-side assumption (runCommand with a top-level \$fromPrimary,
# estimatedDocumentCount() mapping to the count command) is attributable to the mongosh in use.
say "mongosh $(mongosh --version 2>/dev/null | head -1), jar $(basename "$JAR")"

trap 'echo; say "interrupted - cleaning up"; stop_background; $KEEP || stop_cluster; exit 130' INT TERM

start_cluster || { stop_cluster; exit 1; }

start_status_sampler
for p in "${PORTS[@]}"; do start_probe_reader "$p"; done
start_writers

say "letting the load build for 20s before breaking anything"
sleep 20

case "$SCENARIO" in
    all)
        scenario_kill_primary
        scenario_leader_change_mid_sync
        scenario_diverge
        scenario_dump_guard
        scenario_rolling_restart
        ;;
    kill-primary)           scenario_kill_primary ;;
    leader-change-mid-sync) scenario_leader_change_mid_sync ;;
    diverge)                scenario_diverge ;;
    rolling-restart)        scenario_rolling_restart ;;
    dump-guard)             scenario_dump_guard ;;
    *) echo "unknown scenario: $SCENARIO" >&2; stop_background; stop_cluster; exit 2 ;;
esac

quiesce
echo
say "================ results ================"
verify_convergence
verify_no_silent_empty
verify_dump_guard
verify_dumping_resumed
verify_status_honesty
report_errors
report_writes
echo

if [ "$FAILURES" -eq 0 ]; then
    echo "${GRN}PASS${CL} — profile '$PROFILE', scenario '$SCENARIO': no failures"
else
    echo "${RED}FAIL${CL} — profile '$PROFILE', scenario '$SCENARIO': $FAILURES failure(s); logs in $WORKDIR"
fi

if $KEEP; then
    say "leaving the cluster running (--keep). Ports: ${PORTS[*]}. Stop it with: pkill -f poppydb.*cli.jar"
else
    stop_cluster
fi

[ "$FAILURES" -eq 0 ] && exit 0 || exit 1
