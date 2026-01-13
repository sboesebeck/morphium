#!/bin/bash

# This script contains functions for managing a local MorphiumServer instance for testing.
# It is sourced by the main runtests.sh script.

function _ms_local_port_open() {
  local host="$1"
  local port="$2"
  python3 - "$host" "$port" <<'PY'
import socket, sys
host = sys.argv[1]
port = int(sys.argv[2])
s = socket.socket()
s.settimeout(0.5)
try:
    s.connect((host, port))
    sys.exit(0)
except Exception:
    sys.exit(1)
finally:
    try:
        s.close()
    except Exception:
        pass
PY
}

function _ms_local_wait_for_port() {
  local host="$1"
  local port="$2"
  local timeout_s="${3:-30}"
  python3 - "$host" "$port" "$timeout_s" <<'PY'
import socket, sys, time
host = sys.argv[1]
port = int(sys.argv[2])
deadline = time.time() + float(sys.argv[3])
last = None
while time.time() < deadline:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            sys.exit(0)
    except Exception as e:
        last = e
        time.sleep(0.2)
print(f"Timed out waiting for {host}:{port} - last output: {last}", file=sys.stderr)
sys.exit(1)
PY
}

function _ms_local_wait_for_primary() {
  local uri_in="$1"
  local timeout_s="${2:-45}"
  local pid_dir="${morphiumserverLocalPidDir:-.morphiumserver-local}"
  local jar
  jar=$(_ms_local_find_server_cli_jar)
  if [ -z "$jar" ]; then
    echo -e "${RD}Error:${CL} Cannot probe local cluster readiness - server-cli jar missing under target/"
    return 1
  fi

  mkdir -p "$pid_dir"
  local probe_java="${pid_dir}/MorphiumServerProbe_${PID}.java"
  local probe_class="MorphiumServerProbe_${PID}"

  cat >"$probe_java" <<'JAVA'
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wire.PooledDriver;
public class MorphiumServerProbe_0 {
  public static void main(String[] args) throws Exception {
    String uri = args[0];
    String hostsCsv = args[1];
    String[] hosts = hostsCsv.split(",");
    PooledDriver d = new PooledDriver();
    d.setHostSeed(hosts);
    d.connect(null);
    // Primary must be selectable for any write-heavy tests.
    d.getPrimaryConnection(null).close();
    System.out.println("OK primary selectable for " + uri);
  }
}
JAVA

  # Replace placeholder class name with unique name
  if sed --version >/dev/null 2>&1; then
    sed -i.bak "s/public class MorphiumServerProbe_0/public class ${probe_class}/" "$probe_java" 2>/dev/null || true
  else
    sed -i '' -e "s/public class MorphiumServerProbe_0/public class ${probe_class}/" "$probe_java" 2>/dev/null || true
  fi
  rm -f "$probe_java.bak" >/dev/null 2>&1 || true

  javac -cp "$jar" "$probe_java" >/dev/null 2>&1 || true

  # Parse hosts for seeding the driver (no DB part, no scheme)
  local hostports
  hostports=$(_ms_local_parse_hosts_from_uri "$uri_in" | tr '\n' ',' | sed 's/,$//')
  if [ -z "$hostports" ]; then
    hostports="localhost:17017,localhost:17018,localhost:17019"
  fi

  python3 - "$jar" "$pid_dir" "$probe_class" "$uri_in" "$hostports" "$timeout_s" <<'PY'
import subprocess, sys, time
jar, pid_dir, cls, uri, hosts, timeout_s = sys.argv[1:]
deadline = time.time() + float(timeout_s)
last = None
while time.time() < deadline:
    try:
        p = subprocess.run(["java", "-cp", f"{jar}:{pid_dir}", cls, uri, hosts],
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=10)
        if p.returncode == 0:
            sys.exit(0)
        last = p.stdout.strip()
    except Exception as e:
        last = str(e)
    time.sleep(0.5)
print(f"Timed out waiting for primary to become selectable - last output: {last}", file=sys.stderr)
sys.exit(1)
PY
}

function _ms_local_parse_hosts_from_uri() {
  local uri_in="$1"
  local u="$uri_in"

  # Strip scheme
  u="${u#mongodb://}"
  u="${u#mongodb+srv://}"

  # Strip everything after first /
  u="${u%%/*}"

  # Strip credentials if present
  if [[ "$u" == *"@"* ]]; then
    u="${u##*@}"
  fi

  local IFS=,
  local parts=($u)
  local p
  for p in "${parts[@]}"; do
    # remove query (if any leaked into host list)
    p="${p%%\n*}"
    p="${p// /}"
    [ -z "$p" ] && continue

    local host=""
    local port=""

    # IPv6 in brackets: [::1]:17017
    if [[ "$p" == \[*\]* ]]; then
      host="${p%%]*}"
      host="${host#[}"
      local rest="${p#*]}"
      if [[ "$rest" == :* ]]; then
        port="${rest#:}"
      else
        port="17017"
      fi
    else
      if [[ "$p" == *":"* ]]; then
        host="${p%:*}"
        port="${p##*:}"
      else
        host="$p"
        port="17017"
      fi
    fi

    [ -z "$host" ] && host="localhost"
    [ -z "$port" ] && port="17017"
    echo "${host}:${port}"
  done
}

function _ms_local_find_server_cli_jar() {
  local jar
  jar=$(ls -1 target/*server-cli*.jar 2>/dev/null | head -n 1)
  if [ -z "$jar" ]; then
    jar=$(ls -1 target/*-server-cli.jar 2>/dev/null | head -n 1)
  fi
  echo "$jar"
}

function _ms_local_start_cluster() {
  local uri_in="$1"
  local rs_name="${2:-rs0}"
  local pid_dir="${morphiumserverLocalPidDir:-.morphiumserver-local}"
  mkdir -p "$pid_dir" "$pid_dir/logs" test.log

  # Calculate max-connections based on parallel slots if not explicitly set
  # Formula: base 2000 + 500 per parallel slot (MorphiumServer uses NIO, can handle many connections)
  # AsyncOperationTest alone uses 1134+ connections, so we need large pool for parallel tests
  local max_conn="${morphiumserverMaxConnections}"
  local sock_timeout="${morphiumserverSocketTimeout:-30}"
  local heap_size="${morphiumserverHeapSize:-8g}"
  local jvm_opts="${morphiumserverJvmOpts:--Xmx${heap_size} -Xms${heap_size} -XX:+UseG1GC -XX:MaxGCPauseMillis=50 -XX:+ParallelRefProcEnabled}"
  echo -e "${BL}Info:${CL} JVM settings: heap=${heap_size}, GC=G1GC"
  if [ -z "$max_conn" ]; then
    if [ -n "$parallel" ] && [ "$parallel" -gt 1 ]; then
      max_conn=$((2000 + parallel * 500))
      echo -e "${BL}Info:${CL} Auto-calculated max-connections=${max_conn} for ${parallel} parallel slots"
    else
      max_conn=2000
    fi
  fi

  local jar
  jar=$(_ms_local_find_server_cli_jar)
  if [ -z "$jar" ] || find src/main/java -newer "$jar" 2>/dev/null | head -n 1 | grep -q .; then
    echo -e "${BL}Info:${CL} Building MorphiumServer CLI jar (mvn -DskipTests package)..."
    mvn -DskipTests -Dmaven.javadoc.skip=true package
    jar=$(_ms_local_find_server_cli_jar)
  fi

  if [ -z "$jar" ]; then
    echo -e "${RD}Error:${CL} Could not locate server-cli jar under target/ (build failed?)"
    return 1
  fi

  # Copy jar to stable location to prevent issues if mvn clean runs while servers are active
  local stable_jar="${pid_dir}/morphiumserver.jar"
  cp "$jar" "$stable_jar"
  jar="$stable_jar"
  echo -e "${BL}Info:${CL} Copied server jar to ${stable_jar} (safe from mvn clean)"

  local hostports
  hostports=$(_ms_local_parse_hosts_from_uri "$uri_in")
  if [ -z "$hostports" ]; then
    if [ "${morphiumserverSingleNode:-0}" -eq 1 ]; then
      hostports="localhost:17017"
    else
      hostports="localhost:17017'$'\n'localhost:17018'$'\n''localhost:17019'"
    fi
  fi

  # Build seed list from the URI host list to support non-default ports.
  local seed
  seed=$(echo "$hostports" | tr '\n' ',' | sed 's/,$//')

  if [ "${morphiumserverSingleNode:-0}" -eq 1 ]; then
    echo -e "${BL}Info:${CL} Starting MorphiumServer single-node (${seed}) [maxConn=${max_conn}, timeout=${sock_timeout}s]"
  else
    echo -e "${BL}Info:${CL} Starting MorphiumServer replica set ${GN}${rs_name}${CL} (${seed}) [maxConn=${max_conn}, timeout=${sock_timeout}s]"
  fi

  local hp
  while IFS= read -r hp; do
    [ -z "$hp" ] && continue
    local host="${hp%:*}"
    local port="${hp##*:}"

    # Only auto-start for local addresses. If the URI points elsewhere, we can't safely start it.
    if [[ "$host" != "localhost" && "$host" != "127.0.0.1" && "$host" != "::1" ]]; then
      echo -e "${YL}Warning:${CL} --morphium-server only starts local hosts; skipping ${host}:${port}"
      continue
    fi

    if _ms_local_port_open "$host" "$port"; then
      echo -e "${BL}Info:${CL} ${host}:${port} already listening - not starting"
      continue
    fi

    local log_file="${pid_dir}/logs/morphiumserver_${port}.log"
    local conn_args="--max-connections $max_conn --socket-timeout $sock_timeout"
    if [ "${morphiumserverSingleNode:-0}" -eq 1 ]; then
      # Single-node mode: no replica set arguments (using Netty async I/O)
      nohup java $jvm_opts -jar "$jar" --bind "$host" --port "$port" $conn_args >"$log_file" 2>&1 &
    else
      # Replica set mode (using Netty async I/O)
      nohup java $jvm_opts -jar "$jar" --bind "$host" --port "$port" --rs-name "$rs_name" --rs-seed "$seed" $conn_args >"$log_file" 2>&1 &
    fi
    local pid=$!
    # Detach from the job table so that the main status loop ignores these helper processes.
    disown
    echo "$pid" >"${pid_dir}/${host}_${port}.pid"
    morphiumserverLocalStarted=1
    echo -e "${BL}Info:${CL} Started MorphiumServer on ${host}:${port} (pid ${pid}) - log ${log_file}"
  done <<<"$hostports"

  # Wait for the requested ports to be reachable (best-effort).
  while IFS= read -r hp; do
    [ -z "$hp" ] && continue
    local host="${hp%:*}"
    local port="${hp##*:}"
    _ms_local_wait_for_port "$host" "$port" 30 || return 1
  done <<<"$hostports"

  # Ports being open is not enough; wait until a primary is actually selectable.
  _ms_local_wait_for_primary "$uri_in" 60 || return 1

  return 0
}

function _ms_local_cleanup() {
  if [ "${morphiumserverLocalStarted:-0}" -ne 1 ]; then
    return 0
  fi

  local pid_dir="${morphiumserverLocalPidDir:-.morphiumserver-local}"
  if [ ! -d "$pid_dir" ]; then
    return 0
  fi

  local pidfile
  for pidfile in "$pid_dir"/*.pid; do
    [ -f "$pidfile" ] || continue
    local pid
    pid=$(cat "$pidfile" 2>/dev/null)
    if [ -n "$pid" ]; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done

  # Give processes a moment to stop cleanly
  sleep 1

  for pidfile in "$pid_dir"/*.pid; do
    [ -f "$pidfile" ] || continue
    local pid
    pid=$(cat "$pidfile" 2>/dev/null)
    if [ -n "$pid" ]; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
    rm -f "$pidfile" >/dev/null 2>&1 || true
  done

  rmdir "$pid_dir" >/dev/null 2>&1 || true
  morphiumserverLocalStarted=0
  return 0
}

function _ms_local_ensure_cluster() {
  local uri_in="$1"

  local hostports
  hostports=$(_ms_local_parse_hosts_from_uri "$uri_in")
  if [ -z "$hostports" ]; then
    if [ "${morphiumserverSingleNode:-0}" -eq 1 ]; then
      hostports="localhost:17017"
    else
      hostports="localhost:17017$'\n''localhost:17018'$'\n''localhost:17019'"
    fi
  fi

  local all_ok=1
  local hp
  while IFS= read -r hp; do
    [ -z "$hp" ] && continue
    local host="${hp%:*}"
    local port="${hp##*:}"
    if ! _ms_local_port_open "$host" "$port"; then
      all_ok=0
      break
    fi
  done <<<"$hostports"

  if [ "$all_ok" -eq 1 ]; then
    if [ "${startMorphiumserverLocal:-0}" -eq 1 ] && [ "${morphiumserverLocalStarted:-0}" -ne 1 ]; then
      echo -e "${YL}Warning:${CL} Local cluster already listening for ${GN}${uri_in}${CL} - not auto-starting MorphiumServer."
    fi
    return 0
  fi

  if [ "${startMorphiumserverLocal:-0}" -eq 1 ]; then
    _ms_local_start_cluster "$uri_in" "rs0" || return 1
    return 0
  fi

  echo -e "${RD}Error:${CL} Local cluster is not reachable for URI:"
  echo -e "  ${GN}${uri_in}${CL}"
  echo -e ""
  echo -e "Start it first (MongoDB or MorphiumServer), or to auto-start MorphiumServer use:"
  echo -e "  ${BL}./runtests.sh --morphium-server ...${CL}  (single node, recommended)"
  echo -e "  ${BL}./runtests.sh --morphium-server-replicaset ...${CL}  (3-node replica set)"
  echo -e ""
  echo -e "Tip: check ports and MorphiumServer logs under ${GN}${morphiumserverLocalPidDir:-.morphiumserver-local}/logs/${CL}"
  exit 1
}
