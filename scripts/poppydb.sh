#!/bin/bash

# Functions for managing a local PoppyDB instance for testing.
# Sourced by runtests.sh.

function _pdb_port_open() {
  local host="$1"
  local port="$2"
  nc -z "$host" "$port" >/dev/null 2>&1
}

function _pdb_wait_for_port() {
  local host="$1"
  local port="$2"
  local timeout_s="${3:-30}"
  local end=$((SECONDS + timeout_s))
  while [ $SECONDS -lt $end ]; do
    if nc -z "$host" "$port" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.3
  done
  echo -e "${RD}Error:${CL} Timed out waiting for ${host}:${port}" >&2
  return 1
}

function _pdb_wait_for_primary() {
  local uri_in="$1"
  local timeout_s="${2:-45}"
  local end=$((SECONDS + timeout_s))
  while [ $SECONDS -lt $end ]; do
    if mongosh --quiet --norc "$uri_in" --eval "db.hello().isWritablePrimary" 2>/dev/null | grep -q "true"; then
      return 0
    fi
    sleep 0.5
  done
  echo -e "${RD}Error:${CL} Timed out waiting for primary on ${uri_in}" >&2
  return 1
}

function _pdb_parse_hosts_from_uri() {
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

function _pdb_find_cli_jar() {
  local jar
  jar=$(ls -1 poppydb/target/*-cli*.jar 2>/dev/null | head -n 1)
  if [ -z "$jar" ]; then
    jar=$(ls -1 poppydb/target/poppydb-*-cli.jar 2>/dev/null | head -n 1)
  fi
  # Legacy fallback: old location
  if [ -z "$jar" ]; then
    jar=$(ls -1 target/*server-cli*.jar 2>/dev/null | head -n 1)
  fi
  # Fallback to stable copy (survives mvn clean)
  if [ -z "$jar" ]; then
    local pid_dir="${poppydbLocalPidDir:-${morphiumserverLocalPidDir:-.poppydb-local}}"
    local stable="${pid_dir}/poppydb.jar"
    if [ -f "$stable" ]; then
      jar="$stable"
    fi
    if [ -z "$jar" ]; then
      stable="${pid_dir}/morphiumserver.jar"
      if [ -f "$stable" ]; then
        jar="$stable"
      fi
    fi
  fi
  echo "$jar"
}

function _pdb_start_cluster() {
  local uri_in="$1"
  local rs_name="${2:-rs0}"
  local pid_dir="${poppydbLocalPidDir:-${morphiumserverLocalPidDir:-.poppydb-local}}"
  mkdir -p "$pid_dir" "$pid_dir/logs"

  local max_conn="${poppydbMaxConnections:-${morphiumserverMaxConnections}}"
  local sock_timeout="${poppydbSocketTimeout:-${morphiumserverSocketTimeout:-30}}"
  local heap_size="${poppydbHeapSize:-${morphiumserverHeapSize:-8g}}"
  local jvm_opts="${poppydbJvmOpts:-${morphiumserverJvmOpts:--Xmx${heap_size} -Xms${heap_size} -XX:+UseG1GC -XX:MaxGCPauseMillis=50 -XX:+ParallelRefProcEnabled}}"
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
  jar=$(_pdb_find_cli_jar)
  if [ -z "$jar" ] || find poppydb/src/main/java -newer "$jar" 2>/dev/null | head -n 1 | grep -q .; then
    echo -e "${BL}Info:${CL} Building PoppyDB CLI jar..."
    mvn -Dmaven.test.skip=true -Dmaven.javadoc.skip=true package -pl poppydb -am
    jar=$(_pdb_find_cli_jar)
  fi

  if [ -z "$jar" ]; then
    echo -e "${RD}Error:${CL} Could not locate PoppyDB cli jar under poppydb/target/ (build failed?)"
    return 1
  fi

  # Copy jar to stable location (safe from mvn clean)
  local stable_jar="${pid_dir}/poppydb.jar"
  cp "$jar" "$stable_jar"
  jar="$stable_jar"
  echo -e "${BL}Info:${CL} Copied server jar to ${stable_jar}"

  local hostports
  hostports=$(_pdb_parse_hosts_from_uri "$uri_in")
  if [ -z "$hostports" ]; then
    if [ "${poppydbSingleNode:-${morphiumserverSingleNode:-0}}" -eq 1 ]; then
      hostports="localhost:17017"
    else
      hostports=$'localhost:17017\nlocalhost:17018\nlocalhost:17019'
    fi
  fi

  local seed
  seed=$(echo "$hostports" | tr '\n' ',' | sed 's/,$//')

  if [ "${poppydbSingleNode:-${morphiumserverSingleNode:-0}}" -eq 1 ]; then
    echo -e "${BL}Info:${CL} Starting PoppyDB single-node (${seed}) [maxConn=${max_conn}, timeout=${sock_timeout}s]"
  else
    echo -e "${BL}Info:${CL} Starting PoppyDB replica set ${GN}${rs_name}${CL} (${seed}) [maxConn=${max_conn}, timeout=${sock_timeout}s]"
  fi

  local hp
  while IFS= read -r hp; do
    [ -z "$hp" ] && continue
    local host="${hp%:*}"
    local port="${hp##*:}"

    if [[ "$host" != "localhost" && "$host" != "127.0.0.1" && "$host" != "::1" ]]; then
      echo -e "${YL}Warning:${CL} --poppydb only starts local hosts; skipping ${host}:${port}"
      continue
    fi

    if _pdb_port_open "$host" "$port"; then
      echo -e "${BL}Info:${CL} ${host}:${port} already listening - not starting"
      continue
    fi

    local log_file="${pid_dir}/logs/poppydb_${port}.log"
    local conn_args="--max-connections $max_conn --socket-timeout $sock_timeout"
    if [ "${poppydbSingleNode:-${morphiumserverSingleNode:-0}}" -eq 1 ]; then
      nohup java $jvm_opts -jar "$jar" --bind "$host" --port "$port" $conn_args >"$log_file" 2>&1 &
    else
      nohup java $jvm_opts -jar "$jar" --bind "$host" --port "$port" --rs-name "$rs_name" --rs-seed "$seed" $conn_args >"$log_file" 2>&1 &
    fi
    local pid=$!
    disown
    echo "$pid" >"${pid_dir}/${host}_${port}.pid"
    poppydbLocalStarted=1
    morphiumserverLocalStarted=1
    echo -e "${BL}Info:${CL} Started PoppyDB on ${host}:${port} (pid ${pid}) - log ${log_file}"
  done <<<"$hostports"

  # Wait for ports
  while IFS= read -r hp; do
    [ -z "$hp" ] && continue
    local host="${hp%:*}"
    local port="${hp##*:}"
    _pdb_wait_for_port "$host" "$port" 30 || return 1
  done <<<"$hostports"

  # Wait for primary via mongosh
  _pdb_wait_for_primary "$uri_in" 60 || return 1

  return 0
}

function _pdb_cleanup() {
  if [ "${poppydbLocalStarted:-${morphiumserverLocalStarted:-0}}" -ne 1 ]; then
    return 0
  fi

  local pid_dir="${poppydbLocalPidDir:-${morphiumserverLocalPidDir:-.poppydb-local}}"
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
  poppydbLocalStarted=0
  morphiumserverLocalStarted=0
  return 0
}

function _pdb_ensure_cluster() {
  local uri_in="$1"

  local hostports
  hostports=$(_pdb_parse_hosts_from_uri "$uri_in")
  if [ -z "$hostports" ]; then
    if [ "${poppydbSingleNode:-${morphiumserverSingleNode:-0}}" -eq 1 ]; then
      hostports="localhost:17017"
    else
      hostports=$'localhost:17017\nlocalhost:17018\nlocalhost:17019'
    fi
  fi

  local all_ok=1
  local hp
  while IFS= read -r hp; do
    [ -z "$hp" ] && continue
    local host="${hp%:*}"
    local port="${hp##*:}"
    if ! _pdb_port_open "$host" "$port"; then
      all_ok=0
      break
    fi
  done <<<"$hostports"

  if [ "$all_ok" -eq 1 ]; then
    if [ "${startPoppydbLocal:-${startMorphiumserverLocal:-0}}" -eq 1 ] && [ "${poppydbLocalStarted:-${morphiumserverLocalStarted:-0}}" -ne 1 ]; then
      echo -e "${YL}Warning:${CL} Local cluster already listening for ${GN}${uri_in}${CL} - not auto-starting PoppyDB."
    fi
    return 0
  fi

  if [ "${startPoppydbLocal:-${startMorphiumserverLocal:-0}}" -eq 1 ]; then
    _pdb_start_cluster "$uri_in" "rs0" || return 1
    return 0
  fi

  echo -e "${RD}Error:${CL} Local cluster is not reachable for URI:"
  echo -e "  ${GN}${uri_in}${CL}"
  echo -e ""
  echo -e "Start it first (MongoDB or PoppyDB), or to auto-start PoppyDB use:"
  echo -e "  ${BL}./runtests.sh --poppydb ...${CL}  (single node, recommended)"
  echo -e "  ${BL}./runtests.sh --poppydb-replicaset ...${CL}  (3-node replica set)"
  echo -e ""
  echo -e "Tip: check ports and PoppyDB logs under ${GN}${poppydbLocalPidDir:-${morphiumserverLocalPidDir:-.poppydb-local}}/logs/${CL}"
  exit 1
}

# Backward compatibility aliases
function _ms_local_cleanup() { _pdb_cleanup "$@"; }
function _ms_local_ensure_cluster() { _pdb_ensure_cluster "$@"; }
function _ms_local_start_cluster() { _pdb_start_cluster "$@"; }
function _ms_local_find_server_cli_jar() { _pdb_find_cli_jar "$@"; }
function _ms_local_port_open() { _pdb_port_open "$@"; }
function _ms_local_wait_for_port() { _pdb_wait_for_port "$@"; }
function _ms_local_wait_for_primary() { _pdb_wait_for_primary "$@"; }
function _ms_local_parse_hosts_from_uri() { _pdb_parse_hosts_from_uri "$@"; }
