#!/usr/bin/env bash

BASEPORT=17017
NODES=1
COMPILE=true

COMMMAND=$1
TMPDIR=/tmp/poppydb

ONLYNODE=0

if [[ "$1" = "start" ]]; then
  echo "Starting server"

elif [[ "$1" = "startnode" ]]; then
  ONLYNODE=$2
  echo "Starting node $2"
  shift
elif [[ "$1" = "stopnode" ]]; then
  if [ ! -e $TMPDIR/node-$2.pid ]; then
    echo "not running"
    exit 1
  fi
  kill -9 $(<$TMPDIR/node-$2.pid)
  rm $TMPDIR/node-$2.pid
  exit 0
elif [[ "$1" = "stop" ]]; then
  echo "Stopping running server"
  if [ ! -e $TMPDIR/node-1.pid ]; then
    echo "No node1 running..."
    exit 0
  fi
  for i in $TMPDIR/node-*.pid; do
    echo "Stopping $i"
    kill -9 $(<$i)
    rm -f $i
  done
  exit 0
elif [[ "$1" = "status" ]]; then
  if [ ! -d $TMPDIR ]; then
    echo "No PoppyDB nodes configured (TMPDIR $TMPDIR does not exist)."
    exit 0
  fi
  found=false
  for i in $TMPDIR/node-*.pid; do
    if [ ! -e "$i" ]; then
      continue
    fi
    found=true
    node_num=$(basename $i | sed 's/node-\([0-9]*\)\.pid/\1/')
    pid=$(<$i)
    if kill -0 $pid 2>/dev/null; then
       echo "Node $node_num: Running (PID: $pid)"
       lsof -Pan -p $pid -i | grep LISTEN | sed 's/.*TCP \(.*\):\(.*\) (LISTEN)/\tListening on: \1:\2/' || echo "\tNo listening port found for this PID"
    else
       echo "Node $node_num: Not running (PID file exists with PID $pid, but process is dead)"
    fi
  done
  if [ "$found" = false ]; then
    echo "No PoppyDB nodes found in $TMPDIR"
  fi
  exit 0
elif [[ "$1" = "viewlogs" ]]; then
  multitail $TMPDIR/*.log
  exit 0
else
  echo "Unknown command $1 - start | stop | status | viewlogs"
  exit 1
fi
shift

# Start!
while [[ -n $1 ]]; do
  if [[ "$1" = "-p" ]] || [[ "$1" = "--port" ]]; then
    BASEPORT=$2
    echo "Using port $BASEPORT"
    shift
    shift
  elif [[ "$1" = "-nc" ]] || [[ "$1" = "--nocompile" ]]; then
    COMPILE=false
    shift
  elif [[ "$1" = "-n" ]] || [[ "$1" = "--nodes" ]]; then
    NODES=$2
    if [ "$NODES" -gt 5 ]; then
      echo "Too many nodes, max 5"
      exit 1
    fi

    shift
    shift
  elif [[ "$1" = "-h" ]] || [[ "$1" = "--help" ]]; then
    echo "$0 -p PORT -n NUM_NODES"
    exit 0
  else
    echo "Unknown option $1"
    exit 1
  fi
done

if [ ! -e $TMPDIR ]; then
  mkdir $TMPDIR
fi
if $COMPILE; then
  mvn -Dmaven.test.skip=true -Dmaven.javadoc.skip=true package -pl poppydb -am || exit 1
  mv poppydb/target/*-cli.jar $TMPDIR/poppydb.jar
else
  if [ ! -e $TMPDIR/poppydb.jar ]; then
    echo "No PoppyDB installation found"
    exit 1
  fi
fi
cd $TMPDIR

if [ $NODES -eq 1 ]; then
  if lsof -Pi :$BASEPORT -sTCP:LISTEN -t >/dev/null; then
    echo "Port $BASEPORT is already in use!"
    exit 1
  fi
  echo "Starting single node PoppyDB on port $BASEPORT"
  java -Xmx8G -jar $TMPDIR/poppydb.jar -p $BASEPORT >$TMPDIR/poppydb-1.log 2>&1 &
  pid=$!
  echo "$pid" >$TMPDIR/node-1.pid
  sleep 2
  if ! kill -0 $pid 2>/dev/null; then
    echo "Failed to start single node PoppyDB, check $TMPDIR/poppydb-1.log"
    cat $TMPDIR/poppydb-1.log
    rm $TMPDIR/node-1.pid
    exit 1
  fi
else

  p=$BASEPORT
  prio=100
  prioList=""
  nodeList=""
  sep=""
  for n in $(seq $NODES); do
    node="localhost:$p"
    prioList="$prioList$sep$prio"
    nodeList="$nodeList$sep$node"
    sep=","
    let prio=prio-10
    let p=p+1
  done

  p=$BASEPORT
  for n in $(seq $NODES); do
    if lsof -Pi :$p -sTCP:LISTEN -t >/dev/null; then
      echo "Port $p is already in use, skipping node $n"
      continue
    fi
    if [ $ONLYNODE -eq 0 ] || [ $ONLYNODE -eq $n ]; then
      echo "Starting node $n PoppyDB on port $p, replicaset rstst, prios $prioList, nodes: $nodeList"

      java -Xmx8G -jar $TMPDIR/poppydb.jar -p $p --rs-name tstrs --rs-seed "$nodeList" --rs-priorities "$prioList" >$TMPDIR/poppydb-$n.log 2>&1 &
      pid=$!
      echo "$pid" >$TMPDIR/node-$n.pid
      sleep 1
      if ! kill -0 $pid 2>/dev/null; then
        echo "Failed to start node $n PoppyDB, check $TMPDIR/poppydb-$n.log"
        cat $TMPDIR/poppydb-$n.log
        rm $TMPDIR/node-$n.pid
      fi
    fi
    let p=p+1

  done
fi
