#!/usr/bin/env bash

BASEPORT=17017
NODES=1
COMPILE=true

COMMMAND=$1
TMPDIR=/tmp/morphiumServer

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
  echo "Not implemented yet"
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
  mvn -DskipTests clean package || exit 1
  mv target/*server-cli.jar $TMPDIR/mserver.jar
else
  if [ ! -e $TMPDIR/mserver.jar ]; then
    echo "No morphiumserver installation found"
    exit 1
  fi
fi
cd $TMPDIR

if [ $NODES -eq 1 ]; then
  echo "Starting single node morphiumServer"
  java -Xmx8G -jar $TMPDIR/mserver.jar -p $BASEPORT >$TMPDIR/mserver-1.log 2>&1 &
  echo "$!" >$TMPDIR/node1.pid
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
    if [ $ONLYNODE -eq 0 ] || [ $ONLYNODE -eq $n ]; then
      echo "Starting node $n morphiumServer on port $p, replicaset rstst, prios $prioList, nodes: $nodeList"

      java -Xmx8G -jar $TMPDIR/mserver.jar -p $p --rs-name tstrs --rs-seed "$nodeList" --rs-priorities "$prioList" >$TMPDIR/mserver-$n.log 2>&1 &
      echo "$!" >$TMPDIR/node-$n.pid
    fi
    let p=p+1

  done
fi
