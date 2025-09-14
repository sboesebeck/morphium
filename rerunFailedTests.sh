#!/bin/bash

# Forwarding options to runtests.sh
includeTags=""
excludeTags=""
driver=""
uri=""
useExternal=0
verbose=0

pattern=""

while [ $# -ne 0 ]; do
  case "$1" in
    --tags)
      shift; includeTags="$1"; shift ;;
    --exclude-tags)
      shift; excludeTags="$1"; shift ;;
    --driver)
      shift; driver="$1"; shift ;;
    --uri)
      shift; uri="$1"; shift ;;
    --external)
      useExternal=1; shift ;;
    --verbose)
      verbose=1; shift ;;
    --help|-h)
      echo "Usage: $0 [--tags LIST] [--exclude-tags LIST] [--driver NAME] [--uri URI] [--external] [--verbose] [PATTERN]"
      exit 0 ;;
    *)
      # treat as grep pattern for class names
      pattern="$1"; shift ;;
  esac
done

failed=$(./getStats.sh --noreason --nosum)

if [ -n "$pattern" ]; then
  failed=$(echo "$failed" | grep "$pattern")
fi
RD='\033[0;31m'
GN='\033[0;32m'
BL='\033[0;34m'
YL='\033[0;33m'
MG='\033[0;35m'
CN='\033[0;36m'
CL='\033[0m'

echo -e "${MG}Rerunning$CL ${CN}failed tests...$CL"

for f in $failed; do
  echo -e "----- > Failed test: ${RD}$f$CL"
  cls=${f%#*}
  m=${f#*#}
  m=${m/(*/}
  m=$(echo "$m" | tr -d '"')
  if [ "$m" == "$cls" ]; then
    m=""
  fi
  #m=$(echo "$m" | sed -e 's/\\(.*$//' )
  echo -e "${YL}Re-Running$CL tests in $BL$cls$CL Method $GN$m$CL"
  args=(--retry 0 --nodel)
  [ -n "$includeTags" ] && args+=(--tags "$includeTags")
  [ -n "$excludeTags" ] && args+=(--exclude-tags "$excludeTags")
  [ -n "$driver" ] && args+=(--driver "$driver")
  [ -n "$uri" ] && args+=(--uri "$uri")
  [ "$useExternal" -eq 1 ] && args+=(--external)
  [ "$verbose" -eq 1 ] && args+=(--verbose)
  ./runtests.sh "${args[@]}" $cls $m
done
