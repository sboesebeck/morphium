#!/bin/bash

failed=$(./getStats.sh --noreason --nosum)

if [ ! -z "$1" ]; then
  failed=$(./getStats.sh --noreason --nosum | grep "$1")
else
  failed=$(./getStats.sh --noreason --nosum)
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
  ./runtests.sh --retry 0 --nodel $cls $m
done
