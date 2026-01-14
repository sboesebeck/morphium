#!/usr/bin/env bash

cd $(dirname $0)/..
if [ "q$1" = "q--failed" ]; then
  f=$(./runtests.sh --stats --noreason --nosum | grep -v "Calculating" | fzf)
  bat $(find test.log -name $f.log)
elif [ "q$1" = "q--tail" ]; then

  tail -n 100 -f $(FZF_DEFAULT_COMMAND="fd --hidden --exclude .git --no-ignore-vcs . test.log/" fzf)
else

  bat $(FZF_DEFAULT_COMMAND="fd --hidden --exclude .git --no-ignore-vcs . test.log/" fzf)
fi
