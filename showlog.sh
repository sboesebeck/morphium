#!/usr/bin/env bash

cd $(dirname $0)
if [ "q$1" = "q--failed" ]; then
  f=$(./runtests.sh --stats --noreason --nosum | grep -v "Calculating" | fzf)
  bat test.log/$f.log
else

  bat $(FZF_DEFAULT_COMMAND="fd --hidden --exclude .git --no-ignore-vcs . test.log/" fzf)
fi
