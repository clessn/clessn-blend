#!/bin/sh

if [ $1 != "extract" ] && [ $1 != "load" ] && [ $1 != "refine" ]; then
  echo "argument 1 must be extract | load | refine"
  exit
fi

if [ $2 != "detached" ] && [ $2 != "attached" ]; then
  echo "argument 2 must be attached or detached to specify if you want the container must run respectively in the foreground or the background"
  exit
fi

FILE=$CLESSN_ROOT_DIR/clessn-blend/automation/$3.sh

if [ ! -f "$FILE" ]; then
  echo "script $FILE specified in argument 3 does not exist"
  exit
fi

cd $CLESSN_ROOT_DIR/clessn-blend/automation/docker

if [ $2 = "detached" ]; then
  docker compose run  -d clessn-blend /home/clessn/dev/clessn/clessn-blend/automation/$3.sh $@
else
  docker compose run clessn-blend /home/clessn/dev/clessn/clessn-blend/automation/$3.sh $@
  docker container ls --filter status=exited -q | xargs docker container rm
fi

