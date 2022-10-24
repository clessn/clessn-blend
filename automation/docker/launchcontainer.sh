#!/bin/sh

cd $CLESSN_ROOT_DIR/clessn-blend/automation/docker
docker compose run clessn-blend /home/clessn/dev/clessn/clessn-blend/automation/$1 $2 $3 $4 $5 $6 $7
docker container ls --filter status=exited -q | xargs docker container rm
