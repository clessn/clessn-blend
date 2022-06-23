#!/bin/sh


cd /Users/patrick/Dev/CLESSN/clessn-blend/Pipelines/Extractors/e_agoraplus-pressreleases-qc

export result=`jq .status output.json`
echo "initial status " $result

while [ $result = '"running"' ] 
do
   echo "sleeping"
   sleep 10
   curl -X GET http://localhost:8080/status > output.json
   export result=`jq .status output.json`
   echo "first loop " $result
done

echo "starting container"
sudo -u patrick /usr/local/bin/docker-compose up -d pipeline 

echo "sleeping"
sleep 10

echo "launching code"
curl -X POST -H "Content-Type: application/json" -d '{"commands": ["Rscript", "code/code.R"], "env_vars": {"HUB3_URL":"'"$HUB3_URL"'", "HUB3_USERNAME":"'"$HUB3_USERNAME"'", "HUB3_PASSWORD":"'"$HUB3_PASSWORD"'", "LOG_PATH":"."}}' http://localhost:8080/run > output.json

echo "sleeping"
sleep 10
curl -X GET http://localhost:8080/status > output.json
export result=`jq .status output.json`
echo "second stage " $result

while [ $result = '"running"' ] 
do
   echo "sleeping"
   sleep 10
   curl -X GET http://localhost:8080/status > output.json
   export result=`jq .status output.json`
   echo "second loop " $result
done

docker stop e_agoraplus-pressreleases-qc-pipeline-1 
sudo -u patrick /usr/local/bin/docker ps --filter name=pipeline --filter status=exited -aq | xargs docker rm
