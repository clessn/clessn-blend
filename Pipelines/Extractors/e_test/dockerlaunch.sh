#!/bin/sh


# Check if Docker is running
if (! docker stats --no-stream ); then
  # On Mac OS this would be the terminal command to launch Docker
  echo "Docker is not running"
else
  echo "Docker is running"
fi

# Wait until Docker daemon is running and has completed initialisation
while (! docker stats --no-stream ); do
  # Docker takes a few seconds to initialize
  echo "Waiting for Docker to launch..."
  sleep 10
done


cd /Users/patrick/Dev/CLESSN/clessn-blend/Pipelines/Extractors/e_test

if [[ -f "status.json" ]]; then
   result=`cat status.json`
   echo "$(date) starting e_test. current job status: " $result
else
   result='"not running"'
   echo "$(date) starting e_test. current job status: " $result
fi


if [[ $result = '"running"' ]]; then 
   echo "$(date) Waiting for job to finish..."
fi


while [[ $result = '"running"' ]]
do
   sleep 10
   curl -s -X GET http://localhost:8080/status | jq .status > status.json
   echo "$(date) job status $(cat status.json)" 
   result=`cat status.json`
done

dock=`docker container ls -f name=pipeline -aq`
if [[ $dock = "" ]]; then
   echo "$(date) starting container"
   sudo -u patrick /usr/local/bin/docker-compose up -d pipeline 
else
   echo "$(date) waiting for another pipeline container to terminate..."
   while [[ $dock != ""  ]]
   do
      sleep 10
      dock=`docker container ls -f name=pipeline -aq`
      echo "$(date) waiting for another pipeline container to terminate..."
   done
   echo "$(date) starting container"
   sudo -u patrick /usr/local/bin/docker-compose up -d pipeline
fi

sleep 10

echo "$(date) launching code"
curl -s -X POST -H "Content-Type: application/json" -d '{"commands": ["Rscript", "code/code.R"], "env_vars": {"HUB3_URL":"'"$HUB3_URL"'", "HUB3_USERNAME":"'"$HUB3_USERNAME"'", "HUB3_PASSWORD":"'"$HUB3_PASSWORD"'", "LOG_PATH":"."}}' http://localhost:8080/run > status.json

sleep 3
echo "$(date) job status : $(jq .status status.json) - start time: $(jq .start_time status.json)"

curl -s -X GET http://localhost:8080/status | jq .status > status.json
echo "$(date) job status $(cat status.json)"
result=`cat status.json`

while [[ $result = '"running"' ]] 
do
   sleep 10
   curl -s -X GET http://localhost:8080/status | jq .status > status.json
   echo "$(date) job status $(cat status.json)"
   result=`cat status.json`
done


curl -s -X GET http://localhost:8080/status | jq > output.json

jq .stdout output.json | sed 's/\\n/'""'/g' > stdout.json
jq .stderr output.json | sed 's/\\n/'""'/g' > stderr.json
jq .return_code output.json | sed 's/\\n/'""'/g' > return_code.json

echo "$(date) job return_code $(cat return_code.json)"
echo "$(date) job log stdout $(cat stdout.json)"
echo "$(date) job log stderr $(cat stderr.json)"

ret=`cat return_code.json`

if [[ $ret = '1' ]]; then
   echo "$(date) job terminated with an error.  Error log generated in /Users/patrick/Logs/e_test-$(date +%Y-%m-%d.%H:%M:%S).log"
   cat return_code.json stderr.json stdout.json > "/Users/patrick/Logs/e_test-$(date +%Y-%m-%d.%H:%M:%S).log"
fi

if [[ $ret = '2' ]]; then
   echo "$(date) job terminated with warning(s).  Error log generated in /Users/patrick/Logs/e_test-$(date +%Y-%m-%d.%H:%M:%S).log"
   cat return_code.json stderr.json stdout.json > "/Users/patrick/Logs/e_test-$(date +%Y-%m-%d.%H:%M:%S).log"
fi

if [[ $ret = '0' ]]; then
   echo "$(date) job terminated successfully"
fi

docker stop e_test-pipeline-1 
sudo -u patrick /usr/local/bin/docker ps --filter name=pipeline --filter status=exited -aq | xargs docker rm

rm stdout.json
rm stderr.json
rm return_code.json
rm status.json
rm output.json

