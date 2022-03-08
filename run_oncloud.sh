#!/bin/bash

if [ "$#" -lt 3 ]; then
   echo "Usage:   ./run_oncloud.sh project-name bucket-name classname [options] "
   echo "Example: ./run_oncloud.sh cloud-training-demos cloud-training-demos CurrentConditions --bigtable"
   exit
fi

PROJECT=$1
shift
BUCKET=$1
shift
MAIN=org.example.runners.$1
shift
CPU_LOAD=org.example.runners.$1
shift
MEM_LOAD=org.example.runners.$1
shift

echo "Launching $MAIN project=$PROJECT bucket=$BUCKET $* CPU_LOAD=$CPU_LOAD MEM_LOAD=$MEM_LOAD"

export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH
mvn compile -e exec:java \
 -Dexec.mainClass=$MAIN \
      -Dexec.args="--project=$PROJECT \
      --bqTable=demos.sensor_events\
      --cpuLoad=$CPU_LOAD\
      --memLoad=$MEM_LOAD\
      --stagingLocation=gs://$BUCKET/staging/ $* \
      --tempLocation=gs://$BUCKET/staging/ \
      --runner=DataflowRunner\
      --numWorkers=2\
      --jobName=xmlsensoreventtobqv2"


# If you run into quota problems, add this option the command line above
#     --maxNumWorkers=2
# In this case, you will not be able to view autoscaling, however.