#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIALS="/Users/moukhtar/ws/mk-sa-1-key.json"

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
LOAD_FLAG=$1
shift

echo "Launching $MAIN project=$PROJECT bucket=$BUCKET $* LOAD_FLAG=$LOAD_FLAG"

#export PATH=/usr/lib/jvm/java-8-openjdk-amd64/bin/:$PATH

"$MAVEN_HOME"/mvn compile -e exec:java \
 -Dexec.mainClass=$MAIN \
      -Dexec.args="--project=$PROJECT \
      --bqTable=demos.sensor_events\
      --loadFlag=load_Flag\
       --region=europe-central2\
      --stagingLocation=gs://$BUCKET/staging/ $* \
      --tempLocation=gs://$BUCKET/staging/ \
      --runner=DataflowRunner\
      --numWorkers=2\
      --dataflowServiceOptions=enable_google_cloud_profiler\
      --dataflowServiceOptions=enable_google_cloud_heap_sampling"
