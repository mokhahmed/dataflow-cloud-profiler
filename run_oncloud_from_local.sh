#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIALS="/Users/moukhtar/ws/mk-sa-1-key.json"

if [ "$#" -lt 3 ]; then
   echo "Usage:   ./run_oncloud_from_local.sh project-name bucket-name classname [options] "
   exit
fi

PROJECT=$1
shift
BUCKET=$1
shift
MAIN=org.example.runners.$1
shift

echo "Launching $MAIN project=$PROJECT bucket=$BUCKET $*"

"$MAVEN_HOME"/mvn compile -e exec:java \
 -Dexec.mainClass=$MAIN \
      -Dexec.args="--project=$PROJECT \
      --bqTable=sensor_events\
      --region=europe-central2\
      --stagingLocation=gs://$BUCKET/staging/ $* \
      --tempLocation=gs://$BUCKET/staging/ \
      --runner=DataflowRunner\
      --numWorkers=2\
      --dataflowServiceOptions=enable_google_cloud_profiler\
      --dataflowServiceOptions=enable_google_cloud_heap_sampling"
