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

echo "Launching $MAIN project=$PROJECT bucket=$BUCKET $*"

"$MAVEN_HOME"/mvn compile -e exec:java \
 -Dexec.mainClass=$MAIN \
      -Dexec.args="--project=$PROJECT \
       --region=us-east1\
      --stagingLocation=gs://$BUCKET/staging/ $* \
      --tempLocation=gs://$BUCKET/staging/ \
      --runner=DataflowRunner\
      --numWorkers=2\
      --dataflowServiceOptions=enable_google_cloud_profiler\
      --dataflowServiceOptions=enable_google_cloud_heap_sampling"
