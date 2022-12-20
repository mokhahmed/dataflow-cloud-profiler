# dataflow-cloud-profiler

## How to integrate Google cloud profile with Dataflow jobs**

1. By passing the --dataflowServiceOptions options

    1.1 To enable CPU profiling, start the pipeline with the following option.
    
    ```
    --dataflowServiceOptions=enable_google_cloud_profiler
    ```

   1.2 To enable heap profiling, start the pipeline with the following options. Heap profiling requires Java 11 or higher.
    ```shell
    --dataflowServiceOptions=enable_google_cloud_profiler
    --dataflowServiceOptions=enable_google_cloud_heap_sampling
    
    ```

    Example with mvn build. 
    ```shell
    mvn compile -e exec:java \
     -Dexec.mainClass=$MAIN \
          -Dexec.args="--project=$PROJECT \
          --stagingLocation=gs://$BUCKET/staging/ $* \
          --tempLocation=gs://$BUCKET/staging/ \
          --runner=DataflowRunner \
          --numWorkers=2\
          --dataflowServiceOptions=enable_google_cloud_profiler\
          --dataflowServiceOptions=enable_google_cloud_heap_sampling"
    ```

2. Programmatically, By creating and agent object with the below configurations.
 ```java
  DataflowProfilingOptions profilingOptions = options.as(DataflowProfilingOptions.class);    
  DataflowProfilingAgentConfiguration agent = new DataflowProfilingOptions.DataflowProfilingAgentConfiguration();
  agent.put("APICurated", true);
  profilingOptions.setProfilingAgentConfiguration(agent);
```

##**Types of profiling available**

Cloud Profiler supports different types of profiling based on the language in which a program is written. 
The following table summarizes the supported profile types by language:

Profile type|Go|Java|Node.js|Python
---|---|---|---|---
CPU time |Y|Y| |Y
Heap|Y|Y|Y
Allocated heap|Y			
Contention|Y	
Threads|Y			
Wall time| |Y|Y|Y

For complete information on the language requirements and any restrictions, see the language's how-to page.
For more information about these profile types, see[ Profiling concepts.](https://cloud.google.com/profiler/docs/concepts-profiling)

### To Run the Demo 

1. Create a pub/sub topic sensor_events 
2. Run the python script to generate the sample data ```python send_sensor_xml_data.py --project PROJECCT_ID --speedFactor 60```
3. run the dataflow job 
