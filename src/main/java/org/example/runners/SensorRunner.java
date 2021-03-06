package org.example.runners;


import org.apache.beam.runners.dataflow.options.DataflowProfilingOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.example.jobs.MyPipelineOptions;
import org.example.jobs.SensorXmlEventToBQJob;
import org.example.jobs.SensorXmlEventToBQJobOptimized;


public class SensorRunner{

    public static void main(String[] args) {

        PipelineOptionsFactory.register(MyPipelineOptions.class);
        MyPipelineOptions ops = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(MyPipelineOptions.class);



//         DataflowProfilingOptions profilingOptions = ops.as(DataflowProfilingOptions.class);
//         profilingOptions.setSaveProfilesToGcs("gs://" + ops.getProject() + "/profiler");
//
//         DataflowProfilingOptions.DataflowProfilingAgentConfiguration agent = new DataflowProfilingOptions.DataflowProfilingAgentConfiguration();
//         agent.put("APICurated", true);
//         profilingOptions.setProfilingAgentConfiguration(agent);



        Pipeline pipe = Pipeline.create(ops);

        SensorXmlEventToBQJob job = new SensorXmlEventToBQJob();
        job.setProjectName(ops.getProject());
        job.setTopicName(ops.getTopic());
        job.setBqTable(ops.getBqTable());
        job.setJobName(ops.getJobName());
        job.execute(pipe);
        pipe.run();
    }
}
