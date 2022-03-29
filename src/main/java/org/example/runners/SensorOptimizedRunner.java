package org.example.runners;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.example.jobs.MyPipelineOptions;
import org.example.jobs.SensorXmlEventToBQJob;
import org.example.jobs.SensorXmlEventToBQJobOptimized;


public class SensorOptimizedRunner {

    public static void main(String[] args) {

        PipelineOptionsFactory.register(MyPipelineOptions.class);
        MyPipelineOptions ops = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(MyPipelineOptions.class);


        Pipeline pipe = Pipeline.create(ops);
        SensorXmlEventToBQJobOptimized job = new SensorXmlEventToBQJobOptimized();
        job.setProjectName(ops.getProject());
        job.setTopicName(ops.getTopic());
        job.setBqTable(ops.getBqTable());
        job.setJobName(ops.getJobName());
        job.execute(pipe);
        pipe.run();
    }
}
