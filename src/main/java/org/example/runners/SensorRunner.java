package org.example.runners;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowProfilingOptions;
import org.apache.beam.runners.dataflow.options.DataflowProfilingOptions.DataflowProfilingAgentConfiguration;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.example.jobs.AbstractPipeline;
import org.example.jobs.MyPipelineOptions;
import org.example.jobs.SensorXmlEventToBQJob;
import org.example.models.SensorEvent;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Integer.MAX_VALUE;


public class SensorRunner{

    public void init(String[] args) {


    }

    public static void main(String[] args) {

        PipelineOptionsFactory.register(MyPipelineOptions.class);
        MyPipelineOptions ops = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(MyPipelineOptions.class);

        DataflowProfilingOptions profilingOptions = ops.as(DataflowProfilingOptions.class);
        ///profilingOptions.setSaveProfilesToGcs("gs://" + ops.getProject() + "/profiler");

        // --setProfilingAgentConfiguration= {"APICurated", true}
        DataflowProfilingAgentConfiguration agent = new DataflowProfilingOptions.DataflowProfilingAgentConfiguration();
        agent.put("APICurated", true);
        profilingOptions.setProfilingAgentConfiguration(agent);
        Pipeline pipe = Pipeline.create(ops);

        SensorXmlEventToBQJob job = new SensorXmlEventToBQJob();
        job.setProjectName(ops.getProject());
        job.setTopicName(ops.getTopic());
        job.setBqTable(ops.getBqTable());
        job.setCpuLoad(ops.getCpuLoad());
        job.setMemLoad(ops.getMemLoad());

        job.execute(pipe);
        pipe.run();
    }
}
