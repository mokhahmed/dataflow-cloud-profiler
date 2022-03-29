package org.example.jobs;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.example.models.SensorEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;


public class SensorXmlEventToBQJobOptimized extends AbstractPipeline{

    private static int LOAD_FACTOR =1000000;
    private static int NUM_SHARDS =3;

    private String topicName;
    private String projectName;
    private String bqTable;
    private String jobName;


    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getBqTable() {
        return bqTable;
    }

    public void setBqTable(String bqTable) {
        this.bqTable = bqTable;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }


    @Override
    public PCollection<String> extract(Pipeline pipe) {
        String topicName = "projects/" + getProjectName() + "/topics/"+ getTopicName();
        return pipe.apply("Read xml events", readInput(topicName));
    }

    @Override
    public PCollection<SensorEvent> transform(PCollection<String> dataset) {
        return dataset.apply("Parse xml events", parseEvents());
    }


    @Override
    public PDone load(PCollection<SensorEvent> dataset) {
        String bucketName =  getBqTable() + "_optimized";
        return  dataset.apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply("Write to Bucket", writeAvroFilesToGCS(bucketName, NUM_SHARDS));
    }

    public  PubsubIO.Read<String> readInput(String topicName){
        return PubsubIO.readStrings().fromTopic(topicName);
    }

    public  MapElements<String, SensorEvent>  parseEvents(){
        return MapElements.into(new TypeDescriptor<SensorEvent>(){}).via( (String l) -> parseEventFromXml(l));
    }

    public AvroIO.Write<SensorEvent>  writeAvroFilesToGCS(String bucketName, int numShards){
        String bucketPath =  "gs://"+ getProjectName() +"/"+ bucketName;
        String tempPath =  "gs://"+ getProjectName() +"/sensors_temp_location_"+bucketName;
        return AvroIO
                .write(SensorEvent.class)
                .to(bucketPath)
                .withTempDirectory(ValueProvider.StaticValueProvider.of(FileSystems.matchNewResource(tempPath, true)))
                .withWindowedWrites()
                .withNumShards(numShards)
                .withSuffix(".avro");
    }

    public SensorEvent parseEventFromXml(String xmlEvent) {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(SensorEvent.class);
            Unmarshaller jaxbUnMarshaller = jaxbContext.createUnmarshaller();
            String trimmedEvent = xmlEvent.replace("b'", "").replace("\\n'", "");
            SensorEvent sensor = (SensorEvent) jaxbUnMarshaller.unmarshal(new StringReader(trimmedEvent));
            return sensor;
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return null;
    }


}
