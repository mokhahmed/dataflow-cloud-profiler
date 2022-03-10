package org.example.jobs;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import org.apache.beam.runners.dataflow.options.DataflowProfilingOptions;
import org.apache.beam.runners.dataflow.options.DataflowProfilingOptions.DataflowProfilingAgentConfiguration;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.values.PCollection;
import org.example.models.SensorEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import static java.lang.Integer.MAX_VALUE;


public class SensorXmlEventToBQJob extends AbstractPipeline{

    private int LOAD_FACTOR =100000;
    private String topicName;
    private String projectName;
    private String bqTable;
    private Boolean cpuLoad;
    private Boolean loadFlag;

    public Boolean getLoadFlag() {
        return cpuLoad;
    }

    public void setLoadFlag(Boolean cpuLoad) {
        this.cpuLoad = cpuLoad;
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
    public PCollection<TableRow> transform(PCollection<String> dataset) {
         return dataset
                 .apply("Parse xml events", parseEvents())
                 .apply("Convert to BQ row", toBQRow());
    }

    @Override
    public WriteResult load(PCollection<TableRow> dataset) {
        String sensorEventsTable = getProjectName() + ":" + getBqTable();
        return  dataset.apply("Write to BQ", writeToBQTable(sensorEventsTable));
    }

    public  PubsubIO.Read<String> readInput(String topicName){
         return PubsubIO.readStrings().fromTopic(topicName);
    }

    public  MapElements<String, SensorEvent>  parseEvents(){
        return MapElements.into(new TypeDescriptor<SensorEvent>(){})
                .via( (String l) -> buildEvent(l));
    }

    public  MapElements<SensorEvent, TableRow> toBQRow(){

        return MapElements
                .into(new TypeDescriptor<TableRow>() {})
                .via( (SensorEvent event)-> {
                    TableRow row = new TableRow();
                    row.set("timestamp", event.getTimestamp());
                    row.set("latitude", event.getLatitude());
                    row.set("longitude", event.getLongitude());
                    row.set("highway", event.getHighway());
                    row.set("direction", event.getDirection());
                    row.set("lane", event.getLane());
                    row.set("speed", event.getSpeed());
                    row.set("sensorId", event.getId());
                    return row;
                });
    }

    public BigQueryIO.Write<TableRow> writeToBQTable(String tableName){

        if(getLoadFlag()) loadFunc(LOAD_FACTOR);

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("timestamp").setType("STRING"));
        fields.add(new TableFieldSchema().setName("latitude").setType("STRING"));
        fields.add(new TableFieldSchema().setName("longitude").setType("STRING"));
        fields.add(new TableFieldSchema().setName("highway").setType("STRING"));
        fields.add(new TableFieldSchema().setName("direction").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lane").setType("STRING"));
        fields.add(new TableFieldSchema().setName("speed").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sensorId").setType("STRING"));

        TableSchema schema = new TableSchema().setFields(fields);

        return BigQueryIO
                .writeTableRows()
                .to(tableName)
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);
    }


    public SensorEvent buildEvent(String xmlEvent) {
        try {

            if(getLoadFlag()) loadFunc(LOAD_FACTOR);

            JAXBContext jaxbContext = JAXBContext.newInstance(SensorEvent.class);
            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            SensorEvent sensor = (SensorEvent) jaxbUnmarshaller.unmarshal(new StringReader(xmlEvent));
            return sensor;
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void loadFunc(int factor){
        List<Integer> ls = new ArrayList<>();
        for(int i=1;i<factor;i++) {
            ls.add(Integer.valueOf(i));
        }
    }

}
