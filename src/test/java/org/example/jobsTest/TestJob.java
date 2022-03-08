package org.example.jobsTest;

import com.google.api.services.bigquery.model.TableRow;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.example.models.SensorEvent;

import java.io.StringReader;
import java.util.Arrays;


public class TestJob {

    public interface TestPipelineOptions extends PipelineOptions {

        @Description("Input for the pipeline")
        @Default.String("src/main/resources/my-bucket/input/sensor_events.xml")
        String getInput();
        void setInput(String input);

        @Description("Output for the pipeline")
        @Default.String("src/main/resources/my-bucket/output/")
        String getOutput();
        void setOutput(String output);
    }


    public  static MapElements<String, SensorEvent>  parseEvents(){
        return MapElements.into(new TypeDescriptor<SensorEvent>(){})
                .via( (String l) -> buildEvent(l));
    }

    public static SensorEvent buildEvent(String xmlEvent) {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(SensorEvent.class);
            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            SensorEvent sensor = (SensorEvent) jaxbUnmarshaller.unmarshal(new StringReader(xmlEvent));
            return sensor;
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return null;
    }


    public  static  MapElements<SensorEvent, TableRow> toBQRow(){

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


    public static void main(String[] args) {

        PipelineOptionsFactory.register(TestPipelineOptions.class);
        TestPipelineOptions testOption = PipelineOptionsFactory.fromArgs(args)
                                                               .withValidation()
                                                               .as(TestPipelineOptions.class);

        Pipeline p = Pipeline.create(testOption);

        PCollection<String> lines = p.apply("Read Text Files From Bucket",
                TextIO.read().from(testOption.getInput()));

       lines.apply("Parse xml events", parseEvents())
               .apply("Convert to BQ row", toBQRow())
                       .apply(MapElements.into(TypeDescriptors.voids()).via(
                               x -> {System.out.println(x); return null;}
                       ));

               //.apply("write lengths to file",TextIO.write().to(testOption.getOutput()).withNumShards(1).withSuffix(".txt"));

        p.run();
    }
}
