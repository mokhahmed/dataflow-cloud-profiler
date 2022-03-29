package org.example.jobs;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.example.models.SensorEvent;

import java.io.Serializable;

public abstract class AbstractPipeline implements Serializable {

    public abstract PCollection<String> extract(Pipeline pipe) ;
    public abstract PCollection<SensorEvent> transform(PCollection<String> dataset);
    public abstract PDone load(PCollection<SensorEvent> dataset);

    public void execute(Pipeline pipe){
        PCollection<String> inputDS = extract(pipe);
        PCollection<SensorEvent> transformedDS = transform(inputDS);
        load(transformedDS);
    }
}
