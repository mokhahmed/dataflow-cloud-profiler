package org.example.jobs;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

public abstract class AbstractPipeline implements Serializable {

    public abstract PCollection<String> extract(Pipeline pipe) ;
    public abstract PCollection<TableRow> transform(PCollection<String> dataset);
    public abstract WriteResult load(PCollection<TableRow> dataset);

    public void execute(Pipeline pipe){
        PCollection<String> inputDS = extract(pipe);
        PCollection<TableRow> transformedDS = transform(inputDS);
        load(transformedDS);
    }
}
