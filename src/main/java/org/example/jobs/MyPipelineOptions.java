package org.example.jobs;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface MyPipelineOptions extends DataflowPipelineOptions {

    @Description("PubSub topic")
    @Default.String("sensor_events")
    String getTopic();
    void setTopic(String topic);

    @Description("BQ table name")
    @Default.String("dummy-table")
    String getBqTable();
    void setBqTable(String bqTable);

}
