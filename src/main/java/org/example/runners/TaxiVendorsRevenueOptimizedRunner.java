package org.example.runners;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.example.jobs.NewYorkTaxiVendorRevenue;


public class TaxiVendorsRevenueOptimizedRunner {

    public static void main(String[] args) {

        String inputTableName = "bigquery-public-data:new_york.tlc_yellow_trips_2016";
        String outputTableName = "reports.nyc_taxi_vendor_revenue_optimized";

        PipelineOptionsFactory.register(PipelineOptions.class);
        PipelineOptions ops = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);

        Pipeline pipe = Pipeline.create(ops);

        NewYorkTaxiVendorRevenue job = new NewYorkTaxiVendorRevenue();
        job.createRevenueByPaymentMethodOptimized(pipe, inputTableName, outputTableName).run();


    }
}
