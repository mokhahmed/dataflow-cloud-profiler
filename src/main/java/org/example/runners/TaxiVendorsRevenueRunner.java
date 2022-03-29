package org.example.runners;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.example.jobs.NewYorkTaxiVendorRevenue;



public class TaxiVendorsRevenueRunner {

    public static void main(String[] args) {

        String inputTableNameOptimized = "bigquery-public-data:new_york.tlc_yellow_trips_2016";
        String inputTableName = "bigquery-public-data.new_york.tlc_yellow_trips_*";

        String outputTableNameOptimized = "reports.nyc_taxi_vendor_revenue_optimized";
        String outputTableName = "reports.nyc_taxi_vendor_revenue";


        PipelineOptionsFactory.register(PipelineOptions.class);
        PipelineOptions ops = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);

        Pipeline pipe = Pipeline.create(ops);

        NewYorkTaxiVendorRevenue job = new NewYorkTaxiVendorRevenue();
        job.createRevenueByPaymentMethod(pipe, inputTableName, outputTableName).run();


    }
}
