package org.example.jobs;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.example.jobs.taxi.filters.StringDateFilter;
import org.example.jobs.taxi.maps.MapRevenueToBQRow;
import org.example.jobs.taxi.maps.MapWithVendorKey;
import org.example.models.VendorKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class NewYorkTaxiVendorRevenue {

    /**
     ** Query
     *  select vendor_id, payment_type, round(sum(total_amount)) as revenue
     *          from (select * from `bigquery-public-data.new_york.tlc_yellow_trips_*`)
     *          where pickup_datetime >= '2016-01-01T00:00:00' and  pickup_datetime < '2017-01-01T00:00:00'
     *          group by vendor_id, payment_type
     *          having sum(total_amount) > 0;
     *
     ==============================================================================================

     *  Query Optimized:
     *  select vendor_id, payment_type, sum(total_amount) as revenue
     *  from `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2016`
     *  where total_amount > 0
     *  group by vendor_id, payment_type;


    */

    TableSchema schema;

    public NewYorkTaxiVendorRevenue(){
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("vendor_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("payment_type").setType("STRING"));
        fields.add(new TableFieldSchema().setName("revenue").setType("FLOAT64"));
        this.schema = new TableSchema().setFields(fields);
    }

    public BigQueryIO.TypedRead<TableRow> readFromBQOptimized(String tableName) {
        return BigQueryIO
                .readTableRows()
                .from(tableName)
                .withSelectedFields(Arrays.asList("vendor_id", "payment_type", "total_amount"))
                .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ);
    }

    public BigQueryIO.TypedRead<TableRow> readFromBQ(String tableName) {
        return BigQueryIO
                .readTableRows()
                .fromQuery(String.format("SELECT * FROM `%s`", tableName))
                .usingStandardSql();

    }


    public BigQueryIO.Write<TableRow> writeToBQ(String tableName) {
        return  BigQueryIO
                .writeTableRows()
                .to(tableName)
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);

    }


    public Pipeline createRevenueByPaymentMethod(Pipeline pipeline, String inputTableName, String outputTableName) {

        PCollection<TableRow> input =  pipeline.apply("Read NYC Yellow Taxi data using wildcard ", readFromBQ(inputTableName));

        PCollection<TableRow> inputFiltered = input.apply("Filter only 2016 data", Filter.by(new StringDateFilter()));

        PCollection<KV<VendorKey, Double>> inputKV = inputFiltered.apply("Map to KV", ParDo.of(new MapWithVendorKey()));

        PCollection<KV<VendorKey, Double>> vendorRevenue=  inputKV.apply("Total Revenue", Sum.doublesPerKey());

        PCollection<TableRow> output = vendorRevenue.apply("Convert To BQ Row", ParDo.of(new MapRevenueToBQRow()));

        output.apply("Write Report to BigQuery",
                BigQueryIO
                        .writeTableRows()
                        .to(outputTableName)
                        .withSchema(schema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        );

        return pipeline;
    }


    public Pipeline createRevenueByPaymentMethodOptimized(Pipeline pipeline, String inputTableName, String outputTableName) {

        PCollection<TableRow> input =  pipeline.apply("Read NYC Yellow Taxi", readFromBQOptimized(inputTableName));

        PCollection<KV<VendorKey, Double>> inputKV = input.apply("Map to KV", ParDo.of(new MapWithVendorKey()));

        PCollection<KV<VendorKey, Double>> vendorRevenue=  inputKV.apply("Total Revenue", Sum.doublesPerKey());

        PCollection<TableRow> output = vendorRevenue.apply("Convert To BQ Row", ParDo.of(new MapRevenueToBQRow()));

        output.apply("Write Report to BigQuery", writeToBQ(outputTableName));

        return pipeline;
    }


}
