package org.example.jobs.taxi.maps;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.example.models.VendorKey;

public class MapRevenueToBQRow extends DoFn<KV<VendorKey, Double>, TableRow > {

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<VendorKey, Double> record = c.element();
        TableRow row = new TableRow();
        row.set("vendor_id", record.getKey().getVendorId());
        row.set("payment_type", record.getKey().getPaymentType());
        row.set("revenue", record.getValue());
        c.output(row);
    }

}
