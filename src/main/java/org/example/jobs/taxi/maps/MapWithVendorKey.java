package org.example.jobs.taxi.maps;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.example.models.VendorKey;

public class MapWithVendorKey extends DoFn<TableRow,  KV<VendorKey, Double> > {

    @ProcessElement
    public void processElement(ProcessContext c) {
        TableRow r = c.element();
        KV<VendorKey, Double> out = KV.of( new VendorKey(r.get("vendor_id").toString(), r.get("payment_type").toString()),
                Double.parseDouble(r.get("total_amount").toString()));
        c.output(out);
    }

}
