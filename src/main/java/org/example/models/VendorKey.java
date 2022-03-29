package org.example.models;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public class VendorKey implements Serializable {
    private String vendorId;
    private String paymentType;

    public VendorKey(){

    }

    public VendorKey(String vendorId, String paymentType) {
        this.vendorId = vendorId;
        this.paymentType = paymentType;
    }

    public String getVendorId() {
        return vendorId;
    }

    public void setVendorId(String vendorId) {
        this.vendorId = vendorId;
    }

    public String getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(String paymentType) {
        this.paymentType = paymentType;
    }

}
