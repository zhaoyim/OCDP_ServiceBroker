package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model;

import com.google.gson.annotations.SerializedName;
/**
 * Created by baikai on 4/26/17.
 */
public class CustomizeQuotaItem{

    @SerializedName("default")
    long _default;

    @SerializedName("max")
    long max;

    @SerializedName("price")
    long price;

    @SerializedName("unit")
    String unit;

    @SerializedName("step")
    long step;

    @SerializedName("desc")
    String desc;

    public long getDefault(){ return _default;}
    public long getMax() { return max; }
    public long getPrice() { return price; }
    public String getUnit() { return unit; }
    public long getStep() { return step; }
    public String getDesc() { return desc; }
}