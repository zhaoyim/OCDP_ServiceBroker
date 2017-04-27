package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model;

import com.google.gson.annotations.SerializedName;
/**
 * Created by baikai on 4/26/17.
 */
public class CustomizeQuotaItem{

    @SerializedName("default")
    String _default;

    @SerializedName("max")
    String max;

    @SerializedName("price")
    String price;

    @SerializedName("unit")
    String unit;

    @SerializedName("step")
    String step;

    @SerializedName("desc")
    String desc;

    public String getDefault(){ return _default;}
    public String getMax() { return max; }
    public String getPrice() { return price; }
    public String getUnit() { return unit; }
    public String getStep() { return step; }
    public String getDesc() { return desc; }
}