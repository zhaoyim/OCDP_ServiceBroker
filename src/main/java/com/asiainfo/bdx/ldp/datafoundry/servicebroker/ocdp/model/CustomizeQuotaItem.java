package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Created by baikai on 4/26/17.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE)
public class CustomizeQuotaItem{

    @JsonSerialize
    @JsonProperty("default")
    String defaults;

    @JsonSerialize
    @JsonProperty("max")
    String max;

    @JsonSerialize
    @JsonProperty("price")
    String price;

    @JsonSerialize
    @JsonProperty("unit")
    String unit;

    @JsonSerialize
    @JsonProperty("step")
    String step;

    @JsonSerialize
    @JsonProperty("desc")
    String desc;

    public String getDefaults(){ return defaults;}
    public String getMax() {return max;}
}