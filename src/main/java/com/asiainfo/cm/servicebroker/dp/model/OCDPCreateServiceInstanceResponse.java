package com.asiainfo.cm.servicebroker.dp.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.gson.annotations.SerializedName;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceResponse;

import java.util.Map;

/**
 * Created by baikai on 4/22/17.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE)
public class OCDPCreateServiceInstanceResponse extends CreateServiceInstanceResponse{

//    @SerializedName("credentials")
    @JsonSerialize
    @JsonProperty("credentials")
    private Map<String, Object> credentials;

    public OCDPCreateServiceInstanceResponse withCredential(final Map<String, Object> credential){
        this.credentials = credential;
        return this;
    }
}
