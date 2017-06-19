package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.springframework.cloud.servicebroker.model.UpdateServiceInstanceResponse;

import java.util.Map;

/**
 * Created by baikai on 6/17/17.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE)
public class OCDPUpdateServiceInstanceResponse extends UpdateServiceInstanceResponse {
    @JsonSerialize
    @JsonProperty("credentials")
    private Map<String, Object> credentials;

    public OCDPUpdateServiceInstanceResponse withCredential(final Map<String, Object> credential){
        this.credentials = credential;
        return this;
    }
}
