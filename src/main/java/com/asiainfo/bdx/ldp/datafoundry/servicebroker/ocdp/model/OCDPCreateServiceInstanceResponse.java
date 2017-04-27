package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model;

import com.google.gson.annotations.SerializedName;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceResponse;

import java.util.Map;

/**
 * Created by baikai on 4/22/17.
 */
public class OCDPCreateServiceInstanceResponse extends CreateServiceInstanceResponse{

    @SerializedName("credentials")
    private Map<String, String> credentials;

    public OCDPCreateServiceInstanceResponse withCredential(final Map<String, String> credential){
        this.credentials = credential;
        return this;
    }
}
