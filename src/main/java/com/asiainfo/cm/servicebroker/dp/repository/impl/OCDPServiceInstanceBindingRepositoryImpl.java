package com.asiainfo.cm.servicebroker.dp.repository.impl;


import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.asiainfo.cm.servicebroker.dp.client.etcdClient;
import com.asiainfo.cm.servicebroker.dp.config.ClusterConfig;
import com.asiainfo.cm.servicebroker.dp.model.ServiceInstanceBinding;
import com.asiainfo.cm.servicebroker.dp.repository.OCDPServiceInstanceBindingRepository;
import com.asiainfo.cm.servicebroker.dp.utils.OCDPAdminServiceMapper;
import com.asiainfo.cm.servicebroker.dp.utils.OCDPConstants;

/**
 * Implementation of Repository for ServiceInstanceBinding objects
 *
 * @author whitebai1986@gmail.com
 *
 */
@Service
public class OCDPServiceInstanceBindingRepositoryImpl implements OCDPServiceInstanceBindingRepository {

    private Logger logger = LoggerFactory.getLogger(OCDPServiceInstanceBindingRepositoryImpl.class);
	@Autowired
	private ApplicationContext context;
	
    private etcdClient etcdClient;

    @Autowired
    public OCDPServiceInstanceBindingRepositoryImpl(ClusterConfig clusterConfig){
        this.etcdClient = clusterConfig.getEtcdClient();
    }

    @Override
    public ServiceInstanceBinding findOne(String serviceInstanceId, String bindingId) {
        logger.info("Try to find one OCDPServiceInstanceBinding: " + bindingId);
		OCDPAdminServiceMapper mapper = (OCDPAdminServiceMapper) this.context.getBean("OCDPAdminServiceMapper");

        if(etcdClient.read("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" + bindingId) == null){
            return null;
        }
        String serviceDefinitionId = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/id");
        String resourceType = mapper.getOCDPResourceType(serviceDefinitionId);
        String id = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId +"/id");
        String syslogDrainUrl = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/syslogDrainUrl");
        String appGuid = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/appGuid");
        String planId = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId +"/planId");
        String uri = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/uri");
        String username = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/username");
        String password = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/password");
        String host = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/host");
        String port = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/port");
        String resource = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/" + resourceType);
        String rangerPolicyId = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/rangerPolicyId");
        Map<String, Object> credentials = new HashMap<String, Object>() {
            {
                put("uri", uri);
                put("username", username);
                put("host", host);
                put("port", port);
                put(resourceType, resource);
                put("rangerPolicyId", rangerPolicyId);
            }
        };
        if (resourceType.equals(OCDPConstants.HIVE_RESOURCE_TYPE)){
            String thriftUri = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                    bindingId + "/Credentials/thriftUri");
            credentials.put("thriftUri", thriftUri);
        }

        return new ServiceInstanceBinding(id, serviceInstanceId, credentials,syslogDrainUrl, appGuid, planId);
    }

    @Override
    public void save(ServiceInstanceBinding binding) {
        String serviceInstanceId = binding.getServiceInstanceId();
        Map<String, Object> credentials = binding.getCredentials();
        String resourceType = "";
        // Can not get service definition id from binding info, so loop credentials to get resource type
        for(String key : credentials.keySet())
        {
            if( ! key.equals("uri") && ! key.equals("username") && ! key.equals("password") && ! key.equals("host") &&
                    ! key.equals("port") && ! key.equals("rangerPolicyId"))
                resourceType = key;
        }

        String bindingId = binding.getId();
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId +"/id", binding.getId());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId +"/serviceInstanceId", binding.getServiceInstanceId());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/syslogDrainUrl", binding.getSyslogDrainUrl());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/appGuid", binding.getAppGuid());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/planId", binding.getPlanId());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                        bindingId + "/Credentials/uri", (String)credentials.get("uri"));
        if (resourceType.equals(OCDPConstants.HIVE_RESOURCE_TYPE)){
            etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                    bindingId + "/Credentials/thriftUri", (String)credentials.get("thriftUri"));
        }
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                        bindingId + "/Credentials/username", (String)credentials.get("username"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                        bindingId + "/Credentials/password", (String)credentials.get("password"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                        bindingId + "/Credentials/host", (String)credentials.get("host"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                        bindingId + "/Credentials/port", (String)credentials.get("port"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                        bindingId + "/Credentials/" + resourceType, (String)credentials.get("resourceType"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/rangerPolicyId", (String)credentials.get("rangerPolicyId"));
        logger.info("Saved OCDPServiceInstanceBinding: " + bindingId);
    }

    @Override
    public void delete(String serviceInstanceId, String bindingId) {
        etcdClient.deleteDir("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" + bindingId, true);
        logger.info("Deleted OCDPServiceInstanceBinding: " + bindingId);
    }

}