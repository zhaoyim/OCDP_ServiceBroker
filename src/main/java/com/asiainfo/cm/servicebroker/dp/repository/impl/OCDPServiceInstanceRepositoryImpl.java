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
import com.asiainfo.cm.servicebroker.dp.model.ServiceInstance;
import com.asiainfo.cm.servicebroker.dp.repository.OCDPServiceInstanceRepository;
import com.asiainfo.cm.servicebroker.dp.utils.OCDPAdminServiceMapper;
import com.asiainfo.cm.servicebroker.dp.utils.OCDPConstants;

/**
 * Implementation of Repository for ServiceInstance objects
 *
 * @author whitebai1986@gmail.com
 *
 */
@Service
public class OCDPServiceInstanceRepositoryImpl implements OCDPServiceInstanceRepository {

    private Logger logger = LoggerFactory.getLogger(OCDPServiceInstanceRepositoryImpl.class);
	@Autowired
	private ApplicationContext context;
    private etcdClient etcdClient;

    @Autowired
    public OCDPServiceInstanceRepositoryImpl(ClusterConfig clusterConfig){
        this.etcdClient = clusterConfig.getEtcdClient();
    }

    @Override
    public ServiceInstance findOne(String serviceInstanceId) {
        logger.info("Try to find one OCDPServiceInstance: " + serviceInstanceId + " in repository.");
		OCDPAdminServiceMapper mapper = (OCDPAdminServiceMapper) this.context.getBean("OCDPAdminServiceMapper");

        if(etcdClient.read("/servicebroker/ocdp/instance/" + serviceInstanceId) == null){
            return null;
        }
        String orgGuid = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/organizationGuid");
        String spaceGuid = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/spaceGuid");
        String serviceDefinitionId = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/id");
        String resourceType = mapper.getOCDPResourceType(serviceDefinitionId);
        String planId = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/planId");
        String dashboardUrl = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/dashboardUrl");
        ServiceInstance instance = new ServiceInstance(serviceInstanceId, serviceDefinitionId, planId, orgGuid, spaceGuid,
                dashboardUrl);
        String uri = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/Credentials/uri");
        String host = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/Credentials/host");
        String port = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/Credentials/port");
        String resource = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/Credentials/" + resourceType);
        String rangerPolicyId = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/Credentials/rangerPolicyId");
        Map<String, Object> Credential = new HashMap<String, Object>() {
            {
                put("uri", uri);
                put("host", host);
                put("port", port);
                put(resourceType, resource);
                put("rangerPolicyId", rangerPolicyId);
            }
        };
        if (resourceType.equals(OCDPConstants.HIVE_RESOURCE_TYPE)){
            String thriftUri = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                    "/Credentials/thriftUri");
            Credential.put("thriftUri", thriftUri);
        }
        instance.setCredential(Credential);

        return instance;
    }

    @Override
    public void save(ServiceInstance instance) {
		OCDPAdminServiceMapper mapper = (OCDPAdminServiceMapper) this.context.getBean("OCDPAdminServiceMapper");
        String serviceInstanceId = instance.getServiceInstanceId();
        String serviceDefinitionId = instance.getServiceDefinitionId();
        String resourceType = mapper.getOCDPResourceType(serviceDefinitionId);
        Map<String, Object> credentials = instance.getServiceInstanceCredentials();
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/organizationGuid",
                instance.getOrganizationGuid());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/spaceGuid",
                instance.getSpaceGuid());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/id",
                instance.getServiceDefinitionId());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/planId",
                instance.getPlanId());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/Credentials/uri",
                (String)credentials.get("uri"));
        if (resourceType.equals(OCDPConstants.HIVE_RESOURCE_TYPE)){
            etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/Credentials/thriftUri",
                    (String)credentials.get("thriftUri"));
        }
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/Credentials/host",
                (String)credentials.get("host"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/Credentials/port",
                (String)credentials.get("port"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/Credentials/" + resourceType,
                (String)credentials.get(resourceType));
        logger.info("Update ranger policy id to: " + credentials.get("rangerPolicyId"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/Credentials/rangerPolicyId",
                (String)credentials.get("rangerPolicyId"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/dashboardUrl",
                instance.getDashboardUrl());
        etcdClient.createDir("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings");
        logger.info("Save OCDPServiceInstance: " + serviceInstanceId);
    }

    @Override
    public void delete(String serviceInstanceId) {
        logger.info("Delete OCDPServiceInstance: " + serviceInstanceId );
        etcdClient.deleteDir("/servicebroker/ocdp/instance/" + serviceInstanceId, true);
    }

}