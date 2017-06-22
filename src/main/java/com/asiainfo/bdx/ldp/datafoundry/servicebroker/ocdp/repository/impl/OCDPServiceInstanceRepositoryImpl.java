package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.impl;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceRepository;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of Repository for ServiceInstance objects
 *
 * @author whitebai1986@gmail.com
 *
 */
@Service
public class OCDPServiceInstanceRepositoryImpl implements OCDPServiceInstanceRepository {

    private Logger logger = LoggerFactory.getLogger(OCDPServiceInstanceRepositoryImpl.class);

    private etcdClient etcdClient;

    @Autowired
    public OCDPServiceInstanceRepositoryImpl(ClusterConfig clusterConfig){
        this.etcdClient = clusterConfig.getEtcdClient();
    }

    @Override
    public ServiceInstance findOne(String serviceInstanceId) {
        logger.info("Try to find one OCDPServiceInstance: " + serviceInstanceId);

        if(etcdClient.read("/servicebroker/ocdp/instance/" + serviceInstanceId) == null){
            return null;
        }
        String orgGuid = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/organizationGuid");
        String spaceGuid = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/spaceGuid");
        String serviceDefinitionId = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/id");
        String resourceType = OCDPAdminServiceMapper.getOCDPResourceType(serviceDefinitionId);
        String planId = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/planId");
        String dashboardUrl = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/dashboardUrl");
        ServiceInstance instance = new ServiceInstance(serviceInstanceId, serviceDefinitionId, planId, orgGuid, spaceGuid,
                dashboardUrl);
        String username = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/Credentials/username");
        String password = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/Credentials/password");
        String resource = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/Credentials/" + resourceType);
        String rangerPolicyId = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/Credentials/rangerPolicyId");
        String tenantName = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/Credentials/tenantName");
        Map<String, Object> Credential = new HashMap<String, Object>() {
            {
                put("username", username);
                put("password", password);
                put(resourceType, resource);
                put("rangerPolicyId", rangerPolicyId);
                put("tenantName", tenantName);
            }
        };
        instance.setCredential(Credential);

        return instance;
    }

    @Override
    public void save(ServiceInstance instance) {
        String serviceInstanceId = instance.getServiceInstanceId();
        String serviceDefinitionId = instance.getServiceDefinitionId();
        String resourceType = OCDPAdminServiceMapper.getOCDPResourceType(serviceDefinitionId);
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/organizationGuid",
                instance.getOrganizationGuid());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/spaceGuid",
                instance.getSpaceGuid());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/id",
                instance.getServiceDefinitionId());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/planId",
                instance.getPlanId());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/Credentials/username",
                (String)instance.getServiceInstanceCredentials().get("username"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/Credentials/password",
                (String)instance.getServiceInstanceCredentials().get("password"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/Credentials/" + resourceType,
                (String)instance.getServiceInstanceCredentials().get(resourceType));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/Credentials/rangerPolicyId",
                (String)instance.getServiceInstanceCredentials().get("rangerPolicyId"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/Credentials/tenantName",
                (String)instance.getServiceInstanceCredentials().get("tenantName"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/dashboardUrl",
                instance.getDashboardUrl());
        etcdClient.createDir("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings");
        logger.info("save OCDPServiceInstance: " + serviceInstanceId);
    }

    @Override
    public void delete(String serviceInstanceId) {
        logger.info("delete OCDPServiceInstance: " + serviceInstanceId );
        etcdClient.deleteDir("/servicebroker/ocdp/instance/" + serviceInstanceId, true);
    }

}