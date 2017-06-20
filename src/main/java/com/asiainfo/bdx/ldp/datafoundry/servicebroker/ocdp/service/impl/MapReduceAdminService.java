package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common.YarnCommonService;

/**
 * Created by baikai on 8/4/16.
 */
@Service
public class MapReduceAdminService implements OCDPAdminService{

    private Logger logger = LoggerFactory.getLogger(MapReduceAdminService.class);

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private YarnCommonService yarnCommonService;

    private HDFSAdminService hdfsAdminService;

    @Autowired
    public MapReduceAdminService(ClusterConfig clusterConfig, YarnCommonService yarnCommonService, HDFSAdminService hdfsAdminService){
        this.clusterConfig = clusterConfig;
        this.yarnCommonService = yarnCommonService;
        this.hdfsAdminService = hdfsAdminService;
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
                                     String bindingId, Map<String, Object> cuzQuota) throws Exception {
        Map<String, String> quota = this.yarnCommonService.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
        return this.yarnCommonService.createQueue(quota.get("yarnQueueQuota"));
    }

    @Override
    public String createPolicyForTenant(String policyName, final List<String> resources, String tenantName, String groupName) {
        /**
         * Temp fix:
         * Create ranger policy to make sure current tenant can use /user/<account name> folder to store some files generate by spark or mr.
         * For each tenant, just need only one ranger policy about this.
         * If such policy exists, policy create will fail here.
         */
        List <String> hdfsFolderForJobExec = new ArrayList<String>(){
            {
                add("/user/" + tenantName);
                add("/mr-history");
            }
        };
        if (this.hdfsAdminService.createPolicyForTenant(tenantName + "_" + policyName, hdfsFolderForJobExec, tenantName, groupName) != null){
            logger.info("Assign permissions for /user/" + tenantName + " folder.");
        }

        String resource = resources.get(0);
        String yarnPolicyId = this.yarnCommonService.assignPermissionToQueue(policyName, resource, tenantName, groupName);
        // return yarn policy id
        return (yarnPolicyId != null) ? yarnPolicyId : null;
    }

    @Override
    public boolean appendResourceToTenantPolicy(String policyId, String serviceInstanceResource){
        return yarnCommonService.appendResourceToQueuePermission(policyId, serviceInstanceResource);
    }

    @Override
    public boolean appendUserToTenantPolicy(
            String policyId, String groupName, String accountName, List<String> permissions) {
        return this.yarnCommonService.appendUserToQueuePermission(policyId, groupName, accountName, permissions);
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{
        this.yarnCommonService.deleteQueue(serviceInstanceResuorceName);
    }

    @Override
    public boolean deletePolicyForTenant(String policyId) {
        return this.yarnCommonService.unassignPermissionFromQueue(policyId);
    }

    @Override
    public boolean removeResourceFromTenantPolicy(String policyId, String serviceInstanceResource){
        return yarnCommonService.removeResourceFromQueuePermission(policyId, serviceInstanceResource);
    }

    @Override
    public boolean removeUserFromTenantPolicy(String policyId, String accountName) {
        return this.yarnCommonService.removeUserFromQueuePermission(policyId, accountName);
    }

    @Override
    public  List<String> getResourceFromTenantPolicy(String policyId){
        return yarnCommonService.getResourceFromQueuePolicy(policyId);
    }

    @Override
    public Map<String, Object> generateCredentialsInfo(String serviceInstanceId){
        return new HashMap<String, Object>(){
            {
                put("uri", clusterConfig.getYarnRMUrl());
                put("host", clusterConfig.getYarnRMHost());
                put("port", clusterConfig.getYarnRMPort());
            }
        };
    }

    @Override
    public void resizeResourceQuota(ServiceInstance instance, Map<String, Object> cuzQuota){

    }


}
