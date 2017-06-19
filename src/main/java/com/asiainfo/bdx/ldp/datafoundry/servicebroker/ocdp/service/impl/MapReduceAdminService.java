package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService_old;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common.YarnCommonService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by baikai on 8/4/16.
 */
@Service
public class MapReduceAdminService implements OCDPAdminService_old{

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
    public String assignPermissionToResources(String policyName, final List<String> resources, String accountName, String groupName) {
        /**
         * Temp fix:
         * Create ranger policy to make sure current tenant can use /user/<account name> folder to store some files generate by spark or mr.
         * For each tenant, just need only one ranger policy about this.
         * If such policy exists, policy create will fail here.
         */
        List <String> hdfsFolderForJobExec = new ArrayList<String>(){
            {
                add("/user/" + accountName);
                add("/mr-history");
            }
        };
        if (this.hdfsAdminService.assignPermissionToResources(accountName + "_" + policyName, hdfsFolderForJobExec, accountName, groupName) != null){
            logger.info("Assign permissions for /user/" + accountName + " folder.");
        }

        String resource = resources.get(0);
        logger.info("Assign submit-app/admin-queue permission to yarn queue.");
        String yarnPolicyId = this.yarnCommonService.assignPermissionToQueue(policyName, resource, accountName, groupName);
        // return yarn policy id
        return (yarnPolicyId != null) ? yarnPolicyId : null;
    }

    @Override
    public boolean appendUserToResourcePermission(String policyId, String groupName, String accountName) {
        return this.yarnCommonService.appendUserToQueuePermission(policyId, groupName, accountName);
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{
        this.yarnCommonService.deleteQueue(serviceInstanceResuorceName);
    }

    @Override
    public boolean unassignPermissionFromResources(String policyId) {
        logger.info("Unassign submit/admin permission to yarn queue.");
        return this.yarnCommonService.unassignPermissionFromQueue(policyId);
    }

    @Override
    public boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName) {
        return this.yarnCommonService.removeUserFromQueuePermission(policyId, groupName, accountName);
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
    public String getServiceResourceType(){
        return "Yarn Queue";
    }

}
