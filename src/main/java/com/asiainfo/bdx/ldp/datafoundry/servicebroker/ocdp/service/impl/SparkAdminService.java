package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common.YarnCommonService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by baikai on 8/4/16.
 */
@Service
public class SparkAdminService implements OCDPAdminService {
    private Logger logger = LoggerFactory.getLogger(SparkAdminService.class);

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private YarnCommonService yarnCommonService;

    private HDFSAdminService hdfsAdminService;

    @Autowired
    public SparkAdminService(ClusterConfig clusterConfig,
                             YarnCommonService yarnCommonService, HDFSAdminService hdfsAdminService){
        this.clusterConfig = clusterConfig;
        this.yarnCommonService = yarnCommonService;
        this.hdfsAdminService = hdfsAdminService;
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
                                     String bindingId, Map<String, Object> cuzQuota) throws Exception {
        Map<String, String> quota = this.yarnCommonService.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
        return yarnCommonService.createQueue(quota.get(OCDPConstants.YARN_QUEUE_QUOTA));
    }

    @Override
    public String createPolicyForResources(String policyName, final List<String> resources, String userName, String groupName) {
        /**
         * Temp fix:
         * Create ranger policy to make sure current tenant can use /user/<account name> folder to store some files generate by spark or mr.
         * For each tenant, just need only one ranger policy about this.
         * If such policy exists, policy create will fail here.
         */
        List <String> hdfsFolderForJobExec = new ArrayList<String>(){
            {
                add("/user/" + userName);
                add("/spark-history");
            }
        };
        if (this.hdfsAdminService.createPolicyForResources(userName + "_" + policyName, hdfsFolderForJobExec, userName, groupName) != null){
            logger.info("Assign permissions for /user/" + userName + " folder.");
        }

        String resource = resources.get(0);
        String yarnPolicyId = this.yarnCommonService.assignPermissionToQueue(policyName, resource, userName, groupName);
        // return yarn policy id
        return (yarnPolicyId != null) ? yarnPolicyId : null;
    }

    @Override
    public boolean appendResourcesToPolicy(String policyId, String serviceInstanceResource){
        return yarnCommonService.appendResourceToQueuePermission(policyId, serviceInstanceResource);
    }

    @Override
    public boolean appendUserToPolicy(
            String policyId, String groupName, String userName, List<String> permissions) {
        return this.yarnCommonService.appendUserToQueuePermission(policyId, groupName, userName, permissions);
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{
        this.yarnCommonService.deleteQueue(serviceInstanceResuorceName);
    }

    @Override
    public boolean deletePolicyForResources(String policyId) {
        return this.yarnCommonService.unassignPermissionFromQueue(policyId);
    }

    @Override
    public boolean removeResourceFromPolicy(String policyId, String serviceInstanceResource){
        return yarnCommonService.removeResourceFromQueuePermission(policyId, serviceInstanceResource);
    }

    @Override
    public boolean removeUserFromPolicy(String policyId, String userName) {
        return  this.yarnCommonService.removeUserFromQueuePermission(policyId, userName);
    }

    @Override
    public  List<String> getResourceFromPolicy(String policyId){
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
    public void resizeResourceQuota(ServiceInstance instance, Map<String, Object> cuzQuota) throws IOException{
        yarnCommonService.resizeResourceQuota(instance, cuzQuota);
    }

}
