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
import java.util.UUID;

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
                                     Map<String, Object> cuzQuota) throws Exception {
        Map<String, String> quota = this.yarnCommonService.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
        return yarnCommonService.createQueue(quota.get(OCDPConstants.YARN_QUEUE_QUOTA));
    }

    @Override
    public String createPolicyForResources(String policyName, final List<String> resources, String userName, String groupName) {
        List <String> hdfsFolderForJobExec = new ArrayList<String>(){
            {
                add("/user/" + userName);
                add("/spark-history");
                //add dummy path to avoid ranger error of existing resource path
                add("/tmp/dummy_" + UUID.randomUUID().toString());
            }
        };
        String hdfsPolicyId = this.hdfsAdminService.createPolicyForResources(
                userName + "_" + policyName, hdfsFolderForJobExec, userName, groupName);
        if ( hdfsPolicyId != null){
            logger.info("Assign permissions for folder " + hdfsFolderForJobExec.toString()  + " with policy id " + hdfsPolicyId);
        }

        String resource = resources.get(0);
        String yarnPolicyId = this.yarnCommonService.assignPermissionToQueue(policyName, resource, userName, groupName);
        if ( yarnPolicyId != null){
            logger.info("Assign permissions for folder " + resource  + " with policy id " + yarnPolicyId);
        }
        // return policy ids if both yarn policy and hdfs policy create successfully
        return ( hdfsPolicyId != null && yarnPolicyId != null) ? hdfsPolicyId + ":" + yarnPolicyId : null;
    }

    @Override
    public boolean appendResourcesToPolicy(String policyId, String serviceInstanceResource){
        return yarnCommonService.appendResourceToQueuePermission(policyId, serviceInstanceResource);
    }

    @Override
    public boolean appendUserToPolicy(
            String policyId, String groupName, String userName, List<String> permissions) {
        String[] policyIds = policyId.split(":");
        boolean userAppendToHDFSPolicy = this.hdfsAdminService.appendUserToPolicy(
                policyIds[0], groupName, userName, new ArrayList<String>(){{add("read");add("write");add("execute");}});
        boolean resourceAppendToHDFSPolicy = this.hdfsAdminService.appendResourcesToPolicy(
                policyIds[0], "/user/" + userName);
        boolean userAppendToYarnPolicy = this.yarnCommonService.appendUserToQueuePermission(
                policyIds[1], groupName, userName, permissions);
        return userAppendToHDFSPolicy && resourceAppendToHDFSPolicy && userAppendToYarnPolicy;
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{
        this.yarnCommonService.deleteQueue(serviceInstanceResuorceName);
    }

    @Override
    public boolean deletePolicyForResources(String policyId) {
        String[] policyIds = policyId.split(":");
        logger.info("Unassign read/write/execute permission to hdfs folder.");
        boolean hdfsPolicyDeleted = this.hdfsAdminService.deletePolicyForResources(policyIds[0]);
        logger.info("Unassign submit/admin permission to yarn queue.");
        boolean yarnPolicyDeleted = this.yarnCommonService.unassignPermissionFromQueue(policyIds[1]);
        return hdfsPolicyDeleted && yarnPolicyDeleted;
    }

    @Override
    public boolean removeResourceFromPolicy(String policyId, String serviceInstanceResource){
        return yarnCommonService.removeResourceFromQueuePermission(policyId, serviceInstanceResource);
    }

    @Override
    public boolean removeUserFromPolicy(String policyId, String userName) {
        String[] policyIds = policyId.split(":");
        boolean userRemovedFromHDFSPolicy = this.hdfsAdminService.removeUserFromPolicy(policyIds[0], userName);
        boolean resourceRemovedFromHDFSPolicy = this.hdfsAdminService.removeResourceFromPolicy(
                policyIds[0], "/user/" + userName);
        boolean userRemovedFromYarnPolicy = this.yarnCommonService.removeUserFromQueuePermission(
                policyIds[1], userName);
        return userRemovedFromHDFSPolicy && resourceRemovedFromHDFSPolicy && userRemovedFromYarnPolicy;
    }

    //not used
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
