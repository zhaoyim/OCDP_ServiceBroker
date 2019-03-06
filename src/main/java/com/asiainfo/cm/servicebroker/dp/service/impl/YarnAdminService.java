package com.asiainfo.cm.servicebroker.dp.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.asiainfo.cm.servicebroker.dp.config.ClusterConfig;
import com.asiainfo.cm.servicebroker.dp.model.ServiceInstance;
import com.asiainfo.cm.servicebroker.dp.service.OCDPAdminService;
import com.asiainfo.cm.servicebroker.dp.service.common.YarnCommonService;
import com.asiainfo.cm.servicebroker.dp.utils.OCDPConstants;
import com.google.common.collect.Lists;

/**
 * Created by baikai on 8/4/16.
 */
@Service
public class YarnAdminService implements OCDPAdminService {
    private Logger logger = LoggerFactory.getLogger(YarnAdminService.class);

    private ClusterConfig clusterConfig;

    private YarnCommonService yarnCommonService;

    private HDFSAdminService hdfsAdminService;

    @Autowired
    public YarnAdminService(ClusterConfig clusterConfig,
                            YarnCommonService yarnCommonService, HDFSAdminService hdfsAdminService){
    	logger.info("ClusterConfig: " + clusterConfig);
        this.clusterConfig = clusterConfig;
        this.yarnCommonService = yarnCommonService;
        this.hdfsAdminService = hdfsAdminService;
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
                                     Map<String, Object> parameters) throws Exception {
        String queueName = parameters.get("cuzBsiName") == null ? null : String.valueOf(parameters.get("cuzBsiName"));
        Map<String, String> quota = this.yarnCommonService.getQuotaFromPlan(serviceDefinitionId, planId, parameters);
        return yarnCommonService.createQueue(quota.get(OCDPConstants.YARN_QUEUE_QUOTA), queueName);
    }

    @Override
    public String createPolicyForResources(String policyName, final List<String> resources, List<String> userList,
                                           String groupName, List<String> permissions) {
        String historyPath = "/" + policyName.split("_")[0] + "-history";
        // Temp fix: for 'create instance in tenant' case,
        // create one ranger policy for multiple user and multiple /user/<userName> dirs
        // Please refer to: https://github.com/OCManager/OCDP_ServiceBroker/issues/48
        List <String> hdfsFolderForJobExec = new ArrayList<String>(){
            {
                add(historyPath);
                //add dummy path to avoid ranger error of existing resource path
                add("/tmp/dummy_" + UUID.randomUUID().toString());
            	if (historyPath.contains("spark"))
            		add(historyPath.replace("spark", "spark2"));// suport spark2
            }
        };
        for (String userName : userList) {
            hdfsFolderForJobExec.add("/user/" + userName);
            createHdfsPath("/user/" + userName);
            //support for log aggregation
            hdfsFolderForJobExec.add("/app-logs/" + userName);
            createHdfsPath("/app-logs/" + userName);
        }
        String hdfsPolicyId = this.hdfsAdminService.createPolicyForResources(
                policyName, hdfsFolderForJobExec, userList, groupName, null);
        if ( hdfsPolicyId != null){
            logger.info("Assign permissions for folder " + hdfsFolderForJobExec.toString()  + " with policy id " + hdfsPolicyId);
        }

        String resource = resources.get(0);
        String yarnPolicyId = this.yarnCommonService.assignPermissionToQueue(policyName, resource, userList, groupName, null);
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
    public boolean appendUsersToPolicy(
            String policyId, String groupName, List<String> users, List<String> permissions) {
        String[] policyIds = policyId.split(":");
        // Temp fix: when update pass multiple users, append users to policy that create for multiple users and multiple /user/<userName> dirs
        // Please refer to: https://github.com/OCManager/OCDP_ServiceBroker/issues/48
        boolean resourceAppendToHDFSPolicy = false;
        for (String user : users) {
            logger.info("Create /user dir for user ", user);
            createHdfsPath("/user/" + user);
            resourceAppendToHDFSPolicy = this.hdfsAdminService.appendResourcesToPolicy(policyIds[0], "/user/" + user);
        }
        boolean userAppendToHDFSPolicy = this.hdfsAdminService.appendUsersToPolicy(
                policyIds[0], groupName, users, Lists.newArrayList("read", "write","execute"));
        boolean userAppendToYarnPolicy = this.yarnCommonService.appendUsersToQueuePermission(
                policyIds[1], groupName, users, permissions);
        return userAppendToHDFSPolicy && resourceAppendToHDFSPolicy && userAppendToYarnPolicy;
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{
        this.yarnCommonService.deleteQueue(serviceInstanceResuorceName);
    }

    @Override
    public boolean deletePolicyForResources(String policyId) {
        String[] policyIds = policyId.split(":");
        logger.info("Delete hdfs ranger policy " + policyIds[0]);
        boolean hdfsPolicyDeleted = this.hdfsAdminService.deletePolicyForResources(policyIds[0]);
        logger.info("Delete yarn ranger policy " + policyIds[1]);
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
    public Map<String, Object> generateCredentialsInfo(String resource){
        return new HashMap<String, Object>(){
            {
                put("uri", clusterConfig.getYarnRMUrl());
                put("host", clusterConfig.getYarnRMHost());
                put("port", clusterConfig.getYarnRMPort());
                put(OCDPConstants.SPARK_RESOURCE_TYPE, resource);
            }
        };
    }

    @Override
    public void resizeResourceQuota(ServiceInstance instance, Map<String, Object> cuzQuota) throws IOException{
        yarnCommonService.resizeResourceQuota(instance, cuzQuota);
    }

    private void createHdfsPath(String path) {
        try {
            this.hdfsAdminService.createHDFSDir(path, null, null);
        } catch (IOException e) {
            logger.error("Create hdfs user path [{}] failed!", e);
            throw new RuntimeException(e);
        }
    }

}
