package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common.HiveCommonService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common.YarnCommonService;

/**
 * Created by baikai on 5/19/16.
 */
@Service
public class HiveAdminService implements OCDPAdminService {

    private Logger logger = LoggerFactory.getLogger(HiveAdminService.class);

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private HiveCommonService hiveCommonService;

    private HDFSAdminService hdfsAdminService;

    private YarnCommonService yarnCommonService;

    @Autowired
    public HiveAdminService(ClusterConfig clusterConfig, HiveCommonService hiveCommonService, HDFSAdminService hdfsAdminService,
                            YarnCommonService yarnCommonService){
        this.clusterConfig = clusterConfig;
        this.hiveCommonService = hiveCommonService;
        this.hdfsAdminService = hdfsAdminService;
        this.yarnCommonService = yarnCommonService;
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
                                     Map<String, Object> cuzQuota) throws Exception{
        Map<String, String> quota = this.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
        String dbName = hiveCommonService.createDatabase(serviceInstanceId);
        // Set database storage quota
        if(dbName != null){
            hdfsAdminService.setQuota(
                    "/apps/hive/warehouse/" + dbName + ".db", "1000", quota.get(OCDPConstants.HDFS_STORAGE_QUOTA));
        }
        String queueName = yarnCommonService.createQueue(quota.get(OCDPConstants.YARN_QUEUE_QUOTA));
        return dbName + ":" + queueName;
    }

    @Override
    public String createPolicyForResources(String policyName, List<String> resources, String userName, String groupName){
        String[] resourcesList = resources.get(0).split(":");
        String hivePolicyId = this.hiveCommonService.assignPermissionToDatabase(
                policyName, resourcesList[0], userName, groupName);
        logger.info("Create corresponding hdfs policy for hive user");
        List<String> hdfsFolders = new ArrayList<String>(){
            {
                add("/apps/hive/warehouse/" + resourcesList[0] + ".db");
                add("/user/" + userName);
                add("/tmp/hive");
                add("/ats/active");
            }
        };
        String hdfsPolicyId = this.hdfsAdminService.createPolicyForResources(
                "hive_" + policyName, hdfsFolders, userName, groupName);
        logger.info("Create corresponding yarn policy for hive user");
        String yarnPolicyId = this.yarnCommonService.assignPermissionToQueue(
                "hive_" + policyName, resourcesList[1], userName, groupName);
        return (hivePolicyId != null && hdfsPolicyId != null && yarnPolicyId != null) ? hivePolicyId + ":" + hdfsPolicyId + ":" + yarnPolicyId : null;
    }

    @Override
    public boolean appendResourcesToPolicy(String policyId, String serviceInstanceResource){
        String[] resourcesList = serviceInstanceResource.split(":");
        boolean appendResourceToHivePolicy = hiveCommonService.appendResourceToDatabasePermission(
                policyId, resourcesList[0]);
        boolean appendResourceToYarnPolicy = yarnCommonService.appendResourceToQueuePermission(
                policyId, resourcesList[1]);
        return appendResourceToHivePolicy && appendResourceToYarnPolicy;
    }

    @Override
    public boolean appendUserToPolicy(
            String policyId, String groupName, String userName, List<String> permissions){
        String[] policyIds = policyId.split(":");
        boolean userAppendToHivePolicy = this.hiveCommonService.appendUserToDatabasePermission(
                policyIds[0], groupName, userName, permissions);
        boolean userAppendToHDFSPolicy = this.hdfsAdminService.appendUserToPolicy(
                policyIds[1], groupName, userName, permissions);
        boolean userAppendToYarnPolicy = this.yarnCommonService.appendUserToQueuePermission(
                policyIds[2], groupName, userName, permissions);
        return userAppendToHivePolicy && userAppendToHDFSPolicy && userAppendToYarnPolicy;
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{
        String[] resources = serviceInstanceResuorceName.split(":");
        this.hiveCommonService.deleteDatabase(resources[0]);
        this.yarnCommonService.deleteQueue(resources[1]);
    }

    @Override
    public boolean deletePolicyForResources(String policyId){
        String[] policyIds = policyId.split(":");
        logger.info("Unassign select/update/create/drop/alter/index/lock/all permission to hive database.");
        boolean hivePolicyDeleted = this.hiveCommonService.unassignPermissionFromDatabase(policyIds[0]);
        logger.info("Unassign read/write/execute permission to hdfs folder.");
        boolean hdfsPolicyDeleted = this.hdfsAdminService.deletePolicyForResources(policyIds[1]);
        logger.info("Unassign submit/admin permission to yarn queue.");
        boolean yarnPolicyDeleted = this.yarnCommonService.unassignPermissionFromQueue(policyIds[2]);
        return hivePolicyDeleted && hdfsPolicyDeleted && yarnPolicyDeleted;
    }

    @Override
    public boolean removeResourceFromPolicy(String policyId, String serviceInstanceResource){
        String[] resourcesList = serviceInstanceResource.split(":");
        boolean removeResourceFromHivePolicy = hiveCommonService.removeResourceFromDatabasePermission(
                policyId, resourcesList[0]);
        boolean removeResourceFromYarnPolicy = yarnCommonService.removeResourceFromQueuePermission(
                policyId, resourcesList[1]);
        return removeResourceFromHivePolicy && removeResourceFromYarnPolicy;
    }

    @Override
    public boolean removeUserFromPolicy(String policyId, String userName){
        String[] policyIds = policyId.split(":");
        boolean userRemovedFromHivePolicy = this.hiveCommonService.removeUserFromDatabasePermission(
                policyIds[0], userName);
        boolean userRemovedFromHDFSPolicy = this.hdfsAdminService.removeUserFromPolicy(policyIds[1], userName);
        boolean userRemovedFromYarnPolicy = this.yarnCommonService.removeUserFromQueuePermission(
                policyIds[2], userName);
        return userRemovedFromHivePolicy && userRemovedFromHDFSPolicy && userRemovedFromYarnPolicy;
    }

    @Override
    public Map<String, Object> generateCredentialsInfo(String serviceInstanceId){
        String dbName = serviceInstanceId.replaceAll("-", "");
        return new HashMap<String, Object>(){
            {
                put("uri", "jdbc:hive2://" + clusterConfig.getHiveHost() + ":" +
                        clusterConfig.getHivePort() + "/" + dbName + ";principal=" + clusterConfig.getHiveSuperUser());
                put("host", clusterConfig.getHiveHost());
                put("port", clusterConfig.getHivePort());
            }
        };
    }

    @Override
    public List<String> getResourceFromPolicy(String policyId){
        // Get hive ranger policy id
        policyId = policyId.split(":")[0];
        return hiveCommonService.getResourceFromDatabasePolicy(policyId);
    }

    @Override
    public void resizeResourceQuota(ServiceInstance instance, Map<String, Object> cuzQuota) throws IOException{
        // Resize yarn queue
        yarnCommonService.resizeResourceQuota(instance, cuzQuota);
        // Resize hdfs folder
        hdfsAdminService.resizeResourceQuota(instance, cuzQuota);
    }

    private Map<String, String> getQuotaFromPlan(String serviceDefinitionId, String planId,
                                                 Map<String, Object> cuzQuota){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        return catalogConfig.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
    }

}
