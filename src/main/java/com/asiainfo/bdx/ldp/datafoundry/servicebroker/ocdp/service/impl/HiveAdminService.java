package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId, String bindingId,
                                     Map<String, Object> cuzQuota) throws Exception{
        Map<String, String> quota = this.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
        String dbName = hiveCommonService.createDatabase(serviceInstanceId);
        // Set database storage quota
        if(dbName != null){
            hdfsAdminService.setQuota("/apps/hive/warehouse/" + dbName + ".db", "1000", quota.get("storageSpaceQuota"));
        }
        String queueName = yarnCommonService.createQueue(quota.get("yarnQueueQuota"));
        return dbName + ":" + queueName;
    }

    @Override
    public String createPolicyForTenant(String policyName, List<String> resources, String tenantName, String groupName){
        String[] resourcesList = resources.get(0).split(":");
        String hivePolicyId = this.hiveCommonService.assignPermissionToDatabase(policyName, resourcesList[0], tenantName, groupName);
        logger.info("Create corresponding hdfs policy for hive tenant");
        List<String> hdfsFolders = new ArrayList<String>(){
            {
                add("/apps/hive/warehouse/" + resourcesList[0] + ".db");
                add("/user/" + tenantName);
                add("/tmp/hive");
                add("/ats/active");
            }
        };
        String hdfsPolicyId = this.hdfsAdminService.createPolicyForTenant("hive_" + policyName, hdfsFolders, tenantName, groupName);
        logger.info("Create corresponding yarn policy for hive tenant");
        String yarnPolicyId = this.yarnCommonService.assignPermissionToQueue("hive_" + policyName, resourcesList[1], tenantName, groupName);
        return (hivePolicyId != null && hdfsPolicyId != null && yarnPolicyId != null) ? hivePolicyId + ":" + hdfsPolicyId + ":" + yarnPolicyId : null;
    }

    @Override
    public boolean appendResourceToTenantPolicy(String policyId, String serviceInstanceResource){
        return hiveCommonService.appendResourceToDatabasePermission(policyId, serviceInstanceResource);
    }

    @Override
    public boolean appendUserToTenantPolicy(
            String policyId, String groupName, String accountName, List<String> permissions){
        String[] policyIds = policyId.split(":");
        boolean userAppendToHivePolicy = this.hiveCommonService.appendUserToDatabasePermission(
                policyIds[0], groupName, accountName, permissions);
        boolean userAppendToHDFSPolicy = this.hdfsAdminService.appendUserToTenantPolicy(
                policyIds[1], groupName, accountName, permissions);
        boolean userAppendToYarnPolicy = this.yarnCommonService.appendUserToQueuePermission(
                policyIds[2], groupName, accountName, permissions);
        return userAppendToHivePolicy && userAppendToHDFSPolicy && userAppendToYarnPolicy;
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{
        String[] resources = serviceInstanceResuorceName.split(":");
        this.hiveCommonService.deleteDatabase(resources[0]);
        this.yarnCommonService.deleteQueue(resources[1]);
    }

    @Override
    public boolean deletePolicyForTenant(String policyId){
        String[] policyIds = policyId.split(":");
        logger.info("Unassign select/update/create/drop/alter/index/lock/all permission to hive database.");
        boolean hivePolicyDeleted = this.hiveCommonService.unassignPermissionFromDatabase(policyIds[0]);
        logger.info("Unassign read/write/execute permission to hdfs folder.");
        boolean hdfsPolicyDeleted = this.hdfsAdminService.deletePolicyForTenant(policyIds[1]);
        logger.info("Unassign submit/admin permission to yarn queue.");
        boolean yarnPolicyDeleted = this.yarnCommonService.unassignPermissionFromQueue(policyIds[2]);
        return hivePolicyDeleted && hdfsPolicyDeleted && yarnPolicyDeleted;
    }

    @Override
    public boolean removeResourceFromTenantPolicy(String policyId, String serviceInstanceResource){
        return hiveCommonService.removeResourceFromDatabasePermission(policyId, serviceInstanceResource);
    }

    @Override
    public boolean removeUserFromTenantPolicy(String policyId, String accountName){
        String[] policyIds = policyId.split(":");
        boolean userRemovedFromHivePolicy = this.hiveCommonService.removeUserFromDatabasePermission(policyIds[0], accountName);
        boolean userRemovedFromHDFSPolicy = this.hdfsAdminService.removeUserFromTenantPolicy(policyIds[1], accountName);
        boolean userRemovedFromYarnPolicy = this.yarnCommonService.removeUserFromQueuePermission(policyIds[2], accountName);
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
                put("Hive database", dbName);
            }
        };
    }

    @Override
    public  List<String> getResourceFromTenantPolicy(String policyId){
        return hiveCommonService.getResourceFromDatabasePolicy(policyId);
    }

    @Override
    public void resizeResourceQuota(ServiceInstance instance, Map<String, Object> cuzQuota) throws IOException{
        // Resize yarn queue
        yarnCommonService.resizeResourceQuota(instance, cuzQuota);
        // Resize hdfs folder
        hdfsAdminService.resizeResourceQuota(instance, cuzQuota);
    }

    private Map<String, String> getQuotaFromPlan(String serviceDefinitionId, String planId, Map<String, Object> cuzQuota){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        List<String> quotaKeys = new ArrayList<String>(){{add("storageSpaceQuota"); add("yarnQueueQuota");}};
        return catalogConfig.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota, quotaKeys);
    }

}
