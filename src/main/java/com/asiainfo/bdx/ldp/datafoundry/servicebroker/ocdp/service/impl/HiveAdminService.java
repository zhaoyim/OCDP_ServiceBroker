package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.CustomizeQuotaItem;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common.HiveCommonService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common.YarnCommonService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.model.Plan;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

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
            hdfsAdminService.setQuota("/apps/hive/warehouse/" + dbName + ".db", new Long("1000"), new Long(quota.get("hiveStorageQuota")) * 1000000000);
        }
        String queueName = yarnCommonService.createQueue(quota.get("yarnQueueQuota"));
        return dbName + ":" + queueName;
    }

    @Override
    public String assignPermissionToResources(String policyName, List<String> resources, String accountName, String groupName){
        String[] resourcesList = resources.get(0).split(":");
        logger.info("Assign select/update/create/drop/alter/index/lock/all permission to hive database.");
        String hivePolicyId = this.hiveCommonService.assignPermissionToDatabase(policyName, resourcesList[0], accountName, groupName);
        logger.info("Create corresponding hdfs policy for hive tenant");
        List<String> hdfsFolders = new ArrayList<String>(){
            {
                add("/apps/hive/warehouse/" + resourcesList[0] + ".db");
                add("/user/" + accountName);
                add("/tmp/hive");
                add("/ats/active");
            }
        };
        String hdfsPolicyId = this.hdfsAdminService.assignPermissionToResources("hive_" + policyName, hdfsFolders, accountName, groupName);
        logger.info("Create corresponding yarn policy for hive tenant");
        String yarnPolicyId = this.yarnCommonService.assignPermissionToQueue("hive_" + policyName, resourcesList[1], accountName, groupName);
        return (hivePolicyId != null && hdfsPolicyId != null && yarnPolicyId != null) ? hivePolicyId + ":" + hdfsPolicyId + ":" + yarnPolicyId : null;
    }

    @Override
    public boolean appendUserToResourcePermission(String policyId, String groupName, String accountName){
        String[] policyIds = policyId.split(":");
        boolean userAppendToHivePolicy = this.hiveCommonService.appendUserToDatabasePermission(policyIds[0], groupName, accountName);
        boolean userAppendToHDFSPolicy = this.hdfsAdminService.appendUserToResourcePermission(policyIds[1], groupName, accountName);
        boolean userAppendToYarnPolicy = this.yarnCommonService.appendUserToQueuePermission(policyIds[2], groupName, accountName);
        return userAppendToHivePolicy && userAppendToHDFSPolicy && userAppendToYarnPolicy;
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{
        String[] resources = serviceInstanceResuorceName.split(":");
        this.hiveCommonService.deleteDatabase(resources[0]);
        this.yarnCommonService.deleteQueue(resources[1]);
    }

    @Override
    public boolean unassignPermissionFromResources(String policyId){
        String[] policyIds = policyId.split(":");
        logger.info("Unassign select/update/create/drop/alter/index/lock/all permission to hive database.");
        boolean hivePolicyDeleted = this.hiveCommonService.unassignPermissionFromDatabase(policyIds[0]);
        logger.info("Unassign read/write/execute permission to hdfs folder.");
        boolean hdfsPolicyDeleted = this.hdfsAdminService.unassignPermissionFromResources(policyIds[1]);
        logger.info("Unassign submit/admin permission to yarn queue.");
        boolean yarnPolicyDeleted = this.yarnCommonService.unassignPermissionFromQueue(policyIds[2]);
        return hivePolicyDeleted && hdfsPolicyDeleted && yarnPolicyDeleted;
    }

    @Override
    public boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName){
        String[] policyIds = policyId.split(":");
        boolean userRemovedFromHivePolicy = this.hiveCommonService.removeUserFromDatabasePermission(policyIds[0], groupName, accountName);
        boolean userRemovedFromHDFSPolicy = this.hdfsAdminService.removeUserFromResourcePermission(policyIds[1], groupName, accountName);
        boolean userRemovedFromYarnPolicy = this.yarnCommonService.removeUserFromQueuePermission(policyIds[2], groupName, accountName);
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
    public String getServiceResourceType(){
        return "HDFS Path";
    }

    private Map<String, String> getQuotaFromPlan(String serviceDefinitionId, String planId, Map<String, Object> cuzQuota){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        Plan plan = catalogConfig.getServicePlan(serviceDefinitionId, planId);
        Map<String, Object> metadata = plan.getMetadata();
        Object customize = metadata.get("customize");
        String hiveStorageQuota, yarnQueueQuota;
        if(customize != null){
            // Customize quota case
            Map<String, Object> customizeMap = (Map<String,Object>)customize;

            CustomizeQuotaItem hiveStorageQuotaItem = (CustomizeQuotaItem) customizeMap.get("hiveStorageQuota");
            long defaultHiveStorageQuota = hiveStorageQuotaItem.getDefault();
            long maxHiveStorageQuota = hiveStorageQuotaItem.getMax();

            CustomizeQuotaItem yarnQueueQuotaItem = (CustomizeQuotaItem) customizeMap.get("yarnQueueQuota");
            long defaultYarnQueueQuota = yarnQueueQuotaItem.getDefault();
            long maxYarnQueueQuota = yarnQueueQuotaItem.getMax();

            if (cuzQuota.get("hiveStorageQuota") != null && cuzQuota.get("yarnQueueQuota") != null){
                // customize quota have input value
                hiveStorageQuota = (String)cuzQuota.get("hiveStorageQuota");
                yarnQueueQuota = (String)cuzQuota.get("yarnQueueQuota");
                // If customize quota exceeds plan limitation, use default value
                if (Long.parseLong(hiveStorageQuota) > maxHiveStorageQuota){
                    hiveStorageQuota = Long.toString(defaultHiveStorageQuota);
                }
                if(Long.parseLong(yarnQueueQuota) > maxYarnQueueQuota){
                    yarnQueueQuota = Long.toString(defaultYarnQueueQuota);
                }

            }else {
                // customize quota have not input value, use default value
                hiveStorageQuota = Long.toString(defaultHiveStorageQuota);
                yarnQueueQuota = Long.toString(defaultYarnQueueQuota);
            }
        }else{
            // Non customize quota case, use plan.metadata.bullets
            List<String> bullets = (ArrayList)metadata.get("bullets");
            hiveStorageQuota = bullets.get(0).split(":")[1];
            yarnQueueQuota = bullets.get(1).split(":")[1];
        }
        Map<String, String> quota = new HashMap<>();
        quota.put("hiveStorageQuota", hiveStorageQuota);
        quota.put("yarnQueueQuota", yarnQueueQuota);
        return quota;
    }

}
