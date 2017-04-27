package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.ambariClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.yarnClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.OCDPServiceException;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.CapacitySchedulerConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.CustomizeQuotaItem;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.PlanMetadata;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.RangerV2Policy;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.YarnCapacityCaculater;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.model.Plan;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by baikai on 8/5/16.
 */
@Service
public class YarnCommonService {

    private Logger logger = LoggerFactory.getLogger(YarnCommonService.class);

    static final Gson gson = new GsonBuilder().create();

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private rangerClient rc;

    private ambariClient ambClient;

    private yarnClient yClient;

    private YarnCapacityCaculater capacityCaculater;

    @Autowired
    public YarnCommonService(ClusterConfig clusterConfig){
        this.clusterConfig = clusterConfig;

        this.rc = clusterConfig.getRangerClient();

        this.ambClient = clusterConfig.getAmbariClient();

        this.yClient = clusterConfig.getYarnClient();
    }

    public synchronized String createQueue(String quota){
        String csConfig;
        String clusterTotalMemory;
        String provisionedQueue;
        String queuePath = null;
        try {
            csConfig = ambClient.getCapacitySchedulerConfig(clusterConfig.getYarnRMHost());
            CapacitySchedulerConfig csActualConfig = gson.fromJson(csConfig, CapacitySchedulerConfig.class);
            yClient.getClusterMetrics();
            clusterTotalMemory = yClient.getTotalMemory();
            YarnCapacityCaculater capacityCaculater = new YarnCapacityCaculater(clusterTotalMemory,csActualConfig);
            provisionedQueue = capacityCaculater.applyQueue(new Long(quota));
            if(provisionedQueue == null)
                throw new OCDPServiceException("Not Enough Capacity to apply!");
            queuePath = "root."+provisionedQueue;
            this.capacityCaculater = capacityCaculater;
        }catch (Exception e){
            e.printStackTrace();
        }
        return queuePath;
    }

    public synchronized String assignPermissionToQueue(String policyName, final String queueName, String accountName, String groupName){
        ArrayList<String> queueList = new ArrayList<String>(){{add(queueName);}};
        ArrayList<String> groupList = new ArrayList<String>(){{add(groupName);}};
        ArrayList<String> userList = new ArrayList<String>(){{add(accountName);}};
        ArrayList<String> types = new ArrayList<String>(){{add("submit-app");add("admin-queue");}};
        ArrayList<String> conditions = new ArrayList<String>();
        String policyId = this.rc.createYarnPolicy(policyName,"This is Yarn Policy",clusterConfig.getClusterName()+"_yarn",
                queueList,groupList,userList,types,conditions);
        if(policyId != null){
            this.capacityCaculater.addQueueMapping(accountName, queueName);
            ambClient.updateCapacitySchedulerConfig(this.capacityCaculater.getProperties(),clusterConfig.getClusterName());
            ambClient.refreshYarnQueue(clusterConfig.getYarnRMHost());

            logger.info("Complete refresh yarn queues.");
        }
        return policyId;
    }

    public boolean appendUserToQueuePermission(String policyId, String groupName, String accountName){
        return updateUserForQueuePermission(policyId,groupName,accountName,true);
    }

    public synchronized void deleteQueue(String queueName){
        String csConfig;
        String clusterTotalMemory;

        try{
            csConfig = ambClient.getCapacitySchedulerConfig(clusterConfig.getYarnRMHost());
            CapacitySchedulerConfig csActualConfig = gson.fromJson(csConfig, CapacitySchedulerConfig.class);
            yClient.getClusterMetrics();
            clusterTotalMemory = yClient.getTotalMemory();
            YarnCapacityCaculater capacityCaculater = new YarnCapacityCaculater(clusterTotalMemory,csActualConfig);

            capacityCaculater.revokeQueue(queueName);
            capacityCaculater.removeQueueMapping(queueName);
            ambClient.updateCapacitySchedulerConfig(capacityCaculater.getProperties(),clusterConfig.getClusterName());
            ambClient.refreshYarnQueue(clusterConfig.getYarnRMHost());
            logger.info("Complete refresh yarn queues.");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public boolean unassignPermissionFromQueue(String policyId){
        return this.rc.removeV2Policy(policyId);
    }

    public boolean removeUserFromQueuePermission(String policyId, String groupName, String accountName){
        return updateUserForQueuePermission(policyId,groupName,accountName,false);
    }

    private synchronized boolean updateUserForQueuePermission(String policyId, String groupName, String accountName, boolean isAppend){

        String currentPolicy = this.rc.getV2Policy(policyId);
        if (currentPolicy == null)
        {
            return false;
        }
        RangerV2Policy rp = gson.fromJson(currentPolicy, RangerV2Policy.class);
        rp.updatePolicy(
                groupName, accountName, new ArrayList<String>(){{add("submit-app");add("admin-queue");}}, isAppend);
        String queueName = rp.getResourceValues().get(0);
        boolean updateStatus = this.rc.updateV2Policy(policyId, gson.toJson(rp));
        if(updateStatus) {
            try {
                String csConfig = ambClient.getCapacitySchedulerConfig(clusterConfig.getYarnRMHost());
                CapacitySchedulerConfig csActualConfig = gson.fromJson(csConfig, CapacitySchedulerConfig.class);
                yClient.getClusterMetrics();
                String clusterTotalMemory = yClient.getTotalMemory();
                YarnCapacityCaculater capacityCaculater = new YarnCapacityCaculater(clusterTotalMemory, csActualConfig);
                if (isAppend) {
                    capacityCaculater.addQueueMapping(accountName, queueName);
                } else {
                    capacityCaculater.removeQueueMapping(accountName, queueName);
                }
                ambClient.updateCapacitySchedulerConfig(capacityCaculater.getProperties(),clusterConfig.getClusterName());
                ambClient.refreshYarnQueue(clusterConfig.getYarnRMHost());
                logger.info("Complete refresh yarn queues.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return updateStatus;
    }

    public Map<String, String> getQuotaFromPlan(String serviceDefinitionId, String planId, Map<String, Object> cuzQuota){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        Plan plan = catalogConfig.getServicePlan(serviceDefinitionId, planId);
        Map<String, Object> metadata = plan.getMetadata();
        Object customize = metadata.get("customize");
        String yarnQueueQuota, nameSpaceQuota, storageSpaceQuota;
        if(customize != null){
            // Customize quota case
            Map<String, Object> customizeMap = (HashMap<String,Object>)customize;

            CustomizeQuotaItem yarnQueueQuotaItem = (CustomizeQuotaItem) customizeMap.get("yarnQueueQuota");
            String defaultYarnQueueQuota = yarnQueueQuotaItem.getDefault();
            String maxYarnQueueQuota = yarnQueueQuotaItem.getMax();

            CustomizeQuotaItem nameSpaceQuotaItem = (CustomizeQuotaItem) customizeMap.get("nameSpaceQuota");
            String defaultNameSpaceQuota = nameSpaceQuotaItem.getDefault();
            String maxNameSpaceQuota = nameSpaceQuotaItem.getMax();

            CustomizeQuotaItem storageSpaceQuotaItem = (CustomizeQuotaItem) customizeMap.get("storageSpaceQuota");
            String defaultStorageSpaceQuota = storageSpaceQuotaItem.getDefault();
            String maxStorageSpaceQuota = storageSpaceQuotaItem.getMax();

            if (cuzQuota.get("yarnQueueQuota") != null && cuzQuota.get("nameSpaceQuota") != null &&
                    cuzQuota.get("storageSpaceQuota") != null){
                // customize quota have input value
                yarnQueueQuota = (String)cuzQuota.get("yarnQueueQuota");
                nameSpaceQuota = (String)cuzQuota.get("nameSpaceQuota");
                storageSpaceQuota = (String)cuzQuota.get("storageSpaceQuota");
                // If customize quota exceeds plan limitation, use default value
                if (Long.parseLong(yarnQueueQuota) > Long.parseLong(maxYarnQueueQuota)){
                    yarnQueueQuota = defaultYarnQueueQuota;
                }
                if (Long.parseLong(nameSpaceQuota) > Long.parseLong(maxNameSpaceQuota)){
                    nameSpaceQuota = defaultNameSpaceQuota;
                }
                if(Long.parseLong(storageSpaceQuota) > Long.parseLong(maxStorageSpaceQuota)){
                    storageSpaceQuota = defaultStorageSpaceQuota;
                }

            }else {
                // customize quota have not input value, use default value
                yarnQueueQuota = defaultYarnQueueQuota;
                nameSpaceQuota = defaultNameSpaceQuota;
                storageSpaceQuota = defaultStorageSpaceQuota;
            }
        }else{
            // Non customize quota case, use plan.metadata.bullets
            List<String> bullets = (ArrayList)metadata.get("bullets");
            yarnQueueQuota = bullets.get(0).split(":")[1];
            nameSpaceQuota = bullets.get(1).split(":")[1];
            storageSpaceQuota = bullets.get(2).split(":")[1];
        }
        Map<String, String> quota = new HashMap<>();
        quota.put("yarnQueueQuota", yarnQueueQuota);
        quota.put("nameSpaceQuotaa", nameSpaceQuota);
        quota.put("storageSpaceQuota", storageSpaceQuota);
        return quota;
    }

}
