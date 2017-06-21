package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.ambariClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.yarnClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.OCDPServiceException;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.CapacitySchedulerConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.RangerV2Policy;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.YarnCapacityCalculator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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

    private YarnCapacityCalculator capacityCalculator;

    @Autowired
    public YarnCommonService (ClusterConfig clusterConfig) throws IOException{
        this.clusterConfig = clusterConfig;

        this.rc = clusterConfig.getRangerClient();

        this.ambClient = clusterConfig.getAmbariClient();

        this.yClient = clusterConfig.getYarnClient();
    }

    public synchronized String createQueue(String quota) throws IOException{
        String provisionedQueue;
        String queuePath;
        logger.info("Try to calculate queue capacity using quota.");
        try {
            renewCapacityCaculater();
            provisionedQueue = capacityCalculator.applyQueue(new Long(quota));
            if(provisionedQueue == null)
                throw new OCDPServiceException("Not Enough Queue Capacity to apply!");
            queuePath = "root."+provisionedQueue;
        }catch (Exception e){
            e.printStackTrace();
            throw e;
        }
        logger.info("Queue capacity calculated successfully!");
        return queuePath;
    }

    public synchronized String assignPermissionToQueue(String policyName, final String queueName, String accountName, String groupName){
        logger.info("Assign submit-app/admin-queue permission to yarn queue.");
        String policyId = null;
        ArrayList<String> queueList = new ArrayList<String>(){{add(queueName);}};
        ArrayList<String> groupList = new ArrayList<String>(){{add(groupName);}};
        ArrayList<String> userList = new ArrayList<String>(){{add(accountName);}};
        ArrayList<String> types = new ArrayList<String>(){{add("submit-app");add("admin-queue");}};
        ArrayList<String> conditions = new ArrayList<String>();
        RangerV2Policy rp = new RangerV2Policy(
                policyName,"","This is Yarn Policy",clusterConfig.getClusterName()+"_yarn",true,true);
        rp.addResources2("queue", queueList,false,true);
        rp.addPolicyItems(userList,groupList,conditions,false,types);
        String newPolicyString = rc.createV2Policy(rp);
        if (newPolicyString != null){
            RangerV2Policy newPolicyObj = gson.fromJson(newPolicyString, RangerV2Policy.class);
            policyId = newPolicyObj.getPolicyId();
        }
        return policyId;
    }

    public boolean appendResourceToQueuePermission(String policyId, String queueName) {
        boolean updateResult = rc.appendResourceToV2Policy(policyId, queueName, "queue");
        if(updateResult){
            renewCapacityCaculater();
            List<String> users = rc.getUsersFromV2Policy(policyId);
            if(users.size() >= 1){
                for (String user : users){
                    capacityCalculator.addQueueMapping(user, queueName);
                }
                ambClient.updateCapacitySchedulerConfig(this.capacityCalculator.getProperties(),clusterConfig.getClusterName());
                ambClient.refreshYarnQueue(clusterConfig.getYarnRMHost());
            }
        }
        return updateResult;
    }

    public boolean appendUserToQueuePermission(String policyId, String groupName, String accountName, List<String> permissions){
        boolean updateResult = rc.appendUserToV2Policy(policyId, groupName, accountName, permissions);
        if(updateResult){
            renewCapacityCaculater();
            List<String> queues = rc.getResourcsFromV2Policy(policyId, "queue");
            for(String queue : queues) {
                capacityCalculator.addQueueMapping(accountName, queue);
            }
            ambClient.updateCapacitySchedulerConfig(capacityCalculator.getProperties(),clusterConfig.getClusterName());
            ambClient.refreshYarnQueue(clusterConfig.getYarnRMHost());
        }
        return updateResult;
    }

    public synchronized void deleteQueue(String queueName){
        try{
            capacityCalculator.revokeQueue(queueName);
            capacityCalculator.removeQueueMapping(queueName);
            ambClient.updateCapacitySchedulerConfig(capacityCalculator.getProperties(),clusterConfig.getClusterName());
            renewCapacityCaculater();
            capacityCalculator.revokeQueue(queueName);
            capacityCalculator.removeQueueMapping(queueName);
            ambClient.updateCapacitySchedulerConfig(capacityCalculator.getProperties(),clusterConfig.getClusterName());
            ambClient.refreshYarnQueue(clusterConfig.getYarnRMHost());
            logger.info("Complete refresh yarn queues.");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public boolean unassignPermissionFromQueue(String policyId){
        logger.info("Unassign submit/admin permission to yarn queue.");
        return this.rc.removeV2Policy(policyId);
    }

    public boolean removeResourceFromQueuePermission(String policyId, String queueName){
        boolean updateResult = rc.removeResourceFromV2Policy(policyId, queueName, "queue");
        if(updateResult){
            renewCapacityCaculater();
            List<String> users = rc.getUsersFromV2Policy(policyId);
            if(users.size() >= 1){
                for (String user : users){
                    this.capacityCalculator.removeQueueMapping(user, queueName);
                }
                ambClient.updateCapacitySchedulerConfig(this.capacityCalculator.getProperties(),clusterConfig.getClusterName());
                ambClient.refreshYarnQueue(clusterConfig.getYarnRMHost());
            }
        }
        return updateResult;
    }

    public boolean removeUserFromQueuePermission(String policyId, String accountName){
        boolean updateResult = rc.removeUserFromV2Policy(policyId, accountName);
        if(updateResult){
            renewCapacityCaculater();
            List<String> queues = rc.getResourcsFromV2Policy(policyId, "queue");
            for(String queue : queues) {
                capacityCalculator.removeQueueMapping(accountName, queue);
            }
            ambClient.updateCapacitySchedulerConfig(capacityCalculator.getProperties(),clusterConfig.getClusterName());
            ambClient.refreshYarnQueue(clusterConfig.getYarnRMHost());
        }
        return updateResult;
    }

    public  List<String> getResourceFromQueuePolicy(String policyId){
        return rc.getResourcsFromV2Policy(policyId, "queue");
    }

    public Map<String, String> getQuotaFromPlan(String serviceDefinitionId, String planId, Map<String, Object> cuzQuota){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        List<String> quotaKeys = new ArrayList<String>(){{add("yarnQueueQuota");}};
        return catalogConfig.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota, quotaKeys);
    }

    public void resizeResourceQuota(ServiceInstance instance, Map<String, Object> cuzQuota){
        renewCapacityCaculater();
        String serviceDefinitionId = instance.getServiceDefinitionId();
        String planId = instance.getPlanId();
        Map<String, String> quota = getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
        String resourceType = OCDPAdminServiceMapper.getOCDPResourceType(serviceDefinitionId);
        String queueName = (String)instance.getServiceInstanceCredentials().get(resourceType);
        capacityCalculator.updateQueue(queueName, new Long(quota.get("yarnQueueQuota")));
    }

    private void renewCapacityCaculater(){
        String csConfig = ambClient.getCapacitySchedulerConfig(clusterConfig.getYarnRMHost());
        CapacitySchedulerConfig csActualConfig = gson.fromJson(csConfig, CapacitySchedulerConfig.class);
        yClient.getClusterMetrics();
        String clusterTotalMemory = yClient.getTotalMemory();
        this.capacityCalculator = new YarnCapacityCalculator(clusterTotalMemory,csActualConfig);
    }

}
