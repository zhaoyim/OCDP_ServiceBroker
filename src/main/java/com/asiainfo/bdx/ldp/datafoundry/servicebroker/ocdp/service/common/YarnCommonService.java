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
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPConstants;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.YarnCapacityCalculator;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by baikai on 8/5/16.
 */
@Service
public class YarnCommonService {

    private Logger logger = LoggerFactory.getLogger(YarnCommonService.class);

    static final Gson gson = new GsonBuilder().create();

    private static final List<String> ACCESSES = Lists.newArrayList("submit-app", "admin-queue");

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
        String queue_suffix;
        String queuePath;
        logger.info("Try to calculate queue capacity using quota [{}] GB", quota);
        try {
            renewCapacityCaculater();
            queue_suffix = capacityCalculator.applyQueue(new Long(quota));
            if(queue_suffix == null)
                throw new OCDPServiceException("Not Enough Queue Capacity to apply!");
            queuePath = "root."+queue_suffix;
        }catch (Exception e){
            e.printStackTrace();
            throw e;
        }
        logger.info("Name of new queue: " + queuePath);
        ambClient.updateCapacitySchedulerConfig(this.capacityCalculator.getProperties(),clusterConfig.getClusterName());
        ambClient.refreshYarnQueue(clusterConfig.getYarnRMHost());
        logger.info("Queue capacity refreshing...");
        return queuePath;
    }

    public synchronized String assignPermissionToQueue(String policyName, final String queueName, List<String> userList,
                                                       String groupName, List<String> permissions){
        String policyId = null;
        ArrayList<String> queueList = Lists.newArrayList(queueName);
        ArrayList<String> groupList = Lists.newArrayList(groupName);
       // ArrayList<String> userList = new ArrayList<String>(){{add(userName);}};
        //ArrayList<String> types = new ArrayList<String>(){{add("submit-app");add("admin-queue");}};
        ArrayList<String> conditions = Lists.newArrayList();
        RangerV2Policy rp = new RangerV2Policy(
                policyName,"","This is Yarn Policy",clusterConfig.getClusterName()+"_yarn",true,true);
        rp.addResources2(OCDPConstants.YARN_RANGER_RESOURCE_TYPE, queueList,false, true);
        if(permissions == null){
            rp.addPolicyItems(userList,groupList,conditions,false,ACCESSES);
        } else {
            rp.addPolicyItems(userList,groupList,conditions,false,permissions);
        }
        String newPolicyString = rc.createV2Policy(rp);
        if (newPolicyString != null){
            RangerV2Policy newPolicyObj = gson.fromJson(newPolicyString, RangerV2Policy.class);
            policyId = newPolicyObj.getPolicyId();
        }
        else {
            logger.error("Failed to assign permissions to yarn queue [{}] for user [{}]." ,queueName, userList.toString());
        }
        return policyId;
    }
//not used
    public boolean appendResourceToQueuePermission(String policyId, String queueName) {
        boolean updateResult = rc.appendResourceToV2Policy(policyId, queueName, OCDPConstants.YARN_RANGER_RESOURCE_TYPE);
        if(updateResult){
            List<String> users = rc.getUsersFromV2Policy(policyId);
            if(users.size() >= 1){
                for (String user : users){
                    capacityCalculator.addQueueMapping(user, queueName);
                    logger.info("Add queue mapping: user = " + user +", queueName = " + queueName);
                }
                ambClient.updateCapacitySchedulerConfig(this.capacityCalculator.getProperties(),clusterConfig.getClusterName());
                ambClient.refreshYarnQueue(clusterConfig.getYarnRMHost());
                logger.info("Queue capacity refreshing...");

            }
        }
        return updateResult;
    }

    public boolean appendUsersToQueuePermission(String policyId, String groupName, List<String> users, List<String> permissions){
        return rc.appendUsersToV2Policy(policyId, groupName, users, permissions);
    }

    public synchronized void deleteQueue(String queueName){
        try{
            renewCapacityCaculater();
            capacityCalculator.revokeQueue(queueName);
            ambClient.updateCapacitySchedulerConfig(capacityCalculator.getProperties(),clusterConfig.getClusterName());
            ambClient.refreshYarnQueue(clusterConfig.getYarnRMHost());
            logger.info("Refreshing yarn queues...");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public boolean unassignPermissionFromQueue(String policyId){
        if (rc.getV2Policy(policyId) != null) {
            logger.info("Unassign submit/admin permission to yarn queue.");
            return this.rc.removeV2Policy(policyId);
        }
        else {
            logger.warn("ranger policy " + policyId + " doesn't exist, do not need to remove");
            return true;
        }
    }
//not used
    public boolean removeResourceFromQueuePermission(String policyId, String queueName){
        boolean updateResult = rc.removeResourceFromV2Policy(policyId, queueName, OCDPConstants.YARN_RANGER_RESOURCE_TYPE);
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

    public boolean removeUserFromQueuePermission(String policyId, String userName){
        boolean updateResult = rc.removeUserFromV2Policy(policyId, userName);
        return updateResult;
    }

    public  List<String> getResourceFromQueuePolicy(String policyId){
        return rc.getResourcsFromV2Policy(policyId, OCDPConstants.YARN_RANGER_RESOURCE_TYPE);
    }

    public Map<String, String> getQuotaFromPlan(String serviceDefinitionId, String planId, Map<String, Object> cuzQuota){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        return catalogConfig.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
    }

    public void resizeResourceQuota(ServiceInstance instance, Map<String, Object> cuzQuota){
        renewCapacityCaculater();
        String serviceDefinitionId = instance.getServiceDefinitionId();
        String planId = instance.getPlanId();
        Map<String, String> quota = getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
        String resourceType = OCDPAdminServiceMapper.getOCDPResourceType(serviceDefinitionId);
        String queueName = (String)instance.getServiceInstanceCredentials().get(resourceType);
        if(resourceType.equals(OCDPConstants.HIVE_RESOURCE_TYPE)){
        	queueName = queueName.split(":")[1];
        }
        logger.info("Resizing queue " + queueName + "...");
        capacityCalculator.updateQueue(queueName, new Long(quota.get(OCDPConstants.YARN_QUEUE_QUOTA)));
        ambClient.updateCapacitySchedulerConfig(capacityCalculator.getProperties(),clusterConfig.getClusterName());
        ambClient.refreshYarnQueue(clusterConfig.getYarnRMHost());
        logger.info("Resizing queue " + queueName + " successfully to " + quota.get(OCDPConstants.YARN_QUEUE_QUOTA));
    }

    private void renewCapacityCaculater(){
        String csConfig = ambClient.getCapacitySchedulerConfig(clusterConfig.getYarnRMHost());
        logger.info("Get Capacity-scheduler Config from ambari: " + csConfig);
        CapacitySchedulerConfig csActualConfig = gson.fromJson(csConfig, CapacitySchedulerConfig.class);
        yClient.getClusterMetrics();
        String clusterTotalMemory = yClient.getTotalMemory();
        this.capacityCalculator = new YarnCapacityCalculator(clusterTotalMemory,csActualConfig);
    }

}
