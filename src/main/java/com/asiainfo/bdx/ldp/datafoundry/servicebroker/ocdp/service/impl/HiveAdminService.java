package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.PathIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common.HiveCommonService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common.YarnCommonService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPConstants;
import com.google.common.collect.Lists;

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

    private etcdClient etcdClient;

    @Autowired
    public HiveAdminService(ClusterConfig clusterConfig, HiveCommonService hiveCommonService, HDFSAdminService hdfsAdminService,
                            YarnCommonService yarnCommonService){
        this.clusterConfig = clusterConfig;
        this.hiveCommonService = hiveCommonService;
        this.hdfsAdminService = hdfsAdminService;
        this.yarnCommonService = yarnCommonService;
        this.etcdClient = clusterConfig.getEtcdClient();
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
                                     Map<String, Object> parameters) throws Exception{
        String resource = parameters.get("cuzBsiName") == null ? serviceInstanceId : String.valueOf(parameters.get("cuzBsiName"));

        Map<String, String> quota = this.getQuotaFromPlan(serviceDefinitionId, planId, parameters);
//        String dbName = hiveCommonService.createDatabase(serviceInstanceId);
        String dbName = hiveCommonService.createDatabase(resource.replaceAll("-", ""));
        // Set database storage quota
        if(dbName != null){
        	setQuota(dbName, quota);
        }
        String queueName = yarnCommonService.createQueue(quota.get(OCDPConstants.YARN_QUEUE_QUOTA), resource);
        // Cache queue name in etcd
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/Credentials/" +
                OCDPConstants.YARN_RESOURCE_TYPE, queueName);
        return dbName + ":" + queueName;
    }

    private void setQuota(String dbName, Map<String, String> quota) throws IOException, InterruptedException {
    	String path = "/apps/hive/warehouse/" + dbName + ".db";
    	int loop = 0;
    	while (true) {
        	try {
                hdfsAdminService.setQuota(path, "-1", quota.get(OCDPConstants.HDFS_STORAGE_QUOTA));
                return;
			} catch (PathIOException e) {
				// In case path hasn't been created yet
				if (loop++ < 5) {
					logger.warn("Path abnormal while setting quota, waiting and retrying: "+ loop  + ", "+ e.getMessage());
					Thread.sleep(2000);
					continue;
				}
				logger.error("Failed to set quota for path: " + path, e);
				throw e;
			}
		}		
	}

	@Override
    public String createPolicyForResources(String policyName, List<String> resources, List<String> userList,
                                           String groupName, List<String> permissions){
        String[] resourcesList = resources.get(0).split(":");
        String hivePolicyId = this.hiveCommonService.assignPermissionToDatabase(
                policyName, resourcesList[0], userList, groupName, permissions);
        logger.info("Creating hive policy for user [{}] with resource [{}] with result policyid [{}].",
                userList.toString(), resourcesList[0], hivePolicyId);
        // Temp fix: for 'create instance in tenant' case,
        // create one ranger policy for multiple user and multiple /user/<userName> dirs
        // Please refer to: https://github.com/OCManager/OCDP_ServiceBroker/issues/48
        List<String> hdfsFolders = new ArrayList<String>(){
            {
                add("/apps/hive/warehouse/" + resourcesList[0] + ".db");
                add("/tmp/hive");
                add("/ats/active");
            }
        };
        for (String userName : userList) {
            hdfsFolders.add("/user/" + userName);
            createHdfsPath("/user/" + userName);
        }
        String hdfsPolicyId = this.hdfsAdminService.createPolicyForResources(
                policyName, hdfsFolders, userList, groupName, null);
        logger.info("Creating hdfs policy for user [{}] with resource [{}] with result policyid [{}].",
                userList.toString(), hdfsFolders, hdfsPolicyId);
        String yarnPolicyId = this.yarnCommonService.assignPermissionToQueue(
                policyName, resourcesList[1], userList, groupName, null);
        logger.info("Creating yarn policy for user [{}] with resource [{}] with result policyid [{}].",
                userList.toString(), resourcesList[1], yarnPolicyId);
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
    public boolean appendUsersToPolicy(
            String policyId, String groupName, List<String> users, List<String> permissions){
        String[] policyIds = policyId.split(":");
        boolean userAppendToHivePolicy = this.hiveCommonService.appendUsersToDatabasePermission(
                policyIds[0], groupName, users, permissions);
        logger.info("User [{}] added to hive policy [{}] with result [{}].", users, policyIds[0], userAppendToHivePolicy);
        // Temp fix: when update pass multiple users, append users to policy that create for multiple users and multiple /user/<userName> dirs
        // Please refer to: https://github.com/OCManager/OCDP_ServiceBroker/issues/48
        boolean userAppendToHDFSPolicy = this.hdfsAdminService.appendUsersToPolicy(
                policyIds[1], groupName, users, Lists.newArrayList("read", "write","execute"));
        logger.info("User [{}] added to hdfs policy [{}] with result [{}].", users, policyIds[1], userAppendToHDFSPolicy);
        boolean resourceAppendToHDFSPolicy = false;
        for (String user : users) {
            logger.info("Create /user dir for user ", user);
            createHdfsPath("/user/" + user);
            resourceAppendToHDFSPolicy = this.hdfsAdminService.appendResourcesToPolicy(policyIds[1], "/user/" + user);
        }
        boolean userAppendToYarnPolicy = this.yarnCommonService.appendUsersToQueuePermission(
                policyIds[2], groupName, users, Lists.newArrayList("submit-app", "admin-queue"));
        logger.info("User [{}] added to yarn policy [{}] with result [{}].", users, policyIds[2], userAppendToYarnPolicy);
        return userAppendToHivePolicy && resourceAppendToHDFSPolicy && userAppendToHDFSPolicy && userAppendToYarnPolicy;
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{
        String[] resources = serviceInstanceResuorceName.split(":");
        this.hiveCommonService.deleteDatabase(resources[0]);
        logger.info("Delete database [{}] successful!", resources[0]);
        this.yarnCommonService.deleteQueue(resources[1]);
        logger.info("Delete queue [{}] successful!", resources[1]);
    }

    @Override
    public boolean deletePolicyForResources(String policyId){
        String[] policyIds = policyId.split(":");
        boolean hivePolicyDeleted = this.hiveCommonService.unassignPermissionFromDatabase(policyIds[0]);
        logger.info("Delete hive policy [{}] with result [{}].", policyIds[0], hivePolicyDeleted);
        boolean hdfsPolicyDeleted = this.hdfsAdminService.deletePolicyForResources(policyIds[1]);
        logger.info("Delete hdfs policy [{}] with result [{}].", policyIds[1], hdfsPolicyDeleted);
        boolean yarnPolicyDeleted = this.yarnCommonService.unassignPermissionFromQueue(policyIds[2]);
        logger.info("Delete yarn policy [{}] with result [{}].", policyIds[2], yarnPolicyDeleted);
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
        logger.info("User [{}] removed from hive policy [{}] with result [{}].", userName, policyIds[0], userRemovedFromHivePolicy);
        boolean userRemovedFromHDFSPolicy = this.hdfsAdminService.removeUserFromPolicy(policyIds[1], userName);
        logger.info("User [{}] removed from hdfs policy [{}] with result [{}].", userName, policyIds[1], userRemovedFromHDFSPolicy);
        boolean userRemovedFromYarnPolicy = this.yarnCommonService.removeUserFromQueuePermission(
                policyIds[2], userName);
        logger.info("User [{}] removed from yarn policy [{}] with result [{}].", userName, policyIds[2], userRemovedFromYarnPolicy);
        return userRemovedFromHivePolicy && userRemovedFromHDFSPolicy && userRemovedFromYarnPolicy;
    }

    @Override
    public Map<String, Object> generateCredentialsInfo(String resourceName){
        String dbName = resourceName.replaceAll("-", "");
        String queueName = etcdClient.readToString(
                "/servicebroker/ocdp/instance/" + resourceName + "/Credentials/" + OCDPConstants.YARN_RESOURCE_TYPE);
        return new HashMap<String, Object>(){
            {
                put("uri", "jdbc:hive2://" + clusterConfig.getHiveHost() + ":" +
                        clusterConfig.getHivePort() + "/" + dbName + ";principal=" + clusterConfig.getHiveSuperUser());
                // Temp fix for Spark SQL application access to hive database:
                // append a thrift URL to hive bsi's credentials info
                put("thriftUri", "jdbc:hive2://" + clusterConfig.getSparkThriftServer() + ":" +
                        clusterConfig.getSparkThriftPort() + "/" + dbName + ";principal=" + clusterConfig.getHiveSuperUser());
                put("host", clusterConfig.getHiveHost());
                put("port", clusterConfig.getHivePort());
                put(OCDPConstants.HIVE_RESOURCE_TYPE, dbName);
                put(OCDPConstants.YARN_RESOURCE_TYPE, queueName);
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
    
    /**
     * Create hdfs user path
     * @param path
     */
    private void createHdfsPath(String path) {
    	try {
			this.hdfsAdminService.createHDFSDir(path, null, null);
		} catch (IOException e) {
			logger.error("Create hdfs user path [{}] failed!", e);
			throw new RuntimeException(e);
		}
	}

}
