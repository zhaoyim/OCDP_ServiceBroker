package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.model.Plan;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.KafkaClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.OCKafkaException;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.CustomizeQuotaItem;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.RangerV2Policy;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class KafkaAdminService implements OCDPAdminService{
	
	private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminService.class);
	
    private static final List<String> ACCESSES = Lists.newArrayList("publish", "consume", "configure", "describe", "create", "delete", "kafka_admin");
	
    private int repFactor = 1; // topic replication factor

    private Gson gson = new GsonBuilder().create();
    
	private ClusterConfig sys_env;
	
	private rangerClient ranger;
	
	@SuppressWarnings("serial")
	private Map<String, String> parameterMapping = new HashMap<String, String>(){{
		put(Constants.PAR_QUOTA, Constants.CONFIG_PAR_SIZE);
		put(Constants.TOPIC_TTL, Constants.CONFIG_TTL);
	}};
	
    @Autowired
    private ApplicationContext context;

    @Autowired
    public KafkaAdminService(ClusterConfig clusterConfig){
        this.sys_env = clusterConfig;
        this.ranger = clusterConfig.getRangerClient();
        String rep = clusterConfig.getKafka_rep();
        if (rep == null || rep.isEmpty()) {
			LOG.warn("Replication factor of Kafka is set to null, use default(1) instead!");
	        this.repFactor = 1;
		}
        else {
        	LOG.info("Replication factor of Kafka is set to: " + rep);
            this.repFactor = Integer.valueOf(rep);
        }
    }
	
	@Override
	public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
			Map<String, Object> paras) throws Exception {
        String resource = paras.get("cuzBsiName") == null ? serviceInstanceId : String.valueOf(paras.get("cuzBsiName"));
		OCTopic topic = new OCTopic(resource);
		return createResources(topic, quotaPolicy(serviceDefinitionId, planId, paras));
	}

	@Override
	public String createPolicyForResources(String policyName, List<String> resources, List<String> defaultUsers,
			String groupName, List<String> permissions) {
        String policyId = null;
        RangerV2Policy policy = newPolicy(policyName);
        policy.addResources2(Constants.REROURCE_TYPE, resources, false, false);
       // policy.addPolicyItems(Lists.newArrayList(defaultUser), Lists.newArrayList(groupName), Lists.newArrayList(), false, ACCESSES);
		for (String defaultUser : defaultUsers){
			if (permissions == null){
				policy.addPolicyItems(
						Lists.newArrayList(defaultUser), Lists.newArrayList(groupName), Lists.newArrayList(), false, ACCESSES);
			} else {
				policy.addPolicyItems(
						Lists.newArrayList(defaultUser), Lists.newArrayList(groupName), Lists.newArrayList(), false, permissions);
			}
		}
        String newPolicyString = ranger.createV2Policy(sys_env.getClusterName()+"_kafka", policy);
        if (newPolicyString != null){
            RangerV2Policy newPolicyObj = gson.fromJson(newPolicyString, RangerV2Policy.class);
            policyId = newPolicyObj.getPolicyId();
        }
        LOG.info("Create policy [{}] for tenant [{}] successful with policy id [{}]", policyName, defaultUsers.toString(), policyId);
        return policyId;
	}
	
	@Override
	public boolean appendResourcesToPolicy(String policyId, String serviceInstanceResource) {
		boolean result = ranger.appendResourceToV2Policy(policyId, serviceInstanceResource, Constants.REROURCE_TYPE);
		LOG.info("Append kafka resource [{}] to policy [{}] returned by result [{}]", serviceInstanceResource, policyId, result);
		return result;
	}
	
	@Override
	public void deprovisionResources(String serviceInstanceResuorceName) throws Exception {
		try {
			KafkaClient.getClient().deleteTopic(serviceInstanceResuorceName);
			// TODO: topic consumer group need to be taken care of.
			LOG.info("Deprovision Kafka resource {} successful!", serviceInstanceResuorceName);
		} catch (OCKafkaException e) {
			LOG.error("Deprovision Kafka resource {} failed: {}", serviceInstanceResuorceName, e);
			throw e;
		}
	}
	
	@Override
	public boolean deletePolicyForResources(String policyId) {
		boolean deleted = ranger.removeV2Policy(policyId);
		LOG.info("Delete kafka policy [{}] from ranger with result: [{}]", policyId, deleted);
		return deleted;
	}

	@Override
	public boolean removeResourceFromPolicy(String policyId, String serviceInstanceResource) {
		boolean removed = ranger.removeResourceFromV2Policy(policyId, serviceInstanceResource, Constants.REROURCE_TYPE);
		LOG.info("Remove resource [{}] from kafka policy [{}] return [{}]", serviceInstanceResource, policyId, removed);
		return removed;
	}

	@Override
	public void resizeResourceQuota(ServiceInstance instance, Map<String, Object> cuzQuota) {
		String topic = getTopic(instance);
		try {
			changePartitions(topic, cuzQuota); 
			KafkaClient.getClient().changeConfig(topic, trans(cuzQuota));
			LOG.info("Resizing kafka quota for topic [{}] successful with config [{}].", topic, cuzQuota);
		} catch (OCKafkaException e) {
			LOG.error("Change kafka topic [{}] config failed: {}", topic, e);
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public boolean appendUsersToPolicy(String policyId, String groupName, List<String> users,
			List<String> permissions) {
        boolean appended = ranger.appendUsersToV2Policy(policyId, groupName, users, permissions);
        LOG.info("Append user [{}] to kafka ranger policy [{}] by accesses [{}] with result: {}", users, policyId, permissions, appended);
        return appended;
	}

	@Override
	public boolean removeUserFromPolicy(String policyId, String userName) {
        boolean removed = ranger.removeUserFromV2Policy(policyId, userName);
        LOG.info("Remove user [{}] from kafka ranger policy [{}] with result: {}", userName, policyId, removed);
		return removed;
	}
	
	@Override
	public Map<String, Object> generateCredentialsInfo(String topicName) {
		HashMap<String, Object> credential = new HashMap<>();
		genKafkaCredential(credential, new OCTopic(topicName));
		return credential;
	}
	
	@Override
	public List<String> getResourceFromPolicy(String policyId) {
		List<String> resources = ranger.getResourcsFromV2Policy(policyId, Constants.REROURCE_TYPE);
		LOG.info("Kafka Resources from policy [{}]: [{}]", policyId, resources);
        return resources;
	}
	
	private void changePartitions(String topic, Map<String, Object> cuzQuota) throws NumberFormatException, OCKafkaException {
		if (!cuzQuota.containsKey(Constants.TOPIC_QUOTA)) {
			return; // no partition changing request found
		}
		String par = String.valueOf(cuzQuota.get(Constants.TOPIC_QUOTA));
		KafkaClient.getClient().changePartitions(topic, Integer.valueOf(par));
		LOG.info("Kafka topic [{}]'s partition changed to [{}]", topic, par);
	}

	private RangerV2Policy newPolicy(String policyName)
	{
		return new RangerV2Policy(policyName, "", "This is Kafka Policy", sys_env.getClusterName()+"_kafka", true, true);
	}

	private String createResources(OCTopic topic, Map<String, Object> quota) {
		try {
			Properties props = buildProperties(quota);
			String par_num = String.valueOf(quota.get(Constants.TOPIC_QUOTA));
			KafkaClient.getClient().createTopic(topic.name(), Integer.valueOf(par_num), repFactor, props);
			LOG.info("Topic {} created successful with {} partitions and config: {}", topic, par_num, props);
			return topic.name();
		} catch (OCKafkaException e) {
			LOG.error("Create topic error: ", e);
			throw new RuntimeException("Create topic error: ", e);
		}
	}

	private Properties buildProperties(Map<String, Object> quota) {
		Properties prop = new Properties();
		String par_size = String.valueOf(quota.get(Constants.PAR_QUOTA));
		String ttl = String.valueOf(quota.get(Constants.TOPIC_TTL));
		prop.setProperty(Constants.CONFIG_PAR_SIZE, toBytes(par_size));
		prop.setProperty(Constants.CONFIG_TTL, ttl);
		return prop;
	}

	/**
	 * Check user customized quota's validity. If custom quota exceeds maximum that defined in plan, default value
	 * shall be used instead. If not, the user customized quota is used. Bullets value will be used if plan meta corrupted.
	 * @param serviceDefinitionId
	 * @param planId
	 * @param cuzQuota user specified quotas
	 * @return finalized quotas
	 * @throws OCKafkaException 
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> quotaPolicy(String serviceDefinitionId, String planId, Map<String, Object> cuzQuota) throws OCKafkaException {
		
        Plan plan = getServicePlan(serviceDefinitionId, planId);
        Object pQuota = plan.getMetadata().get("customize");
        Map<String, Object> quota = Maps.newHashMap();
        if (pQuota == null) {
        	LOG.error("Kafka plan [{}] meta not intact, setting bullets quota instead of custom quota.", planId);
        	putBulletsQuota(quota, plan);
			return quota;
		}
        Map<String, Object> planQuota = (Map<String,Object>)pQuota;
        checkQuota(quota, planQuota, cuzQuota);
		return quota;
	}
	
	/**
	 * Check if custom quota is valid or not, revise to default from plan if not.
	 * @param quota
	 * @param planQuota
	 * @param cuzQuota 
	 */
	private void checkQuota(Map<String, Object> quota, Map<String, Object> planQuota, Map<String, Object> cuzQuota) {
		CustomizeQuotaItem plan_topicQuota = (CustomizeQuotaItem)planQuota.get(Constants.TOPIC_QUOTA);
		CustomizeQuotaItem plan_topicTTL = (CustomizeQuotaItem)planQuota.get(Constants.TOPIC_TTL);
		CustomizeQuotaItem plan_parQuota = (CustomizeQuotaItem)planQuota.get(Constants.PAR_QUOTA);
		if (cuzQuota == null || cuzQuota.isEmpty()) {
			quota.put(Constants.TOPIC_QUOTA, plan_topicQuota.getDefault());
			quota.put(Constants.TOPIC_TTL, plan_topicTTL.getDefault());
			quota.put(Constants.PAR_QUOTA, plan_parQuota.getDefault());
			return;
		}
		quota.put(Constants.TOPIC_QUOTA, validate(plan_topicQuota, cuzQuota.get(Constants.TOPIC_QUOTA)));
		quota.put(Constants.TOPIC_TTL, validate(plan_topicTTL, cuzQuota.get(Constants.TOPIC_TTL)));
		quota.put(Constants.PAR_QUOTA, validate(plan_parQuota, cuzQuota.get(Constants.PAR_QUOTA)));
	}

	/**
	 * If custom value is greater than plan max value, or custom value is null, return plan default value. 
	 * Otherwise return custom value.
	 * @param planItem
	 * @param cuzValue
	 * @return
	 */
	private String validate(CustomizeQuotaItem planItem, Object cuzValue) {
		if (cuzValue == null) {
			LOG.warn("Kafka quota not set, default value [{}] will be used.", planItem.getDefault());
			return String.valueOf(planItem.getDefault());
		}
		long planMax = planItem.getMax();
		long cuzLong = Long.valueOf(String.valueOf(cuzValue));
		LOG.info("Kafka quota values(custom/maximum/default): [{}]/[{}]/[{}]", cuzLong, planMax, planItem.getDefault());
		if (planMax > 0) {
			return cuzLong > planMax ? String.valueOf(planItem.getDefault()) : String.valueOf(cuzLong);
		} else {
			// when theres no maximum limit, return custom value
			return String.valueOf(cuzLong);
		}
	}

	/**
	 * Get bullets values from plan and put them into quota map
	 * @param quota
	 * @param plan
	 * @throws OCKafkaException
	 */
	@SuppressWarnings("unchecked")
	private void putBulletsQuota(Map<String, Object> quota, Plan plan) throws OCKafkaException {
    	List<String> list = (List<String>) plan.getMetadata().get("bullets");
    	quota.put(Constants.TOPIC_QUOTA, retrieveBulletsValue(Constants.TOPIC_QUOTA, list));
    	quota.put(Constants.TOPIC_TTL, retrieveBulletsValue(Constants.TOPIC_TTL, list));
    	quota.put(Constants.PAR_QUOTA, retrieveBulletsValue(Constants.PAR_QUOTA, list));		
	}

	/**
	 * Iterate over bullets list and get the value by specified name
	 * @param name
	 * @param list
	 * @return
	 * @throws OCKafkaException
	 */
	private String retrieveBulletsValue(String name, List<String> list) throws OCKafkaException
	{
		Iterator<String> it = list.iterator();
		while (it.hasNext()) {
			String string = it.next();
			if (string.startsWith(name)) {
				return string.split(":")[1];
			}
		}
		LOG.error("Name: {} not found in list: {}.", name, list);
		throw new OCKafkaException("Name " + name + " not found in list: " + list);
	}

	private Plan getServicePlan(String serviceDefinitionId, String planId) throws OCKafkaException {
        CatalogConfig catalog = (CatalogConfig) context.getBean("catalogConfig");
        Plan plan = catalog.getServicePlan(serviceDefinitionId, planId);
        if (plan == null) {
			LOG.error("Kafka service plan is null by serviceID/planID: [{}]/[{}]", serviceDefinitionId, planId);
			throw new OCKafkaException("Kafka service plan is null by serviceID/planID:" + serviceDefinitionId + "/" + planId);
		}
        return plan;
	}

	private String getTopic(ServiceInstance instance) {
		Map<String, Object> credentails = instance.getServiceInstanceCredentials();
		String name = String.valueOf(credentails.get(Constants.REROURCE_TYPE));
		return name;
	}
	
	private Map<String, String> trans(Map<String, Object> cuzQuota) {
		Map<String, String> result = Maps.newHashMap();
		for (Entry<String, Object> entry : cuzQuota.entrySet()) {
			if (parameterMapping.containsKey(entry.getKey())) {
				if (entry.getKey().equals(Constants.PAR_QUOTA)) {
					//partition size unit transforming from GB to Bytes
					result.put(parameterMapping.get(entry.getKey()), toBytes(String.valueOf(entry.getValue())));
				}
				else
				{
					result.put(parameterMapping.get(entry.getKey()), String.valueOf(entry.getValue()));
				}
			}
		}
		return result;
	}

	/**
	 * change partition size from GB to Bytes.
	 * @param valueInGB
	 * @return
	 */
	private String toBytes(String valueInGB) {
		long valueInBytes = Long.valueOf(valueInGB) * 1024 * 1024 * 1024;// 1GB = 1024*1024*1024Bytes
		return String.valueOf(valueInBytes);
	}

	private void genKafkaCredential(HashMap<String, Object> credential, OCTopic topic) {
		credential.put(Constants.REROURCE_TYPE, topic.name());
		credential.put("uri", sys_env.getZk_connection());
		credential.put("host", sys_env.getKafka_hosts());
		credential.put("port", sys_env.getKafka_port());
	}
	
	public static class OCTopic
	{
		private String name;
		
		/**
		 * Get OCKafka topic name
		 * @param resourceName
		 * @return
		 */
		public OCTopic(String resourceName)
		{
			if (resourceName == null || resourceName.isEmpty()) {
				LOG.error("Service instanceId must not be null.");
				throw new RuntimeException("Service instanceId must not be null.");
			}
			this.name = resourceName;
		}
		
		public String name()
		{
			return this.name;
		}
		
		public String toString()
		{
			return this.name;
		}
	}
	
	/**
	 * Constants fields of Kafka service.
	 * @author EthanWang
	 *
	 */
	public static class Constants
	{
		/**topicQuota from catalog*/
		public static final String TOPIC_QUOTA = "topicQuota";
		
		/**topicTTL from catalog*/
		public static final String TOPIC_TTL = "topicTTL";
		
		/**partitionQuota from catalog*/
		public static final String PAR_QUOTA = "partitionSize";
		
		/**resource type which is passed into ranger policy as kafka resources name*/
		public static final String REROURCE_TYPE = "topic";
		
		public static final String CONFIG_TTL = "retention.ms";
		
		public static final String CONFIG_PAR_SIZE = "retention.bytes";
		
		public static final String CONFIG_PAR_NUM = "num.partitions";

	}

}
