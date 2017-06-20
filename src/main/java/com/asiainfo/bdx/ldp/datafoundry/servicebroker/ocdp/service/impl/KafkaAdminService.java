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
	
	private static final String ZK_CONN_KEY = "ZooKeeper_URI";
	
    private static final List<String> ACCESSES = Lists.newArrayList("publish", "consume", "configure", "describe", "create", "delete", "kafka admin");
	
    private Gson gson = new GsonBuilder().create();
    
	private ClusterConfig sys_env;
	
	private rangerClient ranger;
	
    @Autowired
    private ApplicationContext context;

    @Autowired
    public KafkaAdminService(ClusterConfig clusterConfig){
        this.sys_env = clusterConfig;
        this.ranger = clusterConfig.getRangerClient();
        initSysProperties();
    }
	
	@Override
	public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
			String bindingId, Map<String, Object> cuzQuota) throws Exception {
		OCTopic topic = new OCTopic(serviceInstanceId);
		return createResources(topic, quotaPolicy(serviceDefinitionId, planId, cuzQuota));
	}

	@Override
	public String createPolicyForTenant(String policyName, List<String> resources, String defaultUser,
			String groupName) {
        String policyId = null;
        RangerV2Policy policy = newPolicy(policyName);
        policy.addResources2(Constants.REROURCE_TYPE, resources, false, true);
        policy.addPolicyItems(Lists.newArrayList(defaultUser), Lists.newArrayList(groupName), Lists.newArrayList(), false, ACCESSES);
        String newPolicyString = ranger.createV2Policy(policy);
        if (newPolicyString != null){
            RangerV2Policy newPolicyObj = gson.fromJson(newPolicyString, RangerV2Policy.class);
            policyId = newPolicyObj.getPolicyId();
        }
        LOG.info("Create policy [{}] for tenant [{}] successful with policy id [{}]", policyName, defaultUser, policyId);
        return policyId;
	}
	
	@Override
	public boolean appendResourceToTenantPolicy(String policyId, String serviceInstanceResource) {
		return ranger.appendResourceToV2Policy(policyId, serviceInstanceResource, Constants.REROURCE_TYPE);
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
	public boolean deletePolicyForTenant(String policyId) {
		boolean deleted = ranger.removeV2Policy(policyId);
		LOG.info("Delete kafka policy [{}] from ranger with result: " + deleted);
		return deleted;
	}

	@Override
	public boolean removeResourceFromTenantPolicy(String policyId, String serviceInstanceResource) {
		boolean removed = ranger.removeResourceFromV2Policy(policyId, serviceInstanceResource, Constants.REROURCE_TYPE);
		LOG.info("Remove resource [{}] from kafka policy [{}] return : " + removed);
		return removed;
	}

	@Override
	public void resizeResourceQuota(ServiceInstance instance, Map<String, Object> cuzQuota) {
		String topic = getTopic(instance);
		try {
			KafkaClient.getClient().changeConfig(topic, trans(cuzQuota));
			LOG.info("Resize kafka quota for topic [{}] successful.");
		} catch (OCKafkaException e) {
			LOG.error("Change kafka topic [{}] config failed: {}", topic, e);
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public boolean appendUserToTenantPolicy(String policyId, String groupName, String accountName,
			List<String> permissions) {
        boolean appended = ranger.appendUserToV2Policy(policyId, groupName, accountName, permissions);
        LOG.info("Append user [{}] to kafka ranger policy [{}] by accesses [{}] with result: {}", accountName, policyId, permissions, appended);
        return appended;
	}

	@Override
	public boolean removeUserFromTenantPolicy(String policyId, String accountName) {
        boolean removed = ranger.removeUserFromV2Policy(policyId, accountName);
        LOG.info("Remove user [{}] from kafka ranger policy [{}] with result: {}" + accountName, policyId, removed);
		return removed;
	}
	
	@Override
	public Map<String, Object> generateCredentialsInfo(String serviceInstanceId) {
		HashMap<String, Object> credential = new HashMap<>();
		genKafkaCredential(credential, new OCTopic(serviceInstanceId));
		return credential;
	}
	
	@Override
	public List<String> getResourceFromTenantPolicy(String policyId) {
        return ranger.getResourcsFromV2Policy(policyId, Constants.REROURCE_TYPE);
	}

	private RangerV2Policy newPolicy(String policyName)
	{
		return new RangerV2Policy(policyName, "", "This is Kafka Policy", sys_env.getClusterName()+"kafka", true, true);
	}

	private String createResources(OCTopic topic, Map<String, Object> quota) {
		try {
			Properties props = buildProperties(quota);
			String par_num = (String) quota.get(Constants.TOPIC_QUOTA);
			KafkaClient.getClient().createTopic(topic.name(), Integer.valueOf(par_num), Constants.REP_FACTOR, props);
			LOG.info("Topic {} created successful with {} partitions and config: {}", topic, par_num, props);
			return topic.name();
		} catch (OCKafkaException e) {
			LOG.error("Create topic error: ", e);
			throw new RuntimeException("Create topic error: ", e);
		}
	}

	private Properties buildProperties(Map<String, Object> quota) {
		Properties prop = new Properties();
		String par_size = (String) quota.get(Constants.PAR_QUOTA);
		String ttl = (String) quota.get(Constants.TOPIC_TTL);
		prop.put(Constants.CONFIG_PAR_SIZE, par_size);
		prop.put(Constants.CONFIG_TTL, ttl);
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
        LOG.info("Kafka quota been set to: " + quota);
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
		quota.put(Constants.TOPIC_QUOTA, validate(plan_topicQuota, cuzQuota.get(Constants.TOPIC_QUOTA)));
		quota.put(Constants.TOPIC_TTL, validate(plan_topicTTL, cuzQuota.get(Constants.TOPIC_TTL)));
		quota.put(Constants.PAR_QUOTA, validate(plan_parQuota, cuzQuota.get(Constants.TOPIC_QUOTA)));
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
		long cuzLong = Long.valueOf((String)cuzValue);
		String result = cuzLong > planMax ? String.valueOf(planItem.getDefault()) : String.valueOf(cuzLong);
		LOG.info("Kafka quota values(custom/maximum/default): [{}]/[{}]/[{}]", cuzLong, planMax, planItem.getDefault());
		return result;
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
			String string = (String) it.next();
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
		String name = (String)credentails.get(Constants.REROURCE_TYPE);
		return name;
	}
	
	private Map<String, String> trans(Map<String, Object> cuzQuota) {
		Map<String, String> result = Maps.newHashMap();
		for (Entry<String, Object> entry : cuzQuota.entrySet()) {
			result.put(entry.getKey(), (String)entry.getValue());
		}
		return result;
	}

	private void genKafkaCredential(HashMap<String, Object> credential, OCTopic topic) {
		credential.put(Constants.REROURCE_TYPE, topic.name());
		credential.put(ZK_CONN_KEY, sys_env.getZk_connection());
	}
	
	private void initSysProperties() {
		if (!KafkaClient.getClient().isSecure()) {
			LOG.info("Kerbros not enabled. Skipping JVM System-Properties-Settings of [krb5.conf, jaas].");
			return;
		}
        System.setProperty("java.security.krb5.conf", this.sys_env.getKrb5FilePath());
		System.setProperty("java.security.auth.login.config", this.sys_env.getKafka_jaas_path());
		LOG.info("Krb conf and Jaas files been set to {}, {}", this.sys_env.getKrb5FilePath(), this.sys_env.getKafka_jaas_path());
	}

	public static class OCTopic
	{
		private static final String TOPIC_PREFIX =  "oc_";
		
		private String name;
		
		/**
		 * Get OCKafka topic name
		 * @param serviceInstanceId
		 * @return
		 */
		public OCTopic(String serviceInstanceId)
		{
			if (serviceInstanceId == null || serviceInstanceId.isEmpty()) {
				LOG.error("Service instanceId must not be null.");
				throw new RuntimeException("Service instanceId must not be null.");
			}
			this.name = TOPIC_PREFIX + serviceInstanceId;
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
		
		public static final int REP_FACTOR = 1; // topic replication factor
		
		public static final String CONFIG_TTL = "retention.ms";
		
		public static final String CONFIG_PAR_SIZE = "retention.bytes";

	}

}
