package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client;

import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.OCKafkaException;
import com.google.common.base.Strings;

import kafka.admin.AdminUtils;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import scala.Tuple2;

/**
 * Client to perform kafka operations. Prior to operations, ensure zookeeper connection-string is 
 * configured in system environment by key <code>'OC_ZK_CONNECTION'</code>, or operations will 
 * fail.
 * @author EthanWang
 *
 */
public class KafkaClient {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaClient.class);
	private static KafkaClient instance;
	private ZKClient zkClient;
	private boolean isSecure;
	
	public static KafkaClient getClient()
	{
		if (instance == null) {
			synchronized(KafkaClient.class)
			{
				if (instance == null) {
					instance = new KafkaClient();
					return instance;
				}
			}
		}
		return instance;
	}
	
	/**
	 * Is kerberos mode on.
	 * @return
	 */
	public boolean isSecure()
	{
		return isSecure;
	}
	
	/**
	 * fetch topic configurations
	 * @param topic
	 * @return
	 */
	public Properties fetchConfigs(String topic)
	{
		Properties prop = AdminUtils.fetchEntityConfig(zkClient, ConfigType.Topic(), topic);
		return prop;
	}

	/**
	 * Create topic.
	 * @param topicName name of topic
	 * @param par partition number
	 * @param rep replication-fator
	 * @param props properties to target topic
	 * @throws OCKafkaException 
	 */
	public void createTopic(String topicName, int par, int rep, Properties props) throws OCKafkaException
	{
		if (hasNullOrEmpty(topicName)) {
			LOG.error("Topic name null");
			throw new OCKafkaException("Topic name null");
		}
		if (props == null) {
			LOG.error("Properties must not be null.");
			throw new OCKafkaException("Properties must not be null.");
		}
		createTopicAction(topicName, par, rep, props);
	}
	
	/**
	 * Create topic.
	 * @param topicName name of topic
	 * @param par partition number
	 * @param rep replication-fator
	 * @throws OCKafkaException 
	 */
	public void createTopic(String topicName, int par, int rep) throws OCKafkaException
	{
		if (hasNullOrEmpty(topicName)) {
			LOG.error("Topic name null");
			throw new OCKafkaException("Topic name null");
		}
		createTopic(topicName, par, rep, new Properties());
	}
	
	/**
	 * Change configurations of specified topic
	 * @param topic
	 * @param key
	 * @param value
	 * @throws OCKafkaException 
	 */
	public void changeConfig(String topic, String key, String value) throws OCKafkaException
	{
		if (hasNullOrEmpty(topic, key)) {
			LOG.error("Neither topic name [{}] nor property key [{}] can be null.", topic, key);
			throw new OCKafkaException("Config key must not be null.");
		}
		reconfigTopic(topic, key, value);
	}
	
	/**
	 * Change the configuration of specified topic
	 * @param topoic
	 * @param properties
	 * @throws OCKafkaException 
	 */
	public void changeConfig(String topic, Map<String, String> properties) throws OCKafkaException
	{
		if (hasNullOrEmpty(topic)) {
			LOG.error("Topic name null");
			throw new OCKafkaException("Topic name null");
		}
		if (properties == null) {
			LOG.error("Input properties can be null, topic: " + topic);
			throw new OCKafkaException("Input properties must not be null.");
		}
		reconfigTopic(topic, properties);
	}
	
	public void deleteTopic(String topicName) throws OCKafkaException
	{
		if (hasNullOrEmpty(topicName)) {
			LOG.error("Topic name null");
			throw new OCKafkaException("Topic name null");
		}
		deleteTopicAction(topicName);
	}
	
	private boolean hasNullOrEmpty(String... strings)
	{
		for (String str : strings) {
			if (Strings.isNullOrEmpty(str)) {
				return true;
			}
		}
		return false;
	}
	
	private void reconfigTopic(String topic, Map<String, String> properties) {
		Properties props = AdminUtils.fetchEntityConfig(zkClient, ConfigType.Topic(), topic);
		props.putAll(properties);
		changeConfigAction(topic, props);
	}

	private void reconfigTopic(String topic, String key, String value) throws OCKafkaException {
		Properties props = AdminUtils.fetchEntityConfig(zkClient, ConfigType.Topic(), topic);
		props.put(key, value);
		changeConfigAction(topic, props);
	}

	private synchronized void changeConfigAction(String topic, Properties props)
	{
		AdminUtils.changeTopicConfig(zkClient, topic, props);
		LOG.info("Topic [{}] config has changed by properties [{}]", topic, props);
	}
	
	private synchronized void createTopicAction(String topic, int par, int rep, Properties props)
	{
		AdminUtils.createTopic(zkClient, topic, par, rep, props);
		LOG.info("Kafka topic [{}] created by partition [{}] and replication [{}]", topic, par, rep );
	}
	
	private synchronized void deleteTopicAction(String topicName)
	{
		AdminUtils.deleteTopic(zkClient, topicName);
		LOG.info("Topic [{}] has been deleted.", topicName);
	}
	
	/**
	 * @param n
	 * @param d default value
	 * @return
	 */
	private static String getEnv(String n, String d)
	{
		String v = System.getenv(n);
		return v != null ? v : d;
	}
	
	private KafkaClient()
	{
		isSecure = JaasUtils.isZkSecurityEnabled();
//		Boolean.valueOf(getEnv("OC_ZK_ISSECURITY", "true"));
		LOG.info("Zookeeper isZkSecurityEnabled : " + isSecure);
		zkClient = new ZKClient(isSecure);
	}
	
	/**
	 * Zookeeper client for rpc with zk server and other operations.</br>
	 * Client first trys to fetch zookeeper connection info from system environment by key <code>'OC_ZK_CONNECTION'</code>, and then
	 * connects to server. Exceptions will be thrown if above config not exist.
	 * @author EthanWang
	 *
	 */
	private static class ZKClient extends ZkUtils
	{
		private static String CONN_TIMEOUT;
		private static String SESSION_TIMEOUT;
		private static String ZK_CONNECTION;
		private static Tuple2<ZkClient, ZkConnection> map;

		static
		{
			try {
				CONN_TIMEOUT = getEnv("OC_ZK_CONN_TIMEOUT_MS", "30000");
				SESSION_TIMEOUT = getEnv("OC_ZK_SESSTION_TIMEOUT_MS", "30000");
				ZK_CONNECTION = getEnv("OC_ZK_CONNECTION", null);
				if (ZK_CONNECTION == null || ZK_CONNECTION.isEmpty()) {
					LOG.error("OC_ZK_CONNECTION not configured in system environment.");
					throw new RuntimeException("OC_ZK_CONNECTION not configured in system environment.");
				}
				map = createZkClientAndConnection(ZK_CONNECTION, Integer.valueOf(SESSION_TIMEOUT), Integer.valueOf(CONN_TIMEOUT));
				LOG.info("OC_ZK_CONNECTION set to : {}", ZK_CONNECTION);
				LOG.info("OC_ZK_CONN_TIMEOUT_MS = {}, OC_ZK_SESSTION_TIMEOUT_MS = {}", CONN_TIMEOUT, SESSION_TIMEOUT);
			} catch (Throwable e) {
				LOG.error("Class init error:", e);
				throw new RuntimeException(e);
			}
		}

		/**
		 * ZKClient
		 * @param isSecure is security mode on
		 */
		public ZKClient(boolean isSecure)
		{
			super(map._1, map._2, isSecure);
		}
		
	}
}
