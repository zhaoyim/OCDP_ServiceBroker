package com.asiainfo.cm.servicebroker.dp.client;

import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import com.asiainfo.cm.servicebroker.dp.config.ClusterConfig;
import com.asiainfo.cm.servicebroker.dp.exception.OCKafkaException;
import com.asiainfo.cm.servicebroker.dp.utils.SpringUtils;
import com.google.common.base.Strings;

import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand.TopicCommandOptions;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import scala.Tuple2;
import scala.collection.mutable.StringBuilder;

/**
 * Client to perform kafka operations. Prior to operations, ensure zookeeper connection-string is 
 * configured in system environment by key <code>'OC_ZK_CONNECTION'</code>, or operations will 
 * fail.
 * @author EthanWang
 *
 */
@DependsOn(value = "springUtils")
@Component
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
				}
			}
		}
		if (instance.isSecure) {
			// ensure jaas file path correct.
			ClusterConfig config = (ClusterConfig) SpringUtils.getContext().getBean("clusterConfig");
			System.setProperty("java.security.auth.login.config", config.getKafka_jaas_path());
            System.setProperty("java.security.krb5.conf", config.getKrb5FilePath());
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
	
	/**
	 * Change the specified topic partitions to target value
	 * @param topic
	 * @param par
	 * @throws OCKafkaException
	 */
	public void changePartitions(String topic, int par) throws OCKafkaException
	{
		try {
			if (hasNullOrEmpty(topic)) {
				LOG.error("Topic name null");
				throw new OCKafkaException("Topic name null");
			}
			changeParAction(topic, par);
		} catch (Exception e) {
			LOG.error("Changing kafka topic partitions failed: ", e);
			throw new RuntimeException(e);
		}
	}

	public void deleteTopic(String topicName) throws OCKafkaException
	{
		if (hasNullOrEmpty(topicName)) {
			LOG.error("Topic name null");
			throw new OCKafkaException("Topic name null");
		}
		deleteTopicAction(topicName);
	}
	
	public void test()
	{
		//--alter --topic mytopic-citic1 --partitions 10
		String[] options = {"--alter", "--topic", "mytopic-citic1", "--partitions", "6"};
		kafka.admin.TopicCommand.alterTopic(zkClient, new TopicCommandOptions(options));
		System.out.println(">>> testfunction end<<<");
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
	
	private void changeParAction(String topic, int par){
		AdminUtils.addPartitions(zkClient, topic, par, "", true);
		LOG.info("Kafka topic [{}] change partitions to [{}]", topic, par);
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
	
	private KafkaClient()
	{
		ClusterConfig config = (ClusterConfig) SpringUtils.getContext().getBean("clusterConfig");
		isSecure = Boolean.valueOf(config.krbEnabled());
        LOG.info("Kerberos enabled: " + isSecure);
        if (isSecure) {
            System.setProperty("java.security.krb5.conf", config.getKrb5FilePath());
    		System.setProperty("java.security.auth.login.config", config.getKafka_jaas_path());
    		LOG.info("Krb conf and Jaas files been set to {}, {}", config.getKrb5FilePath(), config.getKafka_jaas_path());
		}
		zkClient = new ZKClient(isSecure);
	}
	
	@Deprecated
	public static class TopicCommand
	{
		private static final String TOPIC = "--topic";
		private static final String ZOOKEEPER = "--zookeeper";
		private static final String PAR = "--partitions";
		private static final String SEP = " ";
		
		private StringBuilder cmd = new StringBuilder("kafka-topics.sh");
		
		private TopicCommand(){}
		
		/**
		 * Get alter command
		 * @return
		 */
		public static TopicCommand alter()
		{
			return new TopicCommand().alterAction();
		}
		
		private TopicCommand alterAction()
		{
			this.cmd.append(SEP).append("--alter");
			return this;
		}
		
		/**
		 * Specify the topic name
		 * @param topic
		 * @return
		 */
		public TopicCommand topic(String topic)
		{
			this.cmd.append(SEP).append(TOPIC).append(SEP).append(topic);
			return this;
		}
		
		/**
		 * Specify the zookeeper connection string
		 * @param zkIpPort
		 * @return
		 */
		public TopicCommand zk(String zkIpPort)
		{
			this.cmd.append(SEP).append(ZOOKEEPER).append(SEP).append(zkIpPort);
			return this;
		}
		
		/**
		 * Specity the partition number
		 * @param par
		 * @return
		 */
		public TopicCommand partitions(int par)
		{
			this.cmd.append(SEP).append(PAR).append(SEP).append(par);
			return this;
		}
		
		/**
		 * Generate the command.
		 * @return
		 */
		public String getCommand()
		{
			return cmd.toString();
		}
		
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
				CONN_TIMEOUT = "30000";
				SESSION_TIMEOUT = "30000";
				ClusterConfig config = (ClusterConfig) SpringUtils.getContext().getBean("clusterConfig");
				ZK_CONNECTION = config.getZk_connection();
				if (ZK_CONNECTION == null || ZK_CONNECTION.isEmpty()) {
					LOG.error("OC_ZK_CONNECTION not configured in system environment.");
					throw new RuntimeException("OC_ZK_CONNECTION not configured in system environment.");
				}
				map = createZkClientAndConnection(ZK_CONNECTION, Integer.valueOf(SESSION_TIMEOUT), Integer.valueOf(CONN_TIMEOUT));
				LOG.info("OC_ZK_CONNECTION set to : {}", ZK_CONNECTION);
				LOG.info("OC_ZK_CONN_TIMEOUT_MS = {}, OC_ZK_SESSTION_TIMEOUT_MS = {}", CONN_TIMEOUT, SESSION_TIMEOUT);
			} catch (Exception e) {
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
