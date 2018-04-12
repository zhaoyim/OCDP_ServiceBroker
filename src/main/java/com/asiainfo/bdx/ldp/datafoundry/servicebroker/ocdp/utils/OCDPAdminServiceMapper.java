package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.justinsb.etcd.EtcdNode;
import com.justinsb.etcd.EtcdResult;

/**
 * Created by baikai on 5/19/16.
 */
public class OCDPAdminServiceMapper {
	private static final Logger LOG = LoggerFactory.getLogger(OCDPAdminServiceMapper.class);
	@Autowired
	private static ApplicationContext context;
	private static List<String> OCDP_SERVICE_DEFINITION_IDS = new ArrayList<String>();
	private static Map<String, String> OCDP_SERVICE_PLAN_MAP = new HashMap<String, String>();
	private static Map<String, String> OCDP_SERVICE_NAME_MAP = new HashMap<String, String>();
	private static Map<String, String> OCDP_SERVICE_RESOURCE_MAP = new HashMap<String, String>();
	private static Map<String, String> OCDP_ADMIN_SERVICE_MAP = new HashMap<String, String>();

	static {
		try {
			initMappers();
		} catch (Throwable e) {
			LOG.error("Error while init class: ", e);
			throw new RuntimeException("Error while init class: ", e);
		}
	}

	private static void initMappers() {
		ClusterConfig clusterConfig = (ClusterConfig) context.getBean("clusterConfig");
		etcdClient etcdClient = clusterConfig.getEtcdClient();
		List<EtcdNode> catalog = etcdClient.read("/servicebroker/ocdp/catalog").node.nodes;
		if (catalog == null || catalog.isEmpty()) {
			LOG.error("No service found in catalog: " + etcdClient.PATH_PREFIX + "/servicebroker/ocdp/catalog");
			throw new RuntimeException(
					"No service found in catalog: " + etcdClient.PATH_PREFIX + "/servicebroker/ocdp/catalog");
		}
		catalog.forEach(service -> {
			String longid = service.key;
			String id = longid.substring(longid.lastIndexOf("/") + 1);
			OCDP_SERVICE_DEFINITION_IDS.add(id);
			OCDP_SERVICE_PLAN_MAP.put(id, getPlanID(etcdClient, "/servicebroker/ocdp/catalog/"+id));
			String serviceType = getType(etcdClient, "/servicebroker/ocdp/catalog/"+id);
			switch (serviceType) {
			case "hdfs":
				OCDP_SERVICE_NAME_MAP.put(id, "hdfs");
				OCDP_SERVICE_RESOURCE_MAP.put(id, OCDPConstants.HDFS_RESOURCE_TYPE);
				OCDP_ADMIN_SERVICE_MAP.put(id, "HDFSAdminService");
				break;

			case "hbase":
				OCDP_SERVICE_NAME_MAP.put(id, "hbase");
				OCDP_SERVICE_RESOURCE_MAP.put(id, OCDPConstants.HBASE_RESOURCE_TYPE);
				OCDP_ADMIN_SERVICE_MAP.put(id, "HBaseAdminService");
				break;
			case "hive":
				OCDP_SERVICE_NAME_MAP.put(id, "hive");
				OCDP_SERVICE_RESOURCE_MAP.put(id, OCDPConstants.HIVE_RESOURCE_TYPE);
				OCDP_ADMIN_SERVICE_MAP.put(id, "hiveAdminService");
				break;

			case "mapreduce":
				OCDP_SERVICE_NAME_MAP.put(id, "mr");
				OCDP_SERVICE_RESOURCE_MAP.put(id, OCDPConstants.MAPREDUCE_RESOURCE_TYPE);
				OCDP_ADMIN_SERVICE_MAP.put(id, "yarnAdminService");
				break;

			case "spark":
				OCDP_SERVICE_NAME_MAP.put(id, "spark");
				OCDP_SERVICE_RESOURCE_MAP.put(id, OCDPConstants.SPARK_RESOURCE_TYPE);
				OCDP_ADMIN_SERVICE_MAP.put(id, "yarnAdminService");
				break;

			case "kafka":
				OCDP_SERVICE_NAME_MAP.put(id, "kafka");
				OCDP_SERVICE_RESOURCE_MAP.put(id, OCDPConstants.KAFKA_RESOURCE_TYPE);
				OCDP_ADMIN_SERVICE_MAP.put(id, "kafkaAdminService");
				break;

			default:
				LOG.error("Unknow service type: " + serviceType);
				throw new RuntimeException("Unknow service type: " + serviceType);
			}
		});
		if (LOG.isDebugEnabled()) {
			LOG.debug("Services have been loaded from catalog '{}': [{}]",
					etcdClient.PATH_PREFIX + "/servicebroker/ocdp/catalog", OCDP_SERVICE_DEFINITION_IDS);
		}
	}

	private static String getType(etcdClient etcdClient, String serviceid) {
		EtcdResult node = etcdClient.read(serviceid);
		List<EtcdNode> subnodes = node.node.nodes;
		for(EtcdNode subnode : subnodes) {
			if (subnode.key.endsWith("metadata")) {
				JsonElement json = new JsonParser().parse(subnode.value);
				JsonObject obj = json.getAsJsonObject();
				String type = obj.get("type").getAsString();
				if (type == null || type.isEmpty()) {
					LOG.error("Service type not defined: " + serviceid);
					throw new RuntimeException("Service type not defined: " + serviceid);
				}
				return type;
			}
		}
		LOG.error("Metadata not defined in service: " + serviceid);
		throw new RuntimeException("Metadata not defined in service: " + serviceid);
	}

	private static String getPlanID(etcdClient etcdClient, String serviceid) {
		EtcdResult node = etcdClient.read(serviceid);
		List<EtcdNode> nodes = node.node.nodes;
		for(EtcdNode subnode : nodes) {
			if (subnode.key.endsWith("plan")) {
				EtcdResult plan = etcdClient.read(serviceid + "/plan");
				String longPlan = plan.node.nodes.get(0).key;
				return longPlan.substring(longPlan.lastIndexOf("/") + 1);
			}
		}
		LOG.error("No plan defined in service: " + serviceid);
		throw new RuntimeException("No plan defined in service: " + serviceid);
	}

	private static final Map<String, String> OCDP_SERVICE_QUOTA_MAP = new HashMap<String, String>() {
		{
			put(OCDPConstants.HDFS_NAMESPACE_QUOTA, OCDPConstants.HDFS_NAMESPACE_QUOTA_PLAN);
			put(OCDPConstants.HDFS_STORAGE_QUOTA, OCDPConstants.HDFS_STORAGE_QUOTA_PLAN);
			put(OCDPConstants.HBASE_NAMESPACE_TABLE_QUOTA, OCDPConstants.HBASE_NAMESPACE_TABLE_QUOTA_PLAN);
			put(OCDPConstants.HBASE_NAMESPACE_REGION_QUOTA, OCDPConstants.HBASE_NAMESPACE_REGION_QUOTA_PLAN);
			put(OCDPConstants.YARN_QUEUE_QUOTA, OCDPConstants.YARN_QUEUE_QUOTA_PLAN);
		}
	};

	public static void setOCDPServiceIds(List<String> oCDP_SERVICE_DEFINITION_IDS) {
		OCDP_SERVICE_DEFINITION_IDS = oCDP_SERVICE_DEFINITION_IDS;
	}

	public static List<String> getOCDPServiceIds() {
		return OCDP_SERVICE_DEFINITION_IDS;
	}

	public static Map<String, String> getOCDP_ADMIN_SERVICE_MAP() {
		return OCDP_ADMIN_SERVICE_MAP;
	}

	public static void setOCDP_ADMIN_SERVICE_MAP(Map<String, String> oCDP_ADMIN_SERVICE_MAP) {
		OCDP_ADMIN_SERVICE_MAP = oCDP_ADMIN_SERVICE_MAP;
	}

	public static Map<String, String> getOCDP_SERVICE_NAME_MAP() {
		return OCDP_SERVICE_NAME_MAP;
	}

	public static void setOCDP_SERVICE_NAME_MAP(Map<String, String> oCDP_SERVICE_NAME_MAP) {
		OCDP_SERVICE_NAME_MAP = oCDP_SERVICE_NAME_MAP;
	}

	public static Map<String, String> getOCDP_SERVICE_PLAN_MAP() {
		return OCDP_SERVICE_PLAN_MAP;
	}

	public static void setOCDP_SERVICE_PLAN_MAP(Map<String, String> oCDP_SERVICE_PLAN_MAP) {
		OCDP_SERVICE_PLAN_MAP = oCDP_SERVICE_PLAN_MAP;
	}

	public static Map<String, String> getOCDP_SERVICE_RESOURCE_MAP() {
		return OCDP_SERVICE_RESOURCE_MAP;
	}

	public static void setOCDP_SERVICE_RESOURCE_MAP(Map<String, String> oCDP_SERVICE_RESOURCE_MAP) {
		OCDP_SERVICE_RESOURCE_MAP = oCDP_SERVICE_RESOURCE_MAP;
	}

	public static String getOCDPAdminService(String serviceDefinitionId) {
		return OCDP_ADMIN_SERVICE_MAP.get(serviceDefinitionId);
	}

	public static String getOCDPServiceName(String serviceDefinitionId) {
		return OCDP_SERVICE_NAME_MAP.get(serviceDefinitionId);
	}

	public static String getOCDPServicePlan(String serviceDefinitionId) {
		return OCDP_SERVICE_PLAN_MAP.get(serviceDefinitionId);
	}

	public static Set<String> getOCDPServiceQuotaKeys() {
		return OCDP_SERVICE_QUOTA_MAP.keySet();
	}

	public static String getOCDPResourceType(String serviceDefinitionId) {
		return OCDP_SERVICE_RESOURCE_MAP.get(serviceDefinitionId);
	}

	public static String getOCDPPlanQuotaName(String quotaKey) {
		return OCDP_SERVICE_QUOTA_MAP.get(quotaKey);
	}
}
