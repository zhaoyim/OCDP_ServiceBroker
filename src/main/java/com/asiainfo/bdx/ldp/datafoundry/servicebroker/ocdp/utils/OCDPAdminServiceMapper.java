package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.servicebroker.model.ServiceDefinition;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;

/**
 * Created by baikai on 5/19/16.
 */
@DependsOn(value = "springUtils")
@Component("OCDPAdminServiceMapper")
public class OCDPAdminServiceMapper {
	private static final Logger LOG = LoggerFactory.getLogger(OCDPAdminServiceMapper.class);
	private  List<String> OCDP_SERVICE_DEFINITION_IDS = new ArrayList<String>();
	private  Map<String, String> OCDP_SERVICE_PLAN_MAP = new HashMap<String, String>();
	private  Map<String, String> OCDP_SERVICE_NAME_MAP = new HashMap<String, String>();
	private  Map<String, String> OCDP_SERVICE_RESOURCE_MAP = new HashMap<String, String>();
	private  Map<String, String> OCDP_ADMIN_SERVICE_MAP = new HashMap<String, String>();

	public OCDPAdminServiceMapper() {
		initMappers();
	}
	
	private void initMappers() {
		CatalogConfig catalog = (CatalogConfig) SpringUtils.getContext().getBean("catalogConfig");
		List<ServiceDefinition> svcs = catalog.catalog().getServiceDefinitions();
		if (svcs == null || svcs.isEmpty()) {
			LOG.error("No service found in catalog: " + catalog);
			throw new RuntimeException(
					"No service found in catalog: " + catalog);
		}
		svcs.forEach(service -> {
			String longid = service.getId();
			String id = longid.substring(longid.lastIndexOf("/") + 1);
			OCDP_SERVICE_DEFINITION_IDS.add(id);
			OCDP_SERVICE_PLAN_MAP.put(id, service.getPlans().iterator().next().getId());
			String serviceType = service.getMetadata().get("type").toString();
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
			LOG.debug("Services have been loaded from catalog: [{}]", OCDP_SERVICE_DEFINITION_IDS);
		}
	}

	@Override
	public String toString() {
		return "OCDPAdminServiceMapper [OCDP_SERVICE_DEFINITION_IDS=" + OCDP_SERVICE_DEFINITION_IDS
				+ ", OCDP_SERVICE_PLAN_MAP=" + OCDP_SERVICE_PLAN_MAP + ", OCDP_SERVICE_NAME_MAP="
				+ OCDP_SERVICE_NAME_MAP + ", OCDP_SERVICE_RESOURCE_MAP=" + OCDP_SERVICE_RESOURCE_MAP
				+ ", OCDP_ADMIN_SERVICE_MAP=" + OCDP_ADMIN_SERVICE_MAP + "]";
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

	public List<String> getOCDPServiceIds() {
		return OCDP_SERVICE_DEFINITION_IDS;
	}

	public  Map<String, String> getOCDP_ADMIN_SERVICE_MAP() {
		return OCDP_ADMIN_SERVICE_MAP;
	}

	public  Map<String, String> getOCDP_SERVICE_NAME_MAP() {
		return OCDP_SERVICE_NAME_MAP;
	}

	public  Map<String, String> getOCDP_SERVICE_PLAN_MAP() {
		return OCDP_SERVICE_PLAN_MAP;
	}

	public  Map<String, String> getOCDP_SERVICE_RESOURCE_MAP() {
		return OCDP_SERVICE_RESOURCE_MAP;
	}

	public  String getOCDPAdminService(String serviceDefinitionId) {
		return OCDP_ADMIN_SERVICE_MAP.get(serviceDefinitionId);
	}

	public  String getOCDPServiceName(String serviceDefinitionId) {
		return OCDP_SERVICE_NAME_MAP.get(serviceDefinitionId);
	}

	public String getOCDPServicePlan(String serviceDefinitionId) {
		return OCDP_SERVICE_PLAN_MAP.get(serviceDefinitionId);
	}

	public static Set<String> getOCDPServiceQuotaKeys() {
		return OCDP_SERVICE_QUOTA_MAP.keySet();
	}

	public String getOCDPResourceType(String serviceDefinitionId) {
		return OCDP_SERVICE_RESOURCE_MAP.get(serviceDefinitionId);
	}

	public static String getOCDPPlanQuotaName(String quotaKey) {
		return OCDP_SERVICE_QUOTA_MAP.get(quotaKey);
	}
}
