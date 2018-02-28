package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by baikai on 5/19/16.
 */
public class OCDPAdminServiceMapper {

	private static List<String> OCDP_SERVICE_DEFINITION_IDS = new ArrayList<String>();

	public static void setOCDPServiceIds(List<String> oCDP_SERVICE_DEFINITION_IDS) {
		OCDP_SERVICE_DEFINITION_IDS = oCDP_SERVICE_DEFINITION_IDS;
	}

	public static List<String> getOCDPServiceIds() {
		return OCDP_SERVICE_DEFINITION_IDS;
	}

	private static Map<String, String> OCDP_ADMIN_SERVICE_MAP = new HashMap<String, String>();

	public static Map<String, String> getOCDP_ADMIN_SERVICE_MAP() {
		return OCDP_ADMIN_SERVICE_MAP;
	}

	public static void setOCDP_ADMIN_SERVICE_MAP(Map<String, String> oCDP_ADMIN_SERVICE_MAP) {
		OCDP_ADMIN_SERVICE_MAP = oCDP_ADMIN_SERVICE_MAP;
	}

	private static Map<String, String> OCDP_SERVICE_NAME_MAP = new HashMap<String, String>();

	public static Map<String, String> getOCDP_SERVICE_NAME_MAP() {
		return OCDP_SERVICE_NAME_MAP;
	}

	public static void setOCDP_SERVICE_NAME_MAP(Map<String, String> oCDP_SERVICE_NAME_MAP) {
		OCDP_SERVICE_NAME_MAP = oCDP_SERVICE_NAME_MAP;
	}

	private static Map<String, String> OCDP_SERVICE_PLAN_MAP = new HashMap<String, String>();

	public static Map<String, String> getOCDP_SERVICE_PLAN_MAP() {
		return OCDP_SERVICE_PLAN_MAP;
	}

	public static void setOCDP_SERVICE_PLAN_MAP(Map<String, String> oCDP_SERVICE_PLAN_MAP) {
		OCDP_SERVICE_PLAN_MAP = oCDP_SERVICE_PLAN_MAP;
	}

	private static Map<String, String> OCDP_SERVICE_RESOURCE_MAP = new HashMap<String, String>();

	public static Map<String, String> getOCDP_SERVICE_RESOURCE_MAP() {
		return OCDP_SERVICE_RESOURCE_MAP;
	}

	public static void setOCDP_SERVICE_RESOURCE_MAP(Map<String, String> oCDP_SERVICE_RESOURCE_MAP) {
		OCDP_SERVICE_RESOURCE_MAP = oCDP_SERVICE_RESOURCE_MAP;
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
