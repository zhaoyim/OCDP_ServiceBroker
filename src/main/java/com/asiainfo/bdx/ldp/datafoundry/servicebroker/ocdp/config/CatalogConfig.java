package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.model.Catalog;
import org.springframework.cloud.servicebroker.model.Plan;
import org.springframework.cloud.servicebroker.model.ServiceDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.CustomizeQuotaItem;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.PlanMetadata;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPConstants;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Configuration
public class CatalogConfig {

	@Autowired
	private ApplicationContext context;

	private Logger logger = LoggerFactory.getLogger(CatalogConfig.class);

	static final Gson gson = new GsonBuilder().create();

	@Bean
	public Catalog catalog() {
		return new Catalog(this.getServiceDefinitions());
	}

	public ServiceDefinition getServiceDefinition(String serviceDefinitionId) {
		ServiceDefinition serviceDefinition = null;
		Catalog catalog = this.getServiceCatalog();
		if (catalog != null) {
			for (ServiceDefinition sd : catalog.getServiceDefinitions()) {
				if ((sd.getId()).equals(serviceDefinitionId)) {
					serviceDefinition = sd;
					break;
				}
			}
		}
		return serviceDefinition;
	}

	public Plan getServicePlan(String serviceDefinitionId, String planId) {
		Plan plan = null;
		ServiceDefinition sd = getServiceDefinition(serviceDefinitionId);
		if (sd != null) {
			for (Plan p : sd.getPlans()) {
				if ((p.getId()).equals(planId)) {
					plan = p;
					break;
				}
			}
		}
		return plan;
	}

	@SuppressWarnings("unchecked")
	public Map<String, String> getQuotaFromPlan(String serviceDefinitionId, String planId,
			Map<String, Object> cuzQuota) {
		Plan plan = getServicePlan(serviceDefinitionId, planId);
		Map<String, Object> metadata = plan.getMetadata();
		List<String> bullets = (List<String>) metadata.get("bullets");
		// Object customize = metadata.get("customize");
		Map<String, Object> customizeMap = (Map<String, Object>) metadata.get("customize");
		Map<String, String> quotas = new HashMap<>();
		String quota = "";
		for (String quotaKey : OCDPAdminServiceMapper.getOCDPServiceQuotaKeys()) {
			if (customizeMap != null) {
				// Customize quota case
				CustomizeQuotaItem quotaItem = (CustomizeQuotaItem) customizeMap.get(quotaKey);
				// Skip for invalid quota key
				if (quotaItem == null)
					continue;
				long defaultQuota = quotaItem.getDefault();
				long maxQuota = quotaItem.getMax();
				if (cuzQuota != null && cuzQuota.get(quotaKey) != null) {
					// customize quota have input value
					quota = (String) cuzQuota.get(quotaKey);
					// If customize quota exceeds plan limitation, use default value
					logger.info("Quota:[{}], maxQuota:[{}],defaultQuota:[{}].", quota, maxQuota, defaultQuota);
					if (Long.parseLong(quota) > maxQuota) {
						logger.warn("Requested quota exceeded maximum quota, using max quota instead: " + maxQuota);
						quota = Long.toString(maxQuota);
					}
				} else {
					// customize quota have not input value, use default value
					quota = Long.toString(defaultQuota);
				}
			} else {
				// Non customize quota case, use plan.metadata.bullets
				// Convert quota key to plan bullets quota key
				String quotaPlanKey = OCDPAdminServiceMapper.getOCDPPlanQuotaName(quotaKey);
				Iterator<String> it = bullets.iterator();
				while (it.hasNext()) {
					String str = it.next();
					if (str.startsWith(quotaPlanKey)) {
						quota = str.split(":")[1];
					}
				}
			}
			if (quotaKey.equals(OCDPConstants.HDFS_STORAGE_QUOTA)) {
				quotas.put(quotaKey, Long.toString(Long.valueOf(quota) * 1024 * 1024 * 1024));
			} else {
				quotas.put(quotaKey, quota);
			}
		}
		return quotas;
	}

	private Catalog getServiceCatalog() {
		Catalog catalog = null;
		ClusterConfig clusterConfig = (ClusterConfig) this.context.getBean("clusterConfig2");
		etcdClient etcdClient = clusterConfig.getEtcdClient();
		List<ServiceDefinition> sds = new ArrayList<>();
		OCDPAdminServiceMapper mapper = (OCDPAdminServiceMapper) this.context.getBean("OCDPAdminServiceMapper");

		for (String id : mapper.getOCDPServiceIds()) {
			if (etcdClient.read("/servicebroker/ocdp/catalog/" + id) == null) {
				continue;
			}
			String name = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/name");
			String description = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/description");
			// Encoding for Chinese description
			description = new String(description.getBytes(Charset.forName("ISO-8859-1")), Charset.forName("UTF-8"));
			String bindable = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/bindable");
			String planupdatable = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/planupdatable");
			String tags = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/tags");
			String metadata = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/metadata");
			metadata = new String(metadata.getBytes(Charset.forName("ISO-8859-1")), Charset.forName("UTF-8"));
			String planId = mapper.getOCDPServicePlan(id);
			String planName = etcdClient
					.readToString("/servicebroker/ocdp/catalog/" + id + "/plan/" + planId + "/name");
			String planDescription = etcdClient
					.readToString("/servicebroker/ocdp/catalog/" + id + "/plan/" + planId + "/description");
			// Encoding for Chinese description
			planDescription = new String(planDescription.getBytes(Charset.forName("ISO-8859-1")),
					Charset.forName("UTF-8"));
			String planFree = etcdClient
					.readToString("/servicebroker/ocdp/catalog/" + id + "/plan/" + planId + "/free");
			String planMetadata = etcdClient
					.readToString("/servicebroker/ocdp/catalog/" + id + "/plan/" + planId + "/metadata");
			// Encoding for Chinese description in plan metadata
			planMetadata = new String(planMetadata.getBytes(Charset.forName("ISO-8859-1")), Charset.forName("UTF-8"));
			PlanMetadata planMetadataObj = gson.fromJson(planMetadata, PlanMetadata.class);
			Map<String, Object> planMetadataMap = new HashMap<String, Object>() {
				{
					put("costs", planMetadataObj.getCosts());
					put("bullets", planMetadataObj.getBullets());
					put("customize", planMetadataObj.getCustomize());
				}
			};
			Plan plan = new Plan(planId, planName, planDescription, planMetadataMap, Boolean.parseBoolean(planFree));
			List<Plan> plans = new ArrayList<Plan>() {
				{
					add(plan);
				}
			};
			HashMap<String, Object> metadataMap = gson.fromJson(metadata, HashMap.class);
			List<String> tagsList = Arrays.asList(tags.split(","));
			ServiceDefinition sd = new ServiceDefinition(id, name, description, Boolean.parseBoolean(bindable),
					Boolean.parseBoolean(planupdatable), plans, tagsList, metadataMap, null, null);
			sds.add(sd);
			logger.info("ServiceDefinition found for id [{}] name [{}] desc [{}]", id, name, description);
		}
		if (sds.size() != 0) {
			catalog = new Catalog(sds);
		}
		return catalog;
	}

	private List<ServiceDefinition> getServiceDefinitions() {
		ArrayList<ServiceDefinition> serviceDefinitions = new ArrayList<ServiceDefinition>();
		Catalog catalog = getServiceCatalog();
		if (catalog != null) {
			serviceDefinitions.addAll(catalog.getServiceDefinitions());
		}
		System.out.println("INFO: ServiceDefinitions have been found in catalog: " + serviceDefinitions);
		return serviceDefinitions;
	}

}