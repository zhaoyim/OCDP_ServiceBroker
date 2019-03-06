package com.asiainfo.cm.servicebroker.dp.config;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.model.Catalog;
import org.springframework.cloud.servicebroker.model.Plan;
import org.springframework.cloud.servicebroker.model.ServiceDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.asiainfo.cm.servicebroker.dp.utils.OCDPAdminServiceMapper;
import com.asiainfo.cm.servicebroker.dp.utils.OCDPConstants;
import com.google.common.base.Preconditions;

/**
 * Catalog of available services
 *
 * @author EthanWang 2018-10-11
 *
 */
@Component
public class CatalogConfig {
	private static final Logger LOG = LoggerFactory.getLogger(CatalogConfig.class);
	private Catalog catalog;
	
	@Autowired
	public CatalogConfig(ServiceCatalogTemplate template) {
		try {
			List<ServiceDefinition> svcdefs = template.getServices();
			this.catalog = new Catalog(svcdefs);
		} catch (Exception e) {
			LOG.error("Exception while init catalog: ", e);
			throw new RuntimeException("Exception while init catalog: ", e);
		}
	}
	
	@Bean
	public Catalog catalog() {
		Preconditions.checkNotNull(this.catalog, "Catalog is not initialized yet");
		return this.catalog;
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, String> getQuotaFromPlan(String serviceDefinitionId, String planId,
			Map<String, Object> cuzQuota) {
		Plan plan = getServicePlan(serviceDefinitionId, planId);
		Map<String, Object> metadata = plan.getMetadata();
		List<String> bullets = (List<String>) metadata.get("bullets");
		Map<String, Object> customizeMap = (Map<String, Object>) metadata.get("customize");
		Map<String, String> quotas = new HashMap<>();
		String quota = "";
		for (String quotaKey : OCDPAdminServiceMapper.getOCDPServiceQuotaKeys()) {
			if (customizeMap != null) {
				// Customize quota case
				HashMap<String, Object> quotaItem = (HashMap<String, Object>) customizeMap.get(quotaKey);
				// Skip for invalid quota key
				if (quotaItem == null)
					continue;
				long defaultQuota = (int) quotaItem.get("default");
				long maxQuota = (int) quotaItem.get("max");
				if (cuzQuota != null && cuzQuota.get(quotaKey) != null) {
					// customize quota have input value
					quota = (String) cuzQuota.get(quotaKey);
					// If customize quota exceeds plan limitation, use default value
					LOG.info("{} -> quota:[{}], maxQuota:[{}],defaultQuota:[{}].", quotaKey, quota, maxQuota, defaultQuota);
					if (Long.parseLong(quota) > maxQuota) {
						LOG.warn("Requested quota exceeded maximum quota, using max quota instead: " + maxQuota);
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

	public Plan getServicePlan(String serviceDefinitionId, String planId) {
		List<ServiceDefinition> svc = catalog.getServiceDefinitions().stream()
				.filter(s -> s.getId().equals(serviceDefinitionId)).collect(Collectors.toList());
		Preconditions.checkArgument(svc != null && !svc.isEmpty(), "Service not found by id: " + serviceDefinitionId);
		List<Plan> plan = svc.get(0).getPlans().stream().filter(p -> p.getId().equals(planId))
				.collect(Collectors.toList());
		return plan != null && !plan.isEmpty() ? plan.get(0) : null;
	}

}