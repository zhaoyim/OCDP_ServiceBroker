package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.util.List;
import java.util.Map;

/**
 * Utility class for manipulating a OCDP Hadoop services.
 * 
 * @author whitebai1986@gmail.com
 *
 */
public interface OCDPAdminService {

	String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
                              String bindingId, Map<String, Object> cuzQuota) throws Exception;

    String createPolicyForTenant(String policyName, List<String> resources, String tenantName, String groupName);

    boolean appendResourceToTenantPolicy(String policyId, String serviceInstanceResource);

    boolean appendUserToTenantPolicy(String policyId, String groupName, String accountName, List<String> permissions);

    void deprovisionResources(String serviceInstanceResuorceName) throws Exception;

    boolean deletePolicyForTenant(String policyId);

    boolean removeResourceFromTenantPolicy(String policyId, String serviceInstanceResource);

    boolean removeUserFromTenantPolicy(String policyId, String accountName);

    List<String> getResourceFromTenantPolicy(String policyId);

    Map<String, Object> generateCredentialsInfo(String serviceInstanceId);

    void resizeResourceQuota(String serviceInstanceId, Map<String, Object> cuzQuota);

}
