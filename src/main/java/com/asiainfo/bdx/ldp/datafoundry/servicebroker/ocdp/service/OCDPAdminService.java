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

    // For Citic case, append customize quota in parameters
	String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
                              String bindingId, String accountName, Map<String, Object> cuzQuota) throws Exception;

    String assignPermissionToResources(String policyName, List<String> resources, String accountName, String groupName);

    boolean appendUserToResourcePermission(String policyId, String groupName, String accountName);

    void deprovisionResources(String serviceInstanceResuorceName) throws Exception;

    boolean unassignPermissionFromResources(String policyId);

    boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName);

    Map<String, Object> generateCredentialsInfo(String accountName, String accountPwd, String accountKeytab,
                                                String serviceInstanceResource, String rangerPolicyId);

    Map<String, String> getCredentialsInfo(String serviceInstanceId, String accountName, String password);
}
