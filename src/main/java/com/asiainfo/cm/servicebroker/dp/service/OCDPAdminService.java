package com.asiainfo.cm.servicebroker.dp.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.asiainfo.cm.servicebroker.dp.model.ServiceInstance;

/**
 * Utility class for manipulating a OCDP Hadoop services.
 * 
 * @author whitebai1986@gmail.com
 *
 */
public interface OCDPAdminService {

	String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
                              Map<String, Object> parameters) throws Exception;

    String createPolicyForResources(String policyName, List<String> resources, List<String> userList,
                                    String groupName, List<String> permissions);

    boolean appendResourcesToPolicy(String policyId, String serviceInstanceResource);

    boolean appendUsersToPolicy(String policyId, String groupName, List<String> users, List<String> permissions);

    void deprovisionResources(String resourceName) throws  Exception;

    boolean deletePolicyForResources(String policyId);

    boolean removeResourceFromPolicy(String policyId, String serviceInstanceResource);

    boolean removeUserFromPolicy(String policyId, String userName);

    List<String> getResourceFromPolicy(String policyId);

    Map<String, Object> generateCredentialsInfo(String resourceName);

    void resizeResourceQuota(ServiceInstance instance, Map<String, Object> parameters) throws IOException;

}
