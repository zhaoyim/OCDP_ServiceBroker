package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.exception.ServiceBrokerInvalidParametersException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceBindingDoesNotExistException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceBindingExistsException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceDoesNotExistException;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceAppBindingResponse;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceBindingRequest;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceBindingResponse;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceBindingRequest;
import org.springframework.cloud.servicebroker.service.ServiceInstanceBindingService;
import org.springframework.context.ApplicationContext;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.stereotype.Service;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.krbClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.KerberosOperationException;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.OCDPServiceException;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstanceBinding;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceBindingRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.BrokerUtil;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;
import com.google.common.collect.Lists;

/**
 * OCDP implement to bind hadoop services. Binding a service does the following:
 * return the connection/credential info; saves the connection/credential info
 * to the etcd repository. Unbinding service instance does the following: delete
 * the connection/credential info from the etcd repository.
 * 
 * @author whitebai1986@gmail.com
 */
@Service
public class OCDPServiceInstanceBindingService implements ServiceInstanceBindingService {

	private Logger logger = LoggerFactory.getLogger(OCDPServiceInstanceBindingService.class);

	@Autowired
	private OCDPServiceInstanceRepository repository;

	@Autowired
	private OCDPServiceInstanceBindingRepository bindingRepository;

	@Autowired
	private ApplicationContext context;

	private etcdClient etcdClient;

	private rangerClient rc;

	private ClusterConfig clusterConfig;

	private boolean krb_enabled;

	private LdapTemplate ldap;

	private krbClient kc;

	@Autowired
	public OCDPServiceInstanceBindingService(ClusterConfig clusterConfig) {
		this.clusterConfig = clusterConfig;
		this.etcdClient = clusterConfig.getEtcdClient();
		this.rc = clusterConfig.getRangerClient();
		this.ldap = clusterConfig.getLdapTemplate();
		this.kc = new krbClient(clusterConfig);
		this.krb_enabled = clusterConfig.krbEnabled();
		logger.info("Kerberos enabled: " + this.krb_enabled);
	}

	@Override
	public CreateServiceInstanceBindingResponse createServiceInstanceBinding(
			CreateServiceInstanceBindingRequest request) throws OCDPServiceException {
		try {
			logger.info("Receiving binding request: " + request);
			ServiceInstance instance = repository.findOne(request.getServiceInstanceId());
			validate(request, instance);

			Map<String, Object> params = request.getParameters();
			List<String> userList = toList((String) params.get("user_name"));
			
			if (userList.size() > 1) {
				logger.error("Binding of multiple users not supported! " + userList);
				throw new RuntimeException("Binding of multiple users not supported! " + userList);
			}
			
			String password = getPassWd(userList.get(0));

			// Support for multiple users.Refer to: https://github.com/OCManager/OCDP_ServiceBroker/issues/48
			logger.info("Assigning privileges to user:  " + userList + ", in service instance: " + request.getServiceInstanceId());
			
			String accessesStr = (String) params.get("accesses");
			if (accessesStr != null && accessesStr.length() != 0) {
				OCDPAdminService ocdp = getOCDPAdminService(request.getServiceDefinitionId());
				addUserToServiceInstance(ocdp, instance, userList.get(0), password, trimAccesses(accessesStr));
			} else {
				logger.warn("Empty 'accesses' parameter in request, none privileges assigned to user: " + userList);
			}

			Map<String, Object> credentials = assembleCredential(instance, userList.get(0));

			save(request, credentials);
			
			// Remove ranger policy id for binding response
			credentials.remove("rangerPolicyId");
			
			return new CreateServiceInstanceAppBindingResponse().withCredentials(credentials);
		} catch (Exception e) {
			logger.error("Error while binding instance: " + request.getServiceInstanceId(), e);
			throw e;
		}
	}

	private List<String> toList(String users) {
		return Arrays.asList(users.split(","));
	}

	private void save(CreateServiceInstanceBindingRequest request, Map<String, Object> credentials) {
		ServiceInstanceBinding binding = new ServiceInstanceBinding(request.getBindingId(), request.getServiceInstanceId(),
				credentials, null, request.getBoundAppGuid(), request.getPlanId());
		bindingRepository.save(binding);		
	}

	private Map<String, Object> assembleCredential(ServiceInstance instance, String userName) {
		Map<String, Object> credentials = instance.getServiceInstanceCredentials();
		if (krb_enabled) {
			credentials.put("username", userName + "@" + clusterConfig.getKrbRealm());
		} else {
			// only username is needed for authorization.
			credentials.put("username", userName);
		}
		return credentials;
	}

	private String getPassWd(String users) {
		String password = null;
		if (!BrokerUtil.isLDAPUserExist(ldap, users)) {
			password = UUID.randomUUID().toString();
		} else {
			password = etcdClient.readToString("/servicebroker/ocdp/user/krbinfo/" + users + "@"
					+ clusterConfig.getKrbRealm() + "/password");
			// Generate password for exist ldap user if krb password are missing
			if (password == null) {
				password = UUID.randomUUID().toString();
				etcdClient.write("/servicebroker/ocdp/user/krbinfo/" + users + "@" + clusterConfig.getKrbRealm()
						+ "/password", password);
			}
		}
		return password;
	}

	private List<String> trimAccesses(String accessesStr) {
		List<String> accesses = new ArrayList<>();
		// Need trim blank space in accesses string, otherwise call ranger policy api
		// will fail.
		for (String access : accessesStr.split(",")) {
			accesses.add(access.trim());
		}
		return accesses;
	}

	private void validate(CreateServiceInstanceBindingRequest request, ServiceInstance instance) {
		String serviceDefinitionId = request.getServiceDefinitionId();
		String bindingId = request.getBindingId();
		String serviceInstanceId = request.getServiceInstanceId();
		String planId = request.getPlanId();
		Map<String, Object> params = request.getParameters();
		String userName = (String) params.get("user_name");
		if (userName == null || userName.isEmpty()) {
			logger.error("User name in request is null: " + params);
			throw new ServiceBrokerInvalidParametersException("User name in request is null");
		}

		if (bindingRepository.findOne(serviceInstanceId, bindingId) != null) {
			logger.error("Binding [{}] already exist!", bindingId);
			throw new ServiceInstanceBindingExistsException(serviceInstanceId, bindingId);
		}
		if (!planId.equals(OCDPAdminServiceMapper.getOCDPServicePlan(serviceDefinitionId))) {
			logger.error("Unknown plan id: " + planId);
			throw new ServiceBrokerInvalidParametersException("Unknown plan id: " + planId);
		}
		if (instance == null) {
			logger.error("Service instance not exit: " + serviceInstanceId);
			throw new ServiceInstanceDoesNotExistException(serviceInstanceId);
		}		
	}

	@Override
	public void deleteServiceInstanceBinding(DeleteServiceInstanceBindingRequest request) throws OCDPServiceException {
		try {
			logger.info("Receiving unbinding request: " + request);
			String serviceInstanceId = request.getServiceInstanceId();
			String serviceDefinitionId = request.getServiceDefinitionId();
			String bindingId = request.getBindingId();
			String planId = request.getPlanId();
			logger.info(request.toString());
			ServiceInstanceBinding binding = bindingRepository.findOne(serviceInstanceId, bindingId);
			if (binding == null) {
				logger.error("Binding Id doesn't exists.");
				throw new ServiceInstanceBindingDoesNotExistException(bindingId);
			} else if (!planId.equals(binding.getPlanId())) {
				throw new ServiceBrokerInvalidParametersException("Unknown plan id: " + planId);
			}
			// Check service instance exists
			ServiceInstance instance = repository.findOne(serviceInstanceId);
			if (instance == null) {
				throw new ServiceInstanceDoesNotExistException(serviceInstanceId);
			}
			// 1) Remove user from service instance policy or delete service instance policy
			String userName = (String) binding.getCredentials().get("username");
			// Convert principal name to normal user name
			userName = userName.split("@")[0];
			OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
			logger.info("Unbinding username: " + userName + ", service instance id: " + serviceInstanceId);
			removeUserFromServiceInstance(ocdp, instance, userName);
			// 2) Delete service instance binding info from repository/etcd
			bindingRepository.delete(serviceInstanceId, bindingId);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private OCDPAdminService getOCDPAdminService(String serviceDefinitionId) {
		return (OCDPAdminService) this.context.getBean(OCDPAdminServiceMapper.getOCDPAdminService(serviceDefinitionId));
	}

	private void removeUserFromServiceInstance(OCDPAdminService ocdp, ServiceInstance instance, String userName) {
		String serviceInstancePolicyId = (String) instance.getServiceInstanceCredentials().get("rangerPolicyId");
		if (serviceInstancePolicyId == null || serviceInstancePolicyId.length() == 0) {
			throw new OCDPServiceException("Ranger policy not found.");
		}
		try {
			List<String> users = rc.getUsersFromV2Policy(serviceInstancePolicyId.split(":")[0]);
			if (users.size() == 1 && users.contains(userName)) {
				// Delete ranger policy if user is last one
				if (!ocdp.deletePolicyForResources(serviceInstancePolicyId)) {
					throw new OCDPServiceException("Ranger policy delete failed.");
				}
				removeServiceInstanceCredentialsItem(instance, "rangerPolicyId");
			} else {
				// Remove user from ranger policy
				if (!ocdp.removeUserFromPolicy(serviceInstancePolicyId, userName)) {
					throw new OCDPServiceException("Remove user from Ranger policy failed.");
				}
				logger.info("Successfully removed user " + userName + " from policy " + serviceInstancePolicyId);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private void removeServiceInstanceCredentialsItem(ServiceInstance instance, String key) {
		Map<String, Object> credentials = instance.getServiceInstanceCredentials();
		if (!credentials.containsKey(key)) {
			return;
		}
		credentials.remove(key);
		instance.setCredential(credentials);
		logger.debug("service instance credentials update to: " + credentials);
		// delete old service instances
		repository.delete(instance.getServiceInstanceId());
		// save new service instance
		repository.save(instance);
	}

	private void addUserToServiceInstance(OCDPAdminService ocdp, ServiceInstance instance, String username,
			String password, List<String> accesses) {
		// check if user exist in Ldap, create user if not exist
		checkLDAPUser(username);
		
		if (krb_enabled) {
			// check if user exist in KDC, create if not.
			checkKerberosUser(username, password);
		}
		
		// 2) Create policy for service instance or append user to an exists policy
		String serviceInstanceId = instance.getServiceInstanceId();
		String serviceInstancePolicyId = (String) instance.getServiceInstanceCredentials().get("rangerPolicyId");
		String serviceDefinitionId = instance.getServiceDefinitionId();
		String resourceType = OCDPAdminServiceMapper.getOCDPResourceType(serviceDefinitionId);
		String serviceInstanceResource = (String) instance.getServiceInstanceCredentials().get(resourceType);
		if (serviceInstancePolicyId == null || serviceInstancePolicyId.length() == 0) {
			// Create new ranger policy for service instance and update policy to service
			// instance
			serviceInstancePolicyId = createPolicyForResources(ocdp, serviceInstanceResource, username, accesses,
					serviceDefinitionId, serviceInstanceId);
			updateServiceInstanceCredentials(instance, "rangerPolicyId", serviceInstancePolicyId);
		} else {
			// Append users to service instance policy
			updateUsersToPolicy(ocdp, serviceInstancePolicyId, username, accesses);
		}
	}

	private void checkKerberosUser(String username, String password) {
		String principalName = username + "@" + clusterConfig.getKrbRealm();
		logger.info("To create new kerberos principal: " + principalName);
		try {
			// If principal exists, not need to create again.
			if (kc.principalExists(principalName)) {
				logger.info("kerberos principal " + principalName + " exists, no need to create again");
				return;
			}
			kc.createPrincipal(principalName, password);
			// Generate krb password and store it to etcd
			etcdClient.write("/servicebroker/ocdp/user/krbinfo/" + principalName + "/password", password);
		} catch (KerberosOperationException e) {
			logger.error("Kerberos principal [{}] create fail due to: {}", principalName, e.getLocalizedMessage());
			// rollbackLDAPUser(userName);
			throw new OCDPServiceException("Kerberos principal create fail due to: " + e.getLocalizedMessage());
		}
	}

	private void updateServiceInstanceCredentials(ServiceInstance instance, String key, String value) {
		if (value == null) {
			logger.warn("Policy id is null(Policy creating failed), skipping update ETCD of instance: " + instance.getServiceInstanceId());
			return;
		}
		instance.getServiceInstanceCredentials().replace(key, value);
		// delete old service instance
		repository.delete(instance.getServiceInstanceId());
		// save new service instance
		repository.save(instance);
	}

	private void updateUsersToPolicy(OCDPAdminService ocdp, String serviceInstancePolicyId, String user,
			List<String> accesses) {
		int i = 0;
		boolean policyUpdateResult = false;
		List<String> userList = Arrays.asList(new String[] {user});
		while (i++ <= 40) {
			logger.info("Trying to append user [{}] to ranger policy [{}] with privileges [{}]", user, serviceInstancePolicyId, accesses);
			policyUpdateResult = ocdp.appendUsersToPolicy(serviceInstancePolicyId, this.clusterConfig.getLdapGroup(),
					userList, accesses);
			if (!policyUpdateResult) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				logger.info("Successfully append user [{}] to ranger policy [{}] with privileges [{}].", user, serviceInstancePolicyId, accesses);
				break;
			}
		}
		if (!policyUpdateResult) {
			logger.error("Failed to append user [{}] to ranger policy [{}] with privileges [{}].", user, serviceInstancePolicyId, accesses);
			throw new OCDPServiceException("Failed to append user to ranger policy.");
		}
	}

	private void checkLDAPUser(String userName) {
		try {
			if (!BrokerUtil.isLDAPUserExist(this.ldap, userName)) {
				logger.info("User not exist in Ldap, going to create: " + userName);
				BrokerUtil.createLDAPUser(this.ldap, this.etcdClient, userName, clusterConfig.getLdapGroup(),
						clusterConfig.getLdapGroupId());
				logger.info("Successfully created new ldap user: " + userName);
				return;
			}
			logger.info("User already exist in Ldap, no need to create: " + userName);
			return;
		} catch (Exception e) {
			logger.error("LDAP user [{}] creating failed due to: {}", userName, e.getLocalizedMessage());
			throw new OCDPServiceException("LDAP user create fail due to: " + e.getLocalizedMessage());
		}
	}

	private String createPolicyForResources(OCDPAdminService ocdp, String serviceInstanceResource,
			String user, List<String> accesses, String serviceDefinitionId, String serviceInstanceId) {
		String policyId = null;
		// String policyName =
		// OCDPAdminServiceMapper.getOCDPServiceName(serviceDefinitionId) + "_" +
		// serviceInstanceResource;
		String policyName = OCDPAdminServiceMapper.getOCDPServiceName(serviceDefinitionId) + "_" + serviceInstanceId;
		int i = 0;
		List<String> userList = Arrays.asList(new String[] {user});
		while (i++ <= 40) {
			logger.info("Trying to create ranger policy [{}] for user [{}] with resources [{}]", policyName, user, serviceInstanceResource);
			policyId = ocdp.createPolicyForResources(policyName, Lists.newArrayList(serviceInstanceResource), userList,
					clusterConfig.getLdapGroup(), accesses);
			// TODO Need get a way to force sync up ldap users with ranger service, for temp
			// solution will wait 60 sec
			if (policyId == null) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				logger.info("Successfully created ranger policy [{}] of policyID [{}] for user [{}] with privileges [{}] to resources [{}]",policyName, policyId, user, accesses, serviceInstanceResource);
				break;
			}
		}
		if (policyId == null) {
			logger.error("Failed to create ranger policy [{}] for user [{}] with resources [{}]", policyName, user, serviceInstanceResource);
			throw new OCDPServiceException("Failed to create ranger policy " + policyName + " for user " + user);
		}
		return policyId;
	}
}
