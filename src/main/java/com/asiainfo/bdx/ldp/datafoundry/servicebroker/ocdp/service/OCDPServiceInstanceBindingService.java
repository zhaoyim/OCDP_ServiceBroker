package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.util.*;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.*;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstanceBinding;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceBindingRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;

import org.springframework.cloud.servicebroker.exception.ServiceInstanceDoesNotExistException;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceAppBindingResponse;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceBindingRequest;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceBindingResponse;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceBindingRequest;
import org.springframework.cloud.servicebroker.service.ServiceInstanceBindingService;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceBindingExistsException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceBindingDoesNotExistException;
import org.springframework.cloud.servicebroker.exception.ServiceBrokerInvalidParametersException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OCDP implement to bind hadoop services.
 * Binding a service does the following:
 *     return the connection/credential info;
 *     saves the connection/credential  info to the etcd repository.
 * Unbinding service instance does the following:
 *     delete the connection/credential info from the etcd repository.
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

    @Autowired
    public OCDPServiceInstanceBindingService(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.etcdClient = clusterConfig.getEtcdClient();
        this.rc = clusterConfig.getRangerClient();
    }

	@Override
	public CreateServiceInstanceBindingResponse createServiceInstanceBinding(
            CreateServiceInstanceBindingRequest request) throws OCDPServiceException {
		try {
	        logger.info("Receiving binding request: " + request);
	        String serviceDefinitionId = request.getServiceDefinitionId();
	        String bindingId = request.getBindingId();
	        String serviceInstanceId = request.getServiceInstanceId();
	        Map<String, Object> params = request.getParameters();

	        // Check binding instance exists
	        if (bindingRepository.findOne(serviceInstanceId, bindingId) != null) {
	        	logger.error("Binding [{}] already exist!", bindingId);
	            throw new ServiceInstanceBindingExistsException(serviceInstanceId, bindingId);
	        }
	        // Check service plan exists
	        String planId = request.getPlanId();
	        if(! planId.equals(OCDPAdminServiceMapper.getOCDPServicePlan(serviceDefinitionId))){
	        	logger.error("Unknown plan id: " + planId);
	            throw new ServiceBrokerInvalidParametersException("Unknown plan id: " + planId);
	        }
	        // Check service instance exists
	        ServiceInstance instance = repository.findOne(serviceInstanceId);
	        if (instance == null) {
	        	logger.error("Service instance not exit: " + serviceInstanceId);
	            throw new ServiceInstanceDoesNotExistException(serviceInstanceId);
	        }
	        // Construct service instance credentials for binding user
	        String userPrincipal = params.get("user_name") + "@" + clusterConfig.getKrbRealm();
	        String password = etcdClient.readToString("/servicebroker/ocdp/user/krb/" + userPrincipal + "/password");
			String keytab = etcdClient.readToString("/servicebroker/ocdp/user/krb/" + userPrincipal + "/keytab");
	        Map<String, Object> serviceInstanceCredentials = instance.getServiceInstanceCredentials();
	        serviceInstanceCredentials.put("username", userPrincipal);
	        serviceInstanceCredentials.put("password", password);
			serviceInstanceCredentials.put("keytab", keytab);
	        // save service instance binding
	        String appGuid = request.getBoundAppGuid();
	        ServiceInstanceBinding binding = new ServiceInstanceBinding(
	                bindingId, serviceInstanceId, serviceInstanceCredentials, null, appGuid, planId);
	        bindingRepository.save(binding);
	        // Remove ranger policy id for binding response
	        serviceInstanceCredentials.remove("rangerPolicyId");
	        return new CreateServiceInstanceAppBindingResponse().withCredentials(serviceInstanceCredentials);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	@Override
	public void deleteServiceInstanceBinding(DeleteServiceInstanceBindingRequest request)
            throws OCDPServiceException{
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
	        }else if(! planId.equals(binding.getPlanId())){
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
	        logger.info("Unbinding username: " + userName + ", service instance id: " + serviceInstanceId );
	        removeUserFromServiceInstance(ocdp, instance, userName);
	        // 2) Delete service instance binding info from repository/etcd
	        bindingRepository.delete(serviceInstanceId, bindingId);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
    }

    private OCDPAdminService getOCDPAdminService(String serviceDefinitionId){
        return  (OCDPAdminService) this.context.getBean(
                OCDPAdminServiceMapper.getOCDPAdminService(serviceDefinitionId)
        );
    }

    private void removeUserFromServiceInstance(OCDPAdminService ocdp, ServiceInstance instance, String userName) {
        String serviceInstancePolicyId = (String) instance.getServiceInstanceCredentials().get("rangerPolicyId");
        if (serviceInstancePolicyId == null || serviceInstancePolicyId.length() == 0){
            throw new OCDPServiceException("Ranger policy not found.");
        }
        try {
            List<String> users = rc.getUsersFromV2Policy(serviceInstancePolicyId.split(":")[0]);
            if (users.size() == 1 && users.contains(userName)) {
                // Delete ranger policy if user is last one
                if (! ocdp.deletePolicyForResources(serviceInstancePolicyId)){
                    throw new OCDPServiceException("Ranger policy delete failed.");
                }
                removeServiceInstanceCredentialsItem(instance, "rangerPolicyId");
            } else {
                // Remove user from ranger policy
                if (! ocdp.removeUserFromPolicy(serviceInstancePolicyId, userName) ){
                    throw new OCDPServiceException("Remove user from Ranger policy failed.");
                }
                logger.info("Successfully removed user " + userName + " from policy " + serviceInstancePolicyId);
            }
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
    }

    private void removeServiceInstanceCredentialsItem(ServiceInstance instance, String key){
        Map<String, Object> credentials = instance.getServiceInstanceCredentials();
        credentials.remove(key);
        instance.setCredential(credentials);
		// delete old service instances
		repository.delete(instance.getServiceInstanceId());
		// save new service instance
        repository.save(instance);
    }

}
