package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.util.*;

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

    public OCDPServiceInstanceBindingService() {}

	@Override
	public CreateServiceInstanceBindingResponse createServiceInstanceBinding(
            CreateServiceInstanceBindingRequest request) throws OCDPServiceException {
        String serviceDefinitionId = request.getServiceDefinitionId();
        String bindingId = request.getBindingId();
        String serviceInstanceId = request.getServiceInstanceId();
        // Check binding instance exists
        if (bindingRepository.findOne(serviceInstanceId, bindingId) != null) {
            throw new ServiceInstanceBindingExistsException(serviceInstanceId, bindingId);
        }
        // Check service plan exists
        String planId = request.getPlanId();
        if(! planId.equals(OCDPAdminServiceMapper.getOCDPServicePlan(serviceDefinitionId))){
            throw new ServiceBrokerInvalidParametersException("Unknown plan id: " + planId);
        }
        // Check service instance exists
        ServiceInstance instance = repository.findOne(serviceInstanceId);
        if (instance == null) {
            throw new ServiceInstanceDoesNotExistException(serviceInstanceId);
        }
        Map<String, Object> serviceInstanceCredentials = instance.getServiceInstanceCredentials();
        String appGuid = request.getBoundAppGuid();
        // save service instance binding
        ServiceInstanceBinding binding = new ServiceInstanceBinding(
                bindingId, serviceInstanceId, serviceInstanceCredentials, null, appGuid, planId);
        bindingRepository.save(binding);
        return new CreateServiceInstanceAppBindingResponse().withCredentials(serviceInstanceCredentials);
	}

	@Override
	public void deleteServiceInstanceBinding(DeleteServiceInstanceBindingRequest request)
            throws OCDPServiceException{
        String serviceInstanceId = request.getServiceInstanceId();
        String bindingId = request.getBindingId();
        String planId = request.getPlanId();
        ServiceInstanceBinding binding = bindingRepository.findOne(serviceInstanceId, bindingId);
        if (binding == null) {
            throw new ServiceInstanceBindingDoesNotExistException(bindingId);
        }else if(! planId.equals(binding.getPlanId())){
            throw new ServiceBrokerInvalidParametersException("Unknown plan id: " + planId);
        }
        // Delete service instance binding info from repository/etcd
        bindingRepository.delete(serviceInstanceId, bindingId);
    }

    private OCDPAdminService getOCDPAdminService(String serviceDefinitionId){
        return  (OCDPAdminService) this.context.getBean(
                OCDPAdminServiceMapper.getOCDPAdminService(serviceDefinitionId)
        );
    }

}
