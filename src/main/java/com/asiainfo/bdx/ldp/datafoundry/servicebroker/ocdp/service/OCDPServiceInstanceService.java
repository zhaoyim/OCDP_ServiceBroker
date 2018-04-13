package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.exception.ServiceBrokerInvalidParametersException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceDoesNotExistException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceExistsException;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceResponse;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceResponse;
import org.springframework.cloud.servicebroker.model.GetLastServiceOperationRequest;
import org.springframework.cloud.servicebroker.model.GetLastServiceOperationResponse;
import org.springframework.cloud.servicebroker.model.OperationState;
import org.springframework.cloud.servicebroker.model.UpdateServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.UpdateServiceInstanceResponse;
import org.springframework.cloud.servicebroker.service.ServiceInstanceService;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.OCDPServiceException;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.OCDPCreateServiceInstanceResponse;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.OCDPUpdateServiceInstanceResponse;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.OperationType;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common.OCDPServiceInstanceCommonService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;

/**
 * Created by baikai on 7/23/16.
 */
@Service
public class OCDPServiceInstanceService implements ServiceInstanceService {

    private Logger logger = LoggerFactory.getLogger(OCDPServiceInstanceService.class);

    @Autowired
    private ApplicationContext context;

    @Autowired
    private OCDPServiceInstanceRepository repository;

    // Operation response cache
    private Map<String, Future<CreateServiceInstanceResponse>> instanceProvisionStateMap;

    private Map<String, Future<DeleteServiceInstanceResponse>> instanceDeleteStateMap;

    private Map<String, Future<UpdateServiceInstanceResponse>> instanceUpdateStateMap;

    @Autowired
    public OCDPServiceInstanceService(ClusterConfig clusterConfig) {
        this.instanceProvisionStateMap = new HashMap<>();
        this.instanceDeleteStateMap = new HashMap<>();
        this.instanceUpdateStateMap = new HashMap<>();
    }

    @Override
    public CreateServiceInstanceResponse createServiceInstance(
            CreateServiceInstanceRequest request) throws OCDPServiceException {
        	logger.info("Receiving create request: " + request);
            String serviceDefinitionId = request.getServiceDefinitionId();
            String serviceInstanceId = request.getServiceInstanceId();
            String planId = request.getPlanId();
            Map<String, Object> params = request.getParameters();
            String resource = params.get("cuzBsiName") == null ? serviceInstanceId : String.valueOf(params.get("cuzBsiName"));

            ServiceInstance instance = repository.findOne(serviceInstanceId);
            // Check service instance and planid
    		OCDPAdminServiceMapper mapper = (OCDPAdminServiceMapper) this.context.getBean("OCDPAdminServiceMapper");
            if (instance != null) {
                logger.warn("Service instance with the given ID already exists: " + serviceInstanceId + ".");
                throw new ServiceInstanceExistsException(serviceInstanceId, serviceDefinitionId);
            }else if(! planId.equals(mapper.getOCDPServicePlan(serviceDefinitionId))){
                throw new ServiceBrokerInvalidParametersException("Unknown plan id: " + planId);
            }
         try {
            logger.info("Start to create OCDPServiceInstance: " + serviceInstanceId + "...");
            CreateServiceInstanceResponse response;
            OCDPServiceInstanceCommonService service = getOCDPServiceInstanceCommonService();
            if(request.isAsyncAccepted()){
                Future<CreateServiceInstanceResponse> responseFuture = service.doCreateServiceInstanceAsync(request, resource);
                this.instanceProvisionStateMap.put(request.getServiceInstanceId(), responseFuture);
                //CITIC case: return service credential info in provision response body
                Map<String, Object> credential = service.getOCDPServiceCredential(serviceDefinitionId, resource);
                response = new OCDPCreateServiceInstanceResponse().withCredential(credential).withAsync(true);
            } else {
                response = service.doCreateServiceInstance(request, resource);
            }
            return response;
		} catch (Exception e) {
			logger.error("Create service instance error: " + e.getMessage());
			throw new OCDPServiceException(e.getMessage());
		}

    }

    @Override
    public GetLastServiceOperationResponse getLastOperation(
            GetLastServiceOperationRequest request) throws OCDPServiceException {
    	try {
            logger.info("Receiving getLastOperation request: " + request);
            String serviceInstanceId = request.getServiceInstanceId();
            // Determine operation type: provision, delete or update
            OperationType operationType = getOperationType(serviceInstanceId);
            if (operationType == null){
                throw new OCDPServiceException("Service instance " + serviceInstanceId + " not exist.");
            }
            // Get Last operation response object from cache
            boolean is_operation_done = false;
            if( operationType == OperationType.PROVISION){
                Future<CreateServiceInstanceResponse> responseFuture = this.instanceProvisionStateMap.get(serviceInstanceId);
                is_operation_done = responseFuture.isDone();
            } else if( operationType == OperationType.DELETE){
                Future<DeleteServiceInstanceResponse> responseFuture = this.instanceDeleteStateMap.get(serviceInstanceId);
                is_operation_done = responseFuture.isDone();
            } else if ( operationType == OperationType.UPDATE){
                Future<UpdateServiceInstanceResponse> responseFuture = this.instanceUpdateStateMap.get(serviceInstanceId);
                is_operation_done = responseFuture.isDone();
            }
            // Return operation type
            if(is_operation_done){
                removeOperationState(serviceInstanceId, operationType);
                if (checkOperationResult(serviceInstanceId, operationType)){
                    return new GetLastServiceOperationResponse().withOperationState(OperationState.SUCCEEDED);
                } else {
                    return new GetLastServiceOperationResponse().withOperationState(OperationState.FAILED);
                }
            }else{
                return new GetLastServiceOperationResponse().withOperationState(OperationState.IN_PROGRESS);
            }
		} catch (Exception e) {
			logger.error("getLastOperation error: ", e);
			throw new RuntimeException(e);
		}

    }

    @Override
    public DeleteServiceInstanceResponse deleteServiceInstance(DeleteServiceInstanceRequest request)
            throws OCDPServiceException {
    	try {
            logger.info("Receiving delete request: " + request);
            String serviceInstanceId = request.getServiceInstanceId();
            logger.info("To delete service instance " + serviceInstanceId);
            ServiceInstance instance = repository.findOne(serviceInstanceId);
            // Check service instance id
            if (instance == null) {
            	logger.error("Instance not exist: " + serviceInstanceId);
                throw new ServiceInstanceDoesNotExistException(serviceInstanceId);
            }
            logger.info("Start to delete OCDPServiceInstance: " + serviceInstanceId + "...");
            DeleteServiceInstanceResponse response;
            OCDPServiceInstanceCommonService service = getOCDPServiceInstanceCommonService();
            if(request.isAsyncAccepted()){
                Future<DeleteServiceInstanceResponse> responseFuture = service.doDeleteServiceInstanceAsync(
                        request, instance);
                this.instanceDeleteStateMap.put(request.getServiceInstanceId(), responseFuture);
                response = new DeleteServiceInstanceResponse().withAsync(true);
            } else {
                response = service.doDeleteServiceInstance(request, instance);
            }
            return response;
		} catch (Exception e) {
			logger.error("Delete ServiceInstance error: ", e);
			throw new RuntimeException(e);
		}

    }

    @Override
    public UpdateServiceInstanceResponse updateServiceInstance(UpdateServiceInstanceRequest request)
            throws OCDPServiceException {
    	try {
            String serviceInstanceId = request.getServiceInstanceId();
            logger.info("Receiving update request: " + request);
            ServiceInstance instance = repository.findOne(serviceInstanceId);
            // Check service instance id
            if (instance == null) {
                throw new ServiceInstanceDoesNotExistException(serviceInstanceId);
            }
            UpdateServiceInstanceResponse response;
            OCDPServiceInstanceCommonService service = getOCDPServiceInstanceCommonService();
            if (request.isAsyncAccepted()){
                Future<UpdateServiceInstanceResponse> responseFuture = service.doUpdateServiceInstanceAsync(
                        request, instance);
                this.instanceUpdateStateMap.put(request.getServiceInstanceId(), responseFuture);
                response = new OCDPUpdateServiceInstanceResponse().withAsync(true);
            }else {
                response = service.doUpdateServiceInstance(request, instance);
            }
            return response;
		} catch (Exception e) {
			logger.error("Update ServiceInstance error: ", e);
			throw new RuntimeException(e);
		}

    }

    private OCDPServiceInstanceCommonService getOCDPServiceInstanceCommonService() {
        return (OCDPServiceInstanceCommonService)context.getBean("OCDPServiceInstanceCommonService");
    }

    private OperationType getOperationType(String serviceInstanceId){
        if (this.instanceProvisionStateMap.get(serviceInstanceId) != null){
            return OperationType.PROVISION;
        } else if (this.instanceDeleteStateMap.get(serviceInstanceId) != null){
            return OperationType.DELETE;
        } else if (this.instanceUpdateStateMap.get(serviceInstanceId) != null){
            return OperationType.UPDATE;
        } else {
            return null;
        }
    }

    private boolean checkOperationResult(String serviceInstanceId, OperationType operationType){
        if (operationType == OperationType.PROVISION){
            // For instance provision case, return true if instance information existed in etcd
            return (repository.findOne(serviceInstanceId) != null);
        }else if(operationType == OperationType.DELETE){
            // For instance delete case, return true if instance information not existed in etcd
            return (repository.findOne(serviceInstanceId) == null);
        } else if (operationType == OperationType.UPDATE) {
            // Temp solution: For instance update case, just return true if update operation is done
            // Need a better solution in future to determine update operation is fail or success
            return true;
        } else {
            return false;
        }
    }

    private void removeOperationState(String serviceInstanceId, OperationType operationType){
        if (operationType == OperationType.PROVISION){
            this.instanceProvisionStateMap.remove(serviceInstanceId);
        } else if ( operationType == OperationType.DELETE){
            this.instanceDeleteStateMap.remove(serviceInstanceId);
        } else if ( operationType == OperationType.UPDATE){
            this.instanceUpdateStateMap.remove(serviceInstanceId);
        }
    }
}
