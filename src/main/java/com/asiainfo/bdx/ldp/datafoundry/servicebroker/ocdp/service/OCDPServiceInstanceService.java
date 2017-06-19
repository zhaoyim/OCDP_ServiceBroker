package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.OCDPServiceException;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.*;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common.OCDPServiceInstanceCommonService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.BrokerUtil;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.exception.ServiceBrokerInvalidParametersException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceDoesNotExistException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceExistsException;
import org.springframework.cloud.servicebroker.model.*;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import org.springframework.cloud.servicebroker.service.ServiceInstanceService;
import org.springframework.context.ApplicationContext;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Created by baikai on 7/23/16.
 */
@Service
public class OCDPServiceInstanceService implements ServiceInstanceService {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private OCDPServiceInstanceRepository repository;

    private ClusterConfig clusterConfig;

    // Operation response cache
    private Map<String, Future<CreateServiceInstanceResponse>> instanceProvisionStateMap;

    private Map<String, Future<DeleteServiceInstanceResponse>> instanceDeleteStateMap;

    private Map<String, Future<UpdateServiceInstanceResponse>> instanceUpdateStateMap;

    private LdapTemplate ldap;

    private etcdClient etcdClient;

    @Autowired
    public OCDPServiceInstanceService(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.ldap = clusterConfig.getLdapTemplate();
        this.etcdClient = clusterConfig.getEtcdClient();
        this.instanceProvisionStateMap = new HashMap<>();
        this.instanceDeleteStateMap = new HashMap<>();
    }

    @Override
    public CreateServiceInstanceResponse createServiceInstance(
            CreateServiceInstanceRequest request) throws OCDPServiceException {
        String serviceDefinitionId = request.getServiceDefinitionId();
        String serviceInstanceId = request.getServiceInstanceId();
        String planId = request.getPlanId();

        ServiceInstance instance = repository.findOne(serviceInstanceId);
        // Check service instance and planid
        if (instance != null) {
            throw new ServiceInstanceExistsException(serviceInstanceId, serviceDefinitionId);
        }else if(! planId.equals(OCDPAdminServiceMapper.getOCDPServicePlan(serviceDefinitionId))){
            throw new ServiceBrokerInvalidParametersException("Unknown plan id: " + planId);
        }
        CreateServiceInstanceResponse response;
        OCDPServiceInstanceCommonService service = getOCDPServiceInstanceCommonService();
        if(request.isAsyncAccepted()){
            Future<CreateServiceInstanceResponse> responseFuture = service.doCreateServiceInstanceAsync(request);
            this.instanceProvisionStateMap.put(request.getServiceInstanceId(), responseFuture);
            //CITIC case: return service credential info in provision response body
            Map<String, Object> credential = service.getOCDPServiceCredential(serviceDefinitionId, serviceInstanceId);
            response = new OCDPCreateServiceInstanceResponse().withCredential(credential).withAsync(true);
        } else {
            response = service.doCreateServiceInstance(request);
        }
        return response;
    }

    @Override
    public GetLastServiceOperationResponse getLastOperation(
            GetLastServiceOperationRequest request) throws OCDPServiceException {
        String serviceInstanceId = request.getServiceInstanceId();
        // Determine operation type: provision or delete
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
    }

    @Override
    public DeleteServiceInstanceResponse deleteServiceInstance(DeleteServiceInstanceRequest request)
            throws OCDPServiceException {
        String serviceInstanceId = request.getServiceInstanceId();
        ServiceInstance instance = repository.findOne(serviceInstanceId);
        // Check service instance id
        if (instance == null) {
            throw new ServiceInstanceDoesNotExistException(serviceInstanceId);
        }
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
    }

    @Override
    public UpdateServiceInstanceResponse updateServiceInstance(UpdateServiceInstanceRequest request)
            throws OCDPServiceException {
        Map<String, Object> params = request.getParameters();
        String accountName = (String)params.get("user_name");
        String password;
        if(! BrokerUtil.isLDAPUserExist(ldap, accountName)){
            password = UUID.randomUUID().toString();
        }else {
            password = etcdClient.readToString(
                    "/servicebroker/ocdp/user/krb/" + accountName + "@" + clusterConfig.getKrbRealm());
        }
        UpdateServiceInstanceResponse response;
        OCDPServiceInstanceCommonService service = getOCDPServiceInstanceCommonService();
        if (request.isAsyncAccepted()){
            Future<UpdateServiceInstanceResponse> responseFuture = service.doUpdateServiceInstanceAsync(
                    request, password);
            this.instanceUpdateStateMap.put(request.getServiceInstanceId(), responseFuture);
             Map<String, Object> credentials = new HashMap<String, Object>() {
                 {
                     put("username", accountName);
                     put("password", password);
                 }
             };
             response = new OCDPUpdateServiceInstanceResponse().withCredential(credentials).withAsync(true);
        }else {
            response = service.doUpdateServiceInstance(request, password);
        }
        return response;
    }

    private OCDPServiceInstanceCommonService getOCDPServiceInstanceCommonService() {
        return (OCDPServiceInstanceCommonService)context.getBean("OCDPServiceInstanceCommonService");
    }

    private OperationType getOperationType(String serviceInstanceId){
        if (this.instanceProvisionStateMap.get(serviceInstanceId) != null){
            return OperationType.PROVISION;
        } else if (this.instanceDeleteStateMap.get(serviceInstanceId) != null){
            return OperationType.DELETE;
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
        } else {
            return false;
        }
    }

    private void removeOperationState(String serviceInstanceId, OperationType operationType){
        if (operationType == OperationType.PROVISION){
            this.instanceProvisionStateMap.remove(serviceInstanceId);
        } else if ( operationType == OperationType.DELETE){
            this.instanceDeleteStateMap.remove(serviceInstanceId);
        }
    }
}
