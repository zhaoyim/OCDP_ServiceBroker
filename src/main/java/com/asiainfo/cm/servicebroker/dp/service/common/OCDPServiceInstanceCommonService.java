package com.asiainfo.cm.servicebroker.dp.service.common;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceResponse;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceResponse;
import org.springframework.cloud.servicebroker.model.UpdateServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.UpdateServiceInstanceResponse;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import com.asiainfo.cm.servicebroker.dp.config.ClusterConfig;
import com.asiainfo.cm.servicebroker.dp.exception.OCDPServiceException;
import com.asiainfo.cm.servicebroker.dp.model.OCDPCreateServiceInstanceResponse;
import com.asiainfo.cm.servicebroker.dp.model.ServiceInstance;
import com.asiainfo.cm.servicebroker.dp.repository.OCDPServiceInstanceRepository;
import com.asiainfo.cm.servicebroker.dp.service.OCDPAdminService;
import com.asiainfo.cm.servicebroker.dp.utils.OCDPAdminServiceMapper;

/**
 * OCDP impl to manage hadoop service instances.  Creating a service does the following:
 * creates a new service instance user,
 * create hadoop service instance(e.g. hdfs dir, hbase table...),
 * set permission for service instance user,
 * saves the ServiceInstance info to the hdaoop repository.
 *  
 * @author whitebai1986@gmail.com
 */
@Service
public class OCDPServiceInstanceCommonService {

    private Logger logger = LoggerFactory.getLogger(OCDPServiceInstanceCommonService.class);

    @Autowired
	private OCDPServiceInstanceRepository repository;

    @Autowired
    private ApplicationContext context;

    private boolean krb_enabled;

    @Autowired
    public OCDPServiceInstanceCommonService(ClusterConfig clusterConfig) {
        this.krb_enabled = clusterConfig.krbEnabled();
        logger.info("Kerberos enabled: " + this.krb_enabled);
    }

    @Async
    public Future<CreateServiceInstanceResponse> doCreateServiceInstanceAsync(
            CreateServiceInstanceRequest request, String resource) throws OCDPServiceException {
        return new AsyncResult<CreateServiceInstanceResponse>(
                doCreateServiceInstance(request, resource)
        );
    }

    public CreateServiceInstanceResponse doCreateServiceInstance(
            CreateServiceInstanceRequest request, String resource) throws OCDPServiceException {
        String serviceDefinitionId = request.getServiceDefinitionId();
        String serviceInstanceId = request.getServiceInstanceId();
        String planId = request.getPlanId();
        Map<String, Object> params = request.getParameters();
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);

        // 1) Create big data resources like hdfs folder, hbase namespace ...
        String serviceInstanceResource = createTenentResource(
                ocdp, serviceDefinitionId, planId, serviceInstanceId, params);

        // 2) Generate service instance credential info
        Map<String, Object> credentials = ocdp.generateCredentialsInfo(resource);
		OCDPAdminServiceMapper mapper = (OCDPAdminServiceMapper) this.context.getBean("OCDPAdminServiceMapper");
        String serviceResourceType = mapper.getOCDPResourceType(serviceDefinitionId);
        // For spark/mr instance provision, need append queue name into credentials,
        // because function generateCredentialsInfo not append it
//        if(! credentials.containsKey(serviceResourceType))
//        	credentials.put(serviceResourceType, serviceInstanceResource);

    	// must not delete, coz hive need to override the value(from 'dbName' to 'dbName:queueName')
        credentials.put(serviceResourceType, serviceInstanceResource);

        // 3) Save service instance
        ServiceInstance instance = new ServiceInstance(request);
        instance.setCredential(credentials);
        repository.save(instance);

        CreateServiceInstanceResponse response = new OCDPCreateServiceInstanceResponse()
                .withCredential(credentials)
                .withAsync(false);
        logger.info("Create service instance " + serviceInstanceId + " successfully!");
        return response;
    }

    @Async
    public Future<DeleteServiceInstanceResponse> doDeleteServiceInstanceAsync(
            DeleteServiceInstanceRequest request, ServiceInstance instance) throws OCDPServiceException {
        return new AsyncResult<DeleteServiceInstanceResponse>(doDeleteServiceInstance(request, instance));
    }

    public DeleteServiceInstanceResponse doDeleteServiceInstance(
            DeleteServiceInstanceRequest request, ServiceInstance instance) throws OCDPServiceException {
        String serviceDefinitionId = request.getServiceDefinitionId();
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
        String serviceInstanceId = request.getServiceInstanceId();
        Map<String, Object> Credential = instance.getServiceInstanceCredentials();
		OCDPAdminServiceMapper mapper = (OCDPAdminServiceMapper) this.context.getBean("OCDPAdminServiceMapper");
        String serviceResourceType = mapper.getOCDPResourceType(serviceDefinitionId);
        String serviceInstanceResource = (String)Credential.get(serviceResourceType);
        String serviceInstancePolicyId = (String)Credential.get("rangerPolicyId");
        // 1) Remove resource from ranger policy if it exists
        if (serviceInstancePolicyId != null && serviceInstancePolicyId.length() != 0 ) {
        logger.info("Service instance policy exists, start to deleting policy " + serviceInstancePolicyId);
            if (!ocdp.deletePolicyForResources(serviceInstancePolicyId)) {
            	logger.error("Ranger policy [{}] delete failed.", serviceInstancePolicyId);
                throw new OCDPServiceException("Ranger policy delete failed.");
            }
        }
        // 2 )Delete big data resources like hdfs folder, hbase namespace ...
        deleteTenentResource(ocdp, serviceInstanceResource);

        // 3) Clean service instance from etcd
        repository.delete(serviceInstanceId);
        logger.info("Delete service instance " + serviceInstanceId + " successfully!");
        return new DeleteServiceInstanceResponse().withAsync(false);
	}

    @Async
    public Future<UpdateServiceInstanceResponse> doUpdateServiceInstanceAsync(
            UpdateServiceInstanceRequest request, ServiceInstance instance) throws OCDPServiceException {
        return new AsyncResult<UpdateServiceInstanceResponse>(doUpdateServiceInstance(request, instance));
    }

    public UpdateServiceInstanceResponse doUpdateServiceInstance(
            UpdateServiceInstanceRequest request, ServiceInstance instance) throws OCDPServiceException {
        String serviceDefinitionId = request.getServiceDefinitionId();
        String serviceInstanceId = request.getServiceInstanceId();
        Map<String, Object> params = request.getParameters();
        logger.info("Resizing service instance: " + serviceInstanceId);
        
        try{
            OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
            ocdp.resizeResourceQuota(instance, params);
        } catch (IOException e){
            logger.error("Error while resizing resource: ", e);
            throw new RuntimeException(e);
        }
        logger.info("Resizing service instance [{}] with params [{}] successful.", serviceInstanceId, params);
        logger.info("Update service instance [{}] successfully!", serviceInstanceId);
        return new UpdateServiceInstanceResponse().withAsync(false);
    }

    public Map<String, Object> getOCDPServiceCredential(
            String serviceDefinitionId, String resourceName){
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
        return ocdp.generateCredentialsInfo(resourceName);
    }

    private OCDPAdminService getOCDPAdminService(String serviceDefinitionId){
		OCDPAdminServiceMapper mapper = (OCDPAdminServiceMapper) this.context.getBean("OCDPAdminServiceMapper");
        return  (OCDPAdminService) this.context.getBean(
        		mapper.getOCDPAdminService(serviceDefinitionId)
        );
    }

    private String createTenentResource(OCDPAdminService ocdp, String serviceDefinitionId, String planId,
                                        String serviceInstanceId, Map<String, Object> params) {
        String serviceInstanceResource;
        try{
            serviceInstanceResource = ocdp.provisionResources(
                    serviceDefinitionId, planId, serviceInstanceId, params);
        }catch (Exception e){
            logger.error("OCDP resource provision fail due to: " + e);
            //e.printStackTrace();
            throw new OCDPServiceException("OCDP ressource provision fails due to: " +  e);
        }
        return serviceInstanceResource;
    }

    private void deleteTenentResource(OCDPAdminService ocdp, String serviceInstanceResource){
        try{
            ocdp.deprovisionResources(serviceInstanceResource);
        }catch (Exception e){
            logger.error("OCDP resource deprovision fail due to: " + e);
            //e.printStackTrace();
            throw new OCDPServiceException("OCDP resource deprovision fail due to: " + e);
        }
    }
}